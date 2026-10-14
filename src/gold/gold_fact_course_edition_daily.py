from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from pyspark.sql.functions import (
    col, lit, when, coalesce, greatest, to_timestamp, current_timestamp,
    row_number, lpad, regexp_extract, max as smax, expr, current_timestamp as spark_current_timestamp
)
from pyspark.sql.types import TimestampType, IntegerType, FloatType
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.gold_utils_functions import update_ctrl_table,get_max_timestamp_for_table
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_fact_course_edition_daily")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "fact_course_edition_daily"

    src_layer = f"gold{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name = "dim_course_edition"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting process from {last_execution_timestamp}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (

            -- Keys (Daily grain)
            day_key                        DATE          COMMENT 'Date for which the edition is active.',
            org_key                        BIGINT        COMMENT 'References dim_organization.org_key',
            course_edition_key             STRING        COMMENT 'Surrogate SCD2 key (course_edition_cd + seq)',

            -- Course metadata
            course_edition_cd              STRING        COMMENT 'Original course edition identifier (Open edX id)',
            course_cd                      STRING        COMMENT 'Original course identifier (Open edX display_number)',
            edition                        STRING        COMMENT 'Edition part extracted from course_edition_cd',

            -- Audit
            last_update_timestamp          TIMESTAMP     COMMENT 'Timestamp of last update (ETL time)'
        )
        USING iceberg
        PARTITIONED BY (day_key)
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '536870912',
            'write.distribution-mode' = 'none',
            'write.sort.order' = 'day_key ASC, course_edition_key ASC',
            'commit.manifest.min-count-to-merge' = '100',
            'write.merge.enabled' = 'true',
            'read.split.target-size' = '134217728',
            'read.split.open-file-cost' = '4194304',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10',
            'write.parquet.bloom-filter.enabled.column.course_edition_key' = 'true',
            'write.parquet.bloom-filter.enabled.column.org_key'            = 'true'
        );
    """)

    DIM_TBL   = f"{tgt_layer}.{tgt_pipeline}.{src_table_name}"
    TIME_TBL  = f"{tgt_layer}.entidades.dim_time"
    DAILY_TBL = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"

    logging.info("Starting incremental daily build (overwritePartitions strategy)")

    # ============================================================
    # 1. LOAD ONLY UPDATED dim_course_edition ROWS
    # ============================================================
    dce_inc = (
        spark.table(DIM_TBL)
             .filter(col("last_update_timestamp") > F.to_timestamp(lit(last_execution_timestamp)))
             .filter(col("key_end_date").isNull())
    )

    if dce_inc.count() == 0:
        logging.info("Nothing to expand. Exiting.")
        raise SystemExit(0)

    # ============================================================
    # 2. LOAD dim_time
    # ============================================================
    dt = spark.table(TIME_TBL).withColumn("time_key_str", col("time_key").cast("string"))

    # ============================================================
    # 3. CREATE YYYYMMDD STRINGS FOR JOIN
    # ============================================================
    dce_inc = (
        dce_inc
        .withColumn("start_dt_string", F.date_format(F.to_date("start_date"), "yyyyMMdd"))
        .withColumn("end_dt_string",
            F.when(col("end_date").isNotNull(),
                   F.date_format(F.to_date("end_date"), "yyyyMMdd"))
             .otherwise(F.date_format(F.current_date(), "yyyyMMdd"))
        )
    )

    # ============================================================
    # 4. STRING-BASED BETWEEN JOIN
    # ============================================================
    daily = (
        dce_inc.join(
            dt,
            (col("time_key_str") >= col("start_dt_string")) &
            (col("time_key_str") <= col("end_dt_string")),
            "inner"
        )
    )

    daily = daily.withColumn("day_key", col("date"))

    # ============================================================
    # 5. SELECT ONLY COLUMNS IN THE TARGET DDL
    # ============================================================
    daily = (
        daily.select(
            col("day_key"),
            col("org_key"),
            col("course_edition_key"),
            col("course_edition_cd"),
            col("display_number").alias("course_cd"),
            col("edition"),
            F.current_timestamp().alias("last_update_timestamp")
        )
    )

    daily_count = daily.count()
    logging.info(f"Daily rows generated: {daily_count}")

    # ============================================================
    # 6. DELETE affected editions, then INSERT new rows.
    # overwritePartitions() cannot be used: the table is partitioned by
    # day_key, so it would wipe unrelated editions' rows sharing the same
    # day partitions. Incremental batches only cover a subset of editions,
    # so we must scope the delete to the affected course_edition_keys.
    # ============================================================
    logging.info("Deleting affected course_edition_keys and appending new rows...")

    daily.select("course_edition_key").distinct().createOrReplaceTempView("_keys_to_replace")

    spark.sql(f"""
        DELETE FROM {DAILY_TBL}
        WHERE course_edition_key IN (SELECT course_edition_key FROM _keys_to_replace)
    """)

    daily.writeTo(DAILY_TBL).append()

    logging.info("Daily fact written successfully.")

    # ============================================================
    # 7. ICEBERG OPTIMIZATION
    # ============================================================
    try:
        spark.sql(f"""
            CALL {tgt_layer}.system.rewrite_data_files(
                table => '{DAILY_TBL}',
                strategy => 'sort',
                sort_order => 'day_key ASC, course_edition_key ASC'
            )
        """)
        spark.sql(f"""
            CALL {tgt_layer}.system.rewrite_manifests(table => '{DAILY_TBL}')
        """)
        spark.sql(f"""
            CALL {tgt_layer}.system.expire_snapshots(table => '{DAILY_TBL}', retain_last => 5)
        """)
        logging.info("Iceberg optimization completed.")
    except Exception as e:
        logging.warning(f"Iceberg optimization skipped: {e}")

    logging.info("Incremental fact_course_edition_daily successfully completed.")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=daily_count,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
