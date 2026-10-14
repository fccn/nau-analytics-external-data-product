from pyspark.sql import DataFrame #type:ignore
from pyspark.sql import functions as F #type:ignore
from pyspark.sql.functions import col, lit, when, expr, greatest, row_number
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.gold_utils_functions import update_ctrl_table,get_max_timestamp_for_table
from datetime import datetime, timezone
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

    spark = start_iceberg_session("gold_fact_student_grades")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "fact_student_grades"

    src_layer = f"silver{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name = "grades_persistentcoursegrade"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting process from {last_execution_timestamp}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
            -- Keys
            student_grade_key              BIGINT NOT NULL        COMMENT 'The same as id in the source table',
            user_key                       BIGINT NOT NULL        COMMENT 'References dim_user.user_key',
            course_edition_key             STRING NOT NULL        COMMENT 'References dim_course_edition.course_edition_key',

            -- Metadata
            percent_grade                  DOUBLE NOT NULL        COMMENT 'Grade the student obtained',
            letter_grade                   STRING NOT NULL        COMMENT 'The letter associated with the grade the student obtained',
            passed_timestamp               TIMESTAMP              COMMENT 'Only available if the student has a passing grade.',

            -- Audit
            created                        TIMESTAMP NOT NULL     COMMENT 'Date when the record was created in the source',
            modified                       TIMESTAMP NOT NULL     COMMENT 'Date when the record was last updated in the source',
            last_update_timestamp          TIMESTAMP NOT NULL     COMMENT 'Timestamp of last update (ETL time)'
        )
        USING iceberg
        TBLPROPERTIES (
            'write.parquet.compression-codec'                                = 'zstd',
            'write.target-file-size-bytes'                                   = '536870912',
            'write.distribution-mode'                                        = 'none',
            'write.sort.order'                                               = 'course_edition_key ASC, user_key ASC',
            'commit.manifest.min-count-to-merge'                             = '100',
            'write.merge.enabled'                                            = 'true',
            'read.split.target-size'                                         = '134217728',
            'read.split.open-file-cost'                                      = '4194304',
            'write.metadata.delete-after-commit.enabled'                     = 'true',
            'write.metadata.previous-versions-max'                           = '10',
            'write.parquet.bloom-filter.enabled.column.course_edition_key'   = 'true',
            'write.parquet.bloom-filter.enabled.column.user_key'             = 'true'
        )
    """)

    spark.sql(f"""
        ALTER TABLE {tgt_layer}.{tgt_pipeline}.{tgt_table_name}
        SET TBLPROPERTIES (
            'write.parquet.compression-codec'                                = 'zstd',
            'write.target-file-size-bytes'                                   = '536870912',
            'write.distribution-mode'                                        = 'none',
            'write.sort.order'                                               = 'course_edition_key ASC, user_key ASC',
            'commit.manifest.min-count-to-merge'                             = '100',
            'write.merge.enabled'                                            = 'true',
            'read.split.target-size'                                         = '134217728',
            'read.split.open-file-cost'                                      = '4194304',
            'write.metadata.delete-after-commit.enabled'                     = 'true',
            'write.metadata.previous-versions-max'                           = '10',
            'write.parquet.bloom-filter.enabled.column.course_edition_key'   = 'true',
            'write.parquet.bloom-filter.enabled.column.user_key'             = 'true'
        )
    """)

    SOURCE_TABLE  = f"{src_layer}.{src_pipeline}.{src_table_name}"
    TARGET_TABLE  = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"
    DIM_USER      = f"{tgt_layer}.entidades.dim_user"
    DIM_COURSE    = f"{tgt_layer}.entidades.dim_course_edition"

    logging.info(f"Last execution timestamp : {last_execution_timestamp}")
    logging.info(f"Source table             : {SOURCE_TABLE}")
    logging.info(f"Target table             : {TARGET_TABLE}")

    # ── 1. Read incremental slice from source ────────────────────────────────
    logging.info("Reading incremental rows from source...")

    dedup_window = Window.partitionBy("id").orderBy(F.desc("modified"))

    source_df = (
        spark.table(SOURCE_TABLE)
        .filter(
            (F.col("created")  > F.lit(last_execution_timestamp).cast(TimestampType())) |
            (F.col("modified") > F.lit(last_execution_timestamp).cast(TimestampType()))
        )
        .withColumn("rn", row_number().over(dedup_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "id", "user_id", "course_id", "percent_grade", "letter_grade",
            "passed_timestamp", "created", "modified",
        )
    )

    incremental_count = source_df.count()
    logging.info(f"Incremental rows found   : {incremental_count}")

    if incremental_count == 0:
        logging.info("Nothing to process. Exiting.")
        return

    # ── 2. Resolve user_key from dim_user (current SCD2 rows) ───────────────
    logging.info(f"Resolving user_key from {DIM_USER}...")

    dim_user_current = (
        spark.table(DIM_USER)
        .filter(F.col("key_end_date") == F.lit("9999-12-31T00:00:00+00:00").cast(TimestampType()))
        .select(
            F.col("user_cd").alias("dim_user_cd"),
            F.col("user_key").alias("resolved_user_key"),
        )
    )

    # ── 3. Resolve course_edition_key from dim_course_edition (current SCD2) ─
    logging.info(f"Resolving course_edition_key from {DIM_COURSE}...")

    dim_course_current = (
        spark.table(DIM_COURSE)
        .filter(F.col("key_end_date").isNull())
        .select(
            F.col("course_edition_cd").alias("dim_course_cd"),
            F.col("course_edition_key").alias("resolved_course_key"),
        )
    )

    # ── 4. Join dimensions onto incremental slice ────────────────────────────
    logging.info("Joining dimensions...")

    enriched_df = (
        source_df
        .join(dim_user_current, source_df["user_id"] == dim_user_current["dim_user_cd"], how="left")
        .join(dim_course_current, source_df["course_id"] == dim_course_current["dim_course_cd"], how="left")
    )

    # ── 5. Warn about unresolved foreign keys ────────────────────────────────
    unresolved_course = enriched_df.filter(F.col("resolved_course_key").isNull()).count()

    if unresolved_course > 0:
        logging.warning(f"Rows with unresolved course_edition_key: {unresolved_course} — these rows will be SKIPPED.")
        enriched_df.filter(F.col("resolved_course_key").isNull()) \
            .select("course_id") \
            .distinct() \
            .show(truncate=False)

    enriched_df = enriched_df.filter(F.col("resolved_course_key").isNotNull())

    if enriched_df.count() == 0:
        logging.info("No resolvable rows remain after filtering. Exiting.")
        return

    unresolved_user = enriched_df.filter(F.col("resolved_user_key").isNull()).count()
    if unresolved_user > 0:
        logging.warning(f"Rows with unresolved user_key: {unresolved_user} — these rows will be SKIPPED.")
    enriched_df = enriched_df.filter(F.col("resolved_user_key").isNotNull())

    # ── 6. Build the staging DataFrame with target schema ───────────────────
    logging.info("Building staging DataFrame...")

    staging_df = (
        enriched_df
        .select(
            F.col("id").cast("bigint").alias("student_grade_key"),
            F.col("resolved_user_key").alias("user_key"),
            F.col("resolved_course_key").alias("course_edition_key"),
            F.col("percent_grade"),
            F.col("letter_grade"),
            F.col("passed_timestamp"),
            F.col("created"),
            F.col("modified"),
            F.current_timestamp().alias("last_update_timestamp")
        )
    )

    # ── 7. Register staging as a temp view for the MERGE statement ───────────
    staging_df.createOrReplaceTempView("stg_fact_student_grades")

    # ── 8. MERGE (UPSERT) into target Iceberg table ──────────────────────────
    logging.info(f"Executing MERGE into {TARGET_TABLE}...")

    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} AS tgt
        USING stg_fact_student_grades AS src
        ON tgt.student_grade_key = src.student_grade_key

        WHEN MATCHED AND src.modified > tgt.modified THEN
            UPDATE SET
                tgt.student_grade_key  = src.student_grade_key,
                tgt.percent_grade      = src.percent_grade,
                tgt.letter_grade       = src.letter_grade,
                tgt.passed_timestamp   = src.passed_timestamp,
                tgt.created            = src.created,
                tgt.modified           = src.modified,
                tgt.last_update_timestamp = src.last_update_timestamp

        WHEN NOT MATCHED THEN
            INSERT (
                student_grade_key, user_key, course_edition_key, percent_grade,
                letter_grade, passed_timestamp, created, modified, last_update_timestamp
            )
            VALUES (
                src.student_grade_key, src.user_key, src.course_edition_key, src.percent_grade,
                src.letter_grade, src.passed_timestamp, src.created, src.modified,
                src.last_update_timestamp
            )
    """)

    logging.info("MERGE completed successfully.")

    # ── 9. Iceberg maintenance ───────────────────────────────────────────────
    try:
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_data_files(
            table => '{TARGET_TABLE}',
            strategy => 'sort',
            sort_order => 'course_edition_key ASC, user_key ASC'
          )
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_manifests(table => '{TARGET_TABLE}')
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.expire_snapshots(table => '{TARGET_TABLE}', retain_last => 5)
        """)
        logging.info("Iceberg maintenance executed: sort + compact + manifests + expire snapshots.")
    except Exception as e:
        logging.warning(f"Iceberg procedures not executed ({e}).")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=incremental_count,env=ENVIRONMENT)

if __name__ == "__main__":
    main()