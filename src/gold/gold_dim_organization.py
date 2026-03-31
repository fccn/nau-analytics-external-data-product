from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as f #type:ignore
from pyspark.sql.functions import current_timestamp as spark_current_timestamp
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.bronze_utils_functions import update_ctrl_table,get_max_timestamp_for_table
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

    spark = start_iceberg_session("gold_dim_organization")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "dim_organization"

    src_layer = f"silver{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_history_table_name = "organizations_historicalorganization"
    src_table_name = "organizations_organization"

    #Constants
    FIXED_START_DATE = "1900-01-01 00:00:00"

    new_or_update_records = 0

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    src_tbl = f"{src_layer}.{src_pipeline}.{src_table_name}"
    tgt_tbl = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"

    business_key_src = "id"
    business_key_tgt = "org_cd"
    surr_key = "org_key"

    start_col = "key_start_date"
    end_col   = "key_end_date"
    ts_col    = "last_update_timestamp"

    compare_cols = ["name", "short_name", "description", "is_active"]

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        -- Keys
        org_key                 BIGINT       COMMENT 'Surrogate SCD2 key (id + 3 digit seq)',
        org_cd                  INT          COMMENT 'Source system user identifier (id)',

        -- Identity
        name                    STRING       COMMENT 'Organization name',
        short_name              STRING       COMMENT 'Organization short name',
        description             STRING       COMMENT 'Organization description',
        is_active               BOOLEAN      COMMENT 'Active flag',
        registration_date       TIMESTAMP    COMMENT 'Date when the organization registered in the platform',

        -- SCD2 Metadata
        key_start_date          TIMESTAMP    COMMENT 'Start date for SCD Type 2 validity',
        key_end_date            TIMESTAMP    COMMENT 'End date for SCD Type 2 validity',

        -- Audit metadata
        last_update_timestamp   TIMESTAMP    COMMENT 'Timestamp of last update (ETL time)'
    )
    USING iceberg
    """)

    # If this is the first time the script runs, we'll use the historicalorganization
    # table to build the history for the dim_organization table. After that, we'll only
    # use the organization silver table to keep historical records
    if last_execution_timestamp == FIXED_START_DATE:
        df_historical_data = spark.sql(f"""
            WITH hist AS (
                SELECT
                    oh.id AS org_cd,
                    oh.name,
                    oh.short_name,
                    oh.description,
                    oh.active AS is_active,
                    oh.created,
                    oh.history_type,
                    oh.modified,
                    oh.history_id,
                    LAG(oh.modified) OVER (
                        PARTITION BY oh.id
                        ORDER BY oh.history_id ASC
                    ) AS prev_modified
                FROM {src_layer}.{src_pipeline}.{src_history_table_name} oh
            ),
            scd_stage AS (
                SELECT
                    org_cd, name, short_name, description, is_active, created,
                    history_type, history_id,
                    CASE
                        WHEN history_type = '+' THEN CAST(created AS TIMESTAMP)
                        WHEN prev_modified IS NOT NULL THEN prev_modified + INTERVAL 1 SECOND
                        ELSE CAST(created AS TIMESTAMP)
                    END AS key_start_date
                FROM hist
            ),
            scd_next AS (
                SELECT
                    *,
                    LEAD(key_start_date) OVER (
                        PARTITION BY org_cd ORDER BY history_id ASC
                    ) AS next_start_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY org_cd ORDER BY history_id ASC
                    ) AS rn
                FROM scd_stage
            )
            SELECT
                org_cd * 1000 + rn AS org_key,
                org_cd,
                name, short_name, description, is_active,
                created AS registration_date,
                key_start_date,
                CASE
                    WHEN next_start_date IS NULL THEN NULL
                    ELSE next_start_date - INTERVAL 1 SECOND
                END AS key_end_date,
                current_timestamp() AS last_update_timestamp
            FROM scd_next
            ORDER BY org_cd, key_start_date
        """)

        new_or_update_records = df_historical_data.count()
        logging.info(f"Number of new or updated records = {new_or_update_records}")

        df_historical_data.write.format("iceberg").mode("overwrite").saveAsTable(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}")

    # ============================================================
    # 1) LOAD SOURCE — only rows created/modified since last run
    # ============================================================
    src = (
        spark.table(src_tbl)
             .filter(
                 (f.col("created")  > f.to_timestamp(f.lit(last_execution_timestamp))) |
                 (f.col("modified") > f.to_timestamp(f.lit(last_execution_timestamp)))
             )
             .select(
                f.col("id").alias("org_cd_src"),
                "name", "short_name", "description",
                f.col("active").alias("is_active"),
                "created", "modified"
             )
             .alias("s")
    )

    logging.info(f"source rows updated since last run: {src.count()}")

    # ============================================================
    # 2) LOAD TARGET CURRENT ROWS (deduped)
    # ============================================================
    tgt = spark.table(tgt_tbl).alias("t")

    w_tgt = Window.partitionBy("org_cd").orderBy(f.col(ts_col).desc_nulls_last())

    tgt_current = (
        tgt.filter(f.col(end_col).isNull())
           .withColumn("rn", f.row_number().over(w_tgt))
           .filter("rn = 1")
           .drop("rn")
           .alias("tcur")
    )

    logging.info(f"active target scd2 rows: {tgt_current.count()}")

    # ============================================================
    # 3) JOIN SOURCE + TARGET AND DETECT CHANGES
    # ============================================================
    joined = src.join(
        tgt_current,
        src.org_cd_src == tgt_current[business_key_tgt],
        how="left"
    )

    change_cond = None
    for c in compare_cols:
        expr_c = ~f.col(f"s.{c}").eqNullSafe(f.col(f"tcur.{c}"))
        change_cond = expr_c if change_cond is None else (change_cond | expr_c)

    is_new     = f.col(f"tcur.{business_key_tgt}").isNull()
    is_changed = f.col(f"tcur.{business_key_tgt}").isNotNull() & change_cond

    changes = (
        joined.where(is_new | is_changed)
              .select(
                  f.col("s.org_cd_src").alias("org_cd"),
                  f.col("s.name").alias("name"),
                  f.col("s.short_name").alias("short_name"),
                  f.col("s.description").alias("description"),
                  f.col("s.is_active").alias("is_active"),
                  f.col("s.created").alias("created_src"),
                  f.col("s.modified").alias("modified_src"),
                  is_new.alias("is_new"),
                  is_changed.alias("is_changed"),
                  f.col(f"tcur.{surr_key}").alias("old_org_key")
              )
              .cache()
    )

    nr_changes = changes.count()
    logging.info(f"detected new/changed rows: {nr_changes}")

    changed_keys = changes.where("is_changed").select("org_cd", "modified_src").distinct()
    new_keys     = changes.where("is_new").select("org_cd", "created_src").distinct()

    # ============================================================
    # 4) CLOSE OLD TARGET ROWS
    # ============================================================
    to_close = (
        changed_keys
        .withColumn("end_ts", f.expr("modified_src - INTERVAL 1 SECOND"))
        .withColumn("ts_now", spark_current_timestamp())
        .select("org_cd", "end_ts", "ts_now")
    )

    to_close.createOrReplaceTempView("to_close_rows")

    spark.sql(f"""
        MERGE INTO {tgt_tbl} AS tgt
        USING to_close_rows AS s
          ON  tgt.{business_key_tgt} = s.org_cd
         AND  tgt.{end_col} IS NULL
        WHEN MATCHED THEN UPDATE SET
             {end_col} = s.end_ts,
             {ts_col}  = s.ts_now
    """)

    logging.info(f"closed scd2 rows: {changed_keys.count()}")

    # ============================================================
    # 5) PREPARE INSERT DATA (NEW + NEW VERSIONS)
    # ============================================================
    w_key = Window.partitionBy("org_cd").orderBy(f.col(surr_key).desc_nulls_last())

    latest_keys = (
        spark.table(tgt_tbl)
             .withColumn("rn", f.row_number().over(w_key))
             .filter("rn = 1")
             .select("org_cd", f.col(surr_key).alias("max_key"))
    )

    to_insert_staged = (
        changes
        .join(latest_keys, on="org_cd", how="left")
        .withColumn(surr_key,
            f.when(f.col("max_key").isNull(),
                (f.col("org_cd").cast("long") * f.lit(1000)) + f.lit(1))
            .otherwise(f.col("max_key") + f.lit(1))
        )
        .withColumn(start_col,
            f.when(f.col("is_new"), f.col("created_src"))
             .otherwise(f.expr("modified_src + INTERVAL 1 SECOND"))
        )
        .withColumn(end_col, f.lit(None).cast("timestamp"))
        .withColumn(ts_col,  spark_current_timestamp())
        .select(
            surr_key,
            f.col("org_cd").alias(business_key_tgt),
            "name", "short_name", "description", "is_active", "created_src",
            start_col, end_col, ts_col
        )
    )

    to_insert_staged.createOrReplaceTempView("new_versions")

    logging.info(f"new scd2 rows to insert: {to_insert_staged.count()}")

    # ============================================================
    # 6) ICEBERG INSERT (new versions and new records)
    # ============================================================
    spark.sql(f"INSERT INTO {tgt_tbl} SELECT * FROM new_versions")

    logging.info("scd2 upsert completed successfully.")

    total_records = new_or_update_records + nr_changes

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=total_records,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
