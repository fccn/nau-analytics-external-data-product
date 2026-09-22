from pyspark.sql import DataFrame #type:ignore
from pyspark.sql import functions as F #type:ignore
from pyspark.sql.functions import col, lit, when, expr, greatest, row_number
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.gold_utils_functions import update_ctrl_table,get_max_timestamp_for_table
from utils.column_spec import build_conditional_columns
from utils.certificate_column_specs import FACT_DAILY_COLS_SPEC, UPDATE_SET_PARTS_SPEC, INSERT_COLS_SPEC
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Shared between the CREATE TABLE (fresh install) and the guarded ALTER
# TABLE (existing tables) below -- a single source of truth so a future
# wording tweak doesn't need to be made in two places.
VERIFY_UUID_COMMENT = (
    "Public certificate verification UUID (source: "
    "certificates_generatedcertificate.verify_uuid). Combined with the "
    "LMS base URL (CERTIFICATES_HTML_VIEW), yields the public certificate "
    "URL: https://lms.nau.edu.pt/certificates/<verify_uuid>"
)

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    # Feature flag for fccn/nau-technical#981's verify_uuid/mode columns.
    # Defaults to disabled so this shared image can be merged/deployed to
    # every environment (they all track the same docker_image tag) without
    # immediately touching any environment's data -- enable per environment
    # by hardcoding this env var to "true" in that environment's own
    # gold_dag.py driverEnv wiring (nau-analytics-airflow-dags), merged only
    # once that environment is actually ready to roll this out. Must be set
    # consistently with the same flag in
    # silver_certificates_generatedcertificate.py, since gold's verify_uuid
    # is sourced from silver's own (also gated) column.
    VERIFY_UUID_MODE_ENABLED = os.getenv("CERTIFICATE_VERIFY_UUID_MODE_ENABLED", "false").lower() == "true"

    spark = start_iceberg_session("gold_fact_certificate_daily")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "fact_certificate_daily"

    src_layer = f"silver{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name = "certificates_generatedcertificate"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting process from {last_execution_timestamp}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
            -- Grain
            day_key                          DATE        COMMENT 'Date of the event (day of certificate issue)',

            -- Natural key
            certificate_cd                   STRING      COMMENT 'Certificate identifier (source: certificates_generatedcertificate.id)',
            verify_uuid                      STRING      COMMENT '{VERIFY_UUID_COMMENT}',

            -- FKs
            course_edition_key               STRING      COMMENT 'FK → dim_course_edition.course_edition_key',
            user_key                         BIGINT      COMMENT 'FK → dim_user.user_key',
            org_key                          BIGINT      COMMENT 'FK → dim_organization.org_key',

            -- Certificate metadata
            status                           STRING      COMMENT 'Certificate status (downloadable, notpassing, audit_passing, unverified, etc.)',
            mode                             STRING      COMMENT 'Certificate mode (honor, verified, audit, etc.)',
            course_enrollment_start_date     TIMESTAMP   COMMENT 'Enrollment open date for the edition (or start_date)',
            certificate_issue_date           TIMESTAMP   COMMENT 'Date/time the certificate was issued',

            -- Audit
            last_update_timestamp            TIMESTAMP   COMMENT 'ETL timestamp'
        )
        USING iceberg
        TBLPROPERTIES (
          'write.distribution-mode' = 'none',
          'write.target-file-size-bytes' = '536870912',
          'write.parquet.compression-codec' = 'zstd',
          'write.sort.order' = 'day_key ASC, course_edition_key ASC, user_key ASC',
          'read.split.target-size' = '134217728',
          'read.split.open-file-cost' = '4194304',
          'commit.manifest.min-count-to-merge' = '100',
          'write.merge.enabled' = 'true',
          'write.metadata.delete-after-commit.enabled' = 'true',
          'write.metadata.previous-versions-max' = '10',
          'write.parquet.bloom-filter.enabled.column.course_edition_key' = 'true',
          'write.parquet.bloom-filter.enabled.column.user_key'           = 'true',
          'write.parquet.bloom-filter.enabled.column.org_key'            = 'true'
        );
    """)

    SRC_TBL      = f"{src_layer}.{src_pipeline}.{src_table_name}"
    TGT_TBL_DLY  = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"
    DIM_USER_TBL = f"{tgt_layer}.entidades.dim_user"
    DIM_ORG_TBL  = f"{tgt_layer}.entidades.dim_organization"
    DIM_CE_TBL   = f"{tgt_layer}.entidades.dim_course_edition"

    # ---------------------------------------------------------
    # Schema evolution: CREATE TABLE IF NOT EXISTS above only
    # covers a first run. verify_uuid/mode were added after this
    # table already existed in prod/stage/dev, and Iceberg's
    # ADD COLUMNS has no IF NOT EXISTS, so guard it explicitly
    # to keep this script idempotent across DAG runs.
    #
    # Gated by VERIFY_UUID_MODE_ENABLED (see flag comment above) --
    # while disabled, this table is never altered and behaves exactly
    # as it did before fccn/nau-technical#981.
    # ---------------------------------------------------------
    if VERIFY_UUID_MODE_ENABLED:
        existing_columns = {field.name for field in spark.table(TGT_TBL_DLY).schema.fields}
        if "verify_uuid" not in existing_columns:
            spark.sql(f"""
                ALTER TABLE {TGT_TBL_DLY}
                ADD COLUMNS (
                    verify_uuid STRING COMMENT '{VERIFY_UUID_COMMENT}'
                )
            """)
        if "mode" not in existing_columns:
            spark.sql(f"""
                ALTER TABLE {TGT_TBL_DLY}
                ADD COLUMNS (
                    mode STRING COMMENT 'Certificate mode (honor, verified, audit, etc.)'
                )
            """)

    END_INF = F.lit("9999-12-31 00:00:00").cast("timestamp")

    logging.info(f"Starting incremental FACT CERTIFICATE (daily) for ingestion_date > {last_execution_timestamp}")

    # ---------------------------------------------------------
    # 1) Incremental read + dedup by id (latest by event_ts)
    # ---------------------------------------------------------
    src_inc = (
        spark.table(SRC_TBL)
             .filter(col("ingestion_date") > F.to_timestamp(lit(last_execution_timestamp)))
             .withColumn("event_ts", greatest(col("modified_date"), col("created_date")))
    )

    latest_by_event_window = Window.partitionBy("id").orderBy(col("event_ts").desc(), col("ingestion_date").desc())

    src_latest = (
        src_inc
        .withColumn("rn", row_number().over(latest_by_event_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .alias("s")
    )

    # ---------------------------------------------------------
    # 2) Dimensions (SCD2)
    # ---------------------------------------------------------
    dim_user = spark.table(DIM_USER_TBL).alias("du")
    dim_org  = spark.table(DIM_ORG_TBL).alias("do")
    dim_ce   = spark.table(DIM_CE_TBL).alias("dce")

    # ---------------------------------------------------------
    # 3) JOIN → dim_course_edition (SCD2-aware)
    # ---------------------------------------------------------
    with_ce = (
        src_latest.alias("s")
        .join(
            dim_ce,
            on=[
                col("s.course_id") == col("dce.course_edition_cd"),
                col("s.event_ts").between(
                    col("dce.key_start_date"),
                    F.coalesce(col("dce.key_end_date"), END_INF)
                )
            ],
            how="left"
        )
        .alias("x")
    )

    # ---------------------------------------------------------
    # 4) JOIN → dim_user (SCD2-aware)
    # ---------------------------------------------------------
    with_user = (
        with_ce.alias("x")
        .join(
            dim_user,
            on=[
                col("x.user_id") == col("du.user_cd"),
                col("x.event_ts").between(
                    col("du.key_start_date"),
                    F.coalesce(col("du.key_end_date"), END_INF)
                )
            ],
            how="left"
        )
        .alias("y")
    )

    # ---------------------------------------------------------
    # 5) Project fact columns
    # ---------------------------------------------------------
    # Defesa em profundidade: ignora o DEFAULT_START_DATE sentinela do Open
    # edX caso escape da dim. 2030-01-01 é o valor histórico (releases até
    # Sumac), 2040-01-01 é o valor actual no master do edx-platform.
    LMS_DEFAULT_START_SENTINELS = [
        F.lit('2030-01-01 00:00:00').cast("timestamp"),
        F.lit('2040-01-01 00:00:00').cast("timestamp"),
    ]
    def _is_lms_sentinel(c):
        cond = F.lit(False)
        for sentinel in LMS_DEFAULT_START_SENTINELS:
            cond = cond | (c == sentinel)
        return cond
    clean_start = F.when(_is_lms_sentinel(col("y.start_date")), None) \
                   .otherwise(col("y.start_date"))
    clean_enr   = F.when(_is_lms_sentinel(col("y.enrollment_start")), None) \
                   .otherwise(col("y.enrollment_start"))

    # Column selection mirrors the flag: while disabled, verify_uuid/mode
    # are neither read from silver nor written to gold, since the ALTER
    # above never ran and these columns may not exist on the target table
    # yet (mode already exists in silver from before #981, but not yet in
    # this gold table). build_conditional_columns (utils/column_spec.py)
    # keeps this, fact_daily_cols, update_set_parts, and insert_cols below
    # all built from the same ordered-spec pattern, so adding/removing a
    # gated column only needs editing one spec per list rather than
    # hand-writing `if VERIFY_UUID_MODE_ENABLED: ...append(...)` at each
    # of the four call sites independently.
    fact_point_cols = build_conditional_columns([
        col("y.id").cast("string").alias("certificate_cd"),
        (col("y.verify_uuid").alias("verify_uuid"), True),
        col("y.status").alias("status"),
        (col("y.mode").alias("mode"), True),
        col("y.course_edition_key").alias("course_edition_key"),
        col("y.user_key").alias("user_key"),
        col("y.org_key").alias("org_key"),
        F.coalesce(clean_enr, clean_start)
            .cast(TimestampType())
            .alias("course_enrollment_start_date"),
        col("y.created_date").alias("certificate_issue_date"),
        F.current_timestamp().alias("last_update_timestamp"),
    ], enabled=VERIFY_UUID_MODE_ENABLED)

    fact_point = with_user.select(*fact_point_cols)

    # ---------------------------------------------------------
    # 6) Convert to daily grain
    # ---------------------------------------------------------
    fact_daily_cols = build_conditional_columns(FACT_DAILY_COLS_SPEC, enabled=VERIFY_UUID_MODE_ENABLED)

    fact_daily = (
        fact_point
        .withColumn("day_key", F.to_date("certificate_issue_date"))
        .select(*fact_daily_cols)
    )

    dq_nulls = fact_daily.filter(
        col("course_edition_key").isNull() |
        col("user_key").isNull() |
        col("org_key").isNull() |
        col("day_key").isNull()
    ).count()
    logging.info(f"NULL keys or date (daily) before MERGE: {dq_nulls}")

    # ---------------------------------------------------------
    # 7) MERGE (upsert) to daily FACT
    # ---------------------------------------------------------
    fact_daily.createOrReplaceTempView("fact_certificate_daily_tmp")

    new_or_update_records = fact_daily.count()
    logging.info(f"Rows to MERGE (daily): {new_or_update_records}")

    # UPDATE SET / INSERT column lists mirror the flag for the same reason
    # as the SELECT above.
    update_set_parts = build_conditional_columns(UPDATE_SET_PARTS_SPEC, enabled=VERIFY_UUID_MODE_ENABLED)
    update_set_sql = ",\n            ".join(update_set_parts)

    insert_cols = build_conditional_columns(INSERT_COLS_SPEC, enabled=VERIFY_UUID_MODE_ENABLED)
    insert_cols_sql = ", ".join(insert_cols)
    insert_vals_sql = ", ".join(f"s.{col_name}" for col_name in insert_cols)

    spark.sql(f"""
        MERGE INTO {TGT_TBL_DLY} AS t
        USING fact_certificate_daily_tmp AS s
          ON  t.certificate_cd = s.certificate_cd
          AND t.day_key        = s.day_key

        WHEN MATCHED THEN UPDATE SET
            {update_set_sql}

        WHEN NOT MATCHED THEN INSERT (
            {insert_cols_sql}
        ) VALUES (
            {insert_vals_sql}
        )
    """)

    logging.info("fact_certificate_daily updated successfully.")

    # ---------------------------------------------------------
    # 8) Iceberg maintenance
    # ---------------------------------------------------------
    try:
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_data_files(
            table => '{TGT_TBL_DLY}',
            strategy => 'sort',
            sort_order => 'day_key ASC, course_edition_key ASC, user_key ASC'
          )
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_manifests(table => '{TGT_TBL_DLY}')
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.expire_snapshots(table => '{TGT_TBL_DLY}', retain_last => 5)
        """)
        logging.info("Iceberg maintenance executed: sort + compact + manifests + expire snapshots.")
    except Exception as e:
        logging.warning(f"Iceberg procedures not executed ({e}).")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=new_or_update_records,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
