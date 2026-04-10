from pyspark.sql import DataFrame #type:ignore
from pyspark.sql import functions as F #type:ignore
from pyspark.sql.functions import col, lit, when, expr, greatest, row_number
from pyspark.sql.types import TimestampType
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

            -- FKs
            course_edition_key               STRING      COMMENT 'FK → dim_course_edition.course_edition_key',
            user_key                         BIGINT      COMMENT 'FK → dim_user.user_key',
            org_key                          BIGINT      COMMENT 'FK → dim_organization.org_key',

            -- Certificate metadata
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

    w = Window.partitionBy("id").orderBy(col("event_ts").desc(), col("ingestion_date").desc())

    src_latest = (
        src_inc
        .withColumn("rn", row_number().over(w))
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
    fact_point = (
        with_user
        .select(
            col("y.id").cast("string").alias("certificate_cd"),
            col("y.course_edition_key").alias("course_edition_key"),
            col("y.user_key").alias("user_key"),
            col("y.org_key").alias("org_key"),
            F.coalesce(col("y.enrollment_start"), col("y.start_date"))
                .cast(TimestampType())
                .alias("course_enrollment_start_date"),
            col("y.created_date").alias("certificate_issue_date"),
            F.current_timestamp().alias("last_update_timestamp")
        )
    )

    # ---------------------------------------------------------
    # 6) Convert to daily grain
    # ---------------------------------------------------------
    fact_daily = (
        fact_point
        .withColumn("day_key", F.to_date("certificate_issue_date"))
        .select(
            "day_key", "certificate_cd", "course_edition_key",
            "user_key", "org_key", "course_enrollment_start_date",
            "certificate_issue_date", "last_update_timestamp"
        )
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

    spark.sql(f"""
        MERGE INTO {TGT_TBL_DLY} AS t
        USING fact_certificate_daily_tmp AS s
          ON  t.certificate_cd = s.certificate_cd
          AND t.day_key        = s.day_key

        WHEN MATCHED THEN UPDATE SET
            t.course_edition_key             = s.course_edition_key,
            t.user_key                       = s.user_key,
            t.org_key                        = s.org_key,
            t.course_enrollment_start_date   = s.course_enrollment_start_date,
            t.certificate_issue_date         = s.certificate_issue_date,
            t.last_update_timestamp          = s.last_update_timestamp

        WHEN NOT MATCHED THEN INSERT (
            day_key, certificate_cd, course_edition_key, user_key, org_key,
            course_enrollment_start_date, certificate_issue_date, last_update_timestamp
        ) VALUES (
            s.day_key, s.certificate_cd, s.course_edition_key, s.user_key, s.org_key,
            s.course_enrollment_start_date, s.certificate_issue_date, s.last_update_timestamp
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
