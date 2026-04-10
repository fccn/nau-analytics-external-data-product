from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from pyspark.sql.functions import (
    col, lit, when, coalesce, greatest, to_timestamp, current_timestamp,
    row_number, lpad, regexp_extract, max as smax, expr, current_timestamp as spark_current_timestamp, upper
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

    spark = start_iceberg_session("gold_dim_course_edition")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_audit_schema = "audit"
    tgt_pipeline = "entidades"
    tgt_table_name = "dim_course_edition"

    src_layer = f"silver{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name = "course_overviews_courseoverview"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting process from {last_execution_timestamp}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (

            -- Keys
            course_edition_key      STRING       COMMENT 'Surrogate SCD2 key (course_edition_cd + seq)',
            course_edition_cd       STRING       COMMENT 'Original course identifier (Open edX course-v1 / ccx-v1)',
            edition                 STRING       COMMENT 'Edition extracted from course_edition_cd (RUN component)',

            org_key                 BIGINT       COMMENT 'FK → dim_organization.org_key',

            -- Course edition metadata
            display_name            STRING       COMMENT 'Course display name',
            display_number          STRING       COMMENT 'Course display code',
            short_description       STRING       COMMENT 'Short description of the course edition',

            start_date              TIMESTAMP    COMMENT 'Course edition start date',
            end_date                TIMESTAMP    COMMENT 'Course edition end date',
            advertised_start        TIMESTAMP    COMMENT 'Advertised start date',
            version                 STRING       COMMENT 'Course version',

            enrollment_start        TIMESTAMP    COMMENT 'Enrollment window start date',
            enrollment_end          TIMESTAMP    COMMENT 'Enrollment window end date',

            certificates_display_behavior  STRING COMMENT 'Behavior for displaying certificates',
            language               STRING        COMMENT 'Course language',
            certificate_available_date TIMESTAMP COMMENT 'When certificates become available',
            effort                 STRING        COMMENT 'Estimated effort',
            announcement           TIMESTAMP     COMMENT 'Announcement date',
            catalog_visibility     STRING        COMMENT 'Visibility in catalog',

            _location              STRING        COMMENT 'Raw location metadata',
            entrance_exam_id       STRING        COMMENT 'Identifier for entrance exam',

            -- Extra numerical metadata
            lowest_passing_grade               FLOAT COMMENT 'Minimum passing grade',
            days_early_for_beta                INT   COMMENT 'Days before beta release',
            max_student_enrollments_allowed    INT   COMMENT 'Edition-level max enrollments',
            entrance_exam_minimum_score_pct    FLOAT COMMENT 'Minimum exam score percentage',

            -- Binary flags
            certificates_show_before_end           BOOLEAN,
            cert_html_view_enabled                 BOOLEAN,
            has_any_active_web_certificate         BOOLEAN,
            mobile_available                       BOOLEAN,
            visible_to_staff_only                  BOOLEAN,
            invitation_only                        BOOLEAN,
            self_paced                             BOOLEAN,
            eligible_for_financial_aid             BOOLEAN,
            has_highlights                         BOOLEAN,
            allow_proctoring_opt_out               BOOLEAN,
            enable_proctored_exams                 BOOLEAN,
            entrance_exam_enabled                  BOOLEAN,
            force_on_flexible_peer_openassessments BOOLEAN,

            -- SCD2 Metadata
            key_start_date        TIMESTAMP COMMENT 'Start date for SCD2 row',
            key_end_date          TIMESTAMP COMMENT 'End date for SCD2 row',

            -- Audit metadata
            last_update_timestamp TIMESTAMP COMMENT 'ETL timestamp'
        )
        USING iceberg
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '536870912',
            'write.distribution-mode' = 'none',
            'write.sort.order' = 'display_name ASC, edition ASC',
            'commit.manifest.min-count-to-merge' = '100',
            'write.merge.enabled' = 'true',
            'read.split.target-size' = '134217728',
            'read.split.open-file-cost' = '4194304',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10',
            'write.parquet.bloom-filter.enabled.column.course_edition_key' = 'true',
            'write.parquet.bloom-filter.enabled.column.display_name'       = 'true',
            'write.parquet.bloom-filter.enabled.column.edition'            = 'true'
        );
    """)

    src_tbl = f"{src_layer}.{src_pipeline}.{src_table_name}"
    tgt_tbl = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"
    org_tbl = f"{tgt_layer}.entidades.dim_organization"

    logging.info(f"Running SCD2 load for: {src_tbl}")

    # =============================================================
    # 1) Load source incrementally
    # =============================================================
    src_full = spark.table(src_tbl)

    src_inc = src_full.where(
        (col("created")       > to_timestamp(lit(last_execution_timestamp))) |
        (col("modified")      > to_timestamp(lit(last_execution_timestamp))) |
        (col("ingestion_date") > to_timestamp(lit(last_execution_timestamp)))
    )

    w = Window.partitionBy("id").orderBy(
        greatest(col("modified"), col("created")).desc()
    )

    src_latest = (
        src_inc
        .withColumn("event_ts", greatest(col("modified"), col("created")))
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .alias("co")
    )

    # =============================================================
    # 2) JOIN with dim_organization (SCD2-aware)
    # =============================================================
    dim_org = spark.table(org_tbl).alias("oo")

    src_with_org = (
        src_latest
        .join(
            dim_org,
            on=[
                expr("upper(co.org) = upper(oo.short_name)"),
                expr("""
                    co.event_ts BETWEEN oo.key_start_date
                    AND coalesce(oo.key_end_date, timestamp '9999-12-31 23:59:59')
                """)
            ],
            how="left"
        )
        .select("co.*", col("oo.org_key"))
    )

    # =============================================================
    # 3) Project business attributes
    # =============================================================
    start_dt = coalesce(col("start"), col("start_date"))
    end_dt   = coalesce(col("end"), col("end_date"))
    adv_start_ts = to_timestamp(col("advertised_start"))

    business_cols = [
        "org_key", "edition", "display_name", "display_number", "short_description",
        "start_date", "end_date", "advertised_start", "version", "enrollment_start",
        "enrollment_end", "certificates_display_behavior", "language",
        "certificate_available_date", "effort", "announcement", "catalog_visibility",
        "_location", "entrance_exam_id", "lowest_passing_grade", "days_early_for_beta",
        "max_student_enrollments_allowed", "entrance_exam_minimum_score_pct",
        "certificates_show_before_end", "cert_html_view_enabled",
        "has_any_active_web_certificate", "mobile_available", "visible_to_staff_only",
        "invitation_only", "self_paced", "eligible_for_financial_aid", "has_highlights",
        "allow_proctoring_opt_out", "enable_proctored_exams", "entrance_exam_enabled",
        "force_on_flexible_peer_openassessments",
    ]

    src_proj = (
        src_with_org.select(
            col("id").alias("course_edition_cd"),
            regexp_extract(col("id"), r'([^+]+)(?:\+ccx@.*)?$', 1).alias("edition"),
            col("org_key"),
            col("display_name"),
            col("display_number_with_default").alias("display_number"),
            col("short_description"),
            start_dt.cast(TimestampType()).alias("start_date"),
            end_dt.cast(TimestampType()).alias("end_date"),
            adv_start_ts.alias("advertised_start"),
            col("version").cast("string"),
            col("enrollment_start").cast(TimestampType()),
            col("enrollment_end").cast(TimestampType()),
            col("certificates_display_behavior"),
            col("language"),
            col("certificate_available_date").cast(TimestampType()),
            col("effort"),
            col("announcement").cast(TimestampType()),
            col("catalog_visibility"),
            col("_location"),
            col("entrance_exam_id"),
            col("lowest_passing_grade").cast(FloatType()),
            col("days_early_for_beta").cast(IntegerType()),
            col("max_student_enrollments_allowed").cast(IntegerType()),
            col("entrance_exam_minimum_score_pct").cast(FloatType()),
            col("certificates_show_before_end"),
            col("cert_html_view_enabled"),
            col("has_any_active_web_certificate"),
            col("mobile_available"),
            col("visible_to_staff_only"),
            col("invitation_only"),
            col("self_paced"),
            col("eligible_for_financial_aid"),
            col("has_highlights"),
            col("allow_proctoring_opt_out"),
            col("enable_proctored_exams"),
            col("entrance_exam_enabled"),
            col("force_on_flexible_peer_openassessments"),
            col("event_ts")
        )
        .alias("s")
    )

    # =============================================================
    # 4) Load active target rows
    # =============================================================
    tgt_active = (
        spark.table(tgt_tbl)
             .where(col("key_end_date").isNull())
             .alias("t")
    )

    cmp_exprs = [
        expr(f"(NOT (s.`{c}` <=> t.`{c}`))")
        for c in business_cols
    ]

    is_changed = None
    for cond in cmp_exprs:
        is_changed = cond if is_changed is None else (is_changed | cond)

    joined = (
        src_proj.join(
            tgt_active,
            col("s.course_edition_cd") == col("t.course_edition_cd"),
            "left"
        )
    )

    staged = (
        joined
        .withColumn("is_existing", col("t.course_edition_cd").isNotNull())
        .withColumn("is_changed", when(col("is_existing"), is_changed).otherwise(lit(False)))
        .withColumn("is_new", ~col("is_existing"))
        .withColumn("change_time", F.current_timestamp())
        .select(
            col("s.course_edition_cd"),
            *[col(f"s.`{c}`") for c in business_cols],
            col("s.event_ts"),
            "is_existing", "is_changed", "is_new", "change_time"
        )
    )

    # =============================================================
    # 5) Generate sequence number
    # =============================================================
    if spark._jsparkSession.catalog().tableExists(tgt_tbl):
        tgt_all = spark.table(tgt_tbl).select(
            "course_edition_cd",
            regexp_extract(col("course_edition_key"), r"_(\d+)$", 1)
                .cast("int").alias("seq_num")
        )
        max_seq = tgt_all.groupBy("course_edition_cd").agg(F.max("seq_num").alias("max_seq"))
    else:
        max_seq = spark.createDataFrame([], "course_edition_cd STRING, max_seq INT")

    staged2 = (
        staged.alias("x")
              .join(max_seq.alias("m"), on="course_edition_cd", how="left")
              .withColumn("seq_next",
                  when(col("m.max_seq").isNull(), lit(1)).otherwise(col("m.max_seq") + lit(1)))
              .withColumn("course_edition_key_new",
                  expr("concat(course_edition_cd, '_', lpad(seq_next, 3, '0'))"))
    )

    # =============================================================
    # 6) CLOSE current SCD2 version
    # =============================================================
    changed_to_close = (
        staged2.filter(col("is_changed") & col("is_existing"))
               .select("course_edition_cd", "change_time")
    )

    changed_to_close.createOrReplaceTempView("stg_close_rows")

    spark.sql(f"""
        MERGE INTO {tgt_tbl} t
        USING stg_close_rows s
        ON  t.course_edition_cd = s.course_edition_cd
        AND t.key_end_date IS NULL
        WHEN MATCHED THEN UPDATE SET
            t.key_end_date = s.change_time,
            t.last_update_timestamp = s.change_time
    """)

    # =============================================================
    # 7) INSERT new SCD2 versions
    # =============================================================
    to_insert = (
        staged2
        .filter(col("is_new") | col("is_changed"))
        .withColumn("key_start_date",
            when(col("m.max_seq").isNull(), lit("1900-01-01 00:00:00").cast(TimestampType()))
            .otherwise(col("change_time") + expr("INTERVAL 1 SECOND"))
        )
        .select(
            col("course_edition_key_new").alias("course_edition_key"),
            col("course_edition_cd"),
            *[col(c) for c in business_cols],
            col("key_start_date"),
            lit(None).cast(TimestampType()).alias("key_end_date"),
            col("change_time").alias("last_update_timestamp")
        )
    )

    new_or_update_records = to_insert.count()
    logging.info(f"New or updated rows: {new_or_update_records}")

    to_insert.createOrReplaceTempView("stg_new_versions")

    spark.sql(f"""
        MERGE INTO {tgt_tbl} AS tgt
        USING stg_new_versions AS src
        ON tgt.course_edition_key = src.course_edition_key
        WHEN NOT MATCHED THEN INSERT *
    """)

    # =============================================================
    # 8) Iceberg optimization / cleanup
    # =============================================================
    try:
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_data_files(
            table => '{tgt_tbl}',
            strategy => 'sort',
            sort_order => 'course_edition_cd,key_start_date'
          )
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_manifests(table => '{tgt_tbl}')
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.expire_snapshots(table => '{tgt_tbl}', retain_last => 5)
        """)
        logging.info("Iceberg maintenance executed: sort + compact + manifests + expire snapshots.")
    except Exception as e:
        logging.warning(f"Iceberg procedures not executed ({e}).")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=new_or_update_records,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
