from pyspark.sql import DataFrame  # type:ignore
from pyspark.sql import functions as F  # type:ignore
from pyspark.sql.types import TimestampType, IntegerType, FloatType
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
from utils.gold_utils_functions import update_ctrl_table, get_max_timestamp_for_table
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_fact_course_enrollment_daily")

    # FIX 1: Raise shuffle partitions from 8 → 200 to avoid huge skewed partitions
    # after the sequence() explode. AQE will coalesce down where partitions are small.
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # FIX 2: Enable skew join handling — the exploded daily rows are highly skewed
    # (long-running enrollments produce far more rows than short ones).
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    # Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "fact_course_enrollment_daily"

    src_layer = f"silver{ENVIRONMENT}"
    src_pipeline = "entidades"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(
        spark_session=spark, table_name=tgt_table_name, env=ENVIRONMENT
    )

    logging.info(f"Starting process from {last_execution_timestamp}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
            -- Grain
            day_key                        DATE         COMMENT 'Date of fact (one row per day). Partition key',

            -- Natural Key
            course_enrollment_cd           STRING       COMMENT 'Enrollment identifier (original LMS id)',

            -- Foreign Keys
            course_edition_key             STRING       COMMENT 'References dim_course_edition.course_edition_key',
            user_key                       BIGINT       COMMENT 'References dim_user.user_key',
            org_key                        BIGINT       COMMENT 'References dim_organization.org_key',

            -- Status on this day
            course_enrollment_start_date   TIMESTAMP    COMMENT 'Date when the user enrolled in the course edition',
            is_enrolled                    BOOLEAN      COMMENT 'If the student is enrolled on this day',
            unenrollment_date              TIMESTAMP    COMMENT 'Moment the unenrollment happened (if any)',

            -- Audit
            last_update_timestamp          TIMESTAMP    COMMENT 'ETL timestamp'
        )
        USING iceberg
        PARTITIONED BY (days(day_key))
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes' = '536870912',
            'write.distribution-mode' = 'hash',
            'write.sort.order' = 'day_key ASC, course_edition_key ASC',
            'commit.manifest.min-count-to-merge' = '100',
            'write.merge.enabled' = 'true',
            'read.split.target-size' = '134217728',
            'read.split.open-file-cost' = '4194304',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10',
            'write.parquet.bloom-filter.enabled.column.course_edition_key' = 'true',
            'write.parquet.bloom-filter.enabled.column.user_key'           = 'true',
            'write.parquet.bloom-filter.enabled.column.org_key'            = 'true'
        );
    """)

    SRC_ENROLL     = f"{src_layer}.{src_pipeline}.student_courseenrollment"
    SRC_ENROLL_HST = f"{src_layer}.{src_pipeline}.student_courseenrollment_history"
    DIM_USER_TBL   = f"{tgt_layer}.entidades.dim_user"
    DIM_CE_TBL     = f"{tgt_layer}.entidades.dim_course_edition"
    TGT_FACT_DAILY = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"
    ICEBERG_CATALOG = tgt_layer

    END_INF = F.lit("9999-12-31 00:00:00").cast("timestamp")

    logging.info(f"Starting daily load for records with ingestion_date > {last_execution_timestamp}")

    # ------------------------------
    # 1) Load incremental sources
    # ------------------------------
    enroll_df = (
        spark.read.table(SRC_ENROLL)
             .filter(F.col("ingestion_date") > F.to_timestamp(F.lit(last_execution_timestamp)))
    )

    hist_df = (
        spark.read.table(SRC_ENROLL_HST)
             .filter(F.col("ingestion_date") > F.to_timestamp(F.lit(last_execution_timestamp)))
    )

    # ------------------------------
    # 2) Most recent state from history
    # ------------------------------
    w = Window.partitionBy("id").orderBy(
        F.col("history_date").desc(), F.col("ingestion_date").desc()
    )

    latest_hist = (
        hist_df
          .withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .select(
              "id",
              F.col("history_date").alias("last_event_date"),
              F.col("history_type").alias("last_event_type"),
              F.col("is_active").alias("last_is_active")
          )
    )

    merged_df = (
        enroll_df.alias("b")
                 .join(latest_hist.alias("h"), F.col("b.id") == F.col("h.id"), "left")
                 .select(
                     F.col("b.id").alias("course_enrollment_cd"),
                     "b.user_id",
                     "b.course_id",
                     F.coalesce("h.last_is_active", "b.is_active").alias("is_active"),
                     "b.mode",
                     "b.created",
                     "h.last_event_type",
                     "h.last_event_date"
                 )
    )

    # ------------------------------
    # 3) Base fact attributes
    # ------------------------------
    fact_base = (
        merged_df
        .withColumn("course_enrollment_start_date", F.col("created"))
        .withColumn("course_enrollment_end_date",
            F.when(F.col("is_active") == False, F.coalesce("last_event_date", "created"))
        )
        .withColumn("unenrollment_date",
            F.when(
                F.col("is_active") == False,
                F.col("last_event_date")
            )
        )
        .withColumn("is_enrolled",
            F.when(F.col("is_active") == True, F.lit(True)).otherwise(F.lit(False))
        )
    )

    # ------------------------------
    # 4) Dimensions (SCD2)
    # FIX 3: Removed unconditional .hint("broadcast") on both dim tables.
    # Forcing a broadcast on large SCD2 tables causes driver memory pressure.
    # AQE (enabled above) will automatically broadcast if the table fits in memory,
    # and fall back to a sort-merge join if it doesn't.
    # ------------------------------
    dim_user = spark.read.table(DIM_USER_TBL).alias("du")
    dim_ce   = spark.read.table(DIM_CE_TBL).alias("dce")

    # ------------------------------
    # 5) JOIN with dim_course_edition (SCD2)
    # ------------------------------
    fact_with_course = (
        fact_base.alias("f")
                 .join(
                     dim_ce,
                     (
                         (F.col("f.course_id") == F.col("dce.course_edition_cd"))
                         &
                         (F.col("f.created").between(
                             F.col("dce.key_start_date"),
                             F.coalesce(F.col("dce.key_end_date"), END_INF)
                         ))
                     ),
                     "left"
                 )
                 .select(
                     "f.*",
                     F.col("dce.course_edition_key"),
                     F.col("dce.org_key"),
                     F.least(F.col("dce.start_date"), F.col("dce.enrollment_start")).alias("ce_start_date"),
                     F.col("dce.end_date").alias("ce_end_date")
                 )
    )

    # ------------------------------
    # 6) JOIN with dim_user (SCD2)
    # ------------------------------
    fact_with_user = (
        fact_with_course.alias("f")
                        .join(
                            dim_user,
                            (
                                (F.col("f.user_id") == F.col("du.user_cd"))
                                &
                                (F.col("f.created").between(
                                    F.col("du.key_start_date"),
                                    F.coalesce(F.col("du.key_end_date"), END_INF)
                                ))
                            ),
                            "left"
                        )
                        .select("f.*", "du.user_key")
    )

    # ------------------------------
    # 7) Final fields (pre-expansion)
    # ------------------------------
    fact_final = (
        fact_with_user
        .select(
            "course_enrollment_cd", "course_id", "course_edition_key",
            "user_key", "org_key", "course_enrollment_start_date",
            "course_enrollment_end_date", "unenrollment_date", "is_enrolled",
            "ce_start_date", "ce_end_date"
        )
    )

    # ------------------------------
    # 8) Expand to daily grain
    # FIX 4: Cap effective_end to current_date() BEFORE the sequence() call.
    # Without this, an open-ended enrollment (end_date = null → current_date) is
    # fine, but a miscoded far-future date (e.g. 2099) would explode into tens of
    # thousands of rows per enrollment. The cap also prevents generating future
    # day_key rows that would need to be re-merged on every subsequent run.
    # ------------------------------
    today = F.current_date()

    fact_daily = (
        fact_final
        .withColumn("st_aluno", F.to_date("course_enrollment_start_date"))
        .withColumn("en_aluno", F.coalesce(F.to_date("course_enrollment_end_date"), today))
        .withColumn("ce_start", F.to_date("ce_start_date"))
        .withColumn("ce_end",   F.coalesce(F.to_date("ce_end_date"), today))
        .withColumn("effective_start", F.greatest(F.col("st_aluno"), F.col("ce_start")))
        # FIX 4 (cont): clamp effective_end to today — no future rows, no runaway expansions
        .withColumn("effective_end", F.least(
            F.col("en_aluno"),
            F.col("ce_end"),
            today                          # ← hard cap at today
        ))
        .filter(F.col("effective_start") <= F.col("effective_end"))
        .withColumn("date_array", F.expr("sequence(effective_start, effective_end, interval 1 day)"))
        .withColumn("day_key", F.explode("date_array"))
        .withColumn("time_key", F.date_format("day_key", "yyyyMMdd").cast("int"))
        .drop("date_array", "st_aluno", "en_aluno", "ce_start", "ce_end",
              "effective_start", "effective_end", "ce_start_date", "ce_end_date")
        .withColumn("last_update_timestamp", F.current_timestamp())
    )

    # ------------------------------
    # 9) MERGE daily by (course_enrollment_cd, day_key)
    # Dedup before MERGE to prevent MERGE_CARDINALITY_VIOLATION.
    # Duplicates can arise when SCD2 windows in dim_course_edition or dim_user
    # overlap, causing a single enrollment to join multiple dimension versions.
    # We keep the row with the most recent last_update_timestamp; ties are
    # broken arbitrarily (all duplicates carry the same business data).
    # ------------------------------
    w_dedup = Window.partitionBy("course_enrollment_cd", "day_key") \
                    .orderBy(F.col("last_update_timestamp").desc())

    fact_daily_deduped = (
        fact_daily
        .withColumn("_rn", F.row_number().over(w_dedup))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    fact_daily_deduped.createOrReplaceTempView("fact_daily_tmp")

    spark.sql(f"""
        MERGE INTO {TGT_FACT_DAILY} AS t
        USING fact_daily_tmp AS s
            ON  t.course_enrollment_cd = s.course_enrollment_cd
            AND t.day_key = s.day_key

        WHEN MATCHED THEN UPDATE SET
            t.course_edition_key            = s.course_edition_key,
            t.user_key                      = s.user_key,
            t.org_key                       = s.org_key,
            t.course_enrollment_start_date  = s.course_enrollment_start_date,
            t.is_enrolled                   = s.is_enrolled,
            t.unenrollment_date             = s.unenrollment_date,
            t.last_update_timestamp         = s.last_update_timestamp

        WHEN NOT MATCHED THEN INSERT (
            day_key, course_enrollment_cd, course_edition_key, user_key, org_key,
            course_enrollment_start_date, is_enrolled, unenrollment_date, last_update_timestamp
        ) VALUES (
            s.day_key, s.course_enrollment_cd, s.course_edition_key, s.user_key, s.org_key,
            s.course_enrollment_start_date, s.is_enrolled, s.unenrollment_date, s.last_update_timestamp
        )
    """)

    logging.info("Daily MERGE completed successfully.")

    # ------------------------------
    # 10) Iceberg maintenance
    # FIX 5: Added max-concurrent-file-group-rewrites and partial-progress options.
    #
    # The original call processed 2,672 file groups with default (very low) concurrency,
    # generating ~5,000+ Spark jobs serially across only 2 executors.
    #
    # - max-concurrent-file-group-rewrites: run up to 20 file groups in parallel
    #   instead of nearly 1 at a time. Tune this to (num_executors * 2) for your cluster.
    # - partial-progress.enabled: commit file groups in batches rather than holding
    #   all rewrites in memory until the very end. This prevents OOM on large tables
    #   and makes the job resumable if it fails mid-way.
    # - partial-progress.max-commits: commit every 10 batches. Balances commit
    #   overhead vs. memory pressure. Lower if you see OOM; raise if commits are slow.
    # ------------------------------
    try:
        spark.sql(f"""
          CALL {ICEBERG_CATALOG}.system.rewrite_data_files(
            table   => '{TGT_FACT_DAILY}',
            strategy => 'sort',
            sort_order => 'day_key ASC, course_edition_key ASC, user_key ASC',
            options => map(
              'min-input-files',                    '2',
              'rewrite-all',                        'false',
              'max-concurrent-file-group-rewrites', '20',
              'partial-progress.enabled',           'true',
              'partial-progress.max-commits',       '10'
            )
          )
        """)
        spark.sql(f"""
          CALL {ICEBERG_CATALOG}.system.rewrite_manifests(table => '{TGT_FACT_DAILY}')
        """)
        spark.sql(f"""
          CALL {ICEBERG_CATALOG}.system.expire_snapshots(table => '{TGT_FACT_DAILY}', retain_last => 5)
        """)
        logging.info("Iceberg maintenance executed: sort + compact + manifests + expire snapshots.")
    except Exception as e:
        logging.warning(f"Iceberg procedures not executed ({e}).")

    # Finally, update the control table with the run metadata.
    update_ctrl_table(
        spark_session=spark,
        table_name=tgt_table_name,
        current_timestamp=current_timestamp,
        number_of_records=0,
        env=ENVIRONMENT
    )


if __name__ == "__main__":
    main()