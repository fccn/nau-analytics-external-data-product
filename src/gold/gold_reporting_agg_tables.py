from dataclasses import dataclass
from typing import Callable
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
from utils.gold_utils_functions import update_ctrl_table, get_max_timestamp_for_table
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# ============================================================
# Aggregation table definitions
# ============================================================

@dataclass
class AggTable:
    name: str
    sql_fn: Callable[[str], str]   # receives tgt_layer, returns a SELECT SQL string
    partition_by: str
    sort_order: str


# Superset Dataset: Formandos Inscritos
def _fact_enrolled_students_agg_sql(tgt_layer: str) -> str:
    return f"""
        SELECT
            fce.day_key,
            fce.course_enrollment_cd,
            dorg.org_cd,
            dorg.short_name                                             AS org_short_name,
            dce.display_number                                          AS course_cd,
            dce.display_name                                            AS course_name,
            dce.edition,
            du.user_cd,
            du.year_of_birth,
            CASE
                WHEN du.gender = 'm' THEN 'Male'
                WHEN du.gender = 'f' THEN 'Female'
                WHEN du.gender = 'o' THEN 'Other'
                ELSE 'N/A'
            END                                                         AS gender,
            du.level_of_education,
            du.country,
            CASE
                WHEN du.year_of_birth IS NULL THEN NULL
                ELSE year(CURRENT_DATE) - du.year_of_birth
            END                                                         AS age,
            CASE
                WHEN du.year_of_birth IS NULL                                    THEN 'N/A'
                WHEN year(CURRENT_DATE) - du.year_of_birth < 18                 THEN 'Menor 18'
                WHEN year(CURRENT_DATE) - du.year_of_birth BETWEEN 18 AND 29    THEN '18-29'
                WHEN year(CURRENT_DATE) - du.year_of_birth BETWEEN 30 AND 54    THEN '30-54'
                ELSE '55+'
            END                                                         AS age_range,
            CASE
                WHEN du.level_of_education IS NULL  THEN 'N/A'
                WHEN du.level_of_education = 'm'    THEN 'Master''s degree'
                WHEN du.level_of_education = 'jhs'  THEN 'Junior High School'
                WHEN du.level_of_education = 'hs'   THEN 'High School'
                WHEN du.level_of_education = 'b'    THEN 'Bachelor''s degree'
                WHEN du.level_of_education = 'p'    THEN 'PhD / Doctorate'
                WHEN du.level_of_education = 'a'    THEN 'Associate degree'
                ELSE 'Other'
            END                                                         AS escolaridade,
            fce.is_enrolled,
            CASE WHEN du.employment_situation IS NULL THEN 'N/A'
                 ELSE du.employment_situation
            END                                                         AS employment_situation,
            CONCAT(
                CAST(fce.course_enrollment_cd AS STRING),
                CAST(du.user_cd AS STRING)
            )                                                           AS unique_key,
            CONCAT(
                CAST(dt.year AS STRING),
                lpad(CAST(dt.month AS STRING), 2, '0'),
                ' - ',
                dt.month_name
            )                                                           AS month_name
        FROM       {tgt_layer}.entidades.fact_course_enrollment_daily  fce
        LEFT JOIN  {tgt_layer}.entidades.dim_user                      du
               ON  fce.user_key = du.user_key
        LEFT JOIN  {tgt_layer}.entidades.dim_organization              dorg
               ON  fce.org_key  = dorg.org_key
        LEFT JOIN  {tgt_layer}.entidades.dim_course_edition            dce
               ON  fce.course_edition_key = dce.course_edition_key
              AND  fce.org_key            = dce.org_key
        JOIN       {tgt_layer}.entidades.dim_time                      dt
               ON  fce.day_key = dt.date
    """


# Superset Dataset: Taxa Conclusão Final
def _fact_conclusion_rate_agg_sql(tgt_layer: str) -> str:
    # FIX 1: Replaced hardcoded `gold_prod` with `tgt_layer` in the UNION ALL branch.
    # The original query always read from production regardless of ENVIRONMENT,
    # silently breaking non-prod runs and preventing staging validation.
    return f"""
        SELECT
            CAST(fc.day_key AS DATE) AS day_key,
            fc.user_key,
            fc.org_key,
            'certificate' AS event_type,
            NULL AS course_enrollment_cd,
            do.org_cd,
            do.short_name AS org_short_name,
            dce.display_number AS course_cd,
            dce.display_name AS course_name,
            dce.edition,
            du.user_cd
        FROM {tgt_layer}.entidades.fact_certificate_daily fc
        LEFT JOIN {tgt_layer}.entidades.dim_user du
            ON fc.user_key = du.user_key
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON fc.org_key = do.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON fc.course_edition_key = dce.course_edition_key
            AND fc.org_key = dce.org_key

        UNION ALL

        SELECT
            CAST(fce.day_key AS DATE) AS day_key,
            fce.user_key,
            fce.org_key,
            'enrollment' AS event_type,
            fce.course_enrollment_cd,
            do.org_cd,
            do.short_name AS org_short_name,
            dce.display_number AS course_cd,
            dce.display_name AS course_name,
            dce.edition,
            du.user_cd
        FROM {tgt_layer}.entidades.fact_course_enrollment_daily fce
        LEFT JOIN {tgt_layer}.entidades.dim_user du
            ON fce.user_key = du.user_key
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON fce.org_key = do.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON fce.course_edition_key = dce.course_edition_key
            AND fce.org_key = dce.org_key
        JOIN {tgt_layer}.entidades.dim_time dt
            ON fce.day_key = dt.date
    """


# Registry — add new aggregation tables here
AGG_TABLES: list[AggTable] = [
    AggTable(
        name         = "fact_enrolled_students_agg",
        sql_fn       = _fact_enrolled_students_agg_sql,
        partition_by = "days(day_key)",
        sort_order   = "day_key ASC, org_cd ASC, course_cd ASC",
    ),
    AggTable(
        name         = "fact_conclusion_rate_agg",
        sql_fn       = _fact_conclusion_rate_agg_sql,
        partition_by = "days(day_key)",
        sort_order   = "day_key ASC, org_cd ASC, course_cd ASC",
    ),
]


# ============================================================
# Core rebuild helper
# FIX 2: Replaced full DROP + CTAS with incremental partition overwrite.
#
# The original approach dropped and recreated the entire table on every run,
# forcing a full scan and rewrite of billions of rows regardless of how much
# data actually changed. On a 2.35B-row table this dominated the 4-hour runtime.
#
# The new approach:
#   - Creates the table once (if it doesn't exist) via CTAS on first run.
#   - On subsequent runs, identifies which day_key partitions are present in
#     the source data since the last execution, and overwrites only those
#     partitions using Spark's dynamic partition overwrite mode.
#   - Partition overwrite is atomic in Iceberg: each partition is replaced as
#     a whole, so there is no risk of partial data.
#
# Result: instead of rewriting 2,673 partitions every day, only the partitions
# that actually changed (typically the last few days) are rewritten.
# ============================================================

def _ensure_table_exists(spark, tgt_layer: str, pipeline: str, agg: AggTable) -> bool:
    """
    Create the table via CTAS if it doesn't already exist.
    Returns True if the table was just created (first run), False if it already existed.
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {tgt_layer}.{pipeline}").collect()]
    if agg.name in tables:
        return False

    logging.info(f"First run: creating {full_name} via CTAS…")
    spark.sql(f"""
        CREATE TABLE {full_name}
        USING iceberg
        PARTITIONED BY ({agg.partition_by})
        TBLPROPERTIES (
            'write.parquet.compression-codec'            = 'zstd',
            'write.target-file-size-bytes'               = '536870912',
            'write.distribution-mode'                    = 'hash',
            'write.sort.order'                           = '{agg.sort_order}',
            'read.split.target-size'                     = '134217728',
            'read.split.open-file-cost'                  = '4194304',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'       = '10'
        )
        AS
        {agg.sql_fn(tgt_layer)}
    """)
    logging.info(f"Table {full_name} created (first run).")
    return True


def _overwrite_changed_partitions(
    spark, tgt_layer: str, pipeline: str, agg: AggTable, last_execution_timestamp: str
) -> None:
    """
    Recompute and overwrite only the day_key partitions that have changed
    since last_execution_timestamp. Uses Iceberg's dynamic partition overwrite,
    which atomically replaces each affected partition.
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"

    # Compute the full result set scoped to changed partitions only.
    # We first find which day_key values appear in the incremental source
    # data, then filter the agg SQL to only those days.
    #
    # NOTE: Both agg queries are driven by fact_course_enrollment_daily or
    # fact_certificate_daily which are filtered upstream by ingestion_date.
    # We translate that to day_key ranges here so Iceberg can prune partitions.
    changed_days_df = spark.sql(f"""
        SELECT DISTINCT day_key
        FROM {tgt_layer}.entidades.fact_course_enrollment_daily
        WHERE last_update_timestamp > TIMESTAMP '{last_execution_timestamp}'
        UNION
        SELECT DISTINCT CAST(day_key AS DATE) AS day_key
        FROM {tgt_layer}.entidades.fact_certificate_daily
        WHERE last_update_timestamp > TIMESTAMP '{last_execution_timestamp}'
    """)

    changed_days_df.createOrReplaceTempView("_changed_days")

    changed_day_count = changed_days_df.count()
    if changed_day_count == 0:
        logging.info(f"No changed partitions detected for {full_name}, skipping overwrite.")
        return

    logging.info(f"Overwriting {changed_day_count} changed day_key partition(s) in {full_name}…")

    # Wrap the agg SQL to filter to only the changed day_key values.
    incremental_sql = f"""
        SELECT agg.*
        FROM (
            {agg.sql_fn(tgt_layer)}
        ) agg
        INNER JOIN _changed_days cd ON agg.day_key = cd.day_key
    """

    # Dynamic partition overwrite: Spark replaces only the partitions present
    # in the DataFrame, leaving all other partitions untouched.
    (
        spark.sql(incremental_sql)
             .writeTo(full_name)
             .option("overwrite-mode", "dynamic")
             .overwritePartitions()
    )

    logging.info(f"Partition overwrite complete for {full_name}.")


def _run_iceberg_maintenance(spark, tgt_layer: str, full_name: str, sort_order: str) -> None:
    # FIX 3: Added max-concurrent-file-group-rewrites and partial-progress.
    # The original call processed thousands of file groups almost serially on
    # 2 executors. These options allow up to 20 groups to run in parallel and
    # commit in batches of 10, dramatically reducing wall-clock time.
    # Tune max-concurrent-file-group-rewrites to ~(num_executors * 2).
    try:
        spark.sql(f"""
            CALL {tgt_layer}.system.rewrite_data_files(
                table      => '{full_name}',
                strategy   => 'sort',
                sort_order => '{sort_order}',
                options    => map(
                    'min-input-files',                    '2',
                    'rewrite-all',                        'false',
                    'max-concurrent-file-group-rewrites', '20',
                    'partial-progress.enabled',           'true',
                    'partial-progress.max-commits',       '10'
                )
            )
        """)
        spark.sql(f"CALL {tgt_layer}.system.rewrite_manifests(table => '{full_name}')")
        spark.sql(f"CALL {tgt_layer}.system.expire_snapshots(table => '{full_name}', retain_last => 5)")
        logging.info(f"Iceberg maintenance completed for {full_name}.")
    except Exception as e:
        logging.warning(f"Iceberg maintenance skipped for {full_name}: {e}")


def _get_row_count_from_metadata(spark, tgt_layer: str, full_name: str) -> int:
    # FIX 4: Replaced spark.table(full_name).count() with a metadata-only lookup.
    # The original count() triggered a full scan of the entire table (2.35B rows,
    # ~10 GB) just to produce a single number for the control table. Iceberg
    # tracks total-records in snapshot metadata at no I/O cost.
    try:
        row_count = int(spark.sql(f"""
            SELECT summary['total-records']
            FROM {tgt_layer}.entidades.snapshots
            WHERE table_name = '{full_name}'
            ORDER BY committed_at DESC
            LIMIT 1
        """).first()[0])
    except Exception:
        # Fall back to the Iceberg table history approach if the snapshots view
        # is not available in this catalog version.
        try:
            row_count = int(spark.sql(f"""
                SELECT snapshot_summary['total-records']
                FROM {full_name}.history
                ORDER BY made_current_at DESC
                LIMIT 1
            """).first()[0])
        except Exception:
            logging.warning("Could not read row count from Iceberg metadata; defaulting to -1.")
            row_count = -1
    return row_count


def _rebuild_agg_table(
    spark, tgt_layer: str, pipeline: str, agg: AggTable, last_execution_timestamp: str
) -> int:
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"

    first_run = _ensure_table_exists(spark, tgt_layer, pipeline, agg)

    if not first_run:
        _overwrite_changed_partitions(spark, tgt_layer, pipeline, agg, last_execution_timestamp)

    _run_iceberg_maintenance(spark, tgt_layer, full_name, agg.sort_order)

    row_count = _get_row_count_from_metadata(spark, tgt_layer, full_name)
    logging.info(f"Table {full_name} has {row_count:,} total rows.")

    return row_count


# ============================================================
# Entry point
# ============================================================

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_reporting_agg_tables")

    # FIX 5: Raised shuffle.partitions from 8 → 200 and enabled skew join handling.
    # The original value of 8 created enormous shuffle partitions when joining and
    # aggregating billions of rows, making the CTAS write phase very slow.
    # AQE will coalesce small partitions down automatically; the skew join optimizer
    # handles the data imbalance introduced by popular course/org combinations.
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    # Enable dynamic partition overwrite so writeTo().overwritePartitions() only
    # replaces the partitions present in the DataFrame, not the whole table.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    tgt_layer    = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    for agg in AGG_TABLES:
        logging.info(f"--- Starting rebuild: {agg.name} ---")

        # Each agg table tracks its own last execution timestamp so they can
        # drift independently without forcing a full recompute of each other.
        last_execution_timestamp = get_max_timestamp_for_table(
            spark_session=spark, table_name=agg.name, env=ENVIRONMENT
        )
        logging.info(f"Last execution for {agg.name}: {last_execution_timestamp}")

        row_count = _rebuild_agg_table(spark, tgt_layer, tgt_pipeline, agg, last_execution_timestamp)

        update_ctrl_table(
            spark_session     = spark,
            table_name        = agg.name,
            current_timestamp = current_timestamp,
            number_of_records = row_count,
            env               = ENVIRONMENT,
        )
        logging.info(f"--- Finished: {agg.name} ---")

    logging.info("All aggregation tables rebuilt successfully.")


if __name__ == "__main__":
    main()