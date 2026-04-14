import os
from dataclasses import dataclass
from typing import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    # FIX 3: target output partitions per table — controls file count written to S3.
    # Lower values = fewer, larger files = faster S3 writes.
    # Tune per table based on expected output size.
    output_partitions: int = 50
    # Set to True for tables without a day_key grain that require a full
    # replace on every run instead of partition-level incremental overwrite.
    full_refresh: bool = False


# ────────────────────────────────────────────────────────────
# Superset Dataset: Formandos Inscritos
# Pre-aggregated to dimension grain.
# FIX 3: Removed du.year_of_birth from GROUP BY — was creating one row per
# birth year (80+ values) instead of one per age_range bucket (5 values),
# inflating 102M rows unnecessarily. age_range is already derived from
# year_of_birth so the information is preserved.
# FIX 5 (NEW): The age_range CASE expression that references
# du.year_of_birth must appear in the GROUP BY — Spark does not allow
# non-aggregated columns even via a derived expression.  We use a CTE to
# compute the scalar columns first, then GROUP BY the alias.
# Superset metrics: SUM(unique_key_count), SUM(user_count), SUM(enrollment_count)
# ────────────────────────────────────────────────────────────
def _fact_enrolled_students_agg_sql(tgt_layer: str) -> str:
    return f"""
        WITH base AS (
            SELECT
                fce.day_key,
                dorg.org_cd,
                dorg.short_name                                             AS org_short_name,
                dce.display_number                                          AS course_cd,
                dce.display_name                                            AS course_name,
                dce.edition,
                CASE
                    WHEN du.gender = 'm' THEN 'Male'
                    WHEN du.gender = 'f' THEN 'Female'
                    WHEN du.gender = 'o' THEN 'Other'
                    ELSE 'N/A'
                END                                                         AS gender,
                du.level_of_education,
                du.country,
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
                CASE WHEN du.employment_situation IS NULL THEN 'N/A'
                     ELSE du.employment_situation
                END                                                         AS employment_situation,
                fce.is_enrolled,
                CONCAT(
                    CAST(dt.year AS STRING),
                    lpad(CAST(dt.month AS STRING), 2, '0'),
                    ' - ',
                    dt.month_name
                )                                                           AS month_name,
                fce.course_enrollment_cd,
                du.user_cd
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
        )
        SELECT
            day_key,
            org_cd,
            org_short_name,
            course_cd,
            course_name,
            edition,
            gender,
            level_of_education,
            country,
            age_range,
            escolaridade,
            employment_situation,
            is_enrolled,
            month_name,
            COUNT(DISTINCT course_enrollment_cd)                    AS enrollment_count,
            COUNT(DISTINCT user_cd)                                 AS user_count,
            COUNT(DISTINCT CONCAT(
                CAST(course_enrollment_cd AS STRING),
                CAST(user_cd AS STRING)
            ))                                                      AS unique_key_count
        FROM base
        GROUP BY
            day_key,
            org_cd,
            org_short_name,
            course_cd,
            course_name,
            edition,
            gender,
            level_of_education,
            country,
            age_range,
            escolaridade,
            employment_situation,
            is_enrolled,
            month_name
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Taxa Conclusão Final
# FIX 6 (NEW): The original query was a raw UNION ALL with NO aggregation,
# producing the full row count of fact_certificate_daily +
# fact_course_enrollment_daily (potentially hundreds of millions of rows).
# Combined with repartition(10), this concentrated massive data per
# partition, causing repeated executor OOM kills and cascading shuffle
# fetch failures — the query never completed in 4+ hours.
#
# The Superset metric is SUM(user_count), so the data can be pre-aggregated
# to (day_key, org, course, edition, event_type) grain.  This reduces
# output from hundreds of millions of rows to a few thousand, eliminating
# the OOM entirely.
# ────────────────────────────────────────────────────────────
def _fact_conclusion_rate_agg_sql(tgt_layer: str) -> str:
    return f"""
        WITH enrollment_agg AS (
            SELECT
                course_edition_key,
                org_key,
                COUNT(DISTINCT user_key)                                        AS total_enrolled,
                COUNT(DISTINCT CASE WHEN NOT is_enrolled THEN user_key END)     AS total_unenrolled,
                COUNT(DISTINCT CASE WHEN is_enrolled     THEN user_key END)     AS net_enrolled
            FROM {tgt_layer}.entidades.fact_course_enrollment_daily
            GROUP BY course_edition_key, org_key
        ),

        certificate_agg AS (
            SELECT
                course_edition_key,
                org_key,
                COUNT(DISTINCT user_key) AS total_certificates
            FROM {tgt_layer}.entidades.fact_certificate_daily
            GROUP BY course_edition_key, org_key
        )

        SELECT
            do.org_cd,
            do.short_name                                                       AS org_short_name,
            dce.display_number                                                  AS course_cd,
            dce.display_name                                                    AS course_name,
            dce.edition,
            dce.start_date,
            dce.end_date,
            dce.end_date < current_timestamp()                                  AS course_ended,
            ea.total_enrolled,
            ea.total_unenrolled,
            ea.net_enrolled,
            coalesce(ca.total_certificates, 0)                                  AS total_certificates,
            round(
                coalesce(ca.total_certificates, 0) * 100.0 / nullif(ea.net_enrolled, 0),
            2)                                                                  AS conclusion_rate_pct
        FROM enrollment_agg ea
        LEFT JOIN certificate_agg ca
            ON  ea.course_edition_key = ca.course_edition_key
            AND ea.org_key             = ca.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON  ea.course_edition_key = dce.course_edition_key
            AND dce.key_end_date IS NULL
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON ea.org_key = do.org_key
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Tickets vs Cursos
# Unchanged — already lean, driven by low-volume course_edition_daily.
# ────────────────────────────────────────────────────────────
def _tickets_vs_courses_agg_sql(tgt_layer: str) -> str:
    return f"""
        SELECT
            fce.day_key,
            do.org_cd,
            do.name                                                         AS org_name,
            do.short_name                                                   AS org_short_name,
            fce.course_cd,
            dce.display_name                                                AS course_name,
            dce.edition,
            CASE
                WHEN dce.start_date = MAX(dce.start_date) OVER (PARTITION BY fce.course_cd)
                THEN 1
                ELSE 0
            END                                                             AS is_latest_edition,
            CASE
                WHEN dce.start_date = MAX(dce.start_date) OVER (PARTITION BY fce.course_cd)
                THEN 'Novos Cursos'
                ELSE 'Reedições'
            END                                                             AS edition_type,
            t.ticket_type_origin,
            t.ticket_key
        FROM {tgt_layer}.entidades.fact_course_edition_daily fce
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON fce.org_key = do.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON fce.course_edition_key = dce.course_edition_key
           AND fce.org_key = dce.org_key
        LEFT JOIN (
            SELECT
                DATE(created)       AS ticket_date,
                ticket_type_origin,
                key                 AS ticket_key
            FROM {tgt_layer}.gestao.jira_tickets
        ) t ON fce.day_key = t.ticket_date
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Student Performance
# Pre-aggregated to (day_key, org, course, edition, letter_grade) grain.
# Superset metrics: SUM(user_count), SUM(passed_count)
# ────────────────────────────────────────────────────────────
def _student_performance_agg_sql(tgt_layer: str) -> str:
    return f"""
        SELECT
            fce.day_key,
            do.org_cd,
            do.short_name                   AS org_short_name,
            dce.display_number              AS course_cd,
            dce.display_name                AS course_name,
            dce.edition,
            fsg.letter_grade,
            COUNT(DISTINCT du.user_cd)      AS user_count,
            COUNT(CASE WHEN fsg.passed_timestamp IS NOT NULL
                       THEN 1 END)          AS passed_count
        FROM {tgt_layer}.entidades.fact_course_enrollment_daily fce
        LEFT JOIN {tgt_layer}.entidades.dim_user du
            ON fce.user_key = du.user_key
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON fce.org_key = do.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON fce.course_edition_key = dce.course_edition_key
           AND fce.org_key = dce.org_key
        LEFT JOIN {tgt_layer}.entidades.fact_student_grades fsg
            ON fce.user_key = fsg.user_key
           AND fce.course_edition_key = fsg.course_edition_key
        GROUP BY
            fce.day_key,
            do.org_cd,
            do.short_name,
            dce.display_number,
            dce.display_name,
            dce.edition,
            fsg.letter_grade
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Certificates
# Pre-aggregated to (day_key, org, course, edition, month) grain.
# Superset metrics: SUM(certificate_count)
# ────────────────────────────────────────────────────────────
def _certificates_agg_sql(tgt_layer: str) -> str:
    return f"""
        SELECT
            fc.day_key,
            do.org_cd,
            do.short_name                               AS org_short_name,
            dce.display_number                          AS course_cd,
            dce.display_name                            AS course_name,
            dce.edition,
            CONCAT(
                CAST(dt.year AS STRING),
                LPAD(CAST(dt.month AS STRING), 2, '0'),
                ' - ',
                CAST(dt.month_name AS STRING)
            )                                           AS month_name,
            COUNT(DISTINCT fc.certificate_cd)           AS certificate_count
        FROM {tgt_layer}.entidades.fact_certificate_daily fc
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON fc.org_key = do.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON fc.course_edition_key = dce.course_edition_key
           AND fc.org_key = dce.org_key
        JOIN {tgt_layer}.entidades.dim_time dt
            ON fc.day_key = dt.date
        GROUP BY
            fc.day_key,
            do.org_cd,
            do.short_name,
            dce.display_number,
            dce.display_name,
            dce.edition,
            dt.year,
            dt.month,
            dt.month_name
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Inscrições vs Certificados
# Pre-aggregated with SUM(total_days_to_conclusion) so Superset can compute
# AVG as SUM(total_days_to_conclusion) / SUM(certificate_count).
# ────────────────────────────────────────────────────────────
def _enrollments_vs_certificates_agg_sql(tgt_layer: str) -> str:
    return f"""
        SELECT
            fce.day_key,
            do.org_cd,
            do.short_name                                               AS org_short_name,
            dce.display_number                                          AS course_cd,
            dce.display_name                                            AS course_name,
            dce.edition,
            CONCAT(
                CAST(dt.year AS STRING),
                ' - ',
                CAST(dt.month_name AS STRING)
            )                                                           AS month_name,
            CASE
                WHEN fc.certificate_cd IS NOT NULL THEN 'concluded'
                ELSE 'enrolled'
            END                                                         AS status,
            COUNT(DISTINCT fce.course_enrollment_cd)                    AS enrollment_count,
            COUNT(DISTINCT fc.certificate_cd)                           AS certificate_count,
            SUM(CASE
                WHEN fc.certificate_cd IS NOT NULL
                THEN date_diff(DAY, fce.course_enrollment_start_date, fc.certificate_issue_date)
                ELSE 0
            END)                                                        AS total_days_to_conclusion
        FROM {tgt_layer}.entidades.fact_course_enrollment_daily fce
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON fce.org_key = do.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON fce.course_edition_key = dce.course_edition_key
           AND fce.org_key = dce.org_key
        JOIN {tgt_layer}.entidades.dim_time dt
            ON fce.day_key = dt.date
        LEFT JOIN {tgt_layer}.entidades.fact_certificate_daily fc
            ON fce.day_key = fc.day_key
           AND fce.course_edition_key = fc.course_edition_key
           AND fce.user_key = fc.user_key
           AND fce.org_key = fc.org_key
        GROUP BY
            fce.day_key,
            do.org_cd,
            do.short_name,
            dce.display_number,
            dce.display_name,
            dce.edition,
            dt.year,
            dt.month_name,
            fc.certificate_cd
    """


# ────────────────────────────────────────────────────────────
# Superset Dataset: Certificados por Inscritos (para média)
# Kept at user grain — per-user ratios cannot be reconstructed from aggregates.
# ────────────────────────────────────────────────────────────
def _certificados_por_inscritos_agg_sql(tgt_layer: str) -> str:
    return f"""
        SELECT
            fc.day_key,
            do.org_cd,
            do.short_name                   AS org_short_name,
            dce.display_number              AS course_cd,
            dce.edition,
            du.user_cd,
            fc.certificate_cd,
            CONCAT(
                CAST(fce.course_enrollment_cd AS STRING),
                CAST(du.user_cd AS STRING)
            )                               AS unique_key
        FROM {tgt_layer}.entidades.fact_certificate_daily fc
        LEFT JOIN {tgt_layer}.entidades.dim_user du
            ON fc.user_key = du.user_key
        LEFT JOIN {tgt_layer}.entidades.dim_organization do
            ON fc.org_key = do.org_key
        LEFT JOIN {tgt_layer}.entidades.dim_course_edition dce
            ON fc.course_edition_key = dce.course_edition_key
           AND fc.org_key = dce.org_key
        LEFT JOIN {tgt_layer}.entidades.fact_course_enrollment_daily fce
            ON fc.user_key = fce.user_key
           AND fc.course_edition_key = fce.course_edition_key
           AND fc.org_key = fce.org_key
    """


# ────────────────────────────────────────────────────────────
# Registry
# output_partitions tuned per table based on expected output size:
#   - fact_enrolled_students_agg: large after agg → 30 partitions
#   - fact_conclusion_rate_agg:   now aggregated, small → 10 partitions
#   - tickets_vs_courses_agg:     small → 5 partitions
#   - student_performance_agg:    medium → 20 partitions
#   - certificates_agg:           small → 10 partitions
#   - enrollments_vs_certificates_agg: medium → 20 partitions
#   - certificados_por_inscritos_agg:  user grain, larger → 20 partitions
# ────────────────────────────────────────────────────────────
AGG_TABLES: list[AggTable] = [
    AggTable(
        name               = "fact_enrolled_students_agg",
        sql_fn             = _fact_enrolled_students_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions  = 30,
    ),
    AggTable(
        name               = "fact_conclusion_rate_agg",
        sql_fn             = _fact_conclusion_rate_agg_sql,
        partition_by       = "org_cd",
        sort_order         = "org_cd ASC, course_cd ASC, edition ASC",
        output_partitions  = 10,
        full_refresh       = True,
    ),
    AggTable(
        name               = "tickets_vs_courses_agg",
        sql_fn             = _tickets_vs_courses_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, course_cd ASC, ticket_type_origin ASC",
        output_partitions  = 5,
    ),
    AggTable(
        name               = "student_performance_agg",
        sql_fn             = _student_performance_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions  = 20,
    ),
    AggTable(
        name               = "certificates_agg",
        sql_fn             = _certificates_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions  = 10,
    ),
    AggTable(
        name               = "enrollments_vs_certificates_agg",
        sql_fn             = _enrollments_vs_certificates_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions  = 20,
    ),
    AggTable(
        name               = "certificados_por_inscritos_agg",
        sql_fn             = _certificados_por_inscritos_agg_sql,
        partition_by       = "days(day_key)",
        sort_order         = "day_key ASC, org_cd ASC, course_cd ASC",
        output_partitions  = 20,
    ),
]


# ============================================================
# Core rebuild helpers
# ============================================================

def _ensure_table_exists(spark, tgt_layer: str, pipeline: str, agg: AggTable) -> bool:
    """
    Create the table via CTAS if it doesn't already exist.
    FIX 1+2: Uses repartition(output_partitions) before writing to control
    the number of output files written to S3, avoiding the 2,675-file problem
    caused by shuffle partitions × partition keys.
    Returns True if the table was just created (first run).
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {tgt_layer}.{pipeline}").collect()]
    if agg.name in tables:
        return False

    logging.info(f"First run: creating {full_name} (output_partitions={agg.output_partitions})…")

    # Create empty table first with correct schema and properties
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name}
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
        AS SELECT * FROM ({agg.sql_fn(tgt_layer)}) _empty WHERE 1=0
    """)

    # Write data with controlled partition count to limit S3 file count
    (
        spark.sql(agg.sql_fn(tgt_layer))
             .repartition(agg.output_partitions)
             .writeTo(full_name)
             .option("overwrite-mode", "dynamic")
             .overwritePartitions()
    )

    logging.info(f"Table {full_name} created (first run).")
    return True


def _overwrite_changed_partitions(
    spark, tgt_layer: str, pipeline: str, agg: AggTable, last_execution_timestamp: str
) -> None:
    """
    Recompute and overwrite only the day_key partitions that changed
    since last_execution_timestamp.
    FIX 1+2: Uses repartition(output_partitions) to control file count.
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"

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

    logging.info(
        f"Overwriting {changed_day_count} changed day_key partition(s) in {full_name} "
        f"(output_partitions={agg.output_partitions})…"
    )

    incremental_sql = f"""
        SELECT agg.*
        FROM (
            {agg.sql_fn(tgt_layer)}
        ) agg
        INNER JOIN _changed_days cd ON agg.day_key = cd.day_key
    """

    (
        spark.sql(incremental_sql)
             .repartition(agg.output_partitions)
             .writeTo(full_name)
             .option("overwrite-mode", "dynamic")
             .overwritePartitions()
    )

    logging.info(f"Partition overwrite complete for {full_name}.")


def _run_iceberg_maintenance(spark, tgt_layer: str, full_name: str, sort_order: str) -> None:
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
    try:
        row_count = int(spark.sql(f"""
            SELECT summary['total-records']
            FROM {tgt_layer}.entidades.snapshots
            WHERE table_name = '{full_name}'
            ORDER BY committed_at DESC
            LIMIT 1
        """).first()[0])
    except Exception:
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


def _full_refresh_table(spark, tgt_layer: str, pipeline: str, agg: AggTable) -> None:
    """
    Full replace for tables without a day_key grain (full_refresh=True).
    Truncates then rewrites all data in a single pass.
    """
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"
    logging.info(f"Full refresh: truncating and rewriting {full_name}…")
    spark.sql(f"TRUNCATE TABLE {full_name}")
    (
        spark.sql(agg.sql_fn(tgt_layer))
             .repartition(agg.output_partitions)
             .writeTo(full_name)
             .append()
    )
    logging.info(f"Full refresh complete for {full_name}.")


def _rebuild_agg_table(
    spark, tgt_layer: str, pipeline: str, agg: AggTable, last_execution_timestamp: str
) -> int:
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"

    first_run = _ensure_table_exists(spark, tgt_layer, pipeline, agg)

    if not first_run:
        if agg.full_refresh:
            _full_refresh_table(spark, tgt_layer, pipeline, agg)
        else:
            _overwrite_changed_partitions(spark, tgt_layer, pipeline, agg, last_execution_timestamp)

    # FIX 7 (NEW): Skip Iceberg maintenance on first run.
    # On a freshly written table the files are already optimally sized from
    # repartition(), so compaction is wasted work — it adds 2-5 min per table
    # and competes with still-running writes from other parallel threads.
    if not first_run:
        _run_iceberg_maintenance(spark, tgt_layer, full_name, agg.sort_order)
    else:
        logging.info(
            f"Skipping Iceberg maintenance for {full_name} (first run — files already optimal)."
        )

    row_count = _get_row_count_from_metadata(spark, tgt_layer, full_name)
    logging.info(f"Table {full_name} has {row_count:,} total rows.")

    return row_count


# ============================================================
# Entry point
# ============================================================

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_reporting_agg_tables")

    # FIX 1: Reduced shuffle.partitions from 200 → 50.
    # 200 was creating too many output files (2,675 per table) causing slow
    # S3 writes. AQE will coalesce small partitions automatically.
    # The repartition() calls in _ensure_table_exists and
    # _overwrite_changed_partitions further control the final file count.
    spark.conf.set("spark.sql.shuffle.partitions", "50")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # FIX 1: Tune AQE coalescing to target ~512MB files.
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "536870912")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "134217728")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    tgt_layer    = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"

    tables_to_run_env = os.environ.get("TABLES_TO_RUN", "").strip()
    if tables_to_run_env:
        requested = {t.strip() for t in tables_to_run_env.split(",")}
        valid_names = {agg.name for agg in AGG_TABLES}
        unknown = requested - valid_names
        if unknown:
            raise ValueError(
                f"Unknown table(s) in TABLES_TO_RUN: {unknown}. "
                f"Valid options are: {valid_names}"
            )
        tables = [agg for agg in AGG_TABLES if agg.name in requested]
        logging.info(f"TABLES_TO_RUN is set — processing {len(tables)} table(s): {requested}")
    else:
        tables = AGG_TABLES
        logging.info(f"TABLES_TO_RUN not set — processing all {len(tables)} table(s).")

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    # FIX 4 (UPDATED): Default max_workers lowered from 3 → 1.
    # Running heavy tables in parallel on a shared Spark session causes
    # executor OOM kills and cascading shuffle fetch failures when multiple
    # large shuffles compete for the same executor memory.  Sequential
    # execution (max_workers=1) is safer for first-run / backfill.
    # For incremental runs where only a few partitions change per table,
    # set AGG_MAX_WORKERS=3 via env var to re-enable parallelism.
    max_workers = int(os.environ.get("AGG_MAX_WORKERS", "1"))
    logging.info(f"Running {len(tables)} table(s) with max_workers={max_workers}.")

    results: dict[str, int] = {}
    errors: dict[str, Exception] = {}

    def _run(agg: AggTable) -> tuple[str, int]:
        last_execution_timestamp = get_max_timestamp_for_table(
            spark_session=spark, table_name=agg.name, env=ENVIRONMENT
        )
        logging.info(f"--- Starting rebuild: {agg.name} (last_exec={last_execution_timestamp}) ---")
        row_count = _rebuild_agg_table(spark, tgt_layer, tgt_pipeline, agg, last_execution_timestamp)
        logging.info(f"--- Finished: {agg.name} ---")
        return agg.name, row_count

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_agg = {executor.submit(_run, agg): agg for agg in tables}
        for future in as_completed(future_to_agg):
            agg = future_to_agg[future]
            try:
                name, row_count = future.result()
                results[name] = row_count
            except Exception as exc:
                logging.error(f"Table {agg.name} failed: {exc}", exc_info=True)
                errors[agg.name] = exc

    # Update control table sequentially after all tables complete
    for agg in tables:
        if agg.name in results:
            update_ctrl_table(
                spark_session     = spark,
                table_name        = agg.name,
                current_timestamp = current_timestamp,
                number_of_records = results[agg.name],
                env               = ENVIRONMENT,
            )

    if errors:
        failed = list(errors.keys())
        raise RuntimeError(
            f"{len(failed)} table(s) failed: {failed}. "
            f"Check logs above for details."
        )

    logging.info("All aggregation tables rebuilt successfully.")


if __name__ == "__main__":
    main()
