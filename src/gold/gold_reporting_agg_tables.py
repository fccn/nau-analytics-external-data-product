from dataclasses import dataclass
from typing import Callable
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
from utils.gold_utils_functions import update_ctrl_table
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
    return f"""
        SELECT
            CAST(fc.day_key AS DATE)        AS day_key,
            fc.user_key,
            fc.org_key,
            'certificate'                   AS event_type,
            CAST(NULL AS STRING)            AS course_enrollment_cd,
            dorg.org_cd,
            dorg.short_name                 AS org_short_name,
            dce.display_number              AS course_cd,
            dce.display_name                AS course_name,
            dce.edition,
            du.user_cd
        FROM       {tgt_layer}.entidades.fact_certificate_daily       fc
        LEFT JOIN  {tgt_layer}.entidades.dim_user                     du
               ON  fc.user_key = du.user_key
        LEFT JOIN  {tgt_layer}.entidades.dim_organization             dorg
               ON  fc.org_key  = dorg.org_key
        LEFT JOIN  {tgt_layer}.entidades.dim_course_edition           dce
               ON  fc.course_edition_key = dce.course_edition_key
              AND  fc.org_key            = dce.org_key

        UNION ALL

        SELECT
            CAST(fce.day_key AS DATE)       AS day_key,
            fce.user_key,
            fce.org_key,
            'enrollment'                    AS event_type,
            fce.course_enrollment_cd,
            dorg.org_cd,
            dorg.short_name                 AS org_short_name,
            dce.display_number              AS course_cd,
            dce.display_name                AS course_name,
            dce.edition,
            du.user_cd
        FROM       {tgt_layer}.entidades.fact_course_enrollment_daily fce
        LEFT JOIN  {tgt_layer}.entidades.dim_user                     du
               ON  fce.user_key = du.user_key
        LEFT JOIN  {tgt_layer}.entidades.dim_organization             dorg
               ON  fce.org_key  = dorg.org_key
        LEFT JOIN  {tgt_layer}.entidades.dim_course_edition           dce
               ON  fce.course_edition_key = dce.course_edition_key
              AND  fce.org_key            = dce.org_key
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
# ============================================================

def _rebuild_agg_table(spark, tgt_layer: str, pipeline: str, agg: AggTable) -> int:
    full_name = f"{tgt_layer}.{pipeline}.{agg.name}"

    logging.info(f"Dropping {full_name} (if exists)…")
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")

    logging.info(f"Creating {full_name} (CTAS)…")
    spark.sql(f"""
        CREATE TABLE {full_name}
        USING iceberg
        PARTITIONED BY ({agg.partition_by})
        TBLPROPERTIES (
            'write.parquet.compression-codec'  = 'zstd',
            'write.target-file-size-bytes'     = '536870912',
            'write.distribution-mode'          = 'hash',
            'write.sort.order'                 = '{agg.sort_order}',
            'read.split.target-size'           = '134217728',
            'read.split.open-file-cost'        = '4194304',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'       = '10'
        )
        AS
        {agg.sql_fn(tgt_layer)}
    """)

    row_count = spark.table(full_name).count()
    logging.info(f"Table {full_name} created with {row_count:,} rows.")

    # Iceberg maintenance
    try:
        spark.sql(f"""
            CALL {tgt_layer}.system.rewrite_data_files(
                table       => '{full_name}',
                strategy    => 'sort',
                sort_order  => '{agg.sort_order}',
                options     => map('min-input-files', '2', 'rewrite-all', 'false')
            )
        """)
        spark.sql(f"CALL {tgt_layer}.system.rewrite_manifests(table => '{full_name}')")
        spark.sql(f"CALL {tgt_layer}.system.expire_snapshots(table => '{full_name}', retain_last => 5)")
        logging.info(f"Iceberg maintenance completed for {full_name}.")
    except Exception as e:
        logging.warning(f"Iceberg maintenance skipped for {full_name}: {e}")

    return row_count


# ============================================================
# Entry point
# ============================================================

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_reporting_agg_tables")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    tgt_layer    = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    for agg in AGG_TABLES:
        logging.info(f"--- Starting rebuild: {agg.name} ---")
        row_count = _rebuild_agg_table(spark, tgt_layer, tgt_pipeline, agg)
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
