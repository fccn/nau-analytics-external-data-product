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


def _fact_enrollment_daily_agg_sql(tgt_layer: str) -> str:
    return f"""
        SELECT
            fce.day_key,
            dorg.org_cd,
            dorg.short_name                                         AS org_short_name,
            dce.display_number                                      AS course_cd,
            dce.display_name                                        AS course_name,
            dce.edition,
            du.country,
            du.level_of_education,
            du.employment_situation,
            du.gender,
            CASE
                WHEN du.year_of_birth IS NULL                                             THEN 'N/A'
                WHEN year(fce.day_key) - du.year_of_birth < 18                           THEN 'Menor 18'
                WHEN year(fce.day_key) - du.year_of_birth BETWEEN 18 AND 29              THEN '18-29'
                WHEN year(fce.day_key) - du.year_of_birth BETWEEN 30 AND 54              THEN '30-54'
                ELSE '55+'
            END                                                     AS age_range,
            fce.is_enrolled,
            COUNT(DISTINCT fce.course_enrollment_cd)                AS enrollment_count,
            COUNT(DISTINCT du.user_cd)                              AS user_count,
            COUNT(DISTINCT
                CAST(fce.course_enrollment_cd AS STRING)
                || CAST(du.user_cd AS STRING)
            )                                                       AS unique_key_count
        FROM       {tgt_layer}.entidades.fact_course_enrollment_daily  fce
        LEFT JOIN  {tgt_layer}.entidades.dim_user                      du
               ON  fce.user_key = du.user_key
        LEFT JOIN  {tgt_layer}.entidades.dim_organization              dorg
               ON  fce.org_key  = dorg.org_key
        LEFT JOIN  {tgt_layer}.entidades.dim_course_edition            dce
               ON  fce.course_edition_key = dce.course_edition_key
              AND  fce.org_key            = dce.org_key
        GROUP BY
            fce.day_key, dorg.org_cd, dorg.short_name,
            dce.display_number, dce.display_name, dce.edition,
            du.country, du.level_of_education, du.employment_situation,
            du.gender, age_range, fce.is_enrolled
    """


# Registry — add new aggregation tables here
AGG_TABLES: list[AggTable] = [
    AggTable(
        name         = "fact_enrollment_daily_agg",
        sql_fn       = _fact_enrollment_daily_agg_sql,
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
