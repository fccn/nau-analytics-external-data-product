"""
One-off backfill for fccn/nau-technical#981.

silver_certificates_generatedcertificate.py and gold_fact_certificate_d.py
add verify_uuid (silver, gold) and mode (gold) via a guarded, idempotent
ALTER TABLE ADD COLUMNS the first time they run after this change is
deployed. That ALTER only adds the columns -- it does not populate them for
rows that were written before the column existed. This script is a separate,
one-off job that backfills those pre-existing rows:

  1) silver.certificates_generatedcertificate.verify_uuid <- bronze (bronze
     has carried verify_uuid since its own initial CREATE TABLE, so it is
     already fully populated and just needs to be copied over).
  2) gold.fact_certificate_daily.verify_uuid / .mode <- silver (mode was
     already present in silver from day one; verify_uuid comes from step 1
     immediately above, so this step must run after step 1 in the same
     execution).

Both steps are re-runnable: already-backfilled rows are excluded by the
"columns IS NULL" predicate in the MERGE, so running this script twice (or
running it before some rows have a value to backfill from) is safe.

Data-quality note validated live against production data before writing
this script: some certificate ids have multiple rows in bronze/silver
(the source tables are append-only across re-ingestions of the same MySQL
row after a later update), so a naive one-to-one join can fan out a Spark
MERGE ("multiple source rows matched a single target row"). Every id was
confirmed to have at most one DISTINCT non-null verify_uuid value across
all of its rows (verify_uuid is assigned once and never changes), so step
1's dedup (and step 2's verify_uuid half) prefers a non-null value,
tie-broken by latest ingestion_date. mode is NOT guaranteed to have that
same assigned-once property (it can change, e.g. an audit -> verified
upgrade), so step 2 dedups mode separately using the regular pipeline's
own tie-break (greatest(modified_date, created_date) desc, then
ingestion_date desc -- see gold_fact_certificate_d.py's
latest_by_event_window), joining the two independent dedups back
together by id before the MERGE -- see _dedup_by_id_preferring_non_null
and _dedup_by_pipeline_event_order below.

Also note: bronze declares verify_uuid NOT NULL, but ~2,415 rows in dev
use an empty string as a sentinel for failed/incomplete certs rather
than a real value (confirmed live 2026-10-12: 0 actual NULLs, 2,415
empty strings). silver's own SELECT normalizes this to NULL via NULLIF,
so this script -- which reads from silver, not bronze, for step 2 --
only ever sees NULL for that case there; step 1 (which does read
directly from bronze) explicitly excludes '' in its own MERGE predicate
for the same reason.

Run once, manually, right after deploying the updated silver/gold pipeline
scripts (see the certificate-pipeline-columns PR) -- see the dedicated,
schedule=None Airflow DAG (backfill_certificate_verify_uuid_mode_dag.py) in
nau-analytics-airflow-dags for how this gets triggered in each environment.
"""
from pyspark.sql import functions as F  # type:ignore
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)


def _dedup_by_id_preferring_non_null(df, value_col: str):
    """Collapse one row per `id`, preferring a row where value_col is
    populated (falls back to any row if every version has it NULL/empty)."""
    dedup_window = Window.partitionBy("id").orderBy(
        F.when((F.col(value_col).isNotNull()) & (F.col(value_col) != ""), 0).otherwise(1),
        F.col("ingestion_date").desc(),
    )
    return (
        df.withColumn("rn", F.row_number().over(dedup_window))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )


def _dedup_by_pipeline_event_order(df):
    """Collapse one row per `id`, using the exact same tie-break the
    regular pipeline uses to pick the "latest" row for `mode`
    (gold_fact_certificate_d.py's latest_by_event_window):
    greatest(modified_date, created_date) desc, then ingestion_date desc.

    Kept separate from _dedup_by_id_preferring_non_null -- that helper's
    ordering is verify_uuid-oriented (prefer non-null, tie-break by
    ingestion_date) and was never confirmed safe for mode too: mode CAN
    change between an id's re-ingested rows (e.g. an audit -> verified
    upgrade), so reusing verify_uuid's ordering could silently backfill a
    stale mode. This ordering instead guarantees the backfilled mode
    always matches what the regular pipeline would pick for the same id.
    """
    event_window = Window.partitionBy("id").orderBy(
        F.greatest(F.col("modified_date"), F.col("created_date")).desc(),
        F.col("ingestion_date").desc(),
    )
    return (
        df.withColumn("rn", F.row_number().over(event_window))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )


def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("backfill_certificate_verify_uuid_mode")

    bronze_tbl = f"bronze{ENVIRONMENT}.entidades.certificates_generatedcertificate"
    silver_tbl = f"silver{ENVIRONMENT}.entidades.certificates_generatedcertificate"
    gold_tbl = f"gold{ENVIRONMENT}.entidades.fact_certificate_daily"

    # ---------------------------------------------------------
    # Guard: these columns must already exist (added by the regular
    # silver/gold scripts' own ALTER TABLE guard) before this script can
    # do anything useful. Fail loudly instead of silently no-op'ing.
    # ---------------------------------------------------------
    silver_columns = {field.name for field in spark.table(silver_tbl).schema.fields}
    gold_columns = {field.name for field in spark.table(gold_tbl).schema.fields}
    missing = []
    if "verify_uuid" not in silver_columns:
        missing.append(f"{silver_tbl}.verify_uuid")
    if "verify_uuid" not in gold_columns:
        missing.append(f"{gold_tbl}.verify_uuid")
    if "mode" not in gold_columns:
        missing.append(f"{gold_tbl}.mode")
    if missing:
        raise RuntimeError(
            "Missing column(s) that this backfill depends on: "
            f"{', '.join(missing)}. Run the updated silver/gold pipeline "
            "scripts at least once first (their own ALTER TABLE guard adds "
            "these columns) before running this backfill."
        )

    # ---------------------------------------------------------
    # Step 1: silver.verify_uuid <- bronze.verify_uuid
    # ---------------------------------------------------------
    before_silver = spark.sql(
        f"SELECT count(*) AS c FROM {silver_tbl} WHERE verify_uuid IS NULL"
    ).first()["c"]
    logging.info(f"[silver] rows with NULL verify_uuid before backfill: {before_silver}")

    bronze_dedup = _dedup_by_id_preferring_non_null(
        spark.table(bronze_tbl).select("id", "verify_uuid", "ingestion_date"),
        "verify_uuid",
    )
    bronze_dedup.createOrReplaceTempView("bronze_certs_dedup")

    spark.sql(f"""
        MERGE INTO {silver_tbl} AS t
        USING bronze_certs_dedup AS s
        ON t.id = s.id
        WHEN MATCHED AND t.verify_uuid IS NULL
                     AND s.verify_uuid IS NOT NULL
                     AND s.verify_uuid <> ''
        THEN UPDATE SET t.verify_uuid = s.verify_uuid
    """)

    after_silver = spark.sql(
        f"SELECT count(*) AS c FROM {silver_tbl} WHERE verify_uuid IS NULL"
    ).first()["c"]
    logging.info(f"[silver] rows with NULL verify_uuid after backfill: {after_silver}")

    # ---------------------------------------------------------
    # Step 2: gold.verify_uuid / gold.mode <- silver (post step-1 state)
    # ---------------------------------------------------------
    before_gold = spark.sql(
        f"SELECT count(*) AS c FROM {gold_tbl} WHERE verify_uuid IS NULL OR mode IS NULL"
    ).first()["c"]
    logging.info(f"[gold] rows with NULL verify_uuid/mode before backfill: {before_gold}")

    # verify_uuid and mode are deliberately deduped independently, each
    # picking whichever physical silver row is correct for *that*
    # column, then joined back together on id -- see the two helpers'
    # docstrings for why a single shared ordering isn't safe for both.
    verify_uuid_dedup = _dedup_by_id_preferring_non_null(
        spark.table(silver_tbl).select("id", "verify_uuid", "ingestion_date"),
        "verify_uuid",
    )
    mode_dedup = _dedup_by_pipeline_event_order(
        spark.table(silver_tbl).select("id", "mode", "created_date", "modified_date", "ingestion_date")
    )
    silver_dedup = verify_uuid_dedup.select("id", "verify_uuid").join(
        mode_dedup.select("id", "mode"), on="id", how="inner"
    )
    silver_dedup.createOrReplaceTempView("silver_certs_dedup")

    spark.sql(f"""
        MERGE INTO {gold_tbl} AS t
        USING silver_certs_dedup AS s
        ON t.certificate_cd = CAST(s.id AS STRING)
        WHEN MATCHED AND (t.verify_uuid IS NULL OR t.mode IS NULL)
        THEN UPDATE SET
            t.verify_uuid = s.verify_uuid,
            t.mode        = s.mode
    """)

    after_gold = spark.sql(
        f"SELECT count(*) AS c FROM {gold_tbl} WHERE verify_uuid IS NULL OR mode IS NULL"
    ).first()["c"]
    logging.info(f"[gold] rows with NULL verify_uuid/mode after backfill: {after_gold}")

    logging.info("Backfill complete.")


if __name__ == "__main__":
    main()
