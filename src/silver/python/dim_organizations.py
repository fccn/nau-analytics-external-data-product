# dim_organizations_scd2.py

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# Constantes de paths (bronze / silver)
# =============================================================================

BRONZE_BUCKET = os.getenv("S3_BUCKET_BRONZE", "s3a://nau-local-analytics-bronze")
SILVER_BUCKET = os.getenv("S3_BUCKET_SILVER", "s3a://nau-local-analytics-silver")

BRONZE_PATH_ORG_CURRENT = f"{BRONZE_BUCKET.rstrip('/')}/organizations_organization"
BRONZE_PATH_ORG_HIST    = f"{BRONZE_BUCKET.rstrip('/')}/organizations_organizationhistory"

SILVER_PATH_DIM_ORG     = f"{SILVER_BUCKET.rstrip('/')}/dim_organizations"
TARGET_TABLE_DIM_ORG    = "default.dim_organizations"


# =============================================================================
# Criação da SparkSession
# =============================================================================

def get_spark() -> SparkSession:
    """
    Retorna uma SparkSession. Assume que a stack já foi iniciada
    com as configs de Delta via spark-submit / cluster.
    """
    spark = (
        SparkSession.builder
        .appName("NAU - Dim_Organizations SCD2")
        .getOrCreate()
    )
    return spark


# =============================================================================
# 1. Leitura e normalização da Bronze
# =============================================================================

def read_org_current(spark: SparkSession) -> DataFrame:
    """
    Lê a tabela organizations_organization da camada Bronze
    e faz a normalização básica dos campos.
    """
    df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH_ORG_CURRENT)
    )

    df_clean = (
        df.select(
            F.col("id").cast("int").alias("organization_id"),
            F.trim(F.col("short_name")).alias("organization_code"),
            F.trim(F.col("name")).alias("organization_name"),
            F.when(F.trim(F.col("description")) == "", None)
             .otherwise(F.trim(F.col("description"))).alias("description"),
            F.trim(F.col("logo")).alias("logo"),
            F.col("modified").cast("timestamp").alias("modified_at"),
            F.col("ingestion_date").cast("timestamp").alias("ingestion_date"),
            F.col("source_name").alias("source_name"),
        )
    )

    return df_clean


def read_org_history(spark: SparkSession) -> DataFrame:
    """
    Lê a tabela organizations_organizationhistory da Bronze
    e normaliza os campos de histórico.
    """
    df_hist = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH_ORG_HIST)
    )

    df_hist_clean = (
        df_hist.select(
            F.col("id").cast("int").alias("organization_id"),
            F.trim(F.col("short_name")).alias("organization_code"),
            F.trim(F.col("name")).alias("organization_name"),
            F.when(F.trim(F.col("description")) == "", None)
             .otherwise(F.trim(F.col("description"))).alias("description"),
            F.trim(F.col("logo")).alias("logo"),
            F.col("modified").cast("timestamp").alias("modified_at"),
            F.col("history_date").cast("timestamp").alias("history_date"),
            F.col("history_type").alias("history_type"),
            F.col("history_user_id").cast("int").alias("history_user_id"),
            F.col("ingestion_date").cast("timestamp").alias("ingestion_date"),
            F.col("source_name").alias("source_name"),
        )
    )

    return df_hist_clean


# =============================================================================
# 2. Construção dos eventos SCD2 (histórico + sintético)
# =============================================================================

def build_all_events(
    df_org_current: DataFrame,
    df_org_hist: DataFrame
) -> DataFrame:
    """
    Constrói um dataframe com todos os eventos de alteração da organização:

    - Eventos históricos vindos de organizations_organizationhistory.
    - Eventos “sintéticos” para organizações sem histórico (pelo menos 1 linha).
    """

    # IDs que já aparecem na tabela de histórico
    hist_ids = df_org_hist.select("organization_id").distinct()

    # Organizações sem qualquer registo de histórico
    df_orgs_sem_hist = (
        df_org_current
        .join(hist_ids, on="organization_id", how="left_anti")
    )

    # Para estas, criamos um evento sintético (history_type '+')
    df_extra_events_sem_hist = (
        df_orgs_sem_hist.select(
            F.col("organization_id"),
            F.col("organization_code"),
            F.col("organization_name"),
            F.col("description"),
            F.col("logo"),
            F.col("modified_at"),
            # Consideramos o modified_at como history_date para o evento sintético
            F.col("modified_at").alias("history_date"),
            F.lit("+").alias("history_type"),
            F.lit(None).cast("int").alias("history_user_id"),
            F.col("ingestion_date"),
            F.col("source_name"),
        )
    )

    # União de histórico real + sintético
    df_all_events = df_org_hist.unionByName(df_extra_events_sem_hist)

    return df_all_events


# =============================================================================
# 3. Aplicar lógica SCD2 (effective_start, effective_end, is_current)
# =============================================================================

def build_scd2_dataframe(df_all_events: DataFrame) -> DataFrame:
    """
    Ordena os eventos por organização e data, define os intervalos
    de vigência (effective_start, effective_end) e marca is_current.
    Também calcula o record_hash e o surrogate key (organization_sk).
    """

    w = Window.partitionBy("organization_id").orderBy(
        F.col("history_date").asc(),
        F.col("modified_at").asc()
    )

    df_with_lag_lead = (
        df_all_events
        .withColumn("prev_modified_at", F.lag("modified_at").over(w))
        .withColumn("next_org_id", F.lead("organization_id").over(w))
    )

    start_default = F.to_timestamp(F.lit("1900-01-01 00:00:00"))

    df_scd2 = (
        df_with_lag_lead
        # effective_start
        .withColumn(
            "effective_start",
            F.when(F.col("history_type") == F.lit("+"), start_default)
             .otherwise(F.expr("prev_modified_at + INTERVAL 1 SECOND"))
        )
        # effective_end: NULL para o último registo (versão atual)
        .withColumn(
            "effective_end",
            F.when(F.col("next_org_id").isNull(), F.lit(None).cast("timestamp"))
             .otherwise(F.col("modified_at"))
        )
        .drop("prev_modified_at", "next_org_id")
    )

    # record_hash baseado nos campos de negócio principais
    business_cols = [
        "organization_id",
        "organization_code",
        "organization_name",
        "description",
        "logo",
    ]

    df_scd2_hashed = (
        df_scd2.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                        for c in business_cols
                    ],
                ),
                256,
            ),
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn(
            "is_current",
            F.col("effective_end").isNull()
        )
    )

    # Surrogate key: ordem dentro de cada organização pelo effective_start
    w_sk = Window.partitionBy("organization_id").orderBy(
        F.col("effective_start").asc(),
        F.col("history_date").asc(),
        F.col("modified_at").asc()
    )

    df_dim_organizations = (
        df_scd2_hashed
        .withColumn("organization_sk", F.row_number().over(w_sk).cast("long"))
        .select(
            "organization_sk",
            "organization_id",
            "organization_code",
            "organization_name",
            "description",
            "logo",
            "modified_at",
            "history_date",
            "history_type",
            "history_user_id",
            "effective_start",
            "effective_end",
            "is_current",
            "record_hash",
            "ingestion_date",
            "source_name",
            "ingestion_timestamp",
        )
    )

    return df_dim_organizations


# =============================================================================
# 4. Escrita na Silver (full refresh / overwrite)
# =============================================================================

def write_dim_organizations(
    spark: SparkSession,
    df_dim_org: DataFrame
) -> None:
    """
    Escreve a dimensão na Silver em formato Delta, em modo overwrite
    (full refresh) e garante a criação/atualização da tabela Hive.
    """

    (
        df_dim_org.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH_DIM_ORG)
    )

    # Regista/atualiza a tabela no metastore com o mesmo schema
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE_DIM_ORG}
        USING DELTA
        LOCATION '{SILVER_PATH_DIM_ORG}'
    """)

    # Caso já exista, o CREATE TABLE IF NOT EXISTS não altera o schema,
    # mas como gravámos com overwrite + overwriteSchema, o Delta no path
    # já está consistente. Se quiseres forçar REPLACE TABLE, podes adaptar.


# =============================================================================
# 5. main()
# =============================================================================

def main() -> None:
    spark = get_spark()

    df_org_current = read_org_current(spark)
    df_org_hist    = read_org_history(spark)

    df_all_events  = build_all_events(df_org_current, df_org_hist)
    df_dim_org     = build_scd2_dataframe(df_all_events)

    write_dim_organizations(spark, df_dim_org)


if __name__ == "__main__":
    main()
