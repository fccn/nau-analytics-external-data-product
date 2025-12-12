# ===============================================================
#  Dim External Course - Silver ETL
#  Produção | Sistema NAU | FCCN
#  Estrutura com boas práticas para pipelines Spark/Delta
# ===============================================================

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

# ===============================================================
# Logging
# ===============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("dim_external_course_etl")

# ===============================================================
# 1. Configuração do Spark
# ===============================================================

def start_spark():
    from pyspark.sql import SparkSession
    import os

    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "https://rgw.nau.fccn.pt")

    spark = (
        SparkSession.builder
        .appName("dim_external_course_etl")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Config S3A / Ceph
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    return spark.getOrCreate()

spark = start_spark()

# ===============================================================
# 2. Configurações (paths)
# ===============================================================

BRONZE_PATH_COURSE = os.getenv(
    "BRONZE_PATH_COURSE",
    "s3a://nau-local-analytics-bronze/course_overviews_courseoverview"
)

SILVER_PATH_COURSE = os.getenv(
    "SILVER_PATH_COURSE",
    "s3a://nau-local-analytics-silver/dim_external_course"
)

DIM_ORG_PATH = os.getenv(
    "DIM_ORG_PATH",
    "s3a://nau-local-analytics-silver/dim_organizations"
)

TARGET_TABLE = "default.dim_external_course"


# ===============================================================
# 3. Funções Utilitárias
# ===============================================================

def load_delta(path: str):
    """Carrega um Delta Lake com segurança."""
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        raise RuntimeError(f"Erro ao ler Delta em {path}: {e}")


def normalize_org_code(col):
    """Normaliza códigos de organização (uppercase, trim)."""
    return F.upper(F.trim(col))


# ===============================================================
# 4. Leitura das tabelas Bronze e Silver
# ===============================================================

def load_sources():
    df_bronze = load_delta(BRONZE_PATH_COURSE)

    df_dim_org = (
        load_delta(DIM_ORG_PATH)
        .select(
            "organization_sk",
            "organization_id",
            normalize_org_code("organization_code").alias("organization_code"),
            "effective_start",
            "effective_end",
            "is_current"
        )
    )

    return df_bronze, df_dim_org


# ===============================================================
# 5. Construção da Dim_Organizations Enriched (SCD2)
# ===============================================================

def build_dim_org_enriched(df_dim_org):
    """Cria visão SCD2 enriquecida com organization_sk_current."""
    df_current = (
        df_dim_org
        .filter(F.col("is_current") == True)
        .select(
            "organization_id",
            normalize_org_code("organization_code").alias("org_code"),
            "organization_sk"
        )
        .withColumnRenamed("organization_sk", "organization_sk_current")
    )

    # Join entre históricos e atual
    df_enriched = (
        df_dim_org
        .join(df_current, on="organization_id", how="left")
        .select(
            "organization_sk",
            "organization_sk_current",
            "organization_id",
            "organization_code",
            "effective_start",
            "effective_end",
            "is_current"
        )
    )

    return df_enriched


# ===============================================================
# 6. Limpeza e normalização da Bronze
# ===============================================================

def clean_course_bronze(df_bronze):
    df = (
        df_bronze
        .select(
            F.col("id").alias("course_id"),
            F.col("_location").alias("course_location"),
            F.col("version").cast("int").alias("course_version"),
            normalize_org_code("org").alias("course_org_code"),
            "display_org_with_default",
            "display_name",
            "display_number_with_default",
            F.col("created").cast("timestamp").alias("created"),
            F.col("modified").cast("timestamp").alias("modified"),
            F.col("start").cast("timestamp").alias("start"),
            F.col("end").cast("timestamp").alias("end"),
            F.col("enrollment_start").cast("timestamp").alias("enrollment_start"),
            F.col("enrollment_end").cast("timestamp").alias("enrollment_end"),
            F.col("certificate_available_date").cast("timestamp").alias("certificate_available_date"),
            F.col("announcement").cast("timestamp").alias("announcement"),
            "catalog_visibility",
            "self_paced",
            "visible_to_staff_only",
            "invitation_only",
            "mobile_available",
            "eligible_for_financial_aid",
            "certificates_display_behavior",
            "certificates_show_before_end",
            "cert_html_view_enabled",
            "has_any_active_web_certificate",
            "cert_name_short",
            "cert_name_long",
            F.col("lowest_passing_grade").cast("decimal(5,2)").alias("lowest_passing_grade"),
            "advertised_start",
            "effort",
            "short_description",
            "course_image_url",
            "banner_image_url",
            "course_video_url",
            "marketing_url",
            "social_sharing_url",
            "language",
            F.col("max_student_enrollments_allowed").cast("int").alias("max_student_enrollments_allowed"),
            F.col("ingestion_date").cast("timestamp").alias("ingestion_date"),
            "source_name"
        )
        .dropDuplicates(["course_id"])
        .filter(F.col("course_id").isNotNull())
    )

    logger.info(f"Registos após clean_course_bronze: {df.count()}")
    return df

# ===============================================================
# 7. Apply Hash
# ===============================================================

def apply_hash(df):
    """
    Calcula hash determinístico apenas com colunas de negócio.
    Exclui colunas técnicas (SKs, timestamps SCD, ingestão, hash anterior).
    """

    technical_cols = {
        "course_sk",
        "organization_sk",
        "record_hash",
        "valid_from",
        "valid_to",
        "is_current",
        "ingestion_timestamp",
        "ingestion_date",
        "source_name"
    }

    business_cols = [c for c in df.columns if c not in technical_cols]

    return df.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                    for c in business_cols
                ]
            ),
            256
        )
    )



# ===============================================================
# 8. Join com Dim_Organizations enriched
# ===============================================================

def join_dim_org(df_course, df_dim_org_enriched):
    # 1) Criar lookup único por código normalizado
    df_org_lookup = (
        df_dim_org_enriched
        .select(
            normalize_org_code("organization_code").alias("org_code_dim"),
            "organization_sk_current"
        )
        .dropDuplicates(["org_code_dim"])   # 👈 GARANTE 1:1 POR CÓDIGO
    )

    # 2) Fazer o join curso → organização atual
    df_join = (
        df_course.alias("c")
        .join(
            df_org_lookup.alias("o"),
            F.col("c.course_org_code") == F.col("o.org_code_dim"),
            "left"
        )
        .withColumnRenamed("organization_sk_current", "organization_sk")
        .drop("org_code_dim")
    )
    return df_join


# ===============================================================
# 9. Create Course Unique ID (Surrogate Key)
# ===============================================================
def add_course_sk(df):
    """
    Gera course_sk sequencial (1..N), determinístico por course_id.
    """
    if "course_id" not in df.columns:
        raise ValueError("DataFrame não contém coluna 'course_id'; não é possível criar course_sk.")

    window = Window.orderBy("course_id")

    df_with_sk = df.withColumn(
        "course_sk",
        F.row_number().over(window).cast("int")
    )

    # course_sk primeiro, resto na ordem atual
    cols = df_with_sk.columns
    ordered_cols = ["course_sk"] + [c for c in cols if c != "course_sk"]
    return df_with_sk.select(*ordered_cols) 

# ===============================================================
# 10. Build staging for SCD1
# ===============================================================

def build_stage(df):
    return (
        df.withColumn("is_current", F.lit(True))
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


# ===============================================================
# 10. MERGE final (SCD1)
# ===============================================================

def merge_table(df_stage):

    df_stage.createOrReplaceTempView("stg_dim_external_course")

    try:
        spark.sql(f"DESCRIBE TABLE {TARGET_TABLE}")
        table_exists = True
    except AnalysisException:
        table_exists = False

    if not table_exists:
        logger.info("Tabela alvo não existe. Criando Delta inicial em %s", SILVER_PATH_COURSE)

        (
            df_stage.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_PATH_COURSE)
        )

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
            USING DELTA
            LOCATION '{SILVER_PATH_COURSE}'
        """)
        return

    logger.info("Executando MERGE SCD1 em %s", TARGET_TABLE)

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING stg_dim_external_course AS s
        ON t.course_id = s.course_id
        WHEN MATCHED AND t.record_hash <> s.record_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)

# ===============================================================
# 11. Validate Silver Table
# ===============================================================

def validate_silver_table(spark, path: str):
    logger.info(f"Validando tabela Silver em: {path}")
    try:
        df_silver = spark.read.format("delta").load(path)
        logger.info("Schema da Dim_External_Course (Silver):")
        df_silver.printSchema()

        try:
            row_count = df_silver.count()
            logger.info(f"Total de registos na Dim_External_Course (Silver): {row_count}")
        except Exception as e:
            logger.warning(f"Falha ao fazer count() na Silver (possível fecho da sessão Spark). Erro: {e}")

    except Exception as e:
        logger.error(f"Falha ao validar a tabela Silver: {e}")

    return row_count
# ===============================================================
# 12. Pipeline principal
# ===============================================================

def main():
    logger.info("Início do ETL Dim_External_Course")

    df_bronze, df_dim_org = load_sources()

    df_dim_org_enriched = build_dim_org_enriched(df_dim_org)
    df_clean  = clean_course_bronze(df_bronze)
    df_hash   = apply_hash(df_clean)
    df_joined = join_dim_org(df_hash, df_dim_org_enriched)
    df_with_sk = add_course_sk(df_joined)     
    df_stage  = build_stage(df_with_sk)

    merge_table(df_stage)
    validate_silver_table(spark, SILVER_PATH_COURSE)    
    logger.info("Fim do ETL Dim_External_Course")


# ===============================================================
# Entry point
# ===============================================================

if __name__ == "__main__":
    main()
    spark.stop()
    logger.info("Stop Spark Session")
