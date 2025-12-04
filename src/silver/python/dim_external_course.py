# ===============================================================
#  Dim External Course - Silver ETL
#  Produção | Sistema NAU | FCCN
#  Autor: Manoel
#  Estrutura com boas práticas para pipelines Spark/Delta
# ===============================================================

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

# ===============================================================
# 1. Configuração do Spark
# ===============================================================

def start_spark():
    return (
        SparkSession.builder
        .appName("dim_external_course_etl")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

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
            F.col("created").cast("timestamp"),
            F.col("modified").cast("timestamp"),
            F.col("start").cast("timestamp"),
            F.col("end").cast("timestamp"),
            F.col("enrollment_start").cast("timestamp"),
            F.col("enrollment_end").cast("timestamp"),
            "certificate_available_date",
            "announcement",
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
            "lowest_passing_grade",
            "advertised_start",
            "effort",
            "short_description",
            "course_image_url",
            "banner_image_url",
            "course_video_url",
            "marketing_url",
            "social_sharing_url",
            "language",
            "max_student_enrollments_allowed",
            "ingestion_date",
            "source_name"
        )
        .dropDuplicates(["course_id"])
    )
    return df


# ===============================================================
# 7. Apply Hash
# ===============================================================

def apply_hash(df):
    business_cols = [c for c in df.columns if c not in ("record_hash",)]
    df_hash = df.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||", *(F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in business_cols)
            ),
            256
        )
    )
    return df_hash


# ===============================================================
# 8. Join com Dim_Organizations enriched
# ===============================================================

def join_dim_org(df_course, df_dim_org_enriched):
    df_join = (
        df_course.alias("c")
        .join(
            df_dim_org_enriched.select(
                normalize_org_code("organization_code").alias("org_code_dim"),
                "organization_sk_current"
            ).alias("o"),
            F.col("c.course_org_code") == F.col("o.org_code_dim"),
            "left"
        )
        .withColumnRenamed("organization_sk_current", "organization_sk")
    )
    return df_join


# ===============================================================
# 9. Build staging for SCD1
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

    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING stg_dim_external_course AS s
        ON t.course_id = s.course_id
        WHEN MATCHED AND t.record_hash <> s.record_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)


# ===============================================================
# 11. Pipeline principal
# ===============================================================

def main():
    df_bronze, df_dim_org = load_sources()

    df_dim_org_enriched = build_dim_org_enriched(df_dim_org)
    df_clean = clean_course_bronze(df_bronze)
    df_hash = apply_hash(df_clean)
    df_joined = join_dim_org(df_hash, df_dim_org_enriched)
    df_stage = build_stage(df_joined)

    merge_table(df_stage)


# ===============================================================
# Entry point
# ===============================================================

if __name__ == "__main__":
    main()
    spark.stop()
