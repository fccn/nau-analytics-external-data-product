from pyspark.sql import SparkSession, functions as F
import os


# =========================================
# 1. Spark Session (produção)
# =========================================

def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
            .appName("NAU – Dim_Downtimes (Silver)")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Heartbeat/timeouts ajustados para RGW/CEPH
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "60s")
            # S3A / RGW
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY"))
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            # Partições menores (dataset pequeno)
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
    )


spark = get_spark_session()


# =========================================
# 2. Paths Bronze/Silver
# =========================================

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("SILVER_BUCKET")

bronze_path = f"{BRONZE_BUCKET.rstrip('/')}/external/nau_downtimes"
silver_path = f"{SILVER_BUCKET.rstrip('/')}/dim_downtimes"


# =========================================
# 3. Leitura Bronze (Delta)
# =========================================

df_bronze = (
    spark.read
        .format("delta")
        .load(bronze_path)
)


# =========================================
# 4. Filtrar apenas linhas válidas (from/to preenchidos)
# =========================================

df_valid = df_bronze.filter(
    (F.trim("from_lisbon_time") != "") &
    (F.trim("to_lisbon_time") != "")
)


# =========================================
# 5. Normalização, trims, NULLs, booleanos
# =========================================

df_norm = (
    df_valid
        .withColumn("from_lisbon_time", F.trim("from_lisbon_time"))
        .withColumn("to_lisbon_time", F.trim("to_lisbon_time"))
        .withColumn("impact", F.nullif(F.trim("impact"), ""))
        .withColumn("description", F.nullif(F.trim("description"), ""))
        .withColumn("affected_applications", F.nullif(F.trim("affected_applications"), ""))
        .withColumn("expected_bool", F.col("expected") == "TRUE")
        .withColumn("detected_by_nagios_bool", F.col("detected_by_nagios") == "TRUE")
        .withColumn("detected_by_icinga_bool", F.col("detected_by_icinga") == "TRUE")
        .withColumn("detected_by_uptimerobot_bool", F.col("detected_by_uptimerobot") == "TRUE")
        .withColumn("is_lms_affected", F.col("lms_nau_edu_pt_studio_nau_edu_pt") == "TRUE")
        .withColumn("is_www_affected", F.col("www_nau_edu_pt") == "TRUE")
        .withColumn("is_partial_outage", F.col("only_some_sub_service_s_affected") == "TRUE")
)


# =========================================
# 6. Converter timestamps (H:mm e H:mm:ss)
# =========================================

df_norm = (
    df_norm
        .withColumn(
            "from_ts",
            F.coalesce(
                F.to_timestamp("from_lisbon_time", "yyyy-MM-dd H:mm:ss"),
                F.to_timestamp("from_lisbon_time", "yyyy-MM-dd H:mm")
            )
        )
        .withColumn(
            "to_ts",
            F.coalesce(
                F.to_timestamp("to_lisbon_time", "yyyy-MM-dd H:mm:ss"),
                F.to_timestamp("to_lisbon_time", "yyyy-MM-dd H:mm")
            )
        )
)

df_norm = df_norm.filter(
    F.col("from_ts").isNotNull() &
    F.col("to_ts").isNotNull()
)


# =========================================
# 7. Cálculo da duração + mismatch
# =========================================

df_norm = (
    df_norm
        .withColumn("downtime_duration_minutes_source", F.col("duration_in_minutes").cast("int"))
        .withColumn(
            "downtime_duration_minutes",
            F.floor((F.col("to_ts").cast("long") - F.col("from_ts").cast("long")) / 60).cast("int")
        )
        .withColumn(
            "downtime_has_duration_mismatch",
            F.when(
                F.col("downtime_duration_minutes_source").isNotNull() &
                (F.col("downtime_duration_minutes_source") != F.col("downtime_duration_minutes")),
                True
            ).otherwise(False)
        )
)


# =========================================
# 8. Renomear colunas para padrão Silver
# =========================================

df_final = (
    df_norm
        .withColumnRenamed("impact", "downtime_impact")
        .withColumnRenamed("description", "downtime_description")
        .withColumnRenamed("affected_applications", "downtime_affected_applications")
)


# =========================================
# 9. Hash único (downtime_hash)
# =========================================

df_final = (
    df_final.withColumn(
        "downtime_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("from_ts").cast("string"),
                F.col("to_ts").cast("string"),
                F.coalesce(F.col("downtime_impact"), F.lit("")),
                F.coalesce(F.col("downtime_description"), F.lit("")),
                F.coalesce(F.col("downtime_affected_applications"), F.lit(""))
            ),
            256
        )
    )
)


# =========================================
# 10. Seleção final de colunas
# =========================================

df_output = df_final.select(
    "from_ts",
    "to_ts",
    "downtime_impact",
    "downtime_description",
    "downtime_affected_applications",
    "expected_bool",
    "detected_by_nagios_bool",
    "detected_by_icinga_bool",
    "detected_by_uptimerobot_bool",
    "is_lms_affected",
    "is_www_affected",
    "is_partial_outage",
    "downtime_duration_minutes_source",
    "downtime_duration_minutes",
    "downtime_has_duration_mismatch",
    "ingestion_timestamp",
    "source_file_id",
    "source_sheet_name",
    "downtime_hash"
)


# =========================================
# 11. Escrita Silver (Delta) – Produção
# =========================================

(
    df_output
        .coalesce(1)
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
)
