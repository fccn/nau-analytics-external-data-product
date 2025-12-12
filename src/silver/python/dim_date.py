# ===============================================================
#  Dim Date - Silver ETL
#  Produção | Sistema NAU | FCCN
#  Estrutura com boas práticas para pipelines Spark/Delta
# ===============================================================
import os
import sys
import logging
from pyspark.sql import SparkSession, functions as F


# -----------------------------
# Logger
# -----------------------------
def get_logger(name: str = "dim_date") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logger.setLevel(level)

    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
    logger.addHandler(h)
    logger.propagate = False
    return logger


logger = get_logger("dim_date")


# -----------------------------
# SparkSession
# -----------------------------
def get_spark_session() -> SparkSession:
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")

    if not S3_ACCESS_KEY or not S3_SECRET_KEY or not S3_ENDPOINT:
        raise RuntimeError("Missing S3_ACCESS_KEY / S3_SECRET_KEY / S3_ENDPOINT")

    spark = (
        SparkSession.builder
        .appName("NAU Analytics - Dim_Date")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
    return spark


# -----------------------------
# Helpers
# -----------------------------
def is_delta_path(spark: SparkSession, path: str) -> bool:
    try:
        spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()
        return True
    except Exception:
        return False


def enrich_date_attributes(df):
    # df precisa ter coluna: date (date type)
    dias_pt = F.array(
        F.lit("Domingo"),
        F.lit("Segunda-feira"),
        F.lit("Terça-feira"),
        F.lit("Quarta-feira"),
        F.lit("Quinta-feira"),
        F.lit("Sexta-feira"),
        F.lit("Sábado"),
    )

    meses_pt = F.array(
        F.lit("Janeiro"),
        F.lit("Fevereiro"),
        F.lit("Março"),
        F.lit("Abril"),
        F.lit("Maio"),
        F.lit("Junho"),
        F.lit("Julho"),
        F.lit("Agosto"),
        F.lit("Setembro"),
        F.lit("Outubro"),
        F.lit("Novembro"),
        F.lit("Dezembro"),
    )

    df = (
        df
        .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))  # 1=Domingo
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("day_name_pt", dias_pt[F.col("day_of_week") - 1])
        .withColumn("month_name_pt", meses_pt[F.col("month") - 1])
        .withColumn("year_month", (F.col("year") * 100 + F.col("month")).cast("int"))
        .withColumn("semester", F.when(F.col("month") <= 6, F.lit(1)).otherwise(F.lit(2)))
        .withColumn("quarter", F.ceil(F.col("month") / F.lit(3)).cast("int"))
        .withColumn("bimester", F.ceil(F.col("month") / F.lit(2)).cast("int"))
        .withColumn("year_semester", F.concat_ws(".", F.col("year").cast("string"), F.col("semester").cast("string")))
        .withColumn("year_quarter", F.concat_ws(".", F.col("year").cast("string"), F.col("quarter").cast("string")))
        .withColumn("year_bimester", F.concat_ws(".", F.col("year").cast("string"), F.col("bimester").cast("string")))
        .withColumn(
            "date_long_pt",
            F.concat(
                F.lpad(F.col("day").cast("string"), 2, "0"),
                F.lit(" de "),
                F.col("month_name_pt"),
                F.lit(" de "),
                F.col("year").cast("string"),
            )
        )
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), F.lit(True)).otherwise(F.lit(False)))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    return df.select(
        "date_sk",
        "date",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name_pt",
        "month_name_pt",
        "week_of_year",
        "year_month",
        "semester",
        "quarter",
        "bimester",
        "year_semester",
        "year_quarter",
        "year_bimester",
        "date_long_pt",
        "is_weekend",
        "ingestion_timestamp",
    )


def build_dim_date_df(spark: SparkSession, start_date: str, end_date: str):
    # Spark SQL puro, rápido e simples
    df = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date
    """)
    return enrich_date_attributes(df)


def upsert_dim_date(spark: SparkSession, silver_path: str, desired_start: str, desired_end: str):
    if not is_delta_path(spark, silver_path):
        logger.info(f"Delta não existe em {silver_path}. Criando tabela inicial...")
        df_all = build_dim_date_df(spark, desired_start, desired_end)
        df_all.write.format("delta").mode("overwrite").save(silver_path)
        logger.info(f"Dim_Date criada com {df_all.count()} linhas.")
        return

    # Existe: só estende (ou faz upsert de tudo, mas sem necessidade)
    cur = spark.read.format("delta").load(silver_path)

    limits = cur.agg(
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date"),
        F.count("*").alias("cnt"),
    ).collect()[0]

    min_date = limits["min_date"]
    max_date = limits["max_date"]
    logger.info(f"Dim_Date atual: min_date={min_date}, max_date={max_date}, rows={limits['cnt']}")

    # se o desired_end já está coberto, sai
    desired_end_row = spark.sql(f"SELECT to_date('{desired_end}') AS d").collect()[0]
    desired_end_dt = desired_end_row["d"]

    if max_date is not None and max_date >= desired_end_dt:
        logger.info("Nada a fazer: tabela já cobre o intervalo desejado.")
        return

    # novo start = max_date + 1 (ou desired_start se a tabela estiver vazia)
    if max_date is None:
        new_start = desired_start
    else:
        new_start = spark.sql(f"SELECT date_add(to_date('{max_date}'), 1) AS d").collect()[0]["d"]

    logger.info(f"Gerando apenas novas datas: {new_start} -> {desired_end}")
    df_new = build_dim_date_df(spark, str(new_start), desired_end)

    df_new.createOrReplaceTempView("stg_dim_date")

    spark.sql(f"""
        MERGE INTO delta.`{silver_path}` t
        USING stg_dim_date s
        ON t.date = s.date
        WHEN NOT MATCHED THEN INSERT *
    """)

    logger.info(f"Merge concluído. Novas linhas (staging): {df_new.count()}")


def validate_dim_date(spark: SparkSession, silver_path: str):
    df = spark.read.format("delta").load(silver_path)
    stats = df.agg(
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date"),
        F.count("*").alias("total"),
        F.countDistinct("date").alias("distinct_dates"),
        F.countDistinct("date_sk").alias("distinct_sk"),
    ).collect()[0]

    logger.info(f"VALIDATION: total={stats['total']} distinct_dates={stats['distinct_dates']} distinct_sk={stats['distinct_sk']}")
    logger.info(f"VALIDATION: min_date={stats['min_date']} max_date={stats['max_date']}")

    if stats["total"] != stats["distinct_dates"]:
        logger.warning("Duplicatas detectadas em date (total != distinct_dates).")
    if stats["total"] != stats["distinct_sk"]:
        logger.warning("Duplicatas detectadas em date_sk (total != distinct_sk).")


# -----------------------------
# Main
# -----------------------------
def main():
    logger.info("Starting Dim_Date ETL")
    logger.info(f"ENV SILVER_BUCKET={silver_base}")
    spark = None

    try:
        start_date = os.getenv("DIM_DATE_START", "2018-01-01")
        end_date = os.getenv("DIM_DATE_END", "2032-12-31")

        silver_base = os.getenv("SILVER_BUCKET")
        if not silver_base:
            raise RuntimeError("SILVER_BUCKET not set")

        silver_path = f"{silver_base.rstrip('/')}/dim_date"
        logger.info(f"Params: start={start_date} end={end_date}")
        logger.info(f"Target: {silver_path}")

        spark = get_spark_session()

        upsert_dim_date(spark, silver_path, start_date, end_date)
        validate_dim_date(spark, silver_path)

        logger.info("Dim_Date ETL finished successfully")

    except Exception:
        logger.exception("Error while running Dim_Date ETL")
        raise SystemExit(1)

    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("SparkSession stopped")
            except Exception:
                logger.warning("Failed to stop SparkSession cleanly", exc_info=True)


if __name__ == "__main__":
    main()
