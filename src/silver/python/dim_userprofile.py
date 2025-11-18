from pyspark.sql import SparkSession  # type: ignore
import pyspark.sql.functions as F      # type: ignore
import pyspark.sql.types as T          # type: ignore
from delta.tables import DeltaTable    # type: ignore

import argparse
import logging
import os
from pyspark.sql.window import Window


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def get_args() -> argparse.Namespace:
    """
    Parse CLI arguments to control bronze/silver base paths.
    This follows the same pattern used in bronze scripts.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bronze_base",
        type=str,
        required=True,
        help="Base S3 path for bronze tables (e.g. s3a://nau-local-analytics-bronze)",
    )
    parser.add_argument(
        "--silver_base",
        type=str,
        required=True,
        help="Base S3 path for silver tables (e.g. s3a://nau-local-analytics-silver)",
    )
    args = parser.parse_args()
    return args


def get_spark_session(S3_ACCESS_KEY: str, S3_SECRET_KEY: str, S3_ENDPOINT: str) -> SparkSession:
    """
    Create a SparkSession with Delta + S3A support.
    Mirrors the configuration used in bronze ingestion scripts.
    """
    spark = (
        SparkSession.builder.appName("silver_dim_userprofile")
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.375.jar,"
            "/opt/spark/jars/delta-spark_2.12-3.2.1.jar,"
            "/opt/spark/jars/delta-storage-3.2.1.jar,"
            "/opt/spark/jars/delta-kernel-api-3.2.1.jar,"
            "/opt/spark/jars/mysql-connector-j-8.3.0.jar",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark


def read_bronze_auth_userprofile(spark: SparkSession, bronze_auth_userprofile_path: str):
    """
    Read auth_userprofile table from the bronze Delta location.
    """
    logging.info(f"Reading bronze auth_userprofile from {bronze_auth_userprofile_path}")
    df = spark.read.format("delta").load(bronze_auth_userprofile_path)
    return df


def transform_to_dim_userprofile_source(df_bronze):
    """
    Transform bronze auth_userprofile into a clean, analytics-ready source for Dim_UserProfile.
    Includes:
      - type casting
      - trimming and normalization
      - business hash (userprofile_hash) for change detection
    """

    logging.info("Transforming bronze auth_userprofile into Dim_UserProfile source dataframe")

    df = (
        df_bronze.select(
            F.col("id").cast("int").alias("userprofile_id"),
            F.col("user_id").cast("int").alias("user_id"),
            F.trim(F.col("name")).alias("full_name"),
            F.trim(F.col("gender")).alias("gender"),
            F.col("year_of_birth").cast("int").alias("year_of_birth"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("country")).alias("country"),
        )
        .withColumn(
            "userprofile_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("userprofile_id").cast("string"),
                    F.col("user_id").cast("string"),
                    F.col("full_name"),
                    F.col("gender"),
                    F.col("year_of_birth").cast("string"),
                    F.col("city"),
                    F.col("country"),
                ),
                256,
            ),
        )
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )

    return df


def initial_load_dim_userprofile(df_source, silver_dim_userprofile_path: str):
    """
    First-time load:
      - assign userprofile_sk as a surrogate key using row_number
      - write full Delta table to silver path (overwrite mode)
    """
    logging.info("Silver dim_userprofile does not exist. Performing initial load.")

    w = Window.orderBy(F.col("userprofile_id"))
    df_initial = df_source.withColumn("userprofile_sk", F.row_number().over(w))

    (
        df_initial.select(
            "userprofile_sk",
            "userprofile_id",
            "user_id",
            "full_name",
            "gender",
            "year_of_birth",
            "city",
            "country",
            "userprofile_hash",
            "silver_ingestion_timestamp",
        )
        .write.format("delta")
        .mode("overwrite")
        .save(silver_dim_userprofile_path)
    )

    logging.info("Initial load of dim_userprofile completed successfully.")


def incremental_merge_dim_userprofile(
    spark: SparkSession,
    df_source,
    silver_dim_userprofile_path: str,
):
    """
    Incremental MERGE into dim_userprofile:
      - keep existing userprofile_sk for rows where userprofile_id already exists
      - assign new userprofile_sk for new userprofile_id values
      - update only rows where business hash (userprofile_hash) has changed
    """
    logging.info("Silver dim_userprofile already exists. Starting incremental MERGE.")

    dim_table = DeltaTable.forPath(spark, silver_dim_userprofile_path)
    df_current = spark.read.format("delta").load(silver_dim_userprofile_path)

    # Join source with current silver to detect existing vs new profiles
    df_join = df_source.alias("s").join(
        df_current.alias("t"),
        on="userprofile_id",
        how="left",
    )

    # Existing profiles (already have userprofile_sk in silver)
    df_existing = (
        df_join.filter(F.col("t.userprofile_sk").isNotNull())
        .select(
            F.col("t.userprofile_sk").alias("userprofile_sk"),
            F.col("s.userprofile_id"),
            F.col("s.user_id"),
            F.col("s.full_name"),
            F.col("s.gender"),
            F.col("s.year_of_birth"),
            F.col("s.city"),
            F.col("s.country"),
            F.col("s.userprofile_hash"),
            F.col("s.silver_ingestion_timestamp"),
        )
    )

    # New profiles (no userprofile_sk yet)
    df_new = df_join.filter(F.col("t.userprofile_sk").isNull()).select("s.*")

    # Compute next userprofile_sk for new profiles
    max_sk_row = df_current.agg(F.max("userprofile_sk").alias("max_sk")).collect()[0]
    max_sk = max_sk_row["max_sk"] or 0

    w_new = Window.orderBy(F.col("userprofile_id"))
    df_new_with_sk = (
        df_new.withColumn("userprofile_sk", F.row_number().over(w_new) + F.lit(max_sk))
        .select(
            "userprofile_sk",
            "userprofile_id",
            "user_id",
            "full_name",
            "gender",
            "year_of_birth",
            "city",
            "country",
            "userprofile_hash",
            "silver_ingestion_timestamp",
        )
    )

    # Union existing + new records to create a full "candidate" dataset
    df_merged_source = df_existing.unionByName(df_new_with_sk)

    # Perform MERGE based on userprofile_id, updating only changed hashes
    (
        dim_table.alias("t")
        .merge(df_merged_source.alias("s"), "t.userprofile_id = s.userprofile_id")
        .whenMatchedUpdate(
            condition="t.userprofile_hash <> s.userprofile_hash",
            set={
                "user_id": "s.user_id",
                "full_name": "s.full_name",
                "gender": "s.gender",
                "year_of_birth": "s.year_of_birth",
                "city": "s.city",
                "country": "s.country",
                "userprofile_hash": "s.userprofile_hash",
                "silver_ingestion_timestamp": "s.silver_ingestion_timestamp",
            },
        )
        .whenNotMatchedInsert(
            values={
                "userprofile_sk": "s.userprofile_sk",
                "userprofile_id": "s.userprofile_id",
                "user_id": "s.user_id",
                "full_name": "s.full_name",
                "gender": "s.gender",
                "year_of_birth": "s.year_of_birth",
                "city": "s.city",
                "country": "s.country",
                "userprofile_hash": "s.userprofile_hash",
                "silver_ingestion_timestamp": "s.silver_ingestion_timestamp",
            }
        )
        .execute()
    )

    logging.info("Incremental MERGE for dim_userprofile completed successfully.")


def main() -> None:
    """
    Entry point:
      - reads env vars (S3 credentials), like bronze scripts
      - parses CLI args for bronze/silver base paths
      - builds Dim_UserProfile Silver table (initial or incremental)
    """

    # ------------------------------------------------------------------
    # Get S3 credentials from environment (same as bronze scripts)
    # ------------------------------------------------------------------
    S3_ACCESS_KEY = str(os.getenv("S3_ACCESS_KEY"))
    S3_SECRET_KEY = str(os.getenv("S3_SECRET_KEY"))
    S3_ENDPOINT = str(os.getenv("S3_ENDPOINT"))

    if not S3_ACCESS_KEY or not S3_SECRET_KEY or not S3_ENDPOINT:
        logging.error("Missing S3 credentials in environment variables.")
        raise SystemExit(1)

    args = get_args()
    bronze_base = args.bronze_base.rstrip("/")
    silver_base = args.silver_base.rstrip("/")

    bronze_auth_userprofile_path = f"{bronze_base}/auth_userprofile"
    silver_dim_userprofile_path = f"{silver_base}/dim_userprofile"

    spark = get_spark_session(S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)

    try:
        df_bronze = read_bronze_auth_userprofile(spark, bronze_auth_userprofile_path)
        df_source = transform_to_dim_userprofile_source(df_bronze)

        if not DeltaTable.isDeltaTable(spark, silver_dim_userprofile_path):
            initial_load_dim_userprofile(df_source, silver_dim_userprofile_path)
        else:
            incremental_merge_dim_userprofile(spark, df_source, silver_dim_userprofile_path)

        # Small sanity check
        df_dim = spark.read.format("delta").load(silver_dim_userprofile_path)
        total_rows = df_dim.count()
        distinct_profiles = df_dim.select("userprofile_id").distinct().count()
        logging.info(f"dim_userprofile total rows: {total_rows}")
        logging.info(f"dim_userprofile distinct userprofile_id: {distinct_profiles}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
