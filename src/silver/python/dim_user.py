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
    This follows the same pattern used in bronze scripts (get_full_tables, incremental_load).
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
        SparkSession.builder.appName("silver_dim_user")
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


def read_bronze_auth_user(spark: SparkSession, bronze_auth_user_path: str):
    """
    Read auth_user table from the bronze Delta location.
    """
    logging.info(f"Reading bronze auth_user from {bronze_auth_user_path}")
    df = spark.read.format("delta").load(bronze_auth_user_path)
    return df


def transform_to_dim_user_source(df_bronze):
    """
    Transform bronze auth_user into a clean, analytics-ready source for Dim_User.
    Includes:
      - type casting
      - trimming and normalization
      - full_name construction
      - business hash (user_hash) for change detection
    """
    logging.info("Transforming bronze auth_user into Dim_User source dataframe")

    df = (
        df_bronze.select(
            F.col("id").cast("int").alias("user_id"),
            F.trim(F.col("username")).alias("username"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.col("first_name"),
            F.col("last_name"),
            F.col("is_staff"),
            F.col("is_superuser"),
            F.col("is_active"),
            F.col("date_joined"),
            F.col("last_login"),
        )
        .withColumn(
            "full_name",
            F.concat_ws(
                " ",
                F.trim(F.col("first_name")),
                F.trim(F.col("last_name")),
            ),
        )
        .withColumn(
            "user_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("user_id").cast("string"),
                    F.lower(F.col("username")),
                    F.col("email"),
                    F.col("full_name"),
                    F.col("is_staff").cast("string"),
                    F.col("is_superuser").cast("string"),
                    F.col("is_active").cast("string"),
                    F.col("date_joined").cast("string"),
                    F.col("last_login").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
    )

    return df


def initial_load_dim_user(df_source, silver_dim_user_path: str):
    """
    First-time load:
      - assign user_sk as a surrogate key using row_number
      - write full Delta table to silver path (overwrite mode)
    """
    logging.info("Silver dim_user does not exist. Performing initial load.")

    w = Window.orderBy(F.col("user_id"))
    df_initial = df_source.withColumn("user_sk", F.row_number().over(w))

    (
        df_initial.select(
            "user_sk",
            "user_id",
            "username",
            "email",
            "first_name",
            "last_name",
            "full_name",
            "is_staff",
            "is_superuser",
            "is_active",
            "date_joined",
            "last_login",
            "user_hash",
            "silver_ingestion_timestamp",
        )
        .write.format("delta")
        .mode("overwrite")
        .save(silver_dim_user_path)
    )

    logging.info("Initial load of dim_user completed successfully.")


def incremental_merge_dim_user(
    spark: SparkSession,
    df_source,
    silver_dim_user_path: str,
):
    """
    Incremental MERGE into dim_user:
      - keep existing user_sk for rows where user_id already exists
      - assign new user_sk for new user_id values
      - update only rows where business hash (user_hash) has changed
    """
    logging.info("Silver dim_user already exists. Starting incremental MERGE.")

    dim_table = DeltaTable.forPath(spark, silver_dim_user_path)
    df_current = spark.read.format("delta").load(silver_dim_user_path)

    # Join source with current silver to detect existing vs new users
    df_join = df_source.alias("s").join(
        df_current.alias("t"),
        on="user_id",
        how="left",
    )

    # Existing users (already have user_sk in silver)
    df_existing = (
        df_join.filter(F.col("t.user_sk").isNotNull())
        .select(
            F.col("t.user_sk").alias("user_sk"),
            F.col("s.user_id"),
            F.col("s.username"),
            F.col("s.email"),
            F.col("s.first_name"),
            F.col("s.last_name"),
            F.col("s.full_name"),
            F.col("s.is_staff"),
            F.col("s.is_superuser"),
            F.col("s.is_active"),
            F.col("s.date_joined"),
            F.col("s.last_login"),
            F.col("s.user_hash"),
            F.col("s.silver_ingestion_timestamp"),
        )
    )

    # New users (no user_sk yet)
    df_new = df_join.filter(F.col("t.user_sk").isNull()).select("s.*")

    # Compute next user_sk for new users
    max_sk_row = df_current.agg(F.max("user_sk").alias("max_sk")).collect()[0]
    max_sk = max_sk_row["max_sk"] or 0

    w_new = Window.orderBy(F.col("user_id"))
    df_new_with_sk = (
        df_new.withColumn("user_sk", F.row_number().over(w_new) + F.lit(max_sk))
        .select(
            "user_sk",
            "user_id",
            "username",
            "email",
            "first_name",
            "last_name",
            "full_name",
            "is_staff",
            "is_superuser",
            "is_active",
            "date_joined",
            "last_login",
            "user_hash",
            "silver_ingestion_timestamp",
        )
    )

    # Union existing + new records to create a full "candidate" dataset
    df_merged_source = df_existing.unionByName(df_new_with_sk)

    # Perform MERGE based on user_id, updating only changed hashes
    (
        dim_table.alias("t")
        .merge(df_merged_source.alias("s"), "t.user_id = s.user_id")
        .whenMatchedUpdate(
            condition="t.user_hash <> s.user_hash",
            set={
                "username": "s.username",
                "email": "s.email",
                "first_name": "s.first_name",
                "last_name": "s.last_name",
                "full_name": "s.full_name",
                "is_staff": "s.is_staff",
                "is_superuser": "s.is_superuser",
                "is_active": "s.is_active",
                "date_joined": "s.date_joined",
                "last_login": "s.last_login",
                "user_hash": "s.user_hash",
                "silver_ingestion_timestamp": "s.silver_ingestion_timestamp",
            },
        )
        .whenNotMatchedInsert(
            values={
                "user_sk": "s.user_sk",
                "user_id": "s.user_id",
                "username": "s.username",
                "email": "s.email",
                "first_name": "s.first_name",
                "last_name": "s.last_name",
                "full_name": "s.full_name",
                "is_staff": "s.is_staff",
                "is_superuser": "s.is_superuser",
                "is_active": "s.is_active",
                "date_joined": "s.date_joined",
                "last_login": "s.last_login",
                "user_hash": "s.user_hash",
                "silver_ingestion_timestamp": "s.silver_ingestion_timestamp",
            }
        )
        .execute()
    )

    logging.info("Incremental MERGE for dim_user completed successfully.")


def main() -> None:
    """
    Entry point:
      - reads env vars (S3 credentials), like bronze scripts
      - parses CLI args for bronze/silver base paths
      - builds Dim_User Silver table (initial or incremental)
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

    bronze_auth_user_path = f"{bronze_base}/auth_user"
    silver_dim_user_path = f"{silver_base}/dim_user"

    spark = get_spark_session(S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT)

    try:
        df_bronze = read_bronze_auth_user(spark, bronze_auth_user_path)
        df_source = transform_to_dim_user_source(df_bronze)

        if not DeltaTable.isDeltaTable(spark, silver_dim_user_path):
            initial_load_dim_user(df_source, silver_dim_user_path)
        else:
            incremental_merge_dim_user(spark, df_source, silver_dim_user_path)

        # Small sanity check
        df_dim = spark.read.format("delta").load(silver_dim_user_path)
        total_rows = df_dim.count()
        distinct_users = df_dim.select("user_id").distinct().count()
        logging.info(f"dim_user total rows: {total_rows}")
        logging.info(f"dim_user distinct user_id: {distinct_users}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
