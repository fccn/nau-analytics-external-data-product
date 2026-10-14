from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from pyspark.sql.window import Window
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.gold_utils_functions import update_ctrl_table,get_max_timestamp_for_table
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_dim_user")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "dim_user"

    src_layer = f"silver{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name_1 = "auth_user"
    src_table_name_2 = "auth_userprofile"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting process from {last_execution_timestamp}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        -- Keys
        user_key                BIGINT       COMMENT 'Surrogate SCD2 key (9-digit or more, user_cd + seq)',
        user_cd                 INT          COMMENT 'Source system user identifier (auth_user.id)',

        -- Identity
        username                STRING       COMMENT 'User login name',
        first_name              STRING       COMMENT 'User first name',
        last_name               STRING       COMMENT 'User last name',
        full_name               STRING       COMMENT 'Concatenation of first_name and last_name',
        email                   STRING       COMMENT 'User email address',

        -- Demographics
        year_of_birth           INT          COMMENT 'Year of birth from profile',
        gender                  STRING       COMMENT 'Gender from profile',
        level_of_education      STRING       COMMENT 'Highest level of education',
        language                STRING       COMMENT 'Preferred language',
        employment_situation    STRING       COMMENT 'Employment situation of the user (derived from auth_userprofile.meta)',

        -- Location & Contact
        phone_number            STRING       COMMENT 'User phone number',
        city                    STRING       COMMENT 'City from user profile',
        country                 STRING       COMMENT 'Country from user profile',
        location                STRING       COMMENT 'Free-form location (text)',

        -- Status Flags
        is_staff                BOOLEAN      COMMENT 'Staff flag',
        is_superuser            BOOLEAN      COMMENT 'Superuser flag',
        is_active               BOOLEAN      COMMENT 'Active flag',

        -- Activity
        date_joined             TIMESTAMP    COMMENT 'Date when the user created the account',
        last_login              TIMESTAMP    COMMENT 'Last login timestamp',

        -- Hash
        user_hash               STRING       COMMENT 'SHA256 hash of all SCD2 tracked attributes',

        -- SCD2 Metadata
        key_start_date          TIMESTAMP    COMMENT 'Start date for SCD Type 2 validity',
        key_end_date            TIMESTAMP    COMMENT 'End date for SCD Type 2 validity',
        last_update_timestamp   TIMESTAMP    COMMENT 'Timestamp of last update (ETL time)'
    )
    USING iceberg
    TBLPROPERTIES (
        'write.parquet.compression-codec'                    = 'zstd',
        'write.target-file-size-bytes'                       = '134217728',
        'write.distribution-mode'                            = 'none',
        'write.sort.order'                                   = 'user_key ASC',
        'commit.manifest.min-count-to-merge'                 = '100',
        'write.merge.enabled'                                = 'true',
        'read.split.target-size'                             = '134217728',
        'read.split.open-file-cost'                          = '4194304',
        'write.metadata.delete-after-commit.enabled'         = 'true',
        'write.metadata.previous-versions-max'               = '10',
        'write.parquet.bloom-filter.enabled.column.user_key' = 'true',
        'write.parquet.bloom-filter.enabled.column.user_cd'  = 'true'
    )
    """)

    logging.info(f"Starting incremental processing for data after: {last_execution_timestamp}")

    # ---------------------------------------------------------
    # 1. Incremental Read (Silver)
    # ---------------------------------------------------------
    def keep_latest(df, partition_col, order_col):
        w = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
        return df.withColumn("rn", F.row_number().over(w)) \
                 .filter(F.col("rn") == 1) \
                 .drop("rn")

    df_auth_sel = spark.table(f"{src_layer}.{src_pipeline}.{src_table_name_1}") \
        .filter(F.col("ingestion_date") > last_execution_timestamp) \
        .selectExpr(
            "id as u_id", "username", "first_name", "last_name", "email",
            "is_staff", "is_superuser", "is_active", "last_login",
            "date_joined",
            "ingestion_date as u_ingestion_date"
        )

    df_profile_sel = spark.table(f"{src_layer}.{src_pipeline}.{src_table_name_2}") \
        .filter(F.col("ingestion_date") > last_execution_timestamp) \
        .selectExpr(
            "user_id as p_user_id", "name", "year_of_birth", "gender",
            "level_of_education", "city", "country", "phone_number",
            "employment_situation",
            "ingestion_date as p_ingestion_date"
        )

    # ---------------------------------------------------------
    # 2. Intra-batch Deduplication
    # ---------------------------------------------------------
    df_auth_latest    = keep_latest(df_auth_sel,    "u_id",      "u_ingestion_date")
    df_profile_latest = keep_latest(df_profile_sel, "p_user_id", "p_ingestion_date")

    # ---------------------------------------------------------
    # 3. Join + Build Delta
    # ---------------------------------------------------------
    df_delta_raw = df_auth_latest.alias("a").join(
        df_profile_latest.alias("p"),
        F.col("a.u_id") == F.col("p.p_user_id"),
        "full_outer"
    ).select(
        F.coalesce(F.col("a.u_id"), F.col("p.p_user_id")).alias("user_id"),
        F.when(
            F.col("a.u_ingestion_date").isNotNull() & F.col("p.p_ingestion_date").isNotNull(),
            F.greatest(F.col("a.u_ingestion_date"), F.col("p.p_ingestion_date"))
        ).otherwise(
            F.coalesce(F.col("a.u_ingestion_date"), F.col("p.p_ingestion_date"))
        ).alias("event_time"),
        F.trim(F.col("a.username")).alias("username"),
        F.trim(F.col("a.first_name")).alias("first_name"),
        F.trim(F.col("a.last_name")).alias("last_name"),
        F.trim(F.col("a.email")).alias("email"),
        F.col("a.is_staff"), F.col("a.is_superuser"), F.col("a.is_active"),
        F.col("a.last_login"),
        F.col("a.date_joined"),
        F.trim(F.col("p.name")).alias("full_name"),
        F.trim(F.col("p.gender")).alias("gender"),
        F.trim(F.col("p.level_of_education")).alias("level_of_education"),
        F.trim(F.col("p.city")).alias("city"),
        F.trim(F.col("p.country")).alias("country"),
        F.trim(F.col("p.phone_number")).alias("phone_number"),
        F.trim(F.col("p.employment_situation")).alias("employment_situation"),
        F.col("p.year_of_birth")
    ).cache()

    # Early exit — nothing to process
    delta_count = df_delta_raw.count()
    logging.info(f"Delta rows to process: {delta_count}")
    if delta_count == 0:
        logging.info("No new or updated records. Exiting.")
        update_ctrl_table(spark_session=spark, table_name=tgt_table_name,
                          current_timestamp=current_timestamp, number_of_records=0, env=ENVIRONMENT)
        return

    # Keep changed user IDs as a DataFrame for a broadcast join (avoids large IN clause)
    changed_ids_df = df_delta_raw.select(F.col("user_id").alias("user_cd")).distinct()

    # ---------------------------------------------------------
    # 4. Read Gold (Target) — scoped to changed users only
    # ---------------------------------------------------------
    tgt_tbl = f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}"
    try:
        df_target_full = spark.table(tgt_tbl) \
            .join(F.broadcast(changed_ids_df), on="user_cd", how="inner") \
            .cache()
        df_target_full.count()
        df_target_versions = df_target_full \
            .groupBy("user_cd") \
            .agg(F.count("*").alias("max_version"))
        df_target_active = df_target_full \
            .filter(F.col("key_end_date") == F.to_timestamp(F.lit("9999-12-31"))) \
            .select(
                "user_cd", "user_hash", "last_login",
                "username", "first_name", "last_name", "full_name", "email",
                "is_staff", "is_superuser", "is_active",
                "year_of_birth", "gender", "level_of_education",
                "city", "country", "phone_number", "employment_situation"
            )
    except Exception as e:
        logging.warning(f"Gold table not found ({e}). Assuming initial load (First Run).")
        empty_schema = df_delta_raw.limit(0)
        df_target_active   = empty_schema \
            .withColumnRenamed("user_id", "user_cd") \
            .withColumn("user_hash", F.lit(None).cast("string"))
        df_target_versions = empty_schema \
            .withColumnRenamed("user_id", "user_cd") \
            .withColumn("max_version", F.lit(0).cast("long"))

    # ---------------------------------------------------------
    # 5. Enrich: fill nulls with Gold values
    # ---------------------------------------------------------
    COLS_TO_FILL = [
        "username", "first_name", "last_name", "full_name", "email",
        "is_staff", "is_superuser", "is_active", "last_login",
        "year_of_birth", "gender", "level_of_education",
        "city", "country", "phone_number", "employment_situation"
    ]

    df_enriched = df_delta_raw.alias("delta") \
        .join(df_target_active.alias("target"), F.col("delta.user_id") == F.col("target.user_cd"), "left") \
        .select(
            F.col("delta.user_id").alias("user_cd"),
            F.col("delta.event_time"),
            F.col("target.user_hash").alias("target_hash"),
            F.col("target.last_login").alias("target_last_login"),
            *[F.coalesce(F.col(f"delta.{c}"), F.col(f"target.{c}")).alias(c) for c in COLS_TO_FILL],
            F.col("delta.date_joined"),
            F.concat_ws(", ", F.col("delta.city"), F.col("delta.country")).alias("location"),
            F.lit(None).cast("string").alias("language")
        )

    # ---------------------------------------------------------
    # 6. Hash + Change Detection
    # ---------------------------------------------------------
    COLS_FOR_HASH = [
        "username", "first_name", "last_name", "full_name", "email",
        "is_staff", "is_superuser", "is_active",
        "year_of_birth", "gender", "level_of_education",
        "city", "country", "phone_number", "employment_situation"
    ]

    df_changed = df_enriched \
        .withColumn("user_hash", F.xxhash64(*COLS_FOR_HASH).cast("string")) \
        .filter(
            F.col("target_hash").isNull() |
            (F.col("target_hash") != F.col("user_hash")) |
            (F.col("target_last_login") != F.col("last_login"))
        ) \
        .join(
            df_target_versions.withColumnRenamed("user_cd", "v_user_cd"),
            F.col("user_cd") == F.col("v_user_cd"), "left"
        ) \
        .drop("v_user_cd") \
        .withColumn("next_version", F.coalesce(F.col("max_version"), F.lit(0)) + 1) \
        .withColumn("new_user_key",
            (F.col("user_cd").cast("long") * 1000 +
             F.col("next_version").cast("long")).cast("long")
        )

    df_changed = df_changed.cache()
    changed_count = df_changed.count()
    logging.info(f"Changed records (new + SCD1 + SCD2): {changed_count}")

    # ---------------------------------------------------------
    # 7. Staging for MERGE
    # ---------------------------------------------------------
    is_scd2_change = (
        F.col("target_hash").isNotNull() & (F.col("target_hash") != F.col("user_hash"))
    )

    df_close_old = df_changed \
        .filter(is_scd2_change) \
        .withColumn("merge_key", F.col("user_cd")) \
        .withColumn("merge_action", F.lit("CLOSE_SCD2"))

    df_upsert_new = df_changed \
        .withColumn("merge_key", F.when(is_scd2_change, F.lit(None).cast("string")).otherwise(F.col("user_cd"))) \
        .withColumn("merge_action", F.lit("UPSERT_NEW_OR_SCD1"))

    df_staged_updates = df_close_old.unionByName(df_upsert_new).cache()
    df_staged_updates.count()

    df_staged_updates.createOrReplaceTempView("staged_updates")

    # ---------------------------------------------------------
    # 8. MERGE INTO (Iceberg)
    # ---------------------------------------------------------
    logging.info("Executing MERGE INTO...")

    spark.sql(f"""
    MERGE INTO {tgt_tbl} AS target
    USING staged_updates AS source
    ON target.user_cd = source.merge_key

    WHEN MATCHED AND source.merge_action = 'CLOSE_SCD2'
                 AND target.key_end_date = CAST('9999-12-31' AS TIMESTAMP) THEN
      UPDATE SET
        target.key_end_date          = CAST(source.event_time AS TIMESTAMP),
        target.last_update_timestamp = current_timestamp()

    WHEN MATCHED AND source.merge_action = 'UPSERT_NEW_OR_SCD1'
                 AND target.user_hash = source.user_hash THEN
      UPDATE SET
        target.last_login            = CAST(source.last_login AS TIMESTAMP),
        target.last_update_timestamp = current_timestamp()

    WHEN NOT MATCHED THEN
      INSERT (
        user_key, user_cd, username, first_name, last_name, full_name, email,
        date_joined, year_of_birth, gender, level_of_education, language,
        employment_situation, phone_number, city, country, location,
        is_staff, is_superuser, is_active, last_login, user_hash,
        key_start_date, key_end_date, last_update_timestamp
      )
      VALUES (
        CAST(source.new_user_key AS BIGINT),
        CAST(source.user_cd AS INT),
        CAST(source.username AS STRING),
        CAST(source.first_name AS STRING),
        CAST(source.last_name AS STRING),
        CAST(source.full_name AS STRING),
        CAST(source.email AS STRING),
        CAST(source.date_joined AS TIMESTAMP),
        CAST(source.year_of_birth AS INT),
        CAST(source.gender AS STRING),
        CAST(source.level_of_education AS STRING),
        CAST(source.language AS STRING),
        CAST(source.employment_situation AS STRING),
        CAST(source.phone_number AS STRING),
        CAST(source.city AS STRING),
        CAST(source.country AS STRING),
        CAST(source.location AS STRING),
        CAST(source.is_staff AS BOOLEAN),
        CAST(source.is_superuser AS BOOLEAN),
        CAST(source.is_active AS BOOLEAN),
        CAST(source.last_login AS TIMESTAMP),
        CAST(source.user_hash AS STRING),
        CASE
            WHEN source.next_version = 1 THEN CAST('1900-01-01 00:00:00' AS TIMESTAMP)
            ELSE CAST(source.event_time + INTERVAL 1 SECOND AS TIMESTAMP)
        END,
        CAST('9999-12-31 00:00:00' AS TIMESTAMP),
        current_timestamp()
      )
    """)

    df_delta_raw.unpersist()
    df_staged_updates.unpersist()
    try:
        df_target_full.unpersist()
    except Exception:
        pass

    try:
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_data_files(
            table => '{tgt_tbl}',
            strategy => 'sort',
            sort_order => 'user_key ASC',
            options => map('min-input-files', '2', 'rewrite-all', 'false')
          )
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.rewrite_manifests(table => '{tgt_tbl}')
        """)
        spark.sql(f"""
          CALL {tgt_layer}.system.expire_snapshots(table => '{tgt_tbl}', retain_last => 5)
        """)
        logging.info("Iceberg maintenance executed: sort + compact + manifests + expire snapshots.")
    except Exception as e:
        logging.warning(f"Iceberg procedures not executed ({e}).")

    logging.info("Process completed successfully.")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=changed_count,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
