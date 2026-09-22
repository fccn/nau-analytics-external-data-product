from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.silver_utils_functions import update_ctrl_table,get_max_timestamp_for_table
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    # Feature flag for fccn/nau-technical#981's verify_uuid column. Defaults
    # to disabled so this shared image can be merged/deployed to every
    # environment (they all track the same docker_image tag) without
    # immediately touching any environment's data -- enable per environment
    # by hardcoding this env var to "true" in that environment's own
    # silver_dag.py driverEnv wiring (nau-analytics-airflow-dags), merged
    # only once that environment is actually ready to roll this out.
    VERIFY_UUID_MODE_ENABLED = os.getenv("CERTIFICATE_VERIFY_UUID_MODE_ENABLED", "false").lower() == "true"

    spark = start_iceberg_session("silver_certificates_generatedcertificate")

    #Variables
    tgt_layer = f"silver{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "certificates_generatedcertificate"

    src_layer = f"bronze{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name = "certificates_generatedcertificate"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting ingestion from {last_execution_timestamp}")

    #Initial creation of the table (only useful for first run)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        id INT NOT NULL,
        course_id STRING NOT NULL,
        verify_uuid STRING,
        grade STRING NOT NULL,
        key STRING NOT NULL,
        distinction BOOLEAN NOT NULL,
        status STRING NOT NULL,
        mode STRING NOT NULL,
        created_date TIMESTAMP NOT NULL,
        modified_date TIMESTAMP NOT NULL,
        error_reason STRING NOT NULL,
        user_id INT NOT NULL,
        ingestion_date TIMESTAMP NOT NULL
    ) USING ICEBERG
    """)

    # ---------------------------------------------------------
    # Schema evolution: the CREATE TABLE IF NOT EXISTS above only
    # covers a first run. verify_uuid was added after this table
    # already existed in prod/stage/dev with data, so ALTER TABLE
    # here (guarded, since ADD COLUMNS has no IF NOT EXISTS) is
    # what actually applies to those. Declared nullable rather than
    # NOT NULL (even in the fresh-install CREATE TABLE above) since
    # Iceberg does not allow adding a required column to a table that
    # already has rows -- bronze already guarantees non-null values
    # for every row going forward regardless.
    #
    # Gated by VERIFY_UUID_MODE_ENABLED (see flag comment above) --
    # while disabled, this table is never altered and behaves exactly
    # as it did before fccn/nau-technical#981.
    # ---------------------------------------------------------
    if VERIFY_UUID_MODE_ENABLED:
        existing_columns = {f.name for f in spark.table(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}").schema.fields}
        if "verify_uuid" not in existing_columns:
            spark.sql(f"""
                ALTER TABLE {tgt_layer}.{tgt_pipeline}.{tgt_table_name}
                ADD COLUMNS (
                    verify_uuid STRING
                )
            """)

    # Column list mirrors the flag: while disabled, verify_uuid is neither
    # read from bronze nor written to silver, since the ALTER above never
    # ran and the column may not exist on the target table yet.
    select_columns = ["id", "course_id"]
    if VERIFY_UUID_MODE_ENABLED:
        select_columns.append("verify_uuid")
    select_columns += [
        "grade", "key", "distinction", "status", "mode",
        "created_date", "modified_date", "error_reason", "user_id", "ingestion_date",
    ]
    select_columns_sql = ",\n                ".join(select_columns)

    #Load source dataframe
    df_src_data = spark.sql(f"""
        SELECT  {select_columns_sql}
          FROM {src_layer}.{src_pipeline}.{src_table_name}
         WHERE ingestion_date > '{last_execution_timestamp}'
        """)

    new_or_update_records = df_src_data.count()
    logging.info(f"Number of new or updated records = {new_or_update_records}")

    #Insert the new or updated records into the target table
    df_src_data.write.format("iceberg").mode("append").saveAsTable(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=new_or_update_records,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
