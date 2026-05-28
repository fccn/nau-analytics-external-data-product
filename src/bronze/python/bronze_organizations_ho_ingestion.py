from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.bronze_utils_functions import add_ingestion_metadata_column,read_data_from_sql,update_ctrl_table,get_max_timestamp_for_table,validate_ingestion_values
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    MYSQL_DATABASE = get_required_env("MYSQL_DATABASE")
    MYSQL_HOST = get_required_env("MYSQL_HOST")
    MYSQL_PORT = get_required_env("MYSQL_PORT")
    MYSQL_USER = get_required_env("MYSQL_USER")
    MYSQL_SECRET = get_required_env("MYSQL_SECRET")

    ENVIRONMENT = get_required_env("ENVIRONMENT")

    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    spark = start_iceberg_session("ingestion_organizations_historicalorganization")

    #Variables
    src_schema = "edxapp"
    src_table_name = "organizations_historicalorganization"

    tgt_layer = f"bronze{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "organizations_historicalorganization"

    #Constants
    FIXED_START_DATE = "1900-01-01 00:00:00"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    # This table is only meant to be loaded once, so we'll check at the beginning whether
    # it is the first time the table is being loaded based on the last_execution_timestamp
    # from the audit table
    start_date = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    if start_date != FIXED_START_DATE:
        return

    #Initial creation of the table (only useful for first run)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        id INT NOT NULL,
        created TIMESTAMP NOT NULL,
        modified TIMESTAMP NOT NULL,
        name STRING NOT NULL,
        short_name STRING NOT NULL,
        description STRING,
        logo STRING,
        active BOOLEAN NOT NULL,
        history_id INT NOT NULL,
        history_date TIMESTAMP NOT NULL,
        history_change_reason STRING,
        history_type VARCHAR(1) NOT NULL,
        history_user_id INT,
        source_name STRING NOT NULL,
        ingestion_date TIMESTAMP NOT NULL
    )
    USING ICEBERG
    """)

    # Connect to source and load the data.
    src_table = f"""
        (SELECT id,
                created,
                modified,
                name,
                short_name,
                description,
                logo,
                active,
                history_id,
                history_date,
                history_change_reason,
                history_type,
                history_user_id
          FROM {src_schema}.{src_table_name}) AS source_table
        """

    logging.info(f"executing query in db {src_table}")

    src_df = read_data_from_sql(spark_session=spark,query=src_table,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    src_df = add_ingestion_metadata_column(df=src_df,table=tgt_table_name,current_timestamp=current_timestamp)

    new_or_update_records = src_df.count()
    logging.info(f"Number of new or updated records = {new_or_update_records}")

    #Write data to target
    src_df.write.format("iceberg").mode("append").saveAsTable(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=new_or_update_records,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
