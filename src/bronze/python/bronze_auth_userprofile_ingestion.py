from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.bronze_utils_functions import add_ingestion_metadata_column,read_data_from_sql,update_ctrl_table,get_max_timestamp_for_table,validate_table_that_delete_lines,get_delta_dataframe
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
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}" 
    spark = start_iceberg_session("ingestion_auth_user")

    #Variables
    src_schema = "edxapp"
    src_table_name = "auth_userprofile"

    tgt_layer = "bronze_local"
    tgt_pipeline = "entidades"
    tgt_table_name = "auth_userprofile"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    #Initial creation of the table (only useful for first run)
    #Initial creation of the table (only useful for first run)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        id INT NOT NULL,
        name STRING NOT NULL,
        meta STRING NOT NULL,
        courseware STRING NOT NULL,
        language STRING NOT NULL,
        location STRING NOT NULL,
        year_of_birth INT,
        gender STRING,
        level_of_education STRING,
        mailing_address STRING,
        city STRING,
        country STRING,
        goals STRING,
        bio STRING,
        profile_image_uploaded_at TIMESTAMP,
        user_id INT NOT NULL,
        phone_number STRING,
        state STRING,
        row_hash STRING NOT NULL,
        ingestion_date TIMESTAMP NOT NULL,
        source_name STRING NOT NULL  
    )
    USING ICEBERG
    """)


    # Connect to source and load the data. We will create a hash of the fields that we want to ingest 
    # in the target table to be able to identify if a record has changed or not since the last ingestion, 
    # to avoid having to update records in the target table that have not changed in the source.
    src_table = f"""
    (SELECT id,
            name,
            meta,
            courseware,
            language,
            location,
            year_of_birth,
            gender,
            level_of_education,
            mailing_address,
            city,
            country,
            goals,
            bio,
            profile_image_uploaded_at,
            user_id,
            phone_number,
            state,
            SHA1(
                CONCAT_WS('||',
                    name,
                    meta,
                    courseware,
                    language,
                    location,
                    year_of_birth,
                    gender,
                    level_of_education,
                    mailing_address,
                    city,
                    country,
                    goals,
                    bio,
                    user_id,
                    phone_number,
                    state
        )
    ) AS row_hash
      FROM {src_schema}.{src_table_name}) AS source_table
    """

    logging.info(f"executing query in db {src_table}")

    src_df = read_data_from_sql(spark_session=spark,query=src_table,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    src_df = add_ingestion_metadata_column(df=src_df,table=tgt_table_name,current_timestamp=current_timestamp)
    
    tgt_table = spark.sql(f"SELECT * FROM {tgt_layer}.{tgt_pipeline}.{tgt_table_name}")
    
    # We get the delta between the source and the target tables, to only insert or update the records 
    # that have changed in the source since the last ingestion, based on the hash of the fields that 
    # we want to ingest. This way we can avoid having to update records in the target table that have 
    # not changed in the source.
    df = get_delta_dataframe(src_table_df=src_df,tgt_table=tgt_table)

    # We can log the number of records that are going to be inserted or updated in the target table, 
    # for monitoring purposes.
    new_or_update_records = df.count()
    logging.info(f"Number of new or updated records = {new_or_update_records}")

    #Write data to target
    df.write.format("iceberg").mode("append").saveAsTable(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}")

    # Additionally, we can validate if there are records in the target that are not in the source anymore 
    # (deleted records) and abort the process if that is the case, to avoid having deleted records in the 
    # source that are not reflected in the target
    validate_table_that_delete_lines(src_table_df=src_df,tgt_table=tgt_table)

    #Finally, we update the control table with the number of records that were inserted or updated in this run. 
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=new_or_update_records)

if __name__ == "__main__":
    main()