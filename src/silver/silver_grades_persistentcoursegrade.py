from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from silver.utils.silver_utils_functions import update_ctrl_table,get_max_timestamp_for_table
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

    spark = start_iceberg_session("silver_grades_persistentcoursegrade")

    #Variables
    tgt_layer = f"silver{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "grades_persistentcoursegrade"

    src_layer = f"bronze{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name = "grades_persistentcoursegrade"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    logging.info(f"Starting ingestion from {last_execution_timestamp}")

    #Initial creation of the table (only useful for first run)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        id BIGINT NOT NULL,
        user_id INT NOT NULL,
        course_id STRING NOT NULL,
        course_edited_timestamp TIMESTAMP,
        course_version STRING NOT NULL,
        grading_policy_hash STRING NOT NULL,
        percent_grade DOUBLE NOT NULL,
        letter_grade STRING NOT NULL,
        passed_timestamp TIMESTAMP,
        created TIMESTAMP NOT NULL,
        modified TIMESTAMP NOT NULL,
        ingestion_date TIMESTAMP NOT NULL
    ) USING ICEBERG
    """)

    #Load source dataframe
    df_src_data = spark.sql(f"""
        SELECT  id,
                user_id,
                course_id,
                course_edited_timestamp,
                course_version,
                grading_policy_hash,
                percent_grade,
                letter_grade,
                passed_timestamp,
                created,
                modified,
                ingestion_date
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
