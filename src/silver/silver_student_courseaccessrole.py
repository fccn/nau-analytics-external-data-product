from pyspark.sql import DataFrame  # type:ignore
import pyspark.sql.functions as F  # type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
from utils.silver_utils_functions import update_ctrl_table
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

    spark = start_iceberg_session("silver_student_courseaccessrole")

    #Variables
    tgt_layer = f"silver{ENVIRONMENT}"
    tgt_pipeline = "audit"
    tgt_table_name = "student_courseaccessrole"

    src_layer = f"bronze{ENVIRONMENT}"
    src_pipeline = "audit"
    src_table_name = "student_courseaccessrole"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    # Full-refresh audit table: mirrors the bronze daily snapshot.
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        id INT NOT NULL,
        org STRING NOT NULL,
        course_id STRING NOT NULL,
        role STRING NOT NULL,
        user_id INT NOT NULL,
        row_hash STRING NOT NULL,
        ingestion_date TIMESTAMP NOT NULL
    )
    USING ICEBERG
    """)

    spark.sql(f"""
        INSERT OVERWRITE TABLE {tgt_layer}.{tgt_pipeline}.{tgt_table_name}
        SELECT id,
               org,
               course_id,
               role,
               user_id,
               row_hash,
               ingestion_date
          FROM {src_layer}.{src_pipeline}.{src_table_name}
    """)

    nr = spark.sql(f"SELECT COUNT(*) AS c FROM {tgt_layer}.{tgt_pipeline}.{tgt_table_name}").first()["c"]
    logging.info(f"Number of records in silver = {nr}")

    update_ctrl_table(spark_session=spark, table_name=tgt_table_name, current_timestamp=current_timestamp, number_of_records=nr, env=ENVIRONMENT, pipeline=tgt_pipeline)


if __name__ == "__main__":
    main()
