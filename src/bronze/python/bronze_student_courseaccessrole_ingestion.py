from pyspark.sql import DataFrame  # type:ignore
import pyspark.sql.functions as F  # type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
from utils.bronze_utils_functions import add_ingestion_metadata_column, read_data_from_sql, update_ctrl_table
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
    spark = start_iceberg_session("ingestion_student_courseaccessrole")

    #Variables
    src_schema = "edxapp"
    src_table_name = "student_courseaccessrole"

    tgt_layer = f"bronze{ENVIRONMENT}"
    tgt_pipeline = "audit"
    tgt_table_name = "student_courseaccessrole"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    # Full-refresh audit table: holds only the latest snapshot of role
    # assignments. No partitioning — table is small and replaced in full daily.
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
        id INT NOT NULL,
        org STRING NOT NULL,
        course_id STRING NOT NULL,
        role STRING NOT NULL,
        user_id INT NOT NULL,
        row_hash STRING NOT NULL,
        ingestion_date TIMESTAMP NOT NULL,
        source_name STRING NOT NULL
    )
    USING ICEBERG
    """)

    src_query = f"""
        (SELECT id,
                org,
                course_id,
                role,
                user_id,
                SHA1(
                    CONCAT_WS('||',
                        id,
                        org,
                        course_id,
                        role,
                        user_id
                    )
                ) AS row_hash
           FROM {src_schema}.{src_table_name}) AS source_table
    """

    logging.info(f"executing query in db {src_query}")

    src_df = read_data_from_sql(spark_session=spark, query=src_query, jdbc_url=jdbc_url, MYSQL_USER=MYSQL_USER, MYSQL_SECRET=MYSQL_SECRET)
    src_df = add_ingestion_metadata_column(df=src_df, table=tgt_table_name, current_timestamp=current_timestamp)

    src_df.createOrReplaceTempView("src_student_courseaccessrole")

    # Atomic full refresh: replace all rows with today's snapshot.
    spark.sql(f"""
        INSERT OVERWRITE TABLE {tgt_layer}.{tgt_pipeline}.{tgt_table_name}
        SELECT id,
               org,
               course_id,
               role,
               user_id,
               row_hash,
               ingestion_date,
               source_name
          FROM src_student_courseaccessrole
    """)

    nr = spark.sql(f"SELECT COUNT(*) AS c FROM {tgt_layer}.{tgt_pipeline}.{tgt_table_name}").first()["c"]
    logging.info(f"number of records in table {nr}")

    update_ctrl_table(spark_session=spark, table_name=tgt_table_name, current_timestamp=current_timestamp, number_of_records=nr, env=ENVIRONMENT, pipeline=tgt_pipeline)


if __name__ == "__main__":
    main()
