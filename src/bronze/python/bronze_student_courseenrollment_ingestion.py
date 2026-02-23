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
    spark = start_iceberg_session("ingeston_student_courseenrollment")
    table = "student_courseenrollment"
    start_date = get_max_timestamp_for_table(spark_session=spark,table_name=table)
    query = (F"""
    (
    SELECT
    id,
    course_id, 
    created,
    is_active, 
    mode, 
    user_id, 
    SHA1(
        CONCAT_WS('||',
            id,
            course_id, 
            created,
            is_active, 
            mode, 
            user_id 
        )
    ) AS row_hash
    FROM 
        student_courseenrollment
    ) AS T1
    """)
    saveTable = f"bronze_local.entidades.{table}"
    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]
    logging.info(f"executing query in db {query}")
    src_df = read_data_from_sql(spark_session=spark,query=query,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    src_df = add_ingestion_metadata_column(df=src_df,table=table,current_timestamp=current_timestamp)
    tgt_table = spark.sql(f"SELECT * FROM {saveTable}")
    df = get_delta_dataframe(src_table_df=src_df,tgt_table=tgt_table)
    df.write.format("iceberg").mode("append").saveAsTable(saveTable)
    validate_table_that_delete_lines(src_table_df=src_df,tgt_table=tgt_table)
    nr = tgt_table.count()
    logging.info(f"number of record in table {nr}")
    update_ctrl_table(spark_session=spark,table_name=table,current_timestamp=current_timestamp,number_of_records=nr)

if __name__ == "__main__":
    main()