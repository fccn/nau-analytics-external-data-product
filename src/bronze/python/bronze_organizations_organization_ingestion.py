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
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}" 
    ENV = get_required_env("ENVIRONMENT") 
    spark = start_iceberg_session("ingeston_organizations_organization")
    spark.sql(f"""
    CREATE TABLE  bronze{ENV}.entidades.organizations_organization (
    id INT NOT NULL,
    created TIMESTAMP NOT NULL,
    modified TIMESTAMP NOT NULL,
    name STRING NOT NULL,
    short_name STRING NOT NULL,
    description STRING,
    logo STRING,
    active BOOLEAN NOT NULL,
    ingestion_date TIMESTAMP NOT NULL,
    source_name STRING NOT NULL
    )
    USING ICEBERG
    PARTITIONED BY (days(ingestion_date));
    """)
    table = "organizations_organization"
    start_date = get_max_timestamp_for_table(spark_session=spark,table_name=table,env=ENV)
    query = (F"""
    (
    SELECT
    *
    FROM 
        organizations_organization
    WHERE 
        created >='{start_date}' OR modified >='{start_date}'
    ) AS T1
    """)
    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]
    logging.info(f"executing query in db {query}")
    src_df = read_data_from_sql(spark_session=spark,query=query,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    df = add_ingestion_metadata_column(df=src_df,table=table,current_timestamp=current_timestamp)
    saveTable = f"bronze{ENV}.entidades.{table}"
    df.write.format("iceberg").mode("append").saveAsTable(saveTable)
    scr_full_df = read_data_from_sql(spark_session=spark,query=table,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    nr = validate_ingestion_values(spark_session=spark,src_table_df=scr_full_df,table_name=table,env=ENV)
    logging.info(f"number of record in table {nr}")
    update_ctrl_table(spark_session=spark,table_name=table,current_timestamp=current_timestamp,number_of_records=nr,env=ENV)

if __name__ == "__main__":
    main()