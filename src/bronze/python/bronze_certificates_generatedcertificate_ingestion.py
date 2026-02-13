from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore



#from utils.bronze_utils_functions import add_ingestion_metadata_column
import logging




logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def add_ingestion_metadata_column(df: DataFrame,table: str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.current_timestamp()).withColumn("source_name", F.lit(table))
    return tmp_df

spark = start_iceberg_session("ingeston_certificates_generatedcertificate")
query = ("""
( SELECT
    id ,
    course_id ,
    verify_uuid ,
    download_uuid ,
    download_url ,
    grade ,
    key ,
    distinction ,
    status ,
    mode ,
    name ,
    created_date ,
    modified_date ,
    error_reason ,
    user_id 
FROM 
    certificates_generatedcertificate ) AS TABLE


""")
MYSQL_DATABASE = get_required_env("MYSQL_DATABASE")
MYSQL_HOST = get_required_env("MYSQL_HOST")
MYSQL_PORT = get_required_env("MYSQL_PORT")
MYSQL_USER = get_required_env("MYSQL_USER")
MYSQL_SECRET = get_required_env("MYSQL_SECRET")
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_SECRET) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", query) \
    .load()
df = add_ingestion_metadata_column(df,"certificates_generatedcertificate")
saveTable = f"bronze_local.entidades.certificates_generatedcertificate"
df.write.format("iceberg").mode("append").saveAsTable(saveTable)

