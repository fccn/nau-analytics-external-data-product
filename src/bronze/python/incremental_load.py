from datetime import datetime,date, timedelta
from nau_analytics_data_product_utils_lib import Config,get_required_env,get_iceberg_spark_session #type: ignore
from pyspark.sql import SparkSession #type: ignore 
from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F # type: ignore
import pyspark.sql.types as T # type: ignore
import argparse
import logging
import os
from typing import List, Union, Optional,Tuple
import base64

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)





def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadatapath", type = str, required = True, help ="The S3 bucket that contains stores the metada for the process")
    parser.add_argument("--table", type = str, required = True, help ="The S3 bucket that contains stores the metada for the process")
    parser.add_argument("--first_ingestion_flag",type = int,default=0,help="flag to indicate if it is the first ingestion on regular ingestion")
    args = parser.parse_args()
    return args


def get_metadata(metadatapath: str, spark: SparkSession, table:str) -> str | bool:
    metadatapath = f"{metadatapath}/{table}/last_updated_date"
    customSchema = T.StructType([      
        T.StructField("table_name", T.StringType(), True),
        T.StructField("last_date", T.StringType(), True)
    ])
    row = spark.read.csv(metadatapath,schema=customSchema).filter( F.col("table_name") == table).first()
    if row is None or row["last_date"] is None:
        return False
    return str(row["last_date"])

def update_metadata(metadatapath: str ,spark: SparkSession,table:str,last_date:str) -> bool:
    try:
        tmp_df = spark.createDataFrame(
        [
            (table, last_date)
        ],
        T.StructType([T.StructField("table_name", T.StringType(), True),T.StructField("last_date", T.StringType(), True),]),)

        tmp_df.coalesce(1).write.format("csv").mode("overwrite").save(f"{metadatapath}/{table}/last_updated_date")
        return True 
    except Exception as e:
        logging.error(f"Exception {e}")
        return False
    

def add_ingestion_metadata_column(df: DataFrame,table: str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.current_timestamp()).withColumn("source_name", F.lit(table))
    return tmp_df


def get_all_dates_until_today(start_date: date):
    months = []
    year, month = start_date.year, start_date.month
    today = date.today()

    while (year < today.year) or (year == today.year and month <= today.month):
        months.append(date(year, month,1))
        month += 1
        if month > 12:
            month = 1
            year += 1

    return months

def full_initial_ingestion(spark: SparkSession, table: str, jdbc_url:str, MYSQL_USER:str, MYSQL_SECRET:str) -> Tuple[bool, str]:
    processing_dates = get_all_dates_until_today(date(2020,1,1))
    current_year = datetime.now().year
    current_month = datetime.now().month

    
    for d in processing_dates:
        query = f"(SELECT * FROM {table} WHERE YEAR(created) = {d.year} AND MONTH(created) = {d.month}) AS limited_table"
        logging.info(query)
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_SECRET) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", query) \
            .load() 
        df = df.withColumn("created", F.date_format("created", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        df = add_ingestion_metadata_column(df=df,table=table)
                

        if d == processing_dates[-1]:
            last_update =  datetime.now().isoformat()
            logging.info(f"Last ingestion from full tables: {d}")
        saveTale = f"bronze_local.entidades.{table}"
        df.write.format("iceberg").mode("append").saveAsTable(saveTale)
    return (True, last_update)


def delta_load(spark: SparkSession, jdbc_url:str, MYSQL_USER:str, MYSQL_SECRET:str,last_updated:str,table:str) -> Tuple[bool, str]:
    
    query = f"(SELECT * FROM {table} WHERE created >= '{last_updated}') AS limited_table"
    
    logging.info(query)            
    
    new_df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_SECRET) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", query) \
        .load()
    
    last_update =  datetime.now().isoformat()
    incremental_df = new_df.withColumn("created", F.date_format("created", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")) 
    incremental_df = add_ingestion_metadata_column(df=incremental_df,table=table)
    saveTale = f"bronze_local.entidades.{table}"
    incremental_df.write.format("iceberg").mode("append").saveAsTable(saveTale)
    
    return (True,last_update)


def main() -> None:

    MYSQL_DATABASE = get_required_env("MYSQL_DATABASE")
    MYSQL_HOST = get_required_env("MYSQL_HOST")
    MYSQL_PORT = get_required_env("MYSQL_PORT")
    MYSQL_USER = get_required_env("MYSQL_USER")
    MYSQL_SECRET = get_required_env("MYSQL_SECRET")
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

    S3_ACCESS_KEY = get_required_env("S3_ACCESS_KEY")
    S3_SECRET_KEY = get_required_env("S3_SECRET_KEY")
    S3_ENDPOINT = get_required_env("S3_ENDPOINT")
    
    ICEBERG_CATALOG_HOST = get_required_env("ICEBERG_CATALOG_HOST")
    ICEBERG_CATALOG_PORT = get_required_env("ICEBERG_CATALOG_PORT")
    ICEBERG_CATALOG_NAME = get_required_env("ICEBERG_CATALOG_NAME")
    ICEBERG_CATALOG_USER = get_required_env("ICEBERG_CATALOG_USER")
    ICEBERG_CATALOG_PASSWORD = get_required_env("ICEBERG_CATALOG_PASSWORD")
    ICEBERG_CATALOG_WAREHOUSE = get_required_env("ICEBERG_CATALOG_WAREHOUSE")
    ICEBERG_CATALOG_URI = f"jdbc:mysql://{ICEBERG_CATALOG_HOST}:{ICEBERG_CATALOG_PORT}/{ICEBERG_CATALOG_NAME}"
    ICEBERG_CATALOG_PASSWORD = base64.b64decode(ICEBERG_CATALOG_PASSWORD).decode()
    args = get_args()
    table = args.table
    metadata = args.metadatapath
    is_full_ingestion_flag = args.first_ingestion_flag

    icerberg_cfg = Config(
        app_name="Full table ingestion",
        s3_access_key=S3_ACCESS_KEY,
        s3_endpoint=S3_ENDPOINT,
        s3_secret_key=S3_SECRET_KEY,
        iceberg_catalog_uri=ICEBERG_CATALOG_URI,
        iceberg_catalog_user=ICEBERG_CATALOG_USER,
        iceberg_catalog_password=ICEBERG_CATALOG_PASSWORD,
        iceberg_catalog_warehouse=ICEBERG_CATALOG_WAREHOUSE

    )
    spark = get_iceberg_spark_session(cfg=icerberg_cfg)
    
    if is_full_ingestion_flag == 1:
        result = full_initial_ingestion(spark,table,jdbc_url,MYSQL_USER,MYSQL_SECRET)
        logging.info(result)
        if result[0]:
            update_metadata(metadatapath=metadata,spark=spark,table=table,last_date=result[1])
    
    if is_full_ingestion_flag == 0:
        last_date = get_metadata(metadatapath=metadata,spark=spark,table=table)
        if last_date == False:
            raise Exception("No date Found")
        
        last_date = str(last_date)
        result = delta_load(spark=spark,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET,last_updated=last_date,table=table)
        
        if result[0]:
             update_metadata(metadatapath=metadata,spark=spark,table=table,last_date=result[1])
    
    spark.stop()

if __name__=="__main__":
    main()