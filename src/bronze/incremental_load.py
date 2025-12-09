from datetime import datetime,date, timedelta
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F # type: ignore
import pyspark.sql.types as T # type: ignore
import argparse
import logging
import os
from typing import List, Union, Optional,Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)


def get_required_env(env_name:str) -> str:
    env_value = os.getenv(env_name)
    if env_value is None:
        raise ValueError(f"Environment variable {env_name} is not set")
    return env_value



def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--savepath", type = str,required= True, help = "The S3 bucket intended for the data to be stored")
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
    


def get_spark_session(S3_ACCESS_KEY: str,S3_SECRET_KEY: str , S3_ENDPOINT: str) -> SparkSession:
    
    spark = SparkSession.builder \
        .appName("incremental_table_ingestion") \
        .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.375.jar,/opt/spark/jars/delta-spark_2.12-3.2.1.jar,/opt/spark/jars/delta-storage-3.2.1.jar,/opt/spark/jars/delta-kernel-api-3.2.1.jar,/opt/spark/jars/mysql-connector-j-8.3.0.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark 






def add_ingestion_metadata_column(df: DataFrame,table: str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.current_timestamp()).withColumn("source_name", F.lit(table))
    return tmp_df

def add_date_partition_columns(df: DataFrame,column_name:str) -> DataFrame:
    df = df.withColumn("year", F.year(F.col(column_name)))\
        .withColumn("month", F.month(F.col(column_name)))\
        .withColumn("day", F.day(F.col(column_name)))
    return df

def get_all_dates_until_today(start_date: date):
    dates = []
    current = start_date
    today = date.today()
    while current <= today:
        dates.append(current)
        current += timedelta(days=1)
    return dates

def full_initial_ingestion(spark: SparkSession, table: str, savepath: str, jdbc_url:str, MYSQL_USER:str, MYSQL_SECRET:str) -> Tuple[bool, str]:
    processing_dates = get_all_dates_until_today(date(2024,1,1))
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day
    path = f"{savepath}/{table}"
    
    for d in processing_dates:
        query = f"(SELECT * FROM {table} WHERE YEAR(created) = {d.year} AND MONTH(created) = {d.month} AND DAY(created) ={d.day}) AS limited_table"
        logging.info(query)
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_SECRET) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", query) \
            .load() 
        df = df.withColumn("created", F.date_format("created", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        df = add_date_partition_columns(df=df, column_name="created")
        df = add_ingestion_metadata_column(df=df,table=table)
                

        if (d.year == current_year and d.month == current_month and d.day == current_day):
            last_update =  datetime.now().isoformat()
        
        df.write.format("delta").mode("append").partitionBy("year", "month","day").save(path)
    return (True, last_update)


def delta_load(spark: SparkSession, jdbc_url:str, MYSQL_USER:str, MYSQL_SECRET:str,last_updated:str,table:str,savepath: str) -> Tuple[bool, str]:
    
    path = f"{savepath}/{table}"
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
    incremental_df = add_date_partition_columns(df=incremental_df,column_name="created")
    incremental_df = add_ingestion_metadata_column(df=incremental_df,table=table)
    
    incremental_df = incremental_df.withColumn("ingestion_date", F.current_timestamp()).withColumn("source_name", F.lit(table))

                # Append new partitions directly
    incremental_df.write.format("delta").mode("append").partitionBy("year", "month","day").save(path)    
    
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
    
    args = get_args()
    savepath = args.savepath
    metadata = args.metadatapath
    is_full_ingestion_flag = args.first_ingestion_flag
    table = args.table


    spark = get_spark_session(S3_ACCESS_KEY,S3_SECRET_KEY,S3_ENDPOINT)
    
    if is_full_ingestion_flag == 1:
        result = full_initial_ingestion(spark,table,savepath,jdbc_url,MYSQL_USER,MYSQL_SECRET)
        logging.info(result)
        if result[0]:
            update_metadata(metadatapath=metadata,spark=spark,table=table,last_date=result[1])
    
    if is_full_ingestion_flag == 0:
        last_date = get_metadata(metadatapath=metadata,spark=spark,table=table)
        if last_date == False:
            raise Exception("No date Found")
        
        last_date = str(last_date)
        result = delta_load(spark=spark,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET,last_updated=last_date,table=table,savepath=savepath)
        
        if result[0]:
             update_metadata(metadatapath=metadata,spark=spark,table=table,last_date=result[1])
    
    spark.stop()

if __name__=="__main__":
    main()