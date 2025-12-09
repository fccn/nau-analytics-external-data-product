from pyspark.sql import SparkSession #type:ignore
from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
import argparse
import os
import logging
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
    parser.add_argument("--undesired_column", type = str,required= True, help = " the undesired column for a table")
    args = parser.parse_args()
    return args

def get_spark_session(S3_ACCESS_KEY: str,S3_SECRET_KEY: str , S3_ENDPOINT: str) -> SparkSession:
    
    spark = SparkSession.builder \
        .appName("full_table_ingestion") \
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
        .withColumn("day",F.day(column_name))
    return df

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
    S3_SAVEPATH = args.savepath
    undesired_column = args.undesired_column

    TABLES = [
    "course_overviews_courseoverview", 
    "student_courseenrollment", 
    "certificates_generatedcertificate",
    "student_courseaccessrole",
    "auth_userprofile",
    "student_userattribute",
    "organizations_organization",
    "organizations_historicalorganization",
    "grades_persistentcoursegrade",
    "auth_user"
    ]

    spark = get_spark_session(S3_ACCESS_KEY=S3_ACCESS_KEY,S3_SECRET_KEY=S3_SECRET_KEY,S3_ENDPOINT=S3_ENDPOINT)
    for table in TABLES:

        logging.info(f"getting table {table}")
        try:

            df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_SECRET) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", table) \
                .load()
            if table == "auth_user":
                df = df.drop(undesired_column)

            df = add_ingestion_metadata_column(df=df,table=table)
            df = add_date_partition_columns(df,"ingestion_date")
            if table == "auth_user" and undesired_column and undesired_column in df.columns:
                raise Exception("THE undesired column stills in the dataframe")
            
            output_path = f"{S3_SAVEPATH}/{table}"

            df.write.format("delta").mode("append").partitionBy("year", "month","day").save(output_path)

            logging.info(f"Data saved as Delta table to {output_path}")

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise Exception(f"Pipeline fail {e}")
    
    spark.stop()


if __name__=="__main__":
    main()