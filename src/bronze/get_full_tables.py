from pyspark.sql import SparkSession #type:ignore
from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
import argparse
import os
import logging
from typing import List, Union, Optional,Tuple
from utils.utils import Utils

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

utils_obj = Utils()


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--savepath", type = str,required= True, help = "The S3 bucket intended for the data to be stored")
    parser.add_argument("--undesired_column", type = str,required= True, help = " the undesired column for a table")
    args = parser.parse_args()
    return args


def add_ingestion_metadata_column(df: DataFrame,table: str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.current_timestamp()).withColumn("source_name", F.lit(table))
    return tmp_df

def add_date_partition_columns(df: DataFrame,column_name:str) -> DataFrame:
    df = df.withColumn("year", F.year(F.col(column_name)))\
        .withColumn("month", F.month(F.col(column_name)))\
        .withColumn("day",F.day(column_name))
    return df

def main() -> None:
    MYSQL_DATABASE = utils_obj.get_required_env("MYSQL_DATABASE")
    MYSQL_HOST = utils_obj.get_required_env("MYSQL_HOST")
    MYSQL_PORT = utils_obj.get_required_env("MYSQL_PORT")
    MYSQL_USER = utils_obj.get_required_env("MYSQL_USER")
    MYSQL_SECRET = utils_obj.get_required_env("MYSQL_SECRET")
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

    S3_ACCESS_KEY = utils_obj.get_required_env("S3_ACCESS_KEY")
    S3_SECRET_KEY = utils_obj.get_required_env("S3_SECRET_KEY")
    S3_ENDPOINT = utils_obj.get_required_env("S3_ENDPOINT")
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

    spark = utils_obj.get_spark_session(S3_ACCESS_KEY=S3_ACCESS_KEY,S3_SECRET_KEY=S3_SECRET_KEY,S3_ENDPOINT=S3_ENDPOINT,app_name="Full table ingestion")
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

            df.write.format("iceberg").mode("append").partitionBy("year", "month","day").save(output_path)

            logging.info(f"Data saved as Delta table to {output_path}")

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise Exception(f"Pipeline fail {e}")
    
    spark.stop()


if __name__=="__main__":
    main()