from pyspark.sql import DataFrame #type:ignore
from nau_analytics_data_product_utils_lib import Config,get_required_env,get_iceberg_spark_session #type: ignore
import pyspark.sql.functions as F #type:ignore
import argparse
import os
import logging
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
    parser.add_argument("--undesired_column", type = str,required= True, help = " the undesired column for a table")
    args = parser.parse_args()
    return args


def add_ingestion_metadata_column(df: DataFrame,table: str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.current_timestamp()).withColumn("source_name", F.lit(table))
    return tmp_df


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
    undesired_column = args.undesired_column

    ICEBERG_CATALOG_HOST = get_required_env("ICEBERG_CATALOG_HOST")
    ICEBERG_CATALOG_PORT = get_required_env("ICEBERG_CATALOG_PORT")
    ICEBERG_CATALOG_NAME = get_required_env("ICEBERG_CATALOG_NAME")
    ICEBERG_CATALOG_USER = get_required_env("ICEBERG_CATALOG_USER")
    ICEBERG_CATALOG_PASSWORD = get_required_env("ICEBERG_CATALOG_PASSWORD")
    ICEBERG_CATALOG_WAREHOUSE = get_required_env("ICEBERG_CATALOG_WAREHOUSE")
    ICEBERG_CATALOG_URI = f"jdbc:mysql://{ICEBERG_CATALOG_HOST}:{ICEBERG_CATALOG_PORT}/{ICEBERG_CATALOG_NAME}"
    ICEBERG_CATALOG_PASSWORD = base64.b64decode(ICEBERG_CATALOG_PASSWORD).decode()
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

            if table == "auth_user" and undesired_column and undesired_column in df.columns:
                raise Exception("THE undesired column stills in the dataframe")
            

            saveTable = f"bronze_local.entidades.{table}"
            df.write.format("iceberg").mode("append").saveAsTable(saveTable)

            logging.info(f"Data saved as Delta table to {saveTable}")

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise Exception(f"Pipeline fail {e}")
    
    spark.stop()


if __name__=="__main__":
    main()