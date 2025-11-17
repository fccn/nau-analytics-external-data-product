from pyspark.sql import SparkSession #type:ignore
import pyspark.sql.functions as F #type:ignore
import argparse
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--savepath", type = str,required= True, help = "The S3 bucket intended for the data to be stored")
    parser.add_argument("--undesired_column", type = str,required= True, help = " the undesired column for a table")
    args = parser.parse_args()
    return args

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

###################################################################################
#                           GET MYSQL CREDENTIALS                                 #
###################################################################################
def main() -> None:
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = os.getenv("MYSQL_PORT")
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_SECRET = os.getenv("MYSQL_SECRET")
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"



    ###################################################################################
    #                           GET S3 CREDENTIALS                                    #
    ###################################################################################
    S3_ACCESS_KEY = str(os.getenv("S3_ACCESS_KEY"))
    S3_SECRET_KEY = str(os.getenv("S3_SECRET_KEY"))
    S3_ENDPOINT = str(os.getenv("S3_ENDPOINT"))

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
    "auth_user"
    ]

    for table in TABLES:

        logging.info(f"getting table {table}")
        try:
        
            spark = get_spark_session(S3_ACCESS_KEY=S3_ACCESS_KEY,S3_SECRET_KEY=S3_SECRET_KEY,S3_ENDPOINT=S3_ENDPOINT)

            df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_SECRET) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", table) \
                .load()
            if table == "auth_user":
                df = df.drop(undesired_column)

            df = df.withColumn("ingestion_date", F.current_timestamp()) \
                   .withColumn("source_name", F.lit(table))
            if table == "auth_user" and undesired_column and undesired_column in df.columns:
                raise Exception("THE undesired column stills in the dataframe")
            output_path = f"{S3_SAVEPATH}/{table}"

            df.write.format("delta").mode("append").save(output_path)

            logging.info(f"Data saved as Delta table to {output_path}")

        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
    spark.stop()


if __name__=="__main__":
    main()