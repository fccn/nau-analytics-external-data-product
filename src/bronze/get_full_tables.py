from pyspark.sql import SparkSession #type:ignore
import pyspark.sql.functions as F #type:ignore
import os
import logging

logger = logging.getLogger(__name__)
###################################################################################
#                           GET MYSQL CREDENTIALS                                 #
###################################################################################

MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_SECRET = os.getenv("MYSQL_SECRET")
jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")


###################################################################################
#                           GET S3 CREDENTIALS                                    #
###################################################################################
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_SAVEPATH = os.getenv("S3_SAVEPATH")

TABLES = [
"course_overviews_courseoverview", 
"student_courseenrollment", 
"certificates_generatedcertificate",
"student_courseaccessrole"
]

for table in TABLES:
    logging.info(f"getting table {table}")
    try:
    
        spark = SparkSession.builder \
        .appName("MyApp") \
        .config("spark.jars", ",".join([
            f"{SPARK_HOME}/jars/hadoop-aws-3.3.4.jar",
            f"{SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.375.jar",
            f"{SPARK_HOME}/jars/delta-spark_2.12-3.2.1.jar",
            f"{SPARK_HOME}/jars/delta-storage-3.2.1.jar",
            f"{SPARK_HOME}/jars/delta-kernel-api-3.2.1.jar",
            f"{SPARK_HOME}/jars/mysql-connector-j-8.3.0.jar",
        ]))\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_SECRET) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table) \
            .load()

        df = df.withColumn("ingestion_date", F.current_timestamp()) \
               .withColumn("source_name", F.lit(table))
        output_path = f"{S3_SAVEPATH}/{table}"
        
        df.write.format("delta").mode("overwrite").save(output_path)

        logger.info(f"Data saved as Delta table to {output_path}")
        spark.stop()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
