import os
from pyspark.sql import SparkSession #type:ignore

class utils:
    def __init__(self) -> None:
        pass
    
    def get_required_env(self,env_name:str) -> str:
        env_value = os.getenv(env_name)
        if env_value is None:
            raise ValueError(f"Environment variable {env_name} is not set")
        return env_value
    
    def get_spark_session(self,S3_ACCESS_KEY: str,S3_SECRET_KEY: str , S3_ENDPOINT: str,app_name:str) -> SparkSession:
    
        spark = SparkSession.builder \
            .appName(app_name) \
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