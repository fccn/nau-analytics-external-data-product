from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from typing import List, Union, Optional,Tuple
from pyspark.sql import SparkSession #type: ignore


def add_ingestion_metadata_column(df: DataFrame,table: str,) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.current_timestamp()).withColumn("source_name", F.lit(table))
    return tmp_df

def read_data_from_sql(spark_session: SparkSession,query:str,jdbc_url:str,MYSQL_USER:str,MYSQL_SECRET:str) -> DataFrame:
    tmp_df = spark_session.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_SECRET) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", query) \
    .load()
    return tmp_df


