from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from typing import List, Union, Optional,Tuple
from pyspark.sql import SparkSession #type: ignore


def add_ingestion_metadata_column(df: DataFrame,table: str,current_timestamp:str) -> DataFrame:
    tmp_df = df.withColumn("ingestion_date", F.lit(current_timestamp)).withColumn("source_name", F.lit(table))
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


def update_ctrl_table(spark_session: SparkSession, table_name:str,current_timestamp: str ,number_of_records:int) -> bool:
    try:
        spark_session.sql(f"""
            INSERT INTO  bronze_local.audit.pipeline_run_ctrl
            VALUES('entidades', '{table_name}', '{current_timestamp}', {number_of_records})
        """)
        return True 
    except Exception:
        return False
    
def get_max_timestamp_for_table(spark_session: SparkSession, table_name:str) -> str:
    try:
        START_DATE = spark_session.sql(f"SELECT NVL(max(last_execution_ts),'1900-01-01 00:00:00') as ts FROM bronze_local.audit.pipeline_run_ctrl WHERE table_name = '{table_name}' and pipeline = 'entidades'").first()["ts"]
    except Exception:
        raise ValueError("last execution ts not found on table")
    return START_DATE
def validate_ingestion_values(spark_session:SparkSession,src_table_df: DataFrame,table_name:str) -> int:
        src_count = src_table_df.count()
        tgt_count = spark_session.sql(f"SELECT DISTINCT id FROM bronze_local.entidades.{table_name}").count()
        if src_count != tgt_count:
            raise Exception(
                f"Count mismatch! Source = {src_count}, Target = {tgt_count}. Aborting pipeline."
            )
        return tgt_count