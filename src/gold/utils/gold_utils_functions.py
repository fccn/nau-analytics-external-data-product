from pyspark.sql import DataFrame,Window #type:ignore
import pyspark.sql.functions as F #type:ignore
from typing import List, Union, Optional,Tuple
from pyspark.sql import SparkSession #type: ignore
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def update_ctrl_table(spark_session: SparkSession, table_name:str,current_timestamp: str ,number_of_records:int,env: str) -> bool:
    try:
        spark_session.sql(f"""
            INSERT INTO  gold{env}.audit.pipeline_run_ctrl
            VALUES('entidades', '{table_name}', '{current_timestamp}', {number_of_records})
        """)
        return True 
    except Exception:
        return False
    
def get_max_timestamp_for_table(spark_session: SparkSession, table_name:str,env:str) -> str:
    try:
        query = f"SELECT NVL(max(last_execution_ts),'1900-01-01 00:00:00') as ts FROM gold{env}.audit.pipeline_run_ctrl WHERE table_name = '{table_name}' and pipeline = 'entidades'"
        logging.info(query)
        START_DATE = spark_session.sql(query).first()["ts"]
    except Exception:
        raise ValueError("last execution ts not found on table")
    return START_DATE
