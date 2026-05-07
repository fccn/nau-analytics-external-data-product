from pyspark.sql import DataFrame  # type:ignore
import pyspark.sql.functions as F  # type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session, get_required_env  # type: ignore
from utils.gold_utils_functions import update_ctrl_table
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    ENVIRONMENT = get_required_env("ENVIRONMENT")

    spark = start_iceberg_session("gold_dim_course_access_role")

    #Variables
    tgt_layer = f"gold{ENVIRONMENT}"
    tgt_pipeline = "audit"
    tgt_table_name = "dim_course_access_role"

    src_layer = f"silver{ENVIRONMENT}"
    src_pipeline = "audit"
    src_table_name = "student_courseaccessrole"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    # Current-state lookup of role assignments. Full refresh daily — no
    # history is kept; the table answers "who has which role today?".
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
            access_role_cd        INT       COMMENT 'Source row id from edxapp.student_courseaccessrole',
            user_cd               INT       COMMENT 'User id (matches edxapp auth_user.id / dim_user.user_cd)',
            course_id             STRING    COMMENT 'Course identifier (course-v1/...)',
            org                   STRING    COMMENT 'Organization short code as stored in source',
            role                  STRING    COMMENT 'Role granted (e.g. instructor, staff, beta_testers)',
            last_update_timestamp TIMESTAMP COMMENT 'ETL timestamp'
        )
        USING ICEBERG
        TBLPROPERTIES (
            'write.parquet.compression-codec'            = 'zstd',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'       = '10'
        )
    """)

    spark.sql(f"""
        INSERT OVERWRITE TABLE {tgt_layer}.{tgt_pipeline}.{tgt_table_name}
        SELECT id        AS access_role_cd,
               user_id   AS user_cd,
               course_id,
               org,
               role,
               current_timestamp() AS last_update_timestamp
          FROM {src_layer}.{src_pipeline}.{src_table_name}
    """)

    nr = spark.sql(f"SELECT COUNT(*) AS c FROM {tgt_layer}.{tgt_pipeline}.{tgt_table_name}").first()["c"]
    logging.info(f"Number of records in gold {tgt_table_name} = {nr}")

    update_ctrl_table(spark_session=spark, table_name=tgt_table_name, current_timestamp=current_timestamp, number_of_records=nr, env=ENVIRONMENT, pipeline=tgt_pipeline)


if __name__ == "__main__":
    main()
