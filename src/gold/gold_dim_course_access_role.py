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
    # user_username / user_email are denormalised from the active SCD2 row of
    # dim_user so Superset RLS can join on Superset identity (username/email)
    # without an extra hop.
    # course_cd / edition come from the active SCD2 row of dim_course_edition,
    # resolved from the source course_id (course-v1 string). is_org_wide is
    # true for org-level roles (course_id NULL or empty) — used by Superset
    # RLS to short-circuit course matching.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
            access_role_cd        INT       COMMENT 'Source row id from edxapp.student_courseaccessrole',
            user_cd               INT       COMMENT 'User id (matches edxapp auth_user.id / dim_user.user_cd)',
            user_username         STRING    COMMENT 'Username from the active SCD2 row of dim_user',
            user_email            STRING    COMMENT 'Email from the active SCD2 row of dim_user',
            course_id             STRING    COMMENT 'Course-v1 identifier (course edition) as stored in source; empty for org-wide roles',
            course_cd             STRING    COMMENT 'Course display code from dim_course_edition (active SCD2 row); NULL for org-wide roles',
            edition               STRING    COMMENT 'Edition (RUN component) from dim_course_edition (active SCD2 row); NULL for org-wide roles',
            org_cd                STRING    COMMENT 'Organization short code as stored in source',
            is_org_wide           BOOLEAN   COMMENT 'True when role is granted at org level (course_id is NULL/empty)',
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
        SELECT car.id                                                AS access_role_cd,
               car.user_id                                           AS user_cd,
               du.username                                           AS user_username,
               du.email                                              AS user_email,
               car.course_id,
               dce.display_number                                    AS course_cd,
               dce.edition                                           AS edition,
               car.org                                               AS org_cd,
               (car.course_id IS NULL OR car.course_id = '')         AS is_org_wide,
               car.role,
               current_timestamp()                                   AS last_update_timestamp
          FROM       {src_layer}.{src_pipeline}.{src_table_name}    car
          LEFT JOIN  {tgt_layer}.entidades.dim_user                 du
                 ON  du.user_cd       = car.user_id
                AND  du.key_end_date  = CAST('9999-12-31' AS TIMESTAMP)
          LEFT JOIN  {tgt_layer}.entidades.dim_course_edition       dce
                 ON  dce.course_edition_cd = car.course_id
                AND  dce.key_end_date      IS NULL
    """)

    nr = spark.sql(f"SELECT COUNT(*) AS c FROM {tgt_layer}.{tgt_pipeline}.{tgt_table_name}").first()["c"]
    logging.info(f"Number of records in gold {tgt_table_name} = {nr}")

    update_ctrl_table(spark_session=spark, table_name=tgt_table_name, current_timestamp=current_timestamp, number_of_records=nr, env=ENVIRONMENT, pipeline=tgt_pipeline)


if __name__ == "__main__":
    main()
