from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.bronze_utils_functions import add_ingestion_metadata_column,read_data_from_sql,update_ctrl_table,get_max_timestamp_for_table,validate_ingestion_values
import logging




logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def main():
    MYSQL_DATABASE = get_required_env("MYSQL_DATABASE")
    MYSQL_HOST = get_required_env("MYSQL_HOST")
    MYSQL_PORT = get_required_env("MYSQL_PORT")
    MYSQL_USER = get_required_env("MYSQL_USER")
    MYSQL_SECRET = get_required_env("MYSQL_SECRET")
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}" 
    ENV = get_required_env("ENVIRONMENT")
    spark = start_iceberg_session("ingeston_course_overviews_courseoverview")
    logging.info(f"Spark session created {spark}")
    table = "course_overviews_courseoverview"
    start_date = get_max_timestamp_for_table(spark_session=spark,table_name=table, env=ENV)
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze{ENV}.entidades.{table}(
             id STRING NOT NULL,

            created TIMESTAMP NOT NULL,
            modified TIMESTAMP NOT NULL,
            version INT NOT NULL,

            _location STRING NOT NULL,

            display_name STRING,
            display_number_with_default STRING NOT NULL,
            display_org_with_default STRING NOT NULL,

            start TIMESTAMP,
            end TIMESTAMP,

            advertised_start STRING,
            course_image_url STRING NOT NULL,
            social_sharing_url STRING,
            end_of_course_survey_url STRING,

            certificates_display_behavior STRING,
            certificates_show_before_end BOOLEAN NOT NULL,
            cert_html_view_enabled BOOLEAN NOT NULL,
            has_any_active_web_certificate BOOLEAN NOT NULL,

            cert_name_short STRING NOT NULL,
            cert_name_long STRING NOT NULL,

            lowest_passing_grade DECIMAL(5,2),
            days_early_for_beta DOUBLE,

            mobile_available BOOLEAN NOT NULL,
            visible_to_staff_only BOOLEAN NOT NULL,

            _pre_requisite_courses_json STRING NOT NULL,

            enrollment_start TIMESTAMP,
            enrollment_end TIMESTAMP,
            enrollment_domain STRING,

            invitation_only BOOLEAN NOT NULL,
            max_student_enrollments_allowed INT,

            announcement TIMESTAMP,

            catalog_visibility STRING,
            course_video_url STRING,
            effort STRING,
            short_description STRING,

            org STRING NOT NULL,

            self_paced BOOLEAN NOT NULL,
            marketing_url STRING,

            eligible_for_financial_aid BOOLEAN NOT NULL,
            language STRING,

            certificate_available_date TIMESTAMP,
            end_date TIMESTAMP,
            start_date TIMESTAMP,

            banner_image_url STRING NOT NULL,

            has_highlights BOOLEAN,

            allow_proctoring_opt_out BOOLEAN NOT NULL,
            enable_proctored_exams BOOLEAN NOT NULL,

            proctoring_escalation_email STRING,
            proctoring_provider STRING,

            entrance_exam_enabled BOOLEAN NOT NULL,
            entrance_exam_id STRING NOT NULL,
            entrance_exam_minimum_score_pct DOUBLE NOT NULL,

            external_id STRING,

            force_on_flexible_peer_openassessments BOOLEAN NOT NULL,

            ingestion_date TIMESTAMP NOT NULL,
            source_name STRING NOT NULL
        )
        USING ICEBERG
        PARTITIONED BY (days(ingestion_date))
    """)
    query = (F"""
    (
SELECT
    *
    FROM 
        course_overviews_courseoverview 
    WHERE 
        created >='{start_date}' OR modified >='{start_date}'
    ) AS T1
    """)
    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]
    logging.info(f"executing query in db {query}")
    src_df = read_data_from_sql(spark_session=spark,query=query,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    df = add_ingestion_metadata_column(df=src_df,table=table,current_timestamp=current_timestamp)
    saveTable = f"bronze{ENV}.entidades.{table}"
    df.write.format("iceberg").mode("append").saveAsTable(saveTable)
    scr_full_df = read_data_from_sql(spark_session=spark,query=table,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    nr = validate_ingestion_values(spark_session=spark,src_table_df=scr_full_df,table_name=table,   env=ENV)
    logging.info(f"number of record in table {nr}")
    update_ctrl_table(spark_session=spark,table_name=table,current_timestamp=current_timestamp,number_of_records=nr,env=ENV)
if __name__ == "__main__":
    main()