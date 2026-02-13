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
    spark = start_iceberg_session("ingeston_course_overviews_courseoverview")
    table = "course_overviews_courseoverview"
    start_date = get_max_timestamp_for_table(spark_session=spark,table_name=table)
    query = (F"""
    (
        SELECT
SELECT
    id,
    created,
    modified,
    version,

    _location,

    display_name,
    display_number_with_default,
    display_org_with_default,

    start,
    "end",

    advertised_start,
    course_image_url,
    social_sharing_url,
    end_of_course_survey_url,

    certificates_display_behavior,
    certificates_show_before_end,
    cert_html_view_enabled,
    has_any_active_web_certificate,

    cert_name_short,
    cert_name_long,

    lowest_passing_grade,
    days_early_for_beta,

    mobile_available,
    visible_to_staff_only,

    _pre_requisite_courses_json,

    enrollment_start,
    enrollment_end,
    enrollment_domain,

    invitation_only,
    max_student_enrollments_allowed,

    announcement,

    catalog_visibility,
    course_video_url,
    effort,
    short_description,

    org,

    self_paced,
    marketing_url,

    eligible_for_financial_aid,
    language,

    certificate_available_date,
    end_date,
    start_date,

    banner_image_url,

    has_highlights,

    allow_proctoring_opt_out,
    enable_proctored_exams,

    proctoring_escalation_email,
    proctoring_provider,

    entrance_exam_enabled,
    entrance_exam_id,
    entrance_exam_minimum_score_pct,

    external_id,

    force_on_flexible_peer_openassessments
    FROM 
        course_overviews_courseoverview 
    WHERE 
        created >='{start_date}' OR modified >='{start_date}'
    ) AS T1
    """)
    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]
    src_df = read_data_from_sql(spark_session=spark,query=query,jdbc_url=jdbc_url,MYSQL_USER=MYSQL_USER,MYSQL_SECRET=MYSQL_SECRET)
    df = add_ingestion_metadata_column(df=src_df,table=table)
    update_ctrl_table(spark_session=spark,table_name=table,current_timestamp=current_timestamp,number_of_records=nr)
    saveTable = f"bronze_local.entidades.{table}"
    df.write.format("iceberg").mode("append").saveAsTable(saveTable)
    nr = validate_ingestion_values(spark_session=spark,src_table_df=df,table_name=table)

if __name__ == "__main__":
    main()