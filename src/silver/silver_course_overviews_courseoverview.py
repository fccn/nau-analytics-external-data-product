from pyspark.sql import DataFrame #type:ignore
import pyspark.sql.functions as F #type:ignore
from nau_analytics_data_product_utils_lib import start_iceberg_session,get_required_env #type: ignore
from utils.bronze_utils_functions import update_ctrl_table,get_max_timestamp_for_table
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

    spark = start_iceberg_session("silver_course_overviews_courseoverview")

    #Variables
    tgt_layer = f"silver{ENVIRONMENT}"
    tgt_pipeline = "entidades"
    tgt_table_name = "course_overviews_courseoverview"

    src_layer = f"bronze{ENVIRONMENT}"
    src_pipeline = "entidades"
    src_table_name = "course_overviews_courseoverview"

    #Constants
    FIXED_COURSE_START_DATE = "2015-01-01 00:00:00"

    current_timestamp = spark.sql("SELECT current_timestamp() as c").first()["c"]

    last_execution_timestamp = get_max_timestamp_for_table(spark_session=spark,table_name=tgt_table_name,env=ENVIRONMENT)

    #Initial creation of the table (only useful for first run)
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt_layer}.{tgt_pipeline}.{tgt_table_name} (
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
        enrollment_start TIMESTAMP,
        enrollment_end TIMESTAMP,
        enrollment_domain STRING,
        invitation_only BOOLEAN NOT NULL,
        max_student_enrollments_allowed INT,
        announcement TIMESTAMP,
        catalog_visibility STRING,
        effort STRING,
        short_description STRING,
        org STRING NOT NULL,
        self_paced BOOLEAN NOT NULL,
        eligible_for_financial_aid BOOLEAN NOT NULL,
        language STRING,
        certificate_available_date TIMESTAMP,
        end_date TIMESTAMP,
        start_date TIMESTAMP,
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
        ingestion_date TIMESTAMP NOT NULL
    )
    USING ICEBERG
    """)

    #Load source dataframe
    df_src_data = spark.sql(f"""
        SELECT
            id,
            created,
            modified,
            version,
            _location,
            display_name,
            display_number_with_default,
            display_org_with_default,
            GREATEST(start, CAST('{FIXED_COURSE_START_DATE}' AS DATE)) start,
            end,
            advertised_start,
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
            enrollment_start,
            enrollment_end,
            enrollment_domain,
            invitation_only,
            max_student_enrollments_allowed,
            announcement,
            catalog_visibility,
            effort,
            short_description,
            org,
            self_paced,
            eligible_for_financial_aid,
            language,
            certificate_available_date,
            end_date,
            start_date,
            has_highlights,
            allow_proctoring_opt_out,
            enable_proctored_exams,
            proctoring_escalation_email,
            proctoring_provider,
            entrance_exam_enabled,
            entrance_exam_id,
            entrance_exam_minimum_score_pct,
            external_id,
            force_on_flexible_peer_openassessments,
            ingestion_date
          FROM {src_layer}.{src_pipeline}.{src_table_name}
         WHERE ingestion_date > '{last_execution_timestamp}'
        """)

    new_or_update_records = df_src_data.count()
    logging.info(f"Number of new or updated records = {new_or_update_records}")

    #Insert the new or updated records into the target table
    df_src_data.write.format("iceberg").mode("append").saveAsTable(f"{tgt_layer}.{tgt_pipeline}.{tgt_table_name}")

    #Finally, we update the control table with the number of records that were inserted or updated in this run.
    update_ctrl_table(spark_session=spark,table_name=tgt_table_name,current_timestamp=current_timestamp,number_of_records=new_or_update_records,env=ENVIRONMENT)

if __name__ == "__main__":
    main()
