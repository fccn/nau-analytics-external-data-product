CREATE TABLE bronze_local.entidades.course_overviews_courseoverview (
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