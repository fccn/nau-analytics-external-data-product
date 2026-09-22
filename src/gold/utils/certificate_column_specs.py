"""Column-list specs for gold_fact_certificate_d.py's
CERTIFICATE_VERIFY_UUID_MODE_ENABLED-gated columns (fccn/nau-technical#981).

Kept in their own, pyspark-free module (rather than inline in
gold_fact_certificate_d.py, which imports pyspark) so tests can import
them directly without needing pyspark installed or a Spark session --
see tests/test_certificate_column_consistency.py.

These are the three *plain-string* column lists -- fact_point_cols isn't
included here since its entries are pyspark Column expressions, not
strings, and genuinely need a pyspark/Spark context to build.
"""

# Mirrors fact_daily's final SELECT in gold_fact_certificate_d.py.
FACT_DAILY_COLS_SPEC = [
    "day_key", "certificate_cd",
    ("verify_uuid", True),
    "status",
    ("mode", True),
    "course_edition_key", "user_key", "org_key",
    "course_enrollment_start_date", "certificate_issue_date", "last_update_timestamp",
]

# Mirrors the MERGE's WHEN MATCHED THEN UPDATE SET clause.
UPDATE_SET_PARTS_SPEC = [
    ("t.verify_uuid = s.verify_uuid", True),
    "t.status = s.status",
    ("t.mode = s.mode", True),
    "t.course_edition_key = s.course_edition_key",
    "t.user_key = s.user_key",
    "t.org_key = s.org_key",
    "t.course_enrollment_start_date = s.course_enrollment_start_date",
    "t.certificate_issue_date = s.certificate_issue_date",
    "t.last_update_timestamp = s.last_update_timestamp",
]

# Mirrors the MERGE's WHEN NOT MATCHED THEN INSERT column list.
INSERT_COLS_SPEC = [
    "day_key", "certificate_cd",
    ("verify_uuid", True),
    "status",
    ("mode", True),
    "course_edition_key", "user_key", "org_key",
    "course_enrollment_start_date", "certificate_issue_date", "last_update_timestamp",
]
