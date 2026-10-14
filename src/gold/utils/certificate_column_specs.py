"""Column-list specs and schema-migration metadata for
gold_fact_certificate_d.py (fccn/nau-technical#981).

Kept in their own, pyspark-free module (rather than inline in
gold_fact_certificate_d.py, which imports pyspark) so tests can import
them directly without needing pyspark installed or a Spark session --
see tests/test_certificate_column_consistency.py and
tests/test_schema_migration_coverage.py.
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

# ---------------------------------------------------------------------
# Schema-migration metadata, shared between gold_fact_certificate_d.py's
# guarded ALTER TABLE loop and its own CREATE TABLE DDL comments -- also
# the single source of truth tests/test_schema_migration_coverage.py
# checks the DDL against, so a future column added to the DDL without a
# matching entry here (how `status` silently broke stage/dev before
# this fix) gets caught in CI instead.
# ---------------------------------------------------------------------

# Comment text, deduplicated between CREATE TABLE's initial DDL and the
# guarded ALTER TABLE below, so a future wording tweak only needs to be
# made in one place.
STATUS_COMMENT = "Certificate status (downloadable, notpassing, audit_passing, unverified, etc.)"
MODE_COMMENT = "Certificate mode (honor, verified, audit, etc.)"
VERIFY_UUID_COMMENT = (
    "Public certificate verification UUID (source: "
    "certificates_generatedcertificate.verify_uuid). Combined with the "
    "LMS base URL (CERTIFICATES_HTML_VIEW), yields the public certificate "
    "URL: https://lms.nau.edu.pt/certificates/<verify_uuid>"
)

# Columns present in the very first version of this table, before this
# repo's own schema-migration history begins -- never need a migration
# guard since there's no known deployed table without them.
BASE_COLUMNS = {
    "day_key", "certificate_cd", "course_edition_key", "user_key", "org_key",
    "course_enrollment_start_date", "certificate_issue_date", "last_update_timestamp",
}

# Every column added to this table's DDL after BASE_COLUMNS, in the
# order added: (name, sql_type, comment, gated_by_flag).
# `gated_by_flag=True` columns only get their guarded ALTER TABLE when
# CERTIFICATE_VERIFY_UUID_MODE_ENABLED is true; the rest are
# unconditional. A single list (rather than one hand-written
# `if col not in existing_columns: ALTER ...` block per column) means a
# future column addition only needs one new entry here.
COLUMN_MIGRATIONS = [
    ("status", "STRING", STATUS_COMMENT, False),
    ("verify_uuid", "STRING", VERIFY_UUID_COMMENT, True),
    ("mode", "STRING", MODE_COMMENT, True),
]
