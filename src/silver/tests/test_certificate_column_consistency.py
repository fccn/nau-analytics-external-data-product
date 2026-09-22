"""Verifies silver_certificates_generatedcertificate.py's SELECT
projection (SELECT_COLUMNS_SPEC) stays consistent for both
CERTIFICATE_VERIFY_UUID_MODE_ENABLED states (fccn/nau-technical#981).

No Spark session needed -- imports the actual spec the production
script uses (utils/certificate_column_specs.py). See
src/gold/tests/test_certificate_column_consistency.py for the gold-side
equivalent (3 interdependent lists there vs. this single SELECT list).
"""
from utils.column_spec import build_conditional_columns
from utils.certificate_column_specs import SELECT_COLUMNS_SPEC


def test_verify_uuid_only_present_when_enabled():
    for enabled in (True, False):
        select_columns = build_conditional_columns(SELECT_COLUMNS_SPEC, enabled=enabled)
        assert ("verify_uuid" in select_columns) == enabled


def test_disabled_state_matches_pre_981_column_set():
    pre_981_columns = {
        "id", "course_id", "grade", "key", "distinction", "status", "mode",
        "created_date", "modified_date", "error_reason", "user_id", "ingestion_date",
    }
    select_columns = build_conditional_columns(SELECT_COLUMNS_SPEC, enabled=False)
    assert set(select_columns) == pre_981_columns


def test_no_duplicate_or_missing_columns_either_state():
    base_columns = {
        "id", "course_id", "grade", "key", "distinction", "status", "mode",
        "created_date", "modified_date", "error_reason", "user_id", "ingestion_date",
    }
    for enabled in (True, False):
        select_columns = build_conditional_columns(SELECT_COLUMNS_SPEC, enabled=enabled)
        assert len(select_columns) == len(set(select_columns)), "duplicate column in SELECT"
        expected = base_columns | ({"verify_uuid"} if enabled else set())
        assert set(select_columns) == expected
