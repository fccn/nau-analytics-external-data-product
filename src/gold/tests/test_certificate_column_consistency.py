"""Verifies the SELECT-projection daily-grain columns
(FACT_DAILY_COLS_SPEC), the MERGE's WHEN MATCHED UPDATE SET columns
(UPDATE_SET_PARTS_SPEC), and its WHEN NOT MATCHED INSERT column list
(INSERT_COLS_SPEC) in gold_fact_certificate_d.py stay mutually
consistent for both CERTIFICATE_VERIFY_UUID_MODE_ENABLED states
(fccn/nau-technical#981).

This is the "lightweight unit test (no Spark session needed)" asked for
in review -- imports the actual specs the production script uses
(utils/certificate_column_specs.py), not a separately hand-maintained
copy, so a future edit to one of those specs is automatically checked
against this test rather than silently drifting.

Does not cover fact_point_cols (the fourth gated list in
gold_fact_certificate_d.py) -- its entries are pyspark Column
expressions, not plain strings, and need a real pyspark/Spark context
to construct, which this test deliberately avoids.
"""
from utils.column_spec import build_conditional_columns
from utils.certificate_column_specs import (
    FACT_DAILY_COLS_SPEC,
    UPDATE_SET_PARTS_SPEC,
    INSERT_COLS_SPEC,
)


def _update_set_target_columns(update_set_parts):
    """Extracts "verify_uuid" out of "t.verify_uuid = s.verify_uuid"."""
    return [part.split("=")[0].strip().removeprefix("t.") for part in update_set_parts]


def test_insert_cols_match_fact_daily_cols_for_both_flag_states():
    # insert_cols feeds the MERGE's INSERT (...) column list, built
    # from fact_daily's own SELECT -- these two must always list exactly
    # the same columns, in the same order, or the INSERT ... VALUES
    # positional mapping silently shifts.
    for enabled in (True, False):
        fact_daily_cols = build_conditional_columns(FACT_DAILY_COLS_SPEC, enabled=enabled)
        insert_cols = build_conditional_columns(INSERT_COLS_SPEC, enabled=enabled)
        assert insert_cols == fact_daily_cols, (
            f"insert_cols/fact_daily_cols mismatch at enabled={enabled}: "
            f"{insert_cols} != {fact_daily_cols}"
        )


def test_update_set_parts_cover_every_non_key_fact_daily_column():
    # day_key/certificate_cd are the MERGE's ON join keys, never part of
    # UPDATE SET -- every other fact_daily_cols column must have a
    # matching "t.<col> = s.<col>" entry in UPDATE SET for both flag
    # states, or an update would silently leave that column stale.
    join_keys = {"day_key", "certificate_cd"}
    for enabled in (True, False):
        fact_daily_cols = build_conditional_columns(FACT_DAILY_COLS_SPEC, enabled=enabled)
        update_set_parts = build_conditional_columns(UPDATE_SET_PARTS_SPEC, enabled=enabled)
        updated_columns = set(_update_set_target_columns(update_set_parts))
        expected = set(fact_daily_cols) - join_keys
        assert updated_columns == expected, (
            f"UPDATE SET coverage mismatch at enabled={enabled}: "
            f"{updated_columns} != {expected}"
        )


def test_verify_uuid_and_mode_only_present_when_enabled():
    for enabled in (True, False):
        fact_daily_cols = build_conditional_columns(FACT_DAILY_COLS_SPEC, enabled=enabled)
        insert_cols = build_conditional_columns(INSERT_COLS_SPEC, enabled=enabled)
        update_set_parts = build_conditional_columns(UPDATE_SET_PARTS_SPEC, enabled=enabled)
        updated_columns = set(_update_set_target_columns(update_set_parts))

        assert ("verify_uuid" in fact_daily_cols) == enabled
        assert ("mode" in fact_daily_cols) == enabled
        assert ("verify_uuid" in insert_cols) == enabled
        assert ("mode" in insert_cols) == enabled
        assert ("verify_uuid" in updated_columns) == enabled
        assert ("mode" in updated_columns) == enabled


def test_disabled_state_matches_pre_981_column_set():
    # While disabled, the gated columns must never appear anywhere --
    # confirms the flag-off behavior stays byte-for-byte equivalent to
    # the pre-#981 script (no partial/leaked gating).
    pre_981_columns = {
        "day_key", "certificate_cd", "status",
        "course_edition_key", "user_key", "org_key",
        "course_enrollment_start_date", "certificate_issue_date", "last_update_timestamp",
    }
    fact_daily_cols = build_conditional_columns(FACT_DAILY_COLS_SPEC, enabled=False)
    insert_cols = build_conditional_columns(INSERT_COLS_SPEC, enabled=False)
    assert set(fact_daily_cols) == pre_981_columns
    assert set(insert_cols) == pre_981_columns
