"""Regression test for fccn/nau-technical#981: a column added to
gold_fact_certificate_d.py's CREATE TABLE DDL without a matching entry
in COLUMN_MIGRATIONS (or BASE_COLUMNS) breaks the MERGE the moment this
script runs against any real table created before that column existed
-- exactly what happened to `status` (added by a0b7a20, long before
COLUMN_MIGRATIONS existed, with no guard at all; prod's live table
happened to already have it from an out-of-band change, masking the bug
there, but stage's and dev's didn't).

Parses the DDL's column names directly out of gold_fact_certificate_d.py
(regex over the literal source -- the DDL itself is raw SQL text inside
an f-string, not something pyspark-free code can introspect without a
real Spark session) and asserts each one is accounted for by either
BASE_COLUMNS (present since before this repo's migration history began)
or COLUMN_MIGRATIONS (added later, with an explicit guarded ALTER).
"""
import os
import re

from utils.certificate_column_specs import BASE_COLUMNS, COLUMN_MIGRATIONS, FACT_DAILY_COLS_SPEC
from utils.column_spec import build_conditional_columns


def _ddl_column_names():
    this_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(this_dir, "..", "gold_fact_certificate_d.py")
    with open(script_path, encoding="utf-8") as f:
        source = f.read()

    ddl_match = re.search(
        r"CREATE TABLE IF NOT EXISTS \{tgt_layer\}\.\{tgt_pipeline\}\.\{tgt_table_name\} \((.*?)\n\s*\)\s*\n\s*USING iceberg",
        source,
        re.DOTALL,
    )
    assert ddl_match, (
        "Could not locate the CREATE TABLE block in gold_fact_certificate_d.py "
        "-- did its structure change? Update this test's regex to match."
    )
    ddl_body = ddl_match.group(1)

    # Each column line looks like "name TYPE COMMENT '...',"; take the
    # first token of every non-blank, non-comment ("-- ...") line as the
    # column name. Deliberately NOT matched against a fixed list of known
    # SQL types -- a future column typed INT/BOOLEAN/DECIMAL(10,2)/DOUBLE
    # etc. would silently fall through such a list, which is the exact
    # regression class this test exists to catch. Taking the first token
    # of every column-shaped line instead works for any SQL type.
    column_names = []
    for line in ddl_body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        column_names.append(line.split()[0])

    assert column_names, (
        "No columns parsed out of the CREATE TABLE block -- did its "
        "formatting change? Update this test's parsing to match."
    )
    return column_names


def test_every_ddl_column_is_covered_by_base_or_migrations():
    ddl_columns = set(_ddl_column_names())
    migration_columns = {name for name, *_ in COLUMN_MIGRATIONS}
    covered = BASE_COLUMNS | migration_columns
    uncovered = ddl_columns - covered
    assert not uncovered, (
        f"Column(s) {uncovered} are in gold_fact_certificate_d.py's CREATE TABLE "
        "DDL but not in BASE_COLUMNS or COLUMN_MIGRATIONS "
        "(utils/certificate_column_specs.py) -- any real table created before "
        "this column existed will fail its MERGE with UNRESOLVED_COLUMN the "
        "first time this script runs against it (exactly what happened with "
        "`status`, fccn/nau-technical#981). Add an entry to COLUMN_MIGRATIONS "
        "(or BASE_COLUMNS if this column has always existed, with no known "
        "deployed table missing it)."
    )


def test_migration_columns_have_required_shape():
    for entry in COLUMN_MIGRATIONS:
        assert len(entry) == 4, (
            f"COLUMN_MIGRATIONS entry {entry} must be a "
            "(name, sql_type, comment, gated_by_flag) 4-tuple"
        )
        name, sql_type, comment, gated_by_flag = entry
        assert isinstance(name, str) and name
        assert isinstance(sql_type, str) and sql_type
        assert isinstance(comment, str) and comment
        assert isinstance(gated_by_flag, bool)


def test_base_and_migration_columns_do_not_overlap():
    migration_columns = {name for name, *_ in COLUMN_MIGRATIONS}
    overlap = BASE_COLUMNS & migration_columns
    assert not overlap, (
        f"Column(s) {overlap} appear in both BASE_COLUMNS and "
        "COLUMN_MIGRATIONS -- each column should only be listed in one."
    )


def test_every_fact_daily_column_is_covered_by_base_or_migrations():
    # The original bug was `t.status` unresolved in the MERGE itself, not
    # just in the DDL -- a column added to FACT_DAILY_COLS_SPEC (and so to
    # the MERGE's SELECT/UPDATE SET/INSERT) but forgotten in the DDL (and
    # therefore in BASE_COLUMNS/COLUMN_MIGRATIONS) would still break the
    # MERGE even though test_every_ddl_column_is_covered_by_base_or_migrations
    # above only checks the DDL side.
    fact_daily_columns = set(build_conditional_columns(FACT_DAILY_COLS_SPEC, enabled=True))
    migration_columns = {name for name, *_ in COLUMN_MIGRATIONS}
    covered = BASE_COLUMNS | migration_columns
    uncovered = fact_daily_columns - covered
    assert not uncovered, (
        f"Column(s) {uncovered} are referenced by FACT_DAILY_COLS_SPEC (and so "
        "by the MERGE's SELECT/UPDATE SET/INSERT) but not covered by "
        "BASE_COLUMNS or COLUMN_MIGRATIONS -- the MERGE would fail with "
        "UNRESOLVED_COLUMN against any table missing that column."
    )


def test_migration_comments_have_no_apostrophe():
    # Each COLUMN_MIGRATIONS comment is interpolated into a SQL string
    # literal delimited by single quotes (both the DDL and the guarded
    # ALTER TABLE in gold_fact_certificate_d.py). An apostrophe in the
    # comment text would break that literal.
    for name, _sql_type, comment, _gated_by_flag in COLUMN_MIGRATIONS:
        assert "'" not in comment, (
            f"COLUMN_MIGRATIONS comment for {name!r} contains an apostrophe, "
            "which would break the single-quoted SQL literal it's "
            f"interpolated into: {comment!r}"
        )
