"""Column-list spec for silver_certificates_generatedcertificate.py's
CERTIFICATE_VERIFY_UUID_MODE_ENABLED-gated verify_uuid column
(fccn/nau-technical#981).

Kept in its own, pyspark-free module (rather than inline in
silver_certificates_generatedcertificate.py, which imports pyspark) so
tests can import it directly without needing pyspark installed or a
Spark session -- see tests/test_certificate_column_consistency.py.
"""

# Mirrors the SELECT projection in silver_certificates_generatedcertificate.py.
#
# verify_uuid is normalized with NULLIF(..., '') -- bronze declares it
# NOT NULL, but ~2,415 rows in dev use an empty string as a sentinel for
# failed/incomplete certs (confirmed live 2026-10-12: 0 actual NULLs,
# 2,415 empty strings) rather than true NULL. Normalizing here means
# every downstream consumer (this table, gold, the backfill script) can
# rely on a single "IS NULL" check for "no verification URL", instead of
# also having to special-case ''.
SELECT_COLUMNS_SPEC = [
    "id", "course_id",
    ("NULLIF(verify_uuid, '') AS verify_uuid", True),
    "grade", "key", "distinction", "status", "mode",
    "created_date", "modified_date", "error_reason", "user_id", "ingestion_date",
]
