"""Column-list spec for silver_certificates_generatedcertificate.py's
CERTIFICATE_VERIFY_UUID_MODE_ENABLED-gated verify_uuid column
(fccn/nau-technical#981).

Kept in its own, pyspark-free module (rather than inline in
silver_certificates_generatedcertificate.py, which imports pyspark) so
tests can import it directly without needing pyspark installed or a
Spark session -- see tests/test_certificate_column_consistency.py.
"""

# Mirrors the SELECT projection in silver_certificates_generatedcertificate.py.
SELECT_COLUMNS_SPEC = [
    "id", "course_id",
    ("verify_uuid", True),
    "grade", "key", "distinction", "status", "mode",
    "created_date", "modified_date", "error_reason", "user_id", "ingestion_date",
]
