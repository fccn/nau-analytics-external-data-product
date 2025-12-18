from dataclasses import dataclass

@dataclass
class Config:
    s3_access_key: str
    s3_secret_key: str
    s3_endpoint: str
    app_name: str
    iceberg_catalog_uri: str
    iceberg_catalog_user: str
    iceberg_catalog_password: str
    iceberg_catalog_warehouse: str