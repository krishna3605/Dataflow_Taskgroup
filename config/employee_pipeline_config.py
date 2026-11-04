import configparser
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "employee_pipeline.properties"

def load_config():
    parser = configparser.ConfigParser()
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing properties file: {CONFIG_PATH}")
    parser.read(CONFIG_PATH, encoding="utf-8")
    return parser

CONFIG = load_config()

PROJECT_ID = CONFIG.get("GCP", "project_id")
GCS_LOCATION = CONFIG.get("GCP", "gcs_location")
BQ_LOCATION = CONFIG.get("GCP", "bigquery_location")
MANUAL_BUCKET = CONFIG.get("GCP", "manual_bucket")
SOURCE_BUCKET = CONFIG.get("GCP", "source_bucket")

CSV_SOURCE_OBJECT = CONFIG.get("FILES", "csv_source_object")
CSV_LANDING_OBJECT = CONFIG.get("FILES", "csv_landing_object")

STAGE_DATASET = CONFIG.get("DATASETS", "stage_dataset")
HISTORY_DATASET = CONFIG.get("DATASETS", "history_dataset")
AUDIT_DATASET = CONFIG.get("DATASETS", "audit_dataset")
ARCHIVE_DATASET = CONFIG.get("DATASETS", "archive_dataset")
AUTHORIZED_DATASET = CONFIG.get("DATASETS", "authorized_dataset")
VIEWS_DATASET = CONFIG.get("DATASETS", "views_dataset")

STAGE_TABLE_ID = CONFIG.get("TABLES", "stage_table")
HISTORY_TABLE_ID = CONFIG.get("TABLES", "history_table")
STAGE_AUDIT_TABLE_ID = CONFIG.get("TABLES", "stage_audit_table")
HISTORY_AUDIT_TABLE_ID = CONFIG.get("TABLES", "history_audit_table")
ARCHIVE_TABLE_ID = CONFIG.get("TABLES", "archive_table")
AUTHORIZED_TABLE_ID = CONFIG.get("TABLES", "authorized_table")
REPORTING_VIEW_ID = CONFIG.get("TABLES", "reporting_view")
