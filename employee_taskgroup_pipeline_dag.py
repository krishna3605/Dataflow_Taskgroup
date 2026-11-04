import configparser
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ---------------------------------------------------------------------
# BOOTSTRAP SETTINGS
# These values are required before the properties file can be read.
# They MUST match employee_pipeline.properties.
# ---------------------------------------------------------------------
PROJECT_ID = "smart-cursor"
MANUAL_BUCKET = "employee_taskgroup_bkt"
SOURCE_BUCKET = "employee_taskgroup_source_bkt"
GCS_LOCATION = "northamerica-south1"

PROPERTIES_OBJECT = "config/employee_pipeline.properties"
CSV_SOURCE_OBJECT = "usecase/emp_data.csv"
CSV_LANDING_OBJECT = "landing/emp_data.csv"

REQUIRED_SQL_FILES = [
    "validate_stage.sql",
    "stage_audit.sql",
    "history_merge.sql",
    "history_audit.sql",
    "archive.sql",
    "authorized.sql",
    "reporting_view.sql",
]

log = logging.getLogger(__name__)


def gcs():
    return GCSHook(gcp_conn_id="google_cloud_default")


def read_settings():
    """Read properties only after copy_all_files has copied them to SOURCE_BUCKET."""
    hook = gcs()
    properties_bytes = hook.download(
        bucket_name=SOURCE_BUCKET,
        object_name=PROPERTIES_OBJECT,
    )

    parser = configparser.ConfigParser()
    parser.read_string(properties_bytes.decode("utf-8-sig"))

    settings = {
        "project": parser.get("GCP", "project_id"),
        "gcs_location": parser.get("GCP", "gcs_location"),
        "bq_location": parser.get("GCP", "bigquery_location"),
        "manual_bucket": parser.get("GCP", "manual_bucket"),
        "source_bucket": parser.get("GCP", "source_bucket"),
        "csv_source": parser.get("FILES", "csv_source_object"),
        "csv_landing": parser.get("FILES", "csv_landing_object"),
    }

    for key in ("stage", "history", "audit", "archive", "authorized", "views"):
        settings[f"{key}_dataset"] = parser.get("DATASETS", f"{key}_dataset")

    for key in (
        "stage", "history", "stage_audit", "history_audit",
        "archive", "authorized",
    ):
        settings[f"{key}_table"] = parser.get("TABLES", f"{key}_table")

    # The supplied properties file uses reporting_view (not reporting_view_table).
    settings["reporting_view_table"] = parser.get(
        "TABLES", "reporting_view",
        fallback=parser.get("TABLES", "reporting_view_table", fallback=None),
    )
    if not settings["reporting_view_table"]:
        raise ValueError("Missing TABLES.reporting_view in employee_pipeline.properties")
    settings["reporting_view_table"] = settings["reporting_view_table"].strip()

    # Fail early with a clear message if the properties and DAG disagree.
    expected = {
        "project": PROJECT_ID,
        "gcs_location": GCS_LOCATION,
        "manual_bucket": MANUAL_BUCKET,
        "source_bucket": SOURCE_BUCKET,
        "csv_source": CSV_SOURCE_OBJECT,
        "csv_landing": CSV_LANDING_OBJECT,
    }
    for key, expected_value in expected.items():
        if settings[key] != expected_value:
            raise ValueError(
                f"Properties mismatch for {key}: expected '{expected_value}', "
                f"found '{settings[key]}'. Update the properties file or DAG constants."
            )

    return settings


def table(settings, dataset_key, table_key):
    return (
        f'{settings["project"]}.'
        f'{settings[dataset_key + "_dataset"]}.'
        f'{settings[table_key + "_table"]}'
    )


def bq_query(sql, settings):
    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
        location=settings["bq_location"],
    )
    job = hook.insert_job(
        configuration={"query": {"query": sql, "useLegacySql": False}},
        project_id=settings["project"],
        location=settings["bq_location"],
    )
    log.info("BigQuery query completed. Job ID: %s", job.job_id)
    return job


def read_sql(filename, settings, run_id):
    """Download a SQL file from SOURCE_BUCKET and replace its placeholders."""
    content = gcs().download(
        bucket_name=SOURCE_BUCKET,
        object_name=f"sql/{filename}",
    ).decode("utf-8-sig")

    replacements = {
        "__STAGE_TABLE__": table(settings, "stage", "stage"),
        "__HISTORY_TABLE__": table(settings, "history", "history"),
        "__STAGE_AUDIT_TABLE__": table(settings, "audit", "stage_audit"),
        "__HISTORY_AUDIT_TABLE__": table(settings, "audit", "history_audit"),
        "__ARCHIVE_TABLE__": table(settings, "archive", "archive"),
        "__AUTHORIZED_TABLE__": table(settings, "authorized", "authorized"),
        "__REPORTING_VIEW__": table(settings, "views", "reporting_view"),
        "__SOURCE_BUCKET__": SOURCE_BUCKET,
        "__LANDING_OBJECT__": settings["csv_landing"],
    }
    for old_value, new_value in replacements.items():
        content = content.replace(old_value, new_value)

    content = content.replace("{{ run_id }}", run_id)
    return content


# ---------------------------------------------------------------------
# DATA PREPARATION TASKS
# ---------------------------------------------------------------------

def create_source_bucket():
    """Create the source bucket if it does not already exist."""
    from google.cloud import storage

    client = storage.Client(project=PROJECT_ID)
    bucket = client.lookup_bucket(SOURCE_BUCKET)

    if bucket is None:
        client.create_bucket(SOURCE_BUCKET, location=GCS_LOCATION)
        log.info("Created source bucket gs://%s", SOURCE_BUCKET)
    else:
        log.info("Source bucket already exists: gs://%s", SOURCE_BUCKET)


def copy_all_files():
    """Copy all manual-bucket files to source bucket; rename CSV to landing path.

    IMPORTANT: Do not call read_settings() in this function. The properties file
    is in the manual bucket at this point and is copied by this function.
    """
    hook = gcs()

    # Check the input CSV using the manual bucket, not the source bucket.
    if not hook.exists(bucket_name=MANUAL_BUCKET, object_name=CSV_SOURCE_OBJECT):
        raise FileNotFoundError(
            f"CSV not found: gs://{MANUAL_BUCKET}/{CSV_SOURCE_OBJECT}"
        )

    objects = hook.list(bucket_name=MANUAL_BUCKET)
    if not objects:
        raise FileNotFoundError(f"No files found in gs://{MANUAL_BUCKET}")

    copied_objects = set()
    for object_name in objects:
        # Ignore directory marker objects, if any.
        if not object_name or object_name.endswith("/"):
            continue

        destination_object = (
            CSV_LANDING_OBJECT if object_name == CSV_SOURCE_OBJECT else object_name
        )

        hook.copy(
            source_bucket=MANUAL_BUCKET,
            source_object=object_name,
            destination_bucket=SOURCE_BUCKET,
            destination_object=destination_object,
        )
        copied_objects.add(destination_object)
        log.info(
            "Copied gs://%s/%s to gs://%s/%s",
            MANUAL_BUCKET, object_name, SOURCE_BUCKET, destination_object,
        )

    # Check that all runtime files needed by later tasks are now in SOURCE_BUCKET.
    required_objects = {
        PROPERTIES_OBJECT,
        CSV_LANDING_OBJECT,
        "config/employee_pipeline_config.py",
    }
    required_objects.update(f"sql/{filename}" for filename in REQUIRED_SQL_FILES)

    missing_objects = []
    for object_name in sorted(required_objects):
        if not hook.exists(bucket_name=SOURCE_BUCKET, object_name=object_name):
            missing_objects.append(object_name)

    if missing_objects:
        raise FileNotFoundError(
            "These required files were not copied to source bucket: "
            + ", ".join(f"gs://{SOURCE_BUCKET}/{name}" for name in missing_objects)
        )

    log.info("Copied and verified all required files in gs://%s", SOURCE_BUCKET)


def create_bq_resources():
    """Create datasets and empty tables required by the pipeline."""
    settings = read_settings()
    project = settings["project"]
    location = settings["bq_location"]

    dataset_ids = [
        settings["stage_dataset"],
        settings["history_dataset"],
        settings["audit_dataset"],
        settings["archive_dataset"],
        settings["authorized_dataset"],
        settings["views_dataset"],
    ]

    for dataset_id in dataset_ids:
        bq_query(
            f"CREATE SCHEMA IF NOT EXISTS `{project}.{dataset_id}` "
            f"OPTIONS(location='{location}');",
            settings,
        )
        log.info("Dataset is ready: %s.%s", project, dataset_id)

    stage_table = table(settings, "stage", "stage")
    history_table = table(settings, "history", "history")
    archive_table = table(settings, "archive", "archive")
    stage_audit_table = table(settings, "audit", "stage_audit")
    history_audit_table = table(settings, "audit", "history_audit")

    # Stage table: keep all incoming CSV columns as STRING.
    bq_query(f"""
        CREATE TABLE IF NOT EXISTS `{stage_table}` (
            emp_id STRING,
            first_name STRING,
            last_name STRING,
            email STRING,
            department STRING,
            job_title STRING,
            hire_date STRING,
            salary STRING,
            city STRING,
            state STRING,
            country STRING,
            employment_status STRING,
            manager_id STRING
        );
    """, settings)

    # History and archive tables: typed business data plus batch metadata.
    history_columns = """
        emp_id INT64 NOT NULL,
        first_name STRING,
        last_name STRING,
        email STRING,
        department STRING,
        job_title STRING,
        hire_date DATE,
        salary NUMERIC,
        city STRING,
        state STRING,
        country STRING,
        employment_status STRING,
        manager_id INT64,
        batch_id STRING NOT NULL,
        loaded_at TIMESTAMP NOT NULL
    """
    bq_query(
        f"CREATE TABLE IF NOT EXISTS `{history_table}` ({history_columns});",
        settings,
    )
    bq_query(
        f"CREATE TABLE IF NOT EXISTS `{archive_table}` ({history_columns});",
        settings,
    )

    audit_columns = """
        run_id STRING NOT NULL,
        batch_id STRING NOT NULL,
        table_name STRING NOT NULL,
        operation STRING NOT NULL,
        status STRING NOT NULL,
        row_count INT64,
        source_uri STRING,
        started_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP NOT NULL,
        message STRING
    """
    bq_query(
        f"CREATE TABLE IF NOT EXISTS `{stage_audit_table}` ({audit_columns});",
        settings,
    )
    bq_query(
        f"CREATE TABLE IF NOT EXISTS `{history_audit_table}` ({audit_columns});",
        settings,
    )

    log.info("All BigQuery datasets and required tables are ready")


# ---------------------------------------------------------------------
# DATA PROCESSING TASKS
# ---------------------------------------------------------------------

def load_stage():
    """Load the landing CSV into the all-STRING BigQuery stage table."""
    settings = read_settings()
    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
        location=settings["bq_location"],
    )

    job = hook.insert_job(
        configuration={
            "load": {
                "sourceUris": [
                    f'gs://{SOURCE_BUCKET}/{settings["csv_landing"]}'
                ],
                "destinationTable": {
                    "projectId": settings["project"],
                    "datasetId": settings["stage_dataset"],
                    "tableId": settings["stage_table"],
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_NEVER",
                "allowQuotedNewlines": True,
                "schema": {"fields": [
                    {"name": "emp_id", "type": "STRING"},
                    {"name": "first_name", "type": "STRING"},
                    {"name": "last_name", "type": "STRING"},
                    {"name": "email", "type": "STRING"},
                    {"name": "department", "type": "STRING"},
                    {"name": "job_title", "type": "STRING"},
                    {"name": "hire_date", "type": "STRING"},
                    {"name": "salary", "type": "STRING"},
                    {"name": "city", "type": "STRING"},
                    {"name": "state", "type": "STRING"},
                    {"name": "country", "type": "STRING"},
                    {"name": "employment_status", "type": "STRING"},
                    {"name": "manager_id", "type": "STRING"},
                ]},
            }
        },
        project_id=settings["project"],
        location=settings["bq_location"],
    )
    log.info("Stage load completed. BigQuery job ID: %s", job.job_id)


def run_sql_file(filename, **context):
    """Run one SQL file stored in gs://employee_source_bkt/sql/."""
    settings = read_settings()
    sql = read_sql(filename, settings, context["run_id"])
    bq_query(sql, settings)
    log.info("SQL file completed: %s", filename)


def export_archive(**context):
    """Export the archive table to a run-specific folder in the source bucket."""
    settings = read_settings()
    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
        location=settings["bq_location"],
    )

    job = hook.insert_job(
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": settings["project"],
                    "datasetId": settings["archive_dataset"],
                    "tableId": settings["archive_table"],
                },
                "destinationUris": [
                    f'gs://{SOURCE_BUCKET}/archive/emp_data/{context["ts_nodash"]}/part-*.csv'
                ],
                "destinationFormat": "CSV",
                "printHeader": True,
                "fieldDelimiter": ",",
            }
        },
        project_id=settings["project"],
        location=settings["bq_location"],
    )
    log.info("Archive export completed. BigQuery job ID: %s", job.job_id)


# ---------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "gcp_data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="employee_taskgroup_pipeline_dag",
    description="Employee GCS to BigQuery pipeline using TaskGroups and GCS SQL files",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["GCP", "GCS", "BigQuery", "Employee"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="data_preparation") as data_preparation:
        create_bucket_task = PythonOperator(
            task_id="create_source_bucket",
            python_callable=create_source_bucket,
        )
        copy_files_task = PythonOperator(
            task_id="copy_files_from_manual_bucket",
            python_callable=copy_all_files,
        )
        create_resources_task = PythonOperator(
            task_id="create_bigquery_datasets_and_tables",
            python_callable=create_bq_resources,
        )

        create_bucket_task >> copy_files_task >> create_resources_task

    with TaskGroup(group_id="data_processing") as data_processing:
        load_task = PythonOperator(
            task_id="load_csv_to_stage",
            python_callable=load_stage,
        )
        validate_task = PythonOperator(
            task_id="validate_stage",
            python_callable=run_sql_file,
            op_kwargs={"filename": "validate_stage.sql"},
        )
        stage_audit_task = PythonOperator(
            task_id="stage_audit",
            python_callable=run_sql_file,
            op_kwargs={"filename": "stage_audit.sql"},
        )
        history_merge_task = PythonOperator(
            task_id="history_merge",
            python_callable=run_sql_file,
            op_kwargs={"filename": "history_merge.sql"},
        )
        history_audit_task = PythonOperator(
            task_id="history_audit",
            python_callable=run_sql_file,
            op_kwargs={"filename": "history_audit.sql"},
        )
        archive_task = PythonOperator(
            task_id="archive_data",
            python_callable=run_sql_file,
            op_kwargs={"filename": "archive.sql"},
        )
        authorized_task = PythonOperator(
            task_id="create_authorized_data",
            python_callable=run_sql_file,
            op_kwargs={"filename": "authorized.sql"},
        )
        view_task = PythonOperator(
            task_id="create_reporting_view",
            python_callable=run_sql_file,
            op_kwargs={"filename": "reporting_view.sql"},
        )
        export_task = PythonOperator(
            task_id="export_archive_to_gcs",
            python_callable=export_archive,
        )

        load_task >> validate_task >> stage_audit_task >> history_merge_task
        history_merge_task >> history_audit_task
        history_audit_task >> [archive_task, authorized_task]
        archive_task >> export_task
        authorized_task >> view_task

    end = EmptyOperator(task_id="end")

    start >> data_preparation >> data_processing >> end
