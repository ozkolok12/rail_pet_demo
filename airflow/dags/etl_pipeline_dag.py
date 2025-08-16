import os
import glob
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.transform.rail_transformer import RailDataETL
from scripts.transform.load_to_dwh import LoadToDWH

# -----------------------------------------------------------------------------
# Constants for demo mode
# -----------------------------------------------------------------------------
RAW_DIR = os.getenv("RAW_FILES_DIR", "/data/raw_demo")

default_args = {
    "owner": "roman",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# -----------------------------------------------------------------------------
# Helper callable: collect demo files
# -----------------------------------------------------------------------------
def collect_demo_files(**context) -> list[str]:
    """
    Scans RAW_DIR for demo files (CSV/XLSX/XLSB) and returns their paths.
    This replaces the Gmail downloader from the original project.
    """
    patterns = ("*.csv", "*.xlsx", "*.xlsb")
    files = []
    for pat in patterns:
        files.extend(glob.glob(str(Path(RAW_DIR) / pat)))
    files = sorted({str(Path(p).resolve()) for p in files})
    if not files:
        print(f"[DEMO] No files found in {RAW_DIR}. Please put demo files there.")
    else:
        print(f"[DEMO] Found {len(files)} file(s) in {RAW_DIR}:")
        for f in files:
            print(f" - {f}")
    return files

# -----------------------------------------------------------------------------
# DAG definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Demo ETL pipeline: local files -> Staging -> DWH",
    schedule=None,  # no automatic scheduling; trigger manually
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["etl", "demo"],
) as dag:

    task_collect = PythonOperator(
        task_id="collect_demo_files",
        python_callable=collect_demo_files,
        provide_context=True,
    )

    task_transform = PythonOperator(
        task_id="load_into_staging",
        python_callable=lambda: RailDataETL().run(),
    )

    task_merge = PythonOperator(
        task_id="merge_into_dwh",
        python_callable=lambda: LoadToDWH().run(),
    )

    task_collect >> task_transform >> task_merge