"""Airflow 2.10.x DAG: synthetic healthcare records -> audited SQLite tables.

No real patient data and no external network integrations are used. The small
synthetic fixture is returned through XCom for demonstration only.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from clinical_etl.pipeline import (
    audit_database,
    extract_bundle,
    load_sqlite,
    transform_resources,
    validate_tables,
)

ROOT = Path(os.environ.get("CLINICAL_ETL_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
SOURCE = ROOT / "data" / "synthetic_fhir_bundle.json"
DESTINATION = ROOT / "runtime" / "clinical_demo.sqlite"


@dag(
    dag_id="synthetic_clinical_etl",
    description="Load, validate, model and audit synthetic clinical FHIR-shaped data",
    schedule=None,  # Manual trigger; set a schedule only for a real upstream data source.
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # SQLite is the target; avoid concurrent full refreshes.
    tags=["portfolio", "healthcare", "data-quality", "taskflow"],
)
def synthetic_clinical_etl():
    @task(retries=1, retry_delay=timedelta(seconds=30))
    def extract() -> list[dict]:
        return extract_bundle(SOURCE)

    @task()
    def transform(raw: list[dict]) -> dict:
        return transform_resources(raw)

    @task()
    def quality_gate(tables: dict) -> dict:
        counts = validate_tables(tables)
        print(f"Pre-load quality checks PASS: {counts}")
        return tables

    @task()
    def load(validated_tables: dict) -> dict[str, int]:
        return load_sqlite(validated_tables, DESTINATION)

    @task()
    def audit(expected_counts: dict[str, int]) -> dict:
        report = audit_database(DESTINATION, expected_counts)
        print(f"Post-load audit: {report}")
        return report

    audit(load(quality_gate(transform(extract()))))


synthetic_clinical_etl()
