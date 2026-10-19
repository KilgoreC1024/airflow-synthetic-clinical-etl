"""In-browser harness for the demo site: runs the pipeline's functions as the DAG's five tasks.

index.html loads this into Pyodide next to the unmodified clinical_etl package. Each task calls the same
function, in the same order, as dags/synthetic_clinical_etl.py. A failed task marks the tasks after it
upstream_failed, as Airflow does. There is no scheduler here: no retries, XCom, or metadata database.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import sqlite3
import time
from contextlib import closing, redirect_stdout
from pathlib import Path

from clinical_etl.pipeline import (
    audit_database,
    extract_bundle,
    load_sqlite,
    transform_resources,
    validate_tables,
)

SOURCE = Path("/demo/data/synthetic_fhir_bundle.json")
WORK = Path("/demo/work/bundle.json")
DB = Path("/demo/runtime/clinical_demo.sqlite")
TABLES = ("patients", "encounters", "conditions")

# The condition summary from src/clinical_etl/inspect.py.
SUMMARY_SQL = """
    SELECT c.code, c.description, COUNT(*) AS diagnosis_count,
           COUNT(DISTINCT c.patient_id) AS distinct_patients
    FROM conditions c
    JOIN encounters e ON e.id = c.encounter_id
    JOIN patients p ON p.id = e.patient_id
    GROUP BY c.code, c.description
    ORDER BY diagnosis_count DESC, c.code
"""


def _resources(bundle: dict, kind: str) -> list[dict]:
    return [entry["resource"] for entry in bundle["entry"] if entry["resource"]["resourceType"] == kind]


# Each fault mirrors a test in tests/test_pipeline.py.
def _missing_patient(bundle: dict) -> None:
    _resources(bundle, "Encounter")[0]["subject"]["reference"] = "Patient/pat-DOES-NOT-EXIST"


def _cross_patient(bundle: dict) -> None:
    _resources(bundle, "Condition")[0]["subject"]["reference"] = "Patient/pat-004"


def _duplicate_id(bundle: dict) -> None:
    bundle["entry"].append(copy.deepcopy(bundle["entry"][0]))


def _bad_timestamp(bundle: dict) -> None:
    _resources(bundle, "Encounter")[0]["period"]["start"] = "yesterday"


def _unknown_type(bundle: dict) -> None:
    bundle["entry"].append({"resource": {"resourceType": "Medication", "id": "med-001"}})


MUTATIONS = {
    "clean": None,
    "missing_patient": _missing_patient,
    "cross_patient": _cross_patient,
    "duplicate_id": _duplicate_id,
    "bad_timestamp": _bad_timestamp,
    "unknown_type": _unknown_type,
    "storage_failure": None,
}

FAILURE_TRIGGER = """
    CREATE TRIGGER IF NOT EXISTS simulate_failure BEFORE INSERT ON conditions
    WHEN NEW.id = 'con-002'
    BEGIN SELECT RAISE(ABORT, 'simulated insertion failure'); END;
"""


def _has_tables() -> bool:
    if not DB.exists():
        return False
    with closing(sqlite3.connect(DB)) as con:
        found = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    return set(TABLES) <= found


def snapshot() -> dict:
    """Current database contents, plus a fingerprint to tell whether a run changed them."""
    if not _has_tables():
        return {"exists": False, "fingerprint": None}
    with closing(sqlite3.connect(DB)) as con:
        tables = {}
        for name in TABLES:
            cursor = con.execute(f"SELECT * FROM {name} ORDER BY id")
            tables[name] = {"columns": [d[0] for d in cursor.description], "rows": cursor.fetchall()}
        summary = con.execute(SUMMARY_SQL).fetchall()
    digest = hashlib.sha256(json.dumps(tables, sort_keys=True).encode()).hexdigest()[:12]
    return {
        "exists": True,
        "fingerprint": digest,
        "counts": {name: len(tables[name]["rows"]) for name in TABLES},
        "tables": tables,
        "summary": summary,
    }


def _source_text(scenario: str, custom_json: str | None) -> str:
    if scenario == "custom":
        return custom_json or ""
    bundle = json.loads(SOURCE.read_text(encoding="utf-8"))
    mutate = MUTATIONS[scenario]
    if mutate:
        mutate(bundle)
    return json.dumps(bundle, indent=2)


def _describe(task_id: str, value) -> str:
    if task_id == "extract":
        return f"Returned {len(value)} resources"
    if task_id in ("transform", "quality_gate"):
        return "Returned " + ", ".join(f"{name}={len(rows)}" for name, rows in value.items())
    if task_id == "load":
        return f"Loaded {value}"
    return ""


def run(scenario: str, custom_json: str | None = None) -> str:
    """Run the five tasks once and return the result as JSON for the page."""
    if scenario == "storage_failure" and not _has_tables():
        raise RuntimeError("Run the clean scenario first so there is a refresh to protect.")
    WORK.parent.mkdir(parents=True, exist_ok=True)
    WORK.write_text(_source_text(scenario, custom_json), encoding="utf-8")
    before = snapshot()

    if scenario == "storage_failure":
        with closing(sqlite3.connect(DB)) as con:
            con.executescript(FAILURE_TRIGGER)

    def quality_gate(tables: dict) -> dict:
        counts = validate_tables(tables)
        print(f"Pre-load quality checks PASS: {counts}")
        return tables

    def audit(expected_counts: dict) -> dict:
        report = audit_database(DB, expected_counts)
        print(f"Post-load audit: {report}")
        return report

    steps = [
        ("extract", lambda _: extract_bundle(WORK)),
        ("transform", transform_resources),
        ("quality_gate", quality_gate),
        ("load", lambda tables: load_sqlite(tables, DB)),
        ("audit", audit),
    ]
    tasks, value, failed, error = [], None, None, None
    try:
        for task_id, fn in steps:
            if failed:
                tasks.append({"task_id": task_id, "state": "upstream_failed", "ms": None, "log": []})
                continue
            out = io.StringIO()
            started = time.perf_counter()
            try:
                with redirect_stdout(out):
                    value = fn(value)
                state, log = "success", out.getvalue().splitlines() + [_describe(task_id, value)]
            except Exception as exc:  # the task fails; Airflow would record the same exception
                failed, error = task_id, f"{type(exc).__name__}: {exc}"
                state, log = "failed", out.getvalue().splitlines() + [error]
            ms = round((time.perf_counter() - started) * 1000, 1)
            tasks.append({"task_id": task_id, "state": state, "ms": ms, "log": [line for line in log if line]})
    finally:
        if scenario == "storage_failure":
            with closing(sqlite3.connect(DB)) as con:
                con.executescript("DROP TRIGGER IF EXISTS simulate_failure;")

    after = snapshot()
    return json.dumps({
        "scenario": scenario,
        "state": "failed" if failed else "success",
        "failed_task": failed,
        "error": error,
        "tasks": tasks,
        "changed": before["fingerprint"] != after["fingerprint"],
        "db": after,
    }, default=str)
