"""Pure-Python ETL functions, separate from Airflow for easy unit testing.

Demonstrates a full-refresh ETL for *synthetic*, FHIR-shaped JSON. This is a
portfolio example, not a complete FHIR validator or clinical production system.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any


class DataQualityError(ValueError):
    """Raised when an input record fails a required data-quality check."""


def extract_bundle(path: str | Path) -> list[dict[str, Any]]:
    """Extract small synthetic resources from a FHIR-shaped Bundle."""
    path = Path(path)
    with path.open(encoding="utf-8") as file:
        bundle = json.load(file)
    if not isinstance(bundle, dict) or bundle.get("resourceType") != "Bundle":
        raise DataQualityError("Input must have resourceType=Bundle")
    if bundle.get("type") != "collection":
        raise DataQualityError("Only collection Bundles are supported")
    entries = bundle.get("entry")
    if not isinstance(entries, list) or not entries:
        raise DataQualityError("Bundle must have a nonempty entry list")
    resources = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict) or not isinstance(entry.get("resource"), dict):
            raise DataQualityError(f"entry[{i}]: expected resource object")
        resources.append(entry["resource"])
    return resources


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DataQualityError(f"Missing or empty field: {field}")
    return value.strip()


def _nested(record: dict, path: tuple[str | int, ...], label: str) -> Any:
    value: Any = record
    try:
        for item in path:
            value = value[item]
    except (KeyError, IndexError, TypeError) as error:
        raise DataQualityError(f"Missing or invalid field: {label}") from error
    return value


def _ref(value: Any, resource_type: str, field: str) -> str:
    text = _text(value, field)
    prefix = resource_type + "/"
    if not text.startswith(prefix) or len(text) == len(prefix):
        raise DataQualityError(f"{field}: expected reference {resource_type}/<id>")
    return text[len(prefix):]


def _date(value: Any, field: str) -> str:
    text = _text(value, field)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as error:
        raise DataQualityError(f"{field}: invalid YYYY-MM-DD date") from error
    if parsed.isoformat() != text:
        raise DataQualityError(f"{field}: expected YYYY-MM-DD date")
    return text


def _datetime(value: Any, field: str) -> str:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise DataQualityError(f"{field}: invalid ISO 8601 datetime") from error
    if parsed.tzinfo is None:
        raise DataQualityError(f"{field}: datetime must include a timezone")
    return parsed.isoformat()


def transform_resources(resources: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    """Transform supported resource types into simple relational tables.

    Deliberately reject unknown resource types instead of silently discarding
    records; this makes source-contract changes visible.
    """
    tables: dict[str, list[dict[str, str]]] = {
        "patients": [], "encounters": [], "conditions": [],
    }
    for i, resource in enumerate(resources):
        if not isinstance(resource, dict):
            raise DataQualityError(f"resource[{i}] is not an object")
        kind = _text(resource.get("resourceType"), f"resource[{i}].resourceType")
        ident = _text(resource.get("id"), f"resource[{i}].id")
        if kind == "Patient":
            gender = _text(resource.get("gender"), f"Patient/{ident}.gender")
            if gender not in {"male", "female", "other", "unknown"}:
                raise DataQualityError(f"Patient/{ident}: invalid gender code")
            tables["patients"].append({
                "id": ident,
                "birth_date": _date(resource.get("birthDate"), f"Patient/{ident}.birthDate"),
                "gender": gender,
            })
        elif kind == "Encounter":
            tables["encounters"].append({
                "id": ident,
                "patient_id": _ref(
                    _nested(resource, ("subject", "reference"), f"Encounter/{ident}.subject.reference"),
                    "Patient", f"Encounter/{ident}.subject.reference",
                ),
                "status": _text(resource.get("status"), f"Encounter/{ident}.status"),
                "start_at": _datetime(
                    _nested(resource, ("period", "start"), f"Encounter/{ident}.period.start"),
                    f"Encounter/{ident}.period.start",
                ),
            })
        elif kind == "Condition":
            tables["conditions"].append({
                "id": ident,
                "patient_id": _ref(
                    _nested(resource, ("subject", "reference"), f"Condition/{ident}.subject.reference"),
                    "Patient", f"Condition/{ident}.subject.reference",
                ),
                "encounter_id": _ref(
                    _nested(resource, ("encounter", "reference"), f"Condition/{ident}.encounter.reference"),
                    "Encounter", f"Condition/{ident}.encounter.reference",
                ),
                "code": _text(
                    _nested(resource, ("code", "coding", 0, "code"), f"Condition/{ident}.code.coding[0].code"),
                    f"Condition/{ident}.code.coding[0].code",
                ),
                "description": _text(
                    _nested(resource, ("code", "coding", 0, "display"), f"Condition/{ident}.code.coding[0].display"),
                    f"Condition/{ident}.code.coding[0].display",
                ),
            })
        else:
            raise DataQualityError(f"Unsupported resource type: {kind}")
    return tables


def validate_tables(tables: dict[str, list[dict[str, str]]]) -> dict[str, int]:
    """Reject duplicates, missing parents, and cross-patient encounters BEFORE load."""
    expected = ("patients", "encounters", "conditions")
    for table in expected:
        if not tables.get(table):
            raise DataQualityError(f"Required table has no rows: {table}")
        ids = [row["id"] for row in tables[table]]
        if len(ids) != len(set(ids)):
            raise DataQualityError(f"Duplicate IDs found in {table}")
    patients = {row["id"] for row in tables["patients"]}
    encounters = {row["id"]: row for row in tables["encounters"]}
    for encounter in tables["encounters"]:
        if encounter["patient_id"] not in patients:
            raise DataQualityError(f"Encounter/{encounter['id']}: missing patient")
    for condition in tables["conditions"]:
        if condition["patient_id"] not in patients:
            raise DataQualityError(f"Condition/{condition['id']}: missing patient")
        encounter = encounters.get(condition["encounter_id"])
        if encounter is None:
            raise DataQualityError(f"Condition/{condition['id']}: missing encounter")
        if encounter["patient_id"] != condition["patient_id"]:
            raise DataQualityError(f"Condition/{condition['id']}: encounter belongs to another patient")
    return {table: len(tables[table]) for table in expected}


_SCHEMA = """
CREATE TABLE IF NOT EXISTS patients (
    id TEXT PRIMARY KEY,
    birth_date TEXT NOT NULL,
    gender TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS encounters (
    id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL REFERENCES patients(id),
    status TEXT NOT NULL,
    start_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_encounters_patient_id ON encounters(patient_id);
CREATE TABLE IF NOT EXISTS conditions (
    id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL REFERENCES patients(id),
    encounter_id TEXT NOT NULL REFERENCES encounters(id),
    code TEXT NOT NULL,
    description TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_conditions_encounter_id ON conditions(encounter_id);
"""


def load_sqlite(tables: dict[str, list[dict[str, str]]], db_path: str | Path) -> dict[str, int]:
    """Transactionally replace the three demonstration tables (idempotent full refresh)."""
    counts = validate_tables(tables)
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA foreign_keys = ON")
        # executescript commits any pending transaction; create schema first.
        con.executescript(_SCHEMA)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute("DELETE FROM conditions")
            con.execute("DELETE FROM encounters")
            con.execute("DELETE FROM patients")
            con.executemany(
                "INSERT INTO patients (id, birth_date, gender) VALUES (:id, :birth_date, :gender)",
                tables["patients"],
            )
            con.executemany(
                "INSERT INTO encounters (id, patient_id, status, start_at) "
                "VALUES (:id, :patient_id, :status, :start_at)",
                tables["encounters"],
            )
            con.executemany(
                "INSERT INTO conditions (id, patient_id, encounter_id, code, description) "
                "VALUES (:id, :patient_id, :encounter_id, :code, :description)",
                tables["conditions"],
            )
            con.commit()
        except Exception:
            con.rollback()
            raise
    return counts


def audit_database(db_path: str | Path, expected: dict[str, int]) -> dict[str, Any]:
    """Check post-load counts, FK relationships, and patient/encounter consistency."""
    path = Path(db_path)
    with sqlite3.connect(path) as con:
        con.execute("PRAGMA foreign_keys = ON")
        counts = {
            name: con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            for name in ("patients", "encounters", "conditions")
        }
        fk_violations = len(con.execute("PRAGMA foreign_key_check").fetchall())
        cross_patient = con.execute("""
            SELECT COUNT(*) FROM conditions c
            JOIN encounters e ON e.id = c.encounter_id
            WHERE c.patient_id != e.patient_id
        """).fetchone()[0]
    if counts != expected or fk_violations or cross_patient:
        raise DataQualityError(
            "Post-load audit failed: "
            f"counts={counts}, expected={expected}, "
            f"foreign_key_violations={fk_violations}, cross_patient={cross_patient}"
        )
    return {
        "status": "PASS",
        "row_counts": counts,
        "checks": {"foreign_key_violations": fk_violations, "cross_patient_conditions": cross_patient},
    }


def run_pipeline(source: str | Path, destination: str | Path) -> dict[str, Any]:
    """Execute the same business logic without needing an Airflow installation."""
    raw = extract_bundle(source)
    tables = transform_resources(raw)
    validate_tables(tables)
    expected = load_sqlite(tables, destination)
    return audit_database(destination, expected)
