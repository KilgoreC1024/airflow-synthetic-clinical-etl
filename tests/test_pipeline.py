"""Offline tests; no Airflow package or external service required."""
from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from clinical_etl.pipeline import (
    DataQualityError,
    audit_database,
    extract_bundle,
    load_sqlite,
    run_pipeline,
    transform_resources,
    validate_tables,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "data" / "synthetic_fhir_bundle.json"


class ClinicalPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db = Path(self.temp_dir.name) / "clinical.sqlite"
        self.resources = extract_bundle(FIXTURE)
        self.tables = transform_resources(self.resources)

    def test_successful_pipeline_and_postload_audit(self):
        report = run_pipeline(FIXTURE, self.db)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["row_counts"], {
            "patients": 4, "encounters": 6, "conditions": 7,
        })
        self.assertEqual(report["checks"]["foreign_key_violations"], 0)

    def test_repeat_run_is_idempotent(self):
        first = run_pipeline(FIXTURE, self.db)
        second = run_pipeline(FIXTURE, self.db)
        self.assertEqual(first, second)
        with closing(sqlite3.connect(self.db)) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM conditions").fetchone()[0], 7)

    def test_reject_missing_patient_reference_before_load(self):
        data = copy.deepcopy(self.tables)
        data["encounters"][0]["patient_id"] = "does-not-exist"
        with self.assertRaisesRegex(DataQualityError, "missing patient"):
            validate_tables(data)
        self.assertFalse(self.db.exists())

    def test_reject_cross_patient_condition(self):
        data = copy.deepcopy(self.tables)
        data["conditions"][0]["patient_id"] = "pat-004"  # exists, but not encounter owner
        with self.assertRaisesRegex(DataQualityError, "another patient"):
            validate_tables(data)

    def test_reject_duplicate_resource_ids(self):
        data = copy.deepcopy(self.resources)
        data.append(copy.deepcopy(data[0]))
        with self.assertRaisesRegex(DataQualityError, "Duplicate IDs"):
            validate_tables(transform_resources(data))

    def test_reject_non_iso_datetime(self):
        data = copy.deepcopy(self.resources)
        encounter = next(r for r in data if r["resourceType"] == "Encounter")
        encounter["period"]["start"] = "yesterday"
        with self.assertRaisesRegex(DataQualityError, "invalid ISO 8601"):
            transform_resources(data)

    def test_reject_unknown_resource_type(self):
        data = copy.deepcopy(self.resources)
        data.append({"resourceType": "Medication", "id": "med-001"})
        with self.assertRaisesRegex(DataQualityError, "Unsupported resource type"):
            transform_resources(data)

    def test_sqlite_rollback_preserves_prior_refresh(self):
        expected = load_sqlite(self.tables, self.db)
        # Simulate an unexpected storage error after the refresh transaction starts.
        with closing(sqlite3.connect(self.db)) as con:
            con.executescript("""
                CREATE TRIGGER simulate_failure BEFORE INSERT ON conditions
                WHEN NEW.id = 'con-002'
                BEGIN SELECT RAISE(ABORT, 'simulated insertion failure'); END;
            """)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "simulated insertion failure"):
            load_sqlite(self.tables, self.db)
        # Atomic rollback restores *all* previously committed rows.
        self.assertEqual(audit_database(self.db, expected)["status"], "PASS")

    def test_reject_empty_collection(self):
        path = Path(self.temp_dir.name) / "empty.json"
        path.write_text(json.dumps({"resourceType": "Bundle", "type": "collection", "entry": []}))
        with self.assertRaisesRegex(DataQualityError, "nonempty"):
            extract_bundle(path)


if __name__ == "__main__":
    unittest.main()
