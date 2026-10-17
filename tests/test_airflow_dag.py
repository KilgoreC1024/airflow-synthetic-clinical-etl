"""Run the real DAG end to end; skipped unless Apache Airflow is installed.

CI runs these inside the project's Docker image (see .github/workflows/tests.yml).
"""
from __future__ import annotations

import importlib.util
import unittest

AIRFLOW_INSTALLED = importlib.util.find_spec("airflow") is not None
TASK_ORDER = ["extract", "transform", "quality_gate", "load", "audit"]


@unittest.skipUnless(AIRFLOW_INSTALLED, "Apache Airflow is not installed")
class AirflowDagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from airflow.models import DagBag

        cls.dagbag = DagBag(include_examples=False)
        cls.dag = cls.dagbag.get_dag("synthetic_clinical_etl")

    def test_dag_imports_without_errors(self):
        self.assertEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)

    def test_tasks_run_in_order(self):
        self.assertEqual(sorted(self.dag.task_ids), sorted(TASK_ORDER))
        for upstream, downstream in zip(TASK_ORDER, TASK_ORDER[1:]):
            self.assertEqual(self.dag.get_task(upstream).downstream_task_ids, {downstream})

    def test_full_dag_run_succeeds(self):
        run = self.dag.test()
        states = {ti.task_id: ti.state for ti in run.get_task_instances()}
        self.assertEqual(run.state, "success", states)
        self.assertEqual(states, dict.fromkeys(TASK_ORDER, "success"))


if __name__ == "__main__":
    unittest.main()
