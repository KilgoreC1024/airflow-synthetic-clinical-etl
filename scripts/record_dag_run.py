"""Run the DAG once with Apache Airflow and record the run as JSON for the demo site.

Runs inside the project's Docker image (see .github/workflows/pages.yml):

    python /opt/airflow/demo/scripts/record_dag_run.py /out/airflow_run.json
"""
from __future__ import annotations

import json
import os
import platform
import sys
from datetime import datetime, timezone

import airflow
from airflow.models import DagBag
from airflow.models.xcom import XCom

TASK_ORDER = ["extract", "transform", "quality_gate", "load", "audit"]


def main() -> None:
    out_path = sys.argv[1]
    dagbag = DagBag(include_examples=False)
    if dagbag.import_errors:
        raise SystemExit(f"DAG import errors: {dagbag.import_errors}")
    dag = dagbag.get_dag("synthetic_clinical_etl")
    run = dag.test()

    # dag.test() runs tasks in-process and records no per-task start time, so report when each task
    # finished relative to the run start rather than a duration.
    instances = {ti.task_id: ti for ti in run.get_task_instances()}
    tasks = []
    for task_id in TASK_ORDER:
        ti = instances[task_id]
        tasks.append({
            "task_id": task_id,
            "state": str(ti.state),
            "finished_after_s": round((ti.end_date - run.start_date).total_seconds(), 3) if ti.end_date else None,
            "try_number": ti.try_number,
        })
    audit = XCom.get_one(run_id=run.run_id, dag_id=dag.dag_id, task_id="audit", key="return_value")

    record = {
        "dag_id": dag.dag_id,
        "run_id": run.run_id,
        "state": str(run.state),
        "start": run.start_date.isoformat() if run.start_date else None,
        "end": run.end_date.isoformat() if run.end_date else None,
        "tasks": tasks,
        "audit": audit,
        "airflow_version": airflow.__version__,
        "python_version": platform.python_version(),
        "commit": os.environ.get("GITHUB_SHA", "local"),
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    with open(out_path, "w", encoding="utf-8") as file:
        json.dump(record, file, indent=2, default=str)
    print(json.dumps({"state": record["state"], "tasks": [(t["task_id"], t["state"]) for t in tasks]}))
    if record["state"] != "success":
        raise SystemExit("DAG run did not succeed")


if __name__ == "__main__":
    main()
