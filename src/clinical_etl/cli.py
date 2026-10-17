"""Run or inspect the ETL locally without installing Apache Airflow."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from clinical_etl.pipeline import DataQualityError, run_pipeline


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Synthetic clinical data ETL demo")
    parser.add_argument("--source", type=Path, default=root / "data" / "synthetic_fhir_bundle.json")
    parser.add_argument("--db", type=Path, default=root / "runtime" / "clinical_demo.sqlite")
    args = parser.parse_args()
    try:
        report = run_pipeline(args.source, args.db)
    except (DataQualityError, OSError, json.JSONDecodeError) as error:
        parser.exit(status=1, message=f"ETL failed: {error}\n")
    print(json.dumps(report, indent=2))
    print(f"SQLite database: {args.db}")


if __name__ == "__main__":
    main()
