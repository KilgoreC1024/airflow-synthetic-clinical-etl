"""Print a small example analytical query against the synthetic SQLite output."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description="Inspect synthetic condition counts")
    parser.add_argument("--db", type=Path, default=root / "runtime" / "clinical_demo.sqlite")
    args = parser.parse_args()
    if not args.db.is_file():
        parser.exit(status=1, message="Database missing. Run the ETL first.\n")
    with sqlite3.connect(args.db) as con:
        rows = con.execute("""
            SELECT c.code, c.description, COUNT(*) AS diagnosis_count,
                   COUNT(DISTINCT c.patient_id) AS distinct_patients
            FROM conditions c
            JOIN encounters e ON e.id = c.encounter_id
            JOIN patients p ON p.id = e.patient_id
            GROUP BY c.code, c.description
            ORDER BY diagnosis_count DESC, c.code
        """).fetchall()
    print("Code   | Description                | Conditions | Patients")
    print("-" * 60)
    for code, label, conditions, patients in rows:
        print(f"{code:<6} | {label:<26} | {conditions:^10} | {patients:^8}")


if __name__ == "__main__":
    main()
