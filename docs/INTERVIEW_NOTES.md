# Interview walk-through: synthetic clinical ETL

**30-second introduction**

> This is a small, fully synthetic pipeline showing how I structure a reliable
> healthcare-data workflow with Airflow. It uses TaskFlow to extract, transform,
> validate, transactionally load, and audit simplified FHIR-shaped records. I kept
> transformation logic independent of Airflow so it can be unit tested and reused.

## Suggested 3-minute demo

1. Open `dags/synthetic_clinical_etl.py` and explain the **five explicit tasks** and the difference between a retryable extraction and a non-retryable bad-data validation.
2. Open `src/clinical_etl/pipeline.py` and describe **typed resource transformations**, `Patient → Encounter → Condition` relationships, and the **cross-patient mismatch check**.
3. Run `python -m clinical_etl.cli`; show `PASS` and row counts.
4. Run the command again to demonstrate an **idempotent full refresh**.
5. Discuss what you would do differently at scale: data lake staging, incremental loads, schema contracts, partitioning, observability, and secure clinical API access.

## Failure modes / engineering decisions

- **Invalid source reference:** rejected by `validate_tables()` before load; the previous completed database remains unchanged.
- **Storage error during load:** the refresh occurs within a transaction and rolls back on error. `PRAGMA foreign_keys=ON` enforces referential integrity.
- **Incorrect counts after load:** the `audit` task fails. In a production platform, this would page an operator and retain a run-specific audit record.
- **Retry safety:** extraction retries once; data quality does not retry because invalid records will not become valid simply by waiting. Full refresh can be rerun safely.
- **Concurrent runs:** `max_active_runs=1` because SQLite and a full-refresh model are used here.
- **Performance:** the dataset is intentionally tiny; XCom carries data **only** for the demo. In production, persist large payloads to object storage and exchange references.

## Scope disclaimer

The test fixtures are entirely fictional. They mimic portions of FHIR resource layout, **not complete FHIR validation**. This public project is separate from any employer's proprietary Airflow development and should never be described as an employer deployment.

## Possible resume / LinkedIn bullet

> Built a public, synthetic-healthcare Apache Airflow proof of concept using TaskFlow DAGs, automated pre-/post-load quality checks, transactional SQL loading, and unit-tested patient/encounter/condition models; documented architecture and operational trade-offs.

Use this description only after you run and personally review the implementation.
