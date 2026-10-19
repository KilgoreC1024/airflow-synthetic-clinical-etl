#!/usr/bin/env bash
# Assemble the demo site: the page, the unmodified pipeline code and data it runs in the browser, and the
# recorded Airflow run. Usage: scripts/build_site.sh <out_dir> <airflow_run.json>
set -euo pipefail
out="$1"
run_json="$2"
root="$(cd "$(dirname "$0")/.." && pwd)"

rm -rf "$out"
mkdir -p "$out/code/clinical_etl" "$out/code/dags" "$out/data" "$out/img"
cp "$root/site/index.html" "$out/"
cp "$root/site/demo_runner.py" "$out/code/"
cp "$root/src/clinical_etl/__init__.py" "$root/src/clinical_etl/pipeline.py" "$out/code/clinical_etl/"
cp "$root/dags/synthetic_clinical_etl.py" "$out/code/dags/"
cp "$root/data/synthetic_fhir_bundle.json" "$out/data/"
cp "$root/docs/airflow-success.png" "$out/img/"
cp "$run_json" "$out/airflow_run.json"
touch "$out/.nojekyll"
echo "Site assembled in $out"
