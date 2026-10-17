# How to publish this project on GitHub

1. Create a new **public** repository (suggested name: `airflow-synthetic-clinical-etl`). Choose **not** to initialize it with a separate README or license because this project already contains both.
2. Download and extract the ZIP, then open a terminal in the extracted folder.
3. Run the offline unit tests and CLI locally using the README commands.
4. Create your first commit and push it:

```bash
git init
git add .
git commit -m "Add synthetic clinical ETL Airflow portfolio project"
git branch -M main
git remote add origin https://github.com/KilgoreC1024/airflow-synthetic-clinical-etl.git
git push -u origin main
```

5. The README badge is prefilled for `KilgoreC1024/airflow-synthetic-clinical-etl`. If you publish under a different account or repository name, update that link.
6. Add repository topics: `apache-airflow`, `python`, `data-engineering`, `healthcare-data`, `etl`, `data-quality`, `sqlite`, `portfolio`.
7. Optional: take a screenshot of the successful Airflow Graph view **after running the Docker demo**, put it in `docs/`, and reference it from the README. Don't fabricate a runtime screenshot.

**No PHI or employer intellectual property:** Before publishing, inspect the files yourself. This project contains only demo records, code, documentation, and automation.
