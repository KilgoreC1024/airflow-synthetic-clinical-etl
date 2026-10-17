# Local demo only, based on the official Airflow image. Not a production deployment.
FROM apache/airflow:2.10.5-python3.11
USER root
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root src/ /opt/airflow/demo/src/
COPY --chown=airflow:root data/ /opt/airflow/demo/data/
RUN mkdir -p /opt/airflow/state /opt/airflow/demo/runtime && \
    chown -R airflow:root /opt/airflow/state /opt/airflow/demo
USER airflow
ENV AIRFLOW_HOME=/opt/airflow/state \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=SequentialExecutor \
    PYTHONPATH=/opt/airflow/demo/src \
    CLINICAL_ETL_PROJECT_ROOT=/opt/airflow/demo
