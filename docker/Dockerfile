FROM apache/airflow:3.2.2-python3.10

USER root

RUN mkdir -p /opt/airflow/dbt && chown -R 50000:0 /opt/airflow/dbt

COPY --chown=50000:0 lakehouse_dbt /opt/airflow/dbt
COPY --chown=50000:0 pyproject.toml /opt/airflow/pyproject.toml

USER airflow
WORKDIR /opt/airflow

RUN pip install --no-cache-dir -e .