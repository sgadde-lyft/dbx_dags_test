from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

# SQL warehouse HTTP path (same as in the Databricks workspace URL).
SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/5e6e1e71664decbd"

SQL = """
SELECT count(*) 
FROM dev_catalog.staging.dim_airports
"""

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="databricks_sql_read",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["databricks", "sql"],
) as dag:

    read_sql = DatabricksSqlOperator(
        task_id="read_sql",
        databricks_conn_id="databricks_default",
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        catalog="dev_catalog",
        schema="staging",
        sql=SQL
    )


# Airflow connection `databricks_default`: workspace host + auth (e.g. SP OAuth in extras).
# `http_path` above targets this DAG’s warehouse; you can omit it if the same path is set on the connection.
# Trigger: airflow dags trigger databricks_sql_read