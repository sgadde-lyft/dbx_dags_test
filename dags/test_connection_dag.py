from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksStatementQueryOperator

SQL = """
SELECT count(*) 
FROM catalog.schema.table
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

    read_sql = DatabricksStatementQueryOperator(
        task_id="read_sql",
        databricks_conn_id="databricks_default",  # Airflow Connection
        warehouse_id="<your-warehouse-id>",        # SQL Warehouse ID
        catalog="catalog",
        schema="schema",
        statement=SQL,
    )


 
## 1. create a databricks connection in airflow 
## (Conn Id: databricks_default, Host: https://adb-, Token: your PAT or service-principal token)
## 2. create a sql warehouse in databricks
## (Warehouse Id: <your-warehouse-id>)
## trigger command -
## airflow dags trigger databricks_sql_read