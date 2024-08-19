from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator,BranchPythonOperator

with DAG(
    'movie_dynamic_json',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie_dynamic_json',
    schedule="10 0 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 2, 15),
    catchup=True,
    tags=["pyspark","movie","dynamic","json"],
) as dag:

    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")
    
    get_data = EmptyOperator(
                task_id="get.data"
            )
    parsing_parquet = EmptyOperator(
                task_id = "parsing.parquet"
            )
    select_parquet = EmptyOperator(
                task_id="select.parquet"
            )

    """
    get_data = PythonOperator(
                task_id="get.data",
                callable_python=,
            )
    parsing_parquet = PythonOperator(
                task_id = "parsing.parquet",
                callable_python=,

            )
    select_parquet = PythonOperator(
                task_id="select.parquet",
                callable_python=,
            )
    """
    
    task_start >> get_data >> parsing_parquet >> select_parquet >> task_end 
