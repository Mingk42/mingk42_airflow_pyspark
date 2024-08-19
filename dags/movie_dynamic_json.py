from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator,BranchPythonOperator,PythonOperator

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
#    schedule="10 0 * * *",
    schedule="@once",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 1, 5),
    catchup=True,
    tags=["pyspark","movie","dynamic","json"],
) as dag:

    def echo(msg="hello"):
        print(msg)

    def call():
        from movdata.ml import save_movies

        save_movies(year=2023,sleep_time=0.5)

    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")
    """ 
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
    get_data = PythonVirtualenvOperator(
                task_id="get.data",
                python_callable=call,
                requirements=["git+https://github.com/Mingk42/mingk42-movData.git@v0.2.1/for_airflow_spark"],
                system_site_packages=False,
            )
    parsing_parquet = BashOperator(
                task_id = "parsing.parquet",
                bash_command="$SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/parsing_json.py",
            )
    select_parquet = BashOperator(
                task_id = "select.parquet",
                bash_command="$SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/select_parquet.py",
            )

    task_start >> get_data >> parsing_parquet >> select_parquet >> task_end 
