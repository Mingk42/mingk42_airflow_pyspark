from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator,BranchPythonOperator,PythonOperator

with DAG(
    'line-notify',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(seconds=3)
    },
    description="line-notify",
    schedule="10 0 * * *",
#    schedule="@once",
    start_date=datetime(2024, 8, 15),
    catchup=True,
    tags=["line","notify","alert","api"],
) as dag:

    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end", trigger_rule="all_done")

    bash_job=BashOperator(
                task_id="bash.job",
                bash_command="""
                    NUM=$((RANDOM))
                    echo $NUM
                    exit $((NUM%2))
                """
            )

    case_success=BashOperator(
                task_id="notify.success",
                bash_command="""
                    echo success
                    sh /home/root2/airflow_pyspark/sh/send-line.sh [INFO][{{ti.hostname}}][{{ds}}]{{task.task_id}}:::SUCCESS
                """,
                trigger_rule="all_success"
            )
    case_fail=BashOperator(
                task_id="notify.fail",
                bash_command="""
                    echo fail
                    sh /home/root2/airflow_pyspark/sh/send-line.sh [INFO][{{ti.hostname}}][{{ds}}]{{task.task_id}}:::FAIL
                """,
                trigger_rule="all_failed"
            )


    task_start >> bash_job >> [case_success, case_fail] >> task_end 
