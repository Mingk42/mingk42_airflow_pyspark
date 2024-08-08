from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator,BranchPythonOperator

with DAG(
    'pyspark_movie',
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='pyspark_movie',
    schedule="10 0 * * *",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2015, 2, 1),
    catchup=True,
    tags=["pyspark","movie"],
) as dag:

    def repartition(ds_nodash):
        from repartition.repartition import repartition
        
        repartition(ds_nodash)

    def chk_exist(ds_nodash):
        import os

        homePath=os.path.expanduser("~")
        if(os.path.exists(f"{homePath}/data/movie/repartition/load_dt={ds_nodash}")):
            return "rm.dir"
        else:
            return "repartition"

    task_rp = PythonVirtualenvOperator(
            task_id="repartition",
            python_callable=repartition,
            requirements=["git+https://github.com/Mingk42/mingk42_repartition.git"],
            system_site_packages=False,
            trigger_rule="none_failed"
        )
    task_join_df = BashOperator(
            task_id="join.df",
            bash_command="""
                $SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/movie_join_df.py {{ds_nodash}}
            """
        )
    task_agg = BashOperator(
            task_id="agg",
            bash_command="""
                $SPARK_HOME/bin/spark-submit ~/airflow_pyspark/py/movie_agg.py {{ds_nodash}}
            """
        )
    task_rm_dir = BashOperator(
            task_id="rm.dir",
            bash_command="""
                rm -rf ~/data/movie/repartition/load_dt={{ds_nodash}}
            """)
    task_chk_exist=BranchPythonOperator(
            task_id="chk.exist",
            python_callable=chk_exist,
        )

    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")


    task_start >> task_chk_exist >> [task_rp, task_rm_dir]
    task_rp >> task_join_df >> task_agg >> task_end
    task_rm_dir >> task_rp
