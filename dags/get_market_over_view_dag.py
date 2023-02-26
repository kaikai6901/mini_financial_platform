from airflow import DAG
# from airflow.providers.ssh.hooks.ssh import SSHHook
# from airflow.providers.ssh.operators.ssh import SSHOperator
import datetime as dt
from datetime import timedelta
from airflow.operators.python import PythonOperator
from scripts.get_market_overview import *
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="get_market_overview",
    start_date=dt.datetime(2023, 2, 23, tzinfo=local_tz),
    schedule_interval='5 12,15 * * 1-6'
) as dag:
    process =  PythonOperator(
            task_id='push_market_overview',
            python_callable=push_data,
            do_xcom_push=False,
            trigger_rule='all_success'
        )
    # process = ssh_operator('check_git', "scripts/check_git.sh")
    # process = python_operator('push_market_overview', "python3 " + pathProject + "/get_market_overview.py")
    process
