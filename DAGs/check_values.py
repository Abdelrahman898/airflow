from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule


def ProcessingData():
    print("records found...")
    
    
with DAG(dag_id = "check_data", schedule_interval = "*/5 * * * *", start_date = datetime(2022, 1, 1, 10, 00), catchup = False,
        tags = ["sales", 'daily'],
        dagrun_timeout = timedelta(minutes = 60), 
        description = "second dag, check_data") as dag:
    
    check_records = SqlSensor(
        task_id = "check_records",
        conn_id = "postgres_conn",
        sql = ''' SELECT COUNT(*) FROM users WHERE username = 'admin'; ''',
        poke_interval = 30,
        timeout = 60, # deafult = 7 days
        mode = "reschedule", # reschedule the tasks, only use when needed.
        # mode = "poke", # booking the worker all the time untill timeout is reached.
        soft_fail = True   # if the query fails, it will not fail the hole task/dag just marked as skipped.
    )

    process_data = PythonOperator(
        task_id = "process_data",
        python_callable = ProcessingData,
        
    )
    
    task_alert = EmailOperator(
        task_id = "send_email",
        to = "H0S2o@example.com",
        subject = "task failed",
        html_content = "<h1>one of the tasks Failed</h1>",
        trigger_rule = TriggerRule.ONE_FAILED  # execute this (task_alert) task only if one of the tasks failed.


    )
    
    check_records >> process_data, task_alert
    
