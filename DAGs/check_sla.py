from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
import time

# using SLA to let me know if the task is not completed in a period of time.
# different between SLA and timeout is that SLA is just a notification, but timeout TAKE AN ACTION (fail/skip the task).
# SLA is automated by the scheduler who is resposible for SLA monitoring.


def long_running_task():
    time.sleep(60*5)
    
    
def check_sla():
    print("checking SLA")
    
# func for action if the SLA is missed (in our case after 3 minutes and DAG is up and running).
def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    # send an email to the admin.
    subject = "SLA missed in DAG: {dag.dag_id}"
    body = f"""The SLA MISSED CALLBACK Triggered
    
    DAG: {dag.dag_id}
    
    Task: {task_list}
    
    Blocking Tasks: {blocking_task_list}
    
    SLAs: {slas}
    """
    
    send_email(
        to = "abc@de.com",
        subject = subject,
        html_content = body.replace("\n", "<br>"),
    )
    
    
with DAG(dag_id = "check_sla", schedule_interval = "*/8 * * * *",
        start_date = datetime(2022, 1, 1, 10, 00),
        catchup = False,
        tags = ["sales", 'daily'],
        dagrun_timeout = timedelta(minutes = 60),
        sla_miss_callback = _sla_miss_callback,  
        description = "check_sla task") as dag:
    
    long_task = PythonOperator(
        task_id = "long_task",
        python_callable = long_running_task, # fun that 'll trigger our SLA action cuz it's not completed in 3 minutes but 5 minutes.
    )

    check_sla = PythonOperator(
        task_id = "check_sla",
        python_callable = check_sla,
        sla = timedelta(minutes = 3), # if the overall tasks is not completed in 3 minutes, take action that defind in _sla_miss_callback fun (send an email).
        email_on_failure = True,
        email = ["abc@de.com"],
    )


