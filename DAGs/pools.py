from airflow import DAG 
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import time


# pools is about limiting our resources for a specific task. 
# say you have 100 slots but you want a spesific task only to have 10 slots. 
# piority_weight is about which task should start first in the pool. more weight = more priority.


with DAG(dag_id = "pools", schedule_interval = "*/10 * * * *",
        start_date = datetime(2024, 2, 8, 21, 00),
        catchup = False,
        tags = ["sales", 'daily'],
        dagrun_timeout = timedelta(minutes = 40),
        description = "pools task") as dag:
    
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "postgres_default",
        sql = '/sql/create_user.sql',
        
    )
    
    simulate_task_1 = PythonOperator(
        task_id = "simulate_task_1",
        python_callable = lambda: time.sleep(10),
        pool = "pool_1",
        piority_weight = 2,  # this task start first in the pool_1
    )
    
    simulate_task_2 = PythonOperator(
        task_id = "simulate_task_2",
        python_callable = lambda: time.sleep(10),
        pool = "pool_1",
        piority_weight = 1,  # this task start second in the pool_1
    )
    
    simulate_task_3 = PythonOperator(
        task_id = "simulate_task_3",
        python_callable = lambda: time.sleep(10),
        pool = "pool_2",
        piority_weight = 1,  # this task start second in the pool_2
    )
    
    simulate_task_4 = PythonOperator(
        task_id = "simulate_task_4",
        python_callable = lambda: time.sleep(10),
        pool = "pool_2",
        piority_weight = 2,  # this task start first in the pool_2
    ) 
    
    select_values = PostgresOperator(
        task_id = "select_values",
        postgres_conn_id = "postgres_default",
        sql = '/sql/insert_user.sql',
        
    )
    
    
    select_values = PostgresOperator(
    task_id = "select_values",
    postgres_conn_id = "postgres_default",
    sql =''' select * from users where password between %(start_pass)s and %(end_pass)s''',
    parameters = {'start_pass': '{{ var.value.start_pass }}', 'end_pass': '{{ var.value.end_pass }}'},
    
    )

    create_table >> [simulate_task_1, simulate_task_2, simulate_task_3, simulate_task_4] >> select_values
    
    