from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

with DAG(dag_id = "first_dag", schedule_interval = "*/5 * * * *", start_date = datetime(2022, 1, 1, 10, 00), catchup = False,
        tags = ["first_dag"],
        dagrun_timeout = timedelta(minutes = 60), 
        description = "first dag") as dag:
    
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "postgres_default",
        sql = '/sql/create_user.sql',
        
    )
    
    insert_values = PostgresOperator(
        task_id = "insert_values",
        postgres_conn_id = "postgres_default",
        sql = '/sql/insert_user.sql',
        
    )


    select_values = PostgresOperator(
        task_id = "select_values",
        postgres_conn_id = "postgres_default",
        sql =''' select * from users where password between %(start_pass)s and %(end_pass)s''',
        parameters = {'start_pass': '{{ var.value.start_pass }}', 'end_pass': '{{ var.value.end_pass }}'},
        
        )
    
    
    create_table >> insert_values >> select_values
    
    
    
    
