from airflow.sdk import dag,task 
from airflow import DAG
from airflow.operators.python import PythonOperator

def first_task():
    return "Hello World!"

def second_task():
    return "Hello World!"



with DAG(dag_id="python_context_dag") as dag:

    first_task = PythonOperator(
        task_id="first_task",
        python_callable=first_task
    )

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=second_task
    )

    first_task >> second_task
