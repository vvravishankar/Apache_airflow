from datetime import timedelta

from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator


@dag
def kwargs_dag():

    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def fetch_data(**kwargs):
        print("Printing kwargs : ", kwargs)
        ti = kwargs['ti']
        # Simulate fetching data from an API
        data = {"name": "Airflow", "version": "3.0"}
        ti.xcom_push(key="fetched_data", value=data)

    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def process_data(**kwargs):
        ti = kwargs['ti']
        # Pull the data from XCOM manually
        pulled_data = ti.xcom_pull(key="fetched_data", task_ids="fetch_data")

        # Simulate processing the data
        processed_data = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)
    

    @task.bash(retries=3, retry_delay=timedelta(seconds=5))
    def bash_task_with_xcom(**kwargs):
        # ti = kwargs['ti'] # You can't refer any python variable in bash task's bash_command. We can only use runtime variables using jinja templating. Yes, we can use f-strings if we want to refer any python variable.
        my_var = "Ansh Lamba"
        return f"""
                echo "This is a {my_var} task!. The fetched data is {{ ti.xcom_pull(key='fetched_data', task_ids='fetch_data') }}"
                """

    # Define task dependencies
    fetch_data() >> process_data() >> bash_task_with_xcom()

dag = kwargs_dag()