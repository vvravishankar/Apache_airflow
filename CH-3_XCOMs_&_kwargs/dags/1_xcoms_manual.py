from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator


@dag
def xcoms_manual():

    @task.python 
    def fetch_data(ti):
        # Simulate fetching data from an API
        data = {"name": "Airflow", "version": "3.0"}
        ti.xcom_push(key="fetched_data", value=data)
        return data
        # Pushing data to XCOM manually
        


    @task.python
    def process_data(ti):
        # Pull the data from XCOM manually
        pulled_data = ti.xcom_pull(key="fetched_data", task_ids="fetch_data")

        # Simulate processing the data
        processed_data = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)
    
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is a Bash task!'"
    )

    # Define task dependencies
    fetch_data() >> process_data() >> bash_task

dag = xcoms_manual()