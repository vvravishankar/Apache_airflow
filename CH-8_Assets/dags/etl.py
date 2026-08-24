from airflow.sdk import dag, task 
import os
import json
from airflow.sdk.definitions.asset import Asset

weather_asset = Asset(uri="file:///opt/airflow/data/data.json")

@dag(is_paused_upon_creation=False)
def etl():

    @task
    def extract(ti):
        return {"data": {"city":"New York", "temperature": 30}}
    
    @task
    def transform(ti):
        data = ti.xcom_pull(task_ids="extract")
        city = data["data"]["city"]
        temp_celsius = data["data"]["temperature"]
        temp_fahrenheit = (temp_celsius * 9/5) + 32
        return {"city": city, "temp_celsius": temp_celsius, "temp_fahrenheit": temp_fahrenheit}
    
    @task(outlets=[weather_asset])
    def load(ti):
        transformed_data = ti.xcom_pull(task_ids="transform")
        print(f"Loading data: {transformed_data}")

        # Creating a directory to save the output
        output_dir = os.makedirs("/opt/airflow/data", exist_ok=True)
        with open("/opt/airflow/data/data.json", "w") as f:
            json.dump(transformed_data, f)

    # Define the task dependencies
    extract() >> transform() >> load()

etl_dag = etl()

