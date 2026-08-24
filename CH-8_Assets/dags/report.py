from airflow.sdk import dag, task 
import os
import json
from airflow.sdk.definitions.asset import Asset
from etl import weather_asset

@dag(
        is_paused_upon_creation=False,
        schedule=[weather_asset])
def report():

    @task
    def read_data(ti):
        with open("/opt/airflow/data/data.json", "r") as f:
            data = json.load(f)
        return data
    
    read_data()

report_dag = report()
