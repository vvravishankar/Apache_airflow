from datetime import datetime, timedelta
from airflow.sdk import dag, task 
import os 
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.bronze_layer import BronzeLayer
from utils.silver_layer import SilverLayer
from utils.gold_layer import GoldLayer

# -------------------------------- CODE ----------------------------------- #

@dag(
    schedule='@daily',
    is_paused_upon_creation=False
)
def aws_project():

    # Task to extract data from API and load it to S3
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def extract_load_s3():
        # Fethcing data from API and loading it to S3 using Loop
        urls = ["https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv",
                "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/passengers.csv",
                "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/airports.csv"]

        obj = BronzeLayer()

        folder_name = datetime.now().strftime("%Y-%m-%d")

        for url in urls:
            fetched_data = obj.ingest_data_api(url)
            obj.put_data_s3_hook("awss3bucketansh",f"bronze/{folder_name}/{url.split('/')[-1]}",fetched_data)

        return folder_name

    # Task to trigger Spark job in AWS Glue to transform the data and load it to S3
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def transform_load_s3(ti):
        # Fetching the last load date from previous task using XCom
        last_load_date = ti.xcom_pull(task_ids='extract_load_s3', key='return_value')
        
        obj = SilverLayer()

        # Triggering the Spark job in AWS Glue to transform the data and load it to S3
        job_run_id = obj.trigger_spark_job("silver_layer",last_load_date)
        print(f"Glue Job triggered with JobRunId: {job_run_id}")
 
    # Task to trigger Spark job in AWS Glue to transform the data and load it in the parquet format to S3
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def transform_load_s3_parquet(ti):
        # Fetching the last load date from previous task using XCom
        last_load_date = ti.xcom_pull(task_ids='extract_load_s3', key='return_value')
        
        obj = SilverLayer()

        # Triggering the Spark job in AWS Glue to transform the data and load it to S3 in parquet format
        job_run_id = obj.trigger_spark_job("silver_layer_athena",last_load_date)
        print(f"Glue Job triggered with JobRunId: {job_run_id}")

    # Task to trigger the Glue crawler to update the Glue catalog
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def trigger_crawler():
        obj = SilverLayer()
        craller_response = obj.trigger_crawler("crawler_silver")
        print(craller_response)


    # Task to trigger the Databricks job to transform the data and load it to the gold layer
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def trigger_databricks_job():
        obj = GoldLayer()
        response = obj.trigger_databricks_job(job_id=1062568820513153)  # Replace with your actual job ID
        print(response)

    # Task Dependency
    extract_load_s3 = extract_load_s3()
    transform_load_s3 = transform_load_s3()
    transform_load_s3_parquet = transform_load_s3_parquet()
    trigger_crawler = trigger_crawler()
    trigger_databricks_job = trigger_databricks_job()

    extract_load_s3 >> [transform_load_s3, transform_load_s3_parquet] >> trigger_crawler >> trigger_databricks_job
                    

aws_project_dag = aws_project()