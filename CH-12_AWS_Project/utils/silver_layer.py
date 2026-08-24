import time

import boto3 
import os
from dotenv import load_dotenv
load_dotenv()


class SilverLayer:

    def __init__(self):
        pass

    def trigger_spark_job(self,job_name,job_parameter):

        aws_access_key_id = os.environ.get("aws_access_key_id")
        aws_secret_access_key = os.environ.get("aws_secret_access_key")

        # Create a glue client
        glue_client = boto3.client(
            'glue',
            region_name='ca-central-1',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Start the Glue job
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--load_date': job_parameter
            }
        )

        # Adding Polling mechanism to check the status of the job
        job_run_id = response['JobRunId']
        while True:
            status = glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )['JobRun']['JobRunState']

            print(f"Current status: {status}")

            if status in ['SUCCEEDED']:
                break
            elif status in ['FAILED', 'STOPPED']:
                raise Exception(f"Glue job failed with status: {status}")

            time.sleep(30)  # wait before polling again

        return job_run_id

    

    def trigger_crawler(self,crawler_name):
        aws_access_key_id = os.environ.get("aws_access_key_id")
        aws_secret_access_key = os.environ.get("aws_secret_access_key")

        # Create a glue client
        glue_client = boto3.client(
            'glue',
            region_name='ca-central-1',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Start the Glue crawler
        response = glue_client.start_crawler(
            Name=crawler_name
        )

        # Adding Polling mechanism to check the status of the crawler
        while True:
            status = glue_client.get_crawler(
                Name=crawler_name
            )['Crawler']['State']

            print(f"Current status: {status}")

            if status in ['READY']:
                break
            elif status in ['FAILED', 'STOPPED']:
                raise Exception(f"Glue crawler failed with status: {status}")

            time.sleep(30)  # wait before polling again

        return response

        


    
if __name__ == "__main__":
    obj = SilverLayer()
    craller_response = obj.trigger_crawler("crawler_silver")
    print(craller_response)