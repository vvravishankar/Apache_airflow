# URL For the API:
# https://github.com/anshlambagit/ApacheAirflow

import os 
from dotenv import load_dotenv
import boto3
import pandas as pd
import requests 
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Loading environment variables from .env file
load_dotenv()

class BronzeLayer:

    def __init__(self):
        pass 
    
    def ingest_data_api(self,url):
 
        response = requests.get(url)

        if response.status_code == 200:
            data = response.text

            df = pd.read_csv(StringIO(data))

            # Converting the DataFrame to CSV in memory using StringIO
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Return the CSV content
            return csv_buffer.getvalue()
        
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")

    # Method to upload data to S3 using boto3
    def put_data_s3(self, bucket_name, object_key,data):

        aws_access_key_id = os.environ.get("aws_access_key_id")
        aws_secret_access_key = os.environ.get("aws_secret_access_key")

        s3_client = boto3.client(
            's3',
            region_name='ca-central-1',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Upload the data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=data
        )

        return f"Data uploaded to S3 bucket '{bucket_name}' with object key '{object_key}'"

    # Method to upload data to S3 using Airflow's S3Hook
    def put_data_s3_hook(self, bucket_name, object_key, data):

        s3_hook = S3Hook(aws_conn_id='aws_s3_conn')
        s3_hook.load_string(
            string_data=data,
            key=object_key,
            bucket_name=bucket_name,
            replace=True
        )

if __name__ == "__main__":

    obj = BronzeLayer()
    data = obj.ingest_data_api("https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv")
    obj.put_data_s3("awss3bucketansh","bronze/bookings.csv",data)
    obj.put_data_s3_hook("awss3bucketansh","bronze_new/bookings.csv",data)
