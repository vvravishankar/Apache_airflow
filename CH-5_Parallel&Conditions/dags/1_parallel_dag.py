from airflow.sdk import dag, task

@dag
def parallel_dag():

    @task.bash
    def task_bash():
        return "echo 'Hello from Bash'"
    
    @task.python
    def fetch_api():
        data = {"type": "api", "data": ["data1", "data2", "data3"]}
        return data

    @task.python
    def fetch_db():
        data = {"type": "db", "data": ["data4", "data5", "data6"]}
        return data
    
    @task.python
    def fetch_s3():
        data = {"type": "s3", "data": ["data7", "data8", "data9"]}
        return data
    
    @task.python
    def process_data(api_data, db_data, s3_data):
        print("Processing API Data:", api_data)
        print("Processing DB Data:", db_data)
        print("Processing S3 Data:", s3_data)

    task_bash = task_bash()
    api_data = fetch_api()
    db_data = fetch_db()
    s3_data = fetch_s3()
    
    task_bash >> [api_data, db_data, s3_data] >> process_data(api_data, db_data, s3_data)

parallel_dag = parallel_dag()