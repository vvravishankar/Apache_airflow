from airflow.sdk import dag, task, task_group

@dag
def task_group_dag():

    @task.bash
    def task_bash():
        return "echo 'Hello from Bash'"
    
    @task_group
    def fetch_data_group():

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

        # Define task dependencies within the group
        [fetch_api() >> fetch_db() >> fetch_s3()]
    
    @task.python
    def process_data(ti):
        api_data = ti.xcom_pull(task_ids='fetch_api', key='return_value')
        db_data = ti.xcom_pull(task_ids='fetch_db', key='return_value')
        s3_data = ti.xcom_pull(task_ids='fetch_s3', key='return_value')
        
        print("Processing API Data:", api_data)
        print("Processing DB Data:", db_data)
        print("Processing S3 Data:", s3_data)

        processed_data = api_data['data'] + db_data['data'] + s3_data['data']
        return processed_data

    
    @task.branch
    def load_data_branch(ti):

        # Pulling data from previous tasks to decide the branch
        processed_data = ti.xcom_pull(task_ids='process_data', key='return_value')
        if len(processed_data) > 10:
            return 's3_load'
        else:
            return 'glue_load'

    @task.python
    def s3_load(ti):
        data_to_load = ti.xcom_pull(task_ids='process_data', key='return_value')
        print("Loading data to S3:", data_to_load)

    @task.python
    def glue_load(ti):
        data_to_load = ti.xcom_pull(task_ids='process_data', key='return_value')
        print("Loading data to Glue:", data_to_load)


    # Define task dependencies
    task_bash() >> fetch_data_group() >> process_data() >> load_data_branch() >> [s3_load(), glue_load()]


task_group_dag_rec = task_group_dag()