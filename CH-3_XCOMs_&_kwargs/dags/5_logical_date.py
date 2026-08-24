from airflow.sdk import dag, task 

@dag
def logical_date_dag():

    @task.python 
    def print_logical_date(**context):
        logical_date = context['logical_date']
        print(f"The logical date for this task is: {logical_date.strftime('%Y-%m-%d %H:%M:%S')}")

    @task.bash
    def print_logical_date_bash():
        return " echo 'The logical date for this task is: {{ logical_date | ds }}'"
    
    print_logical_date() >> print_logical_date_bash()

logical_date_dag = logical_date_dag()