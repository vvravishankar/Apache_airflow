from airflow.sdk import dag, task 

@dag(dag_id="first_dag")
def first_dag():

    @task(task_id="task_1")
    def task_1():
        print("This is task 1")
    
    @task(task_id="task_2")
    def task_2():
        print("This is task 2")

    @task(task_id="task_3")
    def task_3():
        print("This is task 3")
    
    # Define the task dependencies
    t1 = task_1()
    t2 = task_2()
    t3 = task_3()

    t1 >> t2 >> t3 # This means task_1 will run before task_2, and task_2 will run before task_3

# To run the DAG, we need to create an instance of it
first_dag_instance = first_dag()