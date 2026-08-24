from airflow.sdk import dag, task

@dag
def parallel_dag():
    @task
    def task_1():
        print("Task 1")

    @task
    def task_2():
        print("Task 2")

    @task
    def task_3():
        print("Task 3")
    
    @task(queue="compute_heavy_queue")
    def task_4():
        print("Task 4")

    t1 = task_1()
    t2 = task_2()
    t3 = task_3()
    t4 = task_4()

    [t1, t2, t3] >> t4

parallel_dag = parallel_dag()
