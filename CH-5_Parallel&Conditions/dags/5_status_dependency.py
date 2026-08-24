from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

@dag
def status_dependency_dag():

    @task.python
    def task_a():
        print("Executing Task A")
        return "Task A completed"

    @task.python
    def task_b():
        print("Executing Task B")
        raise Exception("Task B failed")  # Simulate a failure in Task B

    @task.python
    def task_c():
        print("Executing Task C")
        return "Task C completed"

    @task.python(trigger_rule=TriggerRule.ALL_DONE)  # Ensure Task D runs regardless of Task B's status
    def task_d():
        print("Executing Task D")
        return "Task D completed"

    # Define task dependencies
    task_a = task_a()
    task_b = task_b()
    task_c = task_c()
    task_d = task_d()

    task_a >> [task_b, task_c]  >> task_d

status_dependency_dag_rec = status_dependency_dag()

