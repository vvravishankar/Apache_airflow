from airflow.sdk import dag, task
from child_dag_first import child_dag_first_dag
from child_dag_second import child_dag_second_dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag
def parent_dag():

    trigger_child_dag_first = TriggerDagRunOperator(
        task_id = "child_dag_first_dag_trigger",

        # This should match the DAG ID of the child DAG
        trigger_dag_id = "child_dag_first_dag" 
    )

    trigger_child_dag_second = TriggerDagRunOperator(
        task_id = "child_dag_second_dag_trigger",
        trigger_dag_id = "child_dag_second_dag"
    )

    trigger_child_dag_first >> trigger_child_dag_second

parent_dag_dag = parent_dag()