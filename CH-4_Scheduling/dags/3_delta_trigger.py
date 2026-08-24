from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
        schedule=DeltaTriggerTimetable(delta=pendulum.duration(days=10)),
        start_date=pendulum.datetime(year=2026,month=4,day=15,tz="America/Halifax"),
        catchup=False
)
def schedule_delta_trigger():

    @task.python 
    def fetch_data(do_xcom_push: bool = True) -> dict: # This parameter is by default True
        # Simulate fetching data from an API
        data = {"name": "Airflow", "version": "3.0"}
        return data


    @task.python
    def process_data(pulled_data: dict):

        # Simulate processing the data
        processed_data = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)
    
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is a Bash task!'"
    )

    # Define task dependencies
    pulled_data = fetch_data()
    process_data(pulled_data) >> bash_task

dag = schedule_delta_trigger()