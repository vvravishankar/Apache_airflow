from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum
import pendulum

@dag(
        schedule=CronDataIntervalTimetable("0 0 * * *", timezone="America/Halifax"), # This will run daily at midnight
        start_date=pendulum.datetime(year=2026, month=4, day=15, tz="America/Halifax"),
        catchup=True
)
def incremental_load():

    @task.python
    def extract_data(**kwargs):

        # Extracting from and to dates
        from_date = kwargs['data_interval_start']
        to_date = kwargs['data_interval_end']

        # Simulate extracting data from a source
        print(f"Extracting data from {from_date} to {to_date}")
        print(f"SELECT * FROM source_table WHERE date >= '{from_date}' AND date < '{to_date}'   ")

    @task.bash
    def load_data():
        
        return """
        echo "Data loaded from {{data_interval_start | ds}} to {{data_interval_end | ds}}"
        """
    
    extract_data() >> load_data()

dag = incremental_load()