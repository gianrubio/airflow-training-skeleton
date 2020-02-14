import airflow
import requests
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from operators.launch_hook import LaunchLibraryOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(10)}

dag = DAG(dag_id="download_rocket_launches", default_args=args, description="DAG downloading rocket launches from Launch Library.", 
    schedule_interval="20 * * * *")
    
download_rocket_launches = LaunchLibraryOperator(
    task_id="download_rocket_launches", 
    conn_id="launchlibrary",
    endpoint="launch",
    params={"startdate" : "{{ ds }}", "enddate": "{{ tomorrow_ds }}"},
    result_path="/data/rocket_launches/ds={{ ds }}",
    result_filename="launches.json",
    dag=dag,
)
