from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'AirFlow',
    'start_date': datetime(2015, 6, 1),
    'retry_delay': timedelta(minutes=5)

}
weekday_person_to_email = {
    0: "Bob", #Monday
    1: "Joe", #Tuesday
    2: "Alice", #Wednesday
    3: "Joe", #Thursday
    4: "Alice", #Friday
    5: "Alice", #Saturday
    6: "Alice", #Sunday
}

week_days = ("Monday","Tuesday","Wednesday","Thursday","Friday")

def send_email(**context):
    print("Send email to {}".format(weekday_person_to_email[datetime.datetime.today().weekday()]))

def print_weekdays():
    print(week_days)

dag = DAG('Exercise3', default_args=default_args)

t1 = PythonOperator(
    task_id='print_weekdays',
    python_callable=print_weekdays,
    dag = dag 
)

t3 = BashOperator(
    task_id='final_task',
    bash_command='sleep 5',
    dag = dag,
    trigger_rule=TriggerRule.ONE_SUCCESS
)

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=send_email, 
    provide_context=True,
    dag=dag
    # trigger_rule='non_failed'
)

t1 >> branching

for key in weekday_person_to_email:
    branching >> DummyOperator(task_id=weekday_person_to_email[key], dag=dag ) >> t3
