from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'AirFlow',
    'start_date': datetime(2015, 6, 1),
    # 'schedule_interval': '@daily'
    'schedule_interval': ['45 13 * * 1,3,5'],
    'retry_delay': timedelta(minutes=5)

}

dag = DAG('Exercise1', default_args=default_args)

t1 = BashOperator(
    task_id='Task1',
    bash_command='date',
    dag = dag 
)

t2 = BashOperator(
    task_id='Task2',
    bash_command='sleep 20',
    dag = dag 
)

t3 = BashOperator(
    task_id='Task3',
    bash_command='echo task3 > /tmp/task_3',
    dag = dag 
)

t4 = BashOperator(
    task_id='Task4',
    bash_command='echo task4 > /tmp/task_4',
    dag = dag 
)

t5 = BashOperator(
    task_id='Task5',
    bash_command='pwd',
    dag = dag 
)

t1 >> t2 >> [t3,t4] >> t5