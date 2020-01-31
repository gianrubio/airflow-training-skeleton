from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'AirFlow'
}

dag = DAG('Exercise1', default_args=default_args, schedule_interval=None)

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
    bash_command='ls /tmp/task* && cat /tmp/task*',
    dag = dag 
)

t1 >> t2 >> [t3,t4] >> t5