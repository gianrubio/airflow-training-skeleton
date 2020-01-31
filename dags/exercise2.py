from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'AirFlow',
    'start_date': datetime(2015, 6, 1),
    'retry_delay': timedelta(minutes=5)

}

def print_context(**context):
    print(datetime.now())

dag = DAG('Exercise2', default_args=default_args)

t1 = PythonOperator(
    task_id='print_execution_date',
    provide_context=True,
    python_callable=print_context,
    dag = dag 
)

t2 = BashOperator(
    task_id='wait1',
    bash_command='sleep 1',
    dag = dag 
)

t3 = BashOperator(
    task_id='wait5',
    bash_command='sleep 5',
    dag = dag 
)

t4 = BashOperator(
    task_id='wait10',
    bash_command='sleep 10',
    dag = dag 
)

t5 = DummyOperator(
    task_id='the_end',
    dag = dag 
)

t1 >> [t2, t3,t4] >> t5