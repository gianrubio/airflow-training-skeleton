from airflow import models
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'AirFlow',
    'start_date': datetime(2015, 6, 1),
    'retry_delay': timedelta(minutes=5)
}

GCS_BUCKET = 'seth_sucks'
SQL_QUERY="SELECT * FROM land_registry_price_paid_uk LIMIT 1000"

with models.DAG('Exercise4', default_args=default_args) as dag:


    upload_data = PostgresToGoogleCloudStorageOperator(
        postgresq_conn_id='postgres',
        task_id='PostgresToCloud',
        sql=SQL_QUERY,
        filename="bla-123",
        bucket=GCS_BUCKET,
        dag = dag 
    )


