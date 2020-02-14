from airflow import models
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from datetime import datetime, timedelta
from operators.http_to_gcs import HttpToGcsOperator

default_args = {
    'owner': 'AirFlow',
    'start_date': datetime(2015, 6, 1),
    'retry_delay': timedelta(minutes=5)
}

GCS_BUCKET = 'seth_sucks'

with models.DAG('FetchExchangeRates', default_args=default_args) as dag:

    upload_data = HttpToGcsOperator(
        task_id='FetchExchangeRates',
        endpoint='history?start_at=2014-01-01&end_at=2018-01-02&symbols=EUR&base=GBP',
        gcs_bucket=GCS_BUCKET,
        gcs_path="exchange_rates_2014-2018.json",
        dag=dag,
    )


