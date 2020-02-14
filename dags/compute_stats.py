from airflow import models
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcPySparkOperator
from datetime import datetime, timedelta
from operators.http_to_gcs import HttpToGcsOperator

default_args = {
    'owner': 'AirFlow',
    'start_date': datetime(2020, 2, 14),
    'retry_delay': timedelta(minutes=5)
}

GCS_BUCKET = 'seth_sucks'
PYSPARK_JOB = 'gs://' + GCS_BUCKET + '/spark-jobs/compute_aggregates.py'

with models.DAG('ComputeStats', default_args=default_args, schedule_interval="0 0 * * *") as dag:

    create_cluster = DataprocClusterCreateOperator(
        task_id='CreateCluster',
        cluster_name="analyse-pricing-{{ ds }}",
        project_id="afspfeb3-9d4bdb09f618016d0bc39",
        num_workers=2,
        zone="europe-west4-a",
        dag=dag,
    )

    compute_aggregates = DataProcPySparkOperator(
        task_id="compute_aggregates",
        main=PYSPARK_JOB,
        cluster_name="analyse-pricing-{{ ds }}",
        dag=dag,
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='DeleteCluster',
        cluster_name="analyse-pricing-{{ ds }}",
        project_id="afspfeb3-9d4bdb09f618016d0bc39",
        dag=dag,
    )

    create_cluster >> compute_aggregates >> delete_cluster
    


