import json
import pathlib
import posixpath
import airflow
import requests
from airflow.models import DAG, BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import tempfile

def _download_rocket_launches(query, result_path, result_filename, **context):
#    query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"

    response = requests.get(query)
    f_path = posixpath.join(result_path, result_filename)
    
    tf = tempfile.TemporaryFile()
    tf.write(response.text)

    gcs = GoogleCloudStorageHook()
    gcs.upload("rocket-launches", f_path, tf.name)

class LaunchLibraryOperator(BaseOperator):
    template_fields = ("params",)
    @apply_defaults
    def __init__(
            self,
            conn_id: str,
            endpoint: str,
            params: dict,
            result_path: str,
            result_filename: str,
            *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.endpoint = endpoint
        self.params = params
        self.result_path = result_path
        self.result_filename = result_filename

    def execute(self, context):
        print(self.params)
        query = "{}1.4/{}?startdate={}&enddate={}".format(BaseHook.get_connection(self.conn_id).host, self.endpoint,
            self.params['startdate'], self.params['enddate'])
        print(query)
        _download_rocket_launches(query, result_path=self.result_path, result_filename=self.result_filename)


