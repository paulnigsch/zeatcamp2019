"""
code for cleaning sensor data - GCP version
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
import os
import logging



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2018, 2, 1),
}

dag = DAG('fron-gpc-datacleaning', schedule_interval='@daily', default_args=default_args)


############################################################################
# utility functions
############################################################################

def removeFile(path):
    if os.path.isfile(dest):
        os.remove(dest)

def downloadGcSBlob(bucket, name, dest, remove_old=True):
    from google.cloud import storage
    import os
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(name)

    if remove_old:
        removeFile(dest)

    blob.download_to_filename(dest)

    print("downloaded %s/%s to %s" %(bucket, blob, dest))


def get_curent_file_path(ds, prefix=""):
    tmp = ds.replace("-", "_")
    if len(prefix) != '' and prefix[-1] != '/':
        prefix += '/'
    filepath = '%s%s/%s.csv' % (prefix, tmp[0:7], tmp)
    return filepath




############################################################################
# task logic
############################################################################


def input_is_present(ds, **context):
    in_bucket = "landingarea"

    path = "datalanding/fronius"

    logging.info("processing bucket: %s, path %s", in_bucket, path)
    blob_path = get_curent_file_path(ds, '%s/%s' % (in_bucket, path))

    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(in_bucket)

    logging.info("trying to get %s", blob_path)

    ret = bucket.get_blob(blob_path)

    if ret is None:
        logging.info("blob not found")
        return False

    return True





############################################################################
# dag construction
############################################################################

task_input_check = PythonOperator(
    task_id='check_input_availability',
    provide_context=True,
    python_callable=input_is_present,
    depends_on_past = True,
    #inlets={"datasets" :[infile]},
    #outlets={"datasets" :[outfile]},
    dag=dag)

