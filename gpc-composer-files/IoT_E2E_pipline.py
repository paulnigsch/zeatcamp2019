"""
code for cleaning sensor data - GCP version
"""

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
import os
import logging
from tempfile import mkstemp
from pprint import pprint
import pandas as pd
from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.hooks.base_hook import BaseHook

from azure.storage import *

from airflow.contrib.hooks.wasb_hook import WasbHook
from azure.storage.blob import BlockBlobService

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2019, 4, 6),
}

dag = DAG('IoT-E2E-Pipeline', schedule_interval='@daily', default_args=default_args)

working_bucket = "iot-e2e-bucket"

############################################################################
# utility functions
############################################################################

def removeFile(path):
    if os.path.isfile(path):
        os.remove(path)

def downloadGcSBlob(bucket, name, dest, remove_old=True):
    from google.cloud import storage
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(name)

    if remove_old:
        removeFile(dest)

    blob.download_to_filename(dest)

    print("downloaded %s/%s to %s" %(bucket, blob, dest))
    return True

def uploadGcSBlob(bucket, source_file_name, destination_blob_name):

    from google.cloud import storage
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)


    print("file %s uploaded to %s" %(source_file_name, destination_blob_name))
    return True

def get_curent_file_path(ds, prefix="", suffix=""):
    tmp = ds.replace("-", "_")
    if len(prefix) != '' and prefix[-1] != '/':
        prefix += '/'
    filepath = '%s%s/%s' % (prefix, tmp[0:7], tmp)

    if len(suffix) > 0:
        filepath += ".csv"

    return filepath




def get_in_out_paths(date, prev_stage, cur_stage):

    if not isinstance(date, str):
        raise AirflowException("wrong timeformat (should be a string)")


    in_path = "%s/fronius" % prev_stage
    out_path = "%s/fronius" % cur_stage

    infile_path = get_curent_file_path(date, in_path)
    outfile_path = get_curent_file_path(date, out_path)

    return infile_path, outfile_path


def process_iot_hub_message_file(filename):
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import DatumReader, DatumWriter
    import json

    reader = DataFileReader(open(filename, 'rb'), DatumReader())
    messages = []
    for reading in reader:
        parsed_json = json.loads(reading["Body"])
        messages.append(parsed_json)

    df_messages = pd.DataFrame(messages)
    return df_messages





############################################################################
# task logic
############################################################################


def input_is_present(ds, **context):



    path = "datalanding/fronius"

    logging.info("processing bucket: %s, path %s", working_bucket, path)
    blob_path = get_curent_file_path(ds, path, suffix="csv")

    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(working_bucket)

    logging.info("trying to get %s", blob_path)

    ret = bucket.get_blob(blob_path)
    if ret is None:
        logging.info("blob not found")
        import sys
        sys.exit(-1)
        return False
        raise AirflowException("blob not found")

    return True


def copy_files_from_azure_to_google(ds, **context):

    azure_conn_id = "custom_azure_credentials"
    connection = BaseHook.get_connection(azure_conn_id)

    block_blob_service = BlockBlobService(account_name=connection.login, account_key=connection.password)

    # Create a container called 'quickstartblobs'.
    container_name ='e2econtainer'
    block_blob_service.create_container(container_name)


    print("\nList blobs in the container")
    generator = block_blob_service.list_blobs(container_name)

    ds_split = ds.split("-")
    print(ds)
    print(ds_split)
    blobs = [b for b in generator]
    print(blobs)
    import re
    file_name_regex = re.compile(r'[A-z0-9]+/[0-9]{2}/%s/%s/%s/[0-2][0-9]/[0-5][0-9]' % tuple(ds_split))


    blobs_filtered = [ b for b in blobs if  file_name_regex.match(b.name)]

    from tempfile import NamedTemporaryFile
    from avro.io import DatumReader, DatumWriter
    from avro.datafile import DataFileReader
    import json
    from google.cloud import storage


    data = []

    for blob in blobs_filtered:
        print("processing " + blob.name)

        with NamedTemporaryFile(mode='a+b', delete=True) as f:

            blob = block_blob_service.get_blob_to_stream(
                container_name,
                blob.name,
                f.file)

            file_len = f.tell()
            f.seek(0)

            avro_reader = DataFileReader(f, DatumReader())
            for d in avro_reader:
                d["Body"] =  str(d["Body"])
                data.append(d)


    with NamedTemporaryFile(mode='a+', delete=True) as f:
        json.dump(data, f)
        f.seek(0)

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(working_bucket)
        blob = bucket.blob("raw/%s/%s.json" % (ds, ds) )
        blob.upload_from_file(f)



def stored_paresed_data_in_azure (ds, **context):

    fd, filename = mkstemp(suffix=".json")
    print("trying to download ")
    downloadGcSBlob(working_bucket, "raw/%s/%s.json" %(ds, ds), filename, remove_old=True)

    azure_conn_id = "custom_azure_credentials"
    connection = BaseHook.get_connection(azure_conn_id)

    block_blob_service = BlockBlobService(account_name=connection.login, account_key=connection.password)
    block_blob_service.create_blob_from_path(
        container_name="e2emessgageconverted",
        blob_name="json/%s/%s.json" %(ds, ds),
        file_path=filename)

    removeFile(filename)



############################################################################
# dag construction
############################################################################



task_copy_azure_gcp = PythonOperator(
    task_id="copy_files_azure_to_google",
    provide_context=True,
    depends_on_past=True,
    python_callable=copy_files_from_azure_to_google,
    dag=dag,
)



task_store_paresed_in_azure = PythonOperator(
    task_id="stored_paresed_data_in_azure",
    provide_context=True,
    depends_on_past=True,
    python_callable=stored_paresed_data_in_azure,
    dag=dag,
)

task_copy_azure_gcp >> task_store_paresed_in_azure
