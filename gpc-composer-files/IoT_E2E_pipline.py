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
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from tempfile import NamedTemporaryFile

import json
from azure.storage import *

from airflow.contrib.hooks.wasb_hook import WasbHook
from azure.storage.blob import BlockBlobService
#from airflow.contrib.operators.gcp_sql_operator import CloudSqlInstanceImportOperator

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

dag = DAG('new-IoT-e2e-pipeline', schedule_interval='@daily', default_args=default_args)

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


def flattenjson( b, delim ):
    val = {}
    for i in b.keys():
        if isinstance( b[i], dict ):
            get = flattenjson( b[i], delim )
            for j in get.keys():
                val[ i + delim + j ] = get[j]
        else:
            val[i] = b[i]

    return val


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
                message_body = d["Body"]
                if len(message_body) > 0:
                    try:
                        d["Body"] = json.loads(message_body)
                    except Exception as e:
                        logging.error("Error: while parsing json")
                        print(type(message_body))
                        pprint(message_body)

                        from sys import exit
                        exit(-1)


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

def store_every_msg_in_azure(ds, **context):

    fd, filename = mkstemp(suffix=".json")
    print("trying to download ")
    downloadGcSBlob(working_bucket, "raw/%s/%s.json" %(ds, ds), filename, remove_old=True)

    azure_conn_id = "custom_azure_credentials"
    connection = BaseHook.get_connection(azure_conn_id)

    import json
    with open(filename, 'r') as f:
        content = json.load(f)
        block_blob_service = BlockBlobService(account_name=connection.login, account_key=connection.password)
        count = 0
        for mesg in content:
            with NamedTemporaryFile(mode='a+b', delete=True) as outfile:
                try:
                    outfile.write( json.dumps(mesg).encode())
                except Exception as e :
                    logging.error(str(e))
                    print(type(mesg))
                    pprint(mesg)
                    from sys import exit
                    exit(-1)

                outfile.seek(0)
                block_blob_service.create_blob_from_stream(
                    container_name="e2emessgageconverted",
                    blob_name="single_files/%s/%i.json" %(ds, count),
                    stream=outfile)
                count += 1

    removeFile(filename)



def create_big_query_input(ds, **context):

    print("\nstart create_big_query_input")
    #downloadGcSBlob(working_bucket, name, dest)

    fd, filename = mkstemp(suffix=".json")
    logging.info("start downloading")
    downloadGcSBlob(working_bucket, "raw/%s/%s.json" %(ds, ds), filename, remove_old=True)

    outputfile, outputfile_name = mkstemp(suffix=".json")
    with open(filename, 'r') as inputfile:
        with open( outputfile_name, "a+") as outputfile:
            logging.info("parsing json file")
            content = json.load(inputfile)
            logging.info("start processing messages")
            for msg in content:
                del msg["Properties"]
                #outputfile.write(json.dumps(msg["Body"]))
                outputfile.write(json.dumps(msg))
                outputfile.write('\n')

            logging.info("start uploading")
            uploadGcSBlob(working_bucket, outputfile_name, 'bigtable/%s/%s.json' % (ds, ds))


    removeFile(filename)
    removeFile(outputfile_name)



def flatten_files(ds, **context):

    fd, filename = mkstemp(suffix=".json")
    logging.info("start downloading")
    downloadGcSBlob(working_bucket, "raw/%s/%s.json" %(ds, ds), filename, remove_old=True)

    with open(filename, 'r') as f:
        messages = json.load(f)

    columns = ['EnqueuedTimeUtc', 'SystemProperties.messageId', 'SystemProperties.correlationId', 'SystemProperties.connectionDeviceId', 'SystemProperties.connectionAuthMethod', 'SystemProperties.connectionDeviceGenerationId', 'SystemProperties.contentType', 'SystemProperties.enqueuedTime', 'Body.deviceId', 'Body.messageId', 'Body.temperature', 'Body.humidity']


    messages_flat = [ flattenjson(m,'.') for m in messages  ]
    df = pd.DataFrame(messages_flat)

    fd, outfile = mkstemp(suffix=".csv")
    df.to_csv(outfile)


    uploadGcSBlob(working_bucket, outfile, 'csv/%s/%s.json' % (ds, ds) )

    logging.info("write to mysql")
    from sqlalchemy import create_engine

    host="XXXXXXXXXXXX"
    database="powerbi_stuff"
    user="powerbi"
    password="XXXXXXXXXXXXXXXX"

    engine=create_engine("mysql://%s:%s@%s/%s" % (user, password, host, database) )
    df.head(100).to_sql(ds.replace('-',"_"), engine, if_exists='replace', chunksize=100)


    removeFile(outfile)
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

bigquery_schema = """
[
	{
		"name": "EnqueuedTimeUtc",
		"type": "STRING",
		"mode" : "NULLABLE" 
	},
	{
		"name": "SystemProperties",
		"mode" : "NULLABLE",
		"type": "RECORD",
		"fields": [
			{
				"name": "messageId",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "correlationId",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "connectionDeviceId",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "connectionAuthMethod",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "connectionDeviceGenerationId",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "contentType",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "enqueuedTime",
		        "mode" : "NULLABLE",
				"type": "STRING"
			}
		]
	},
	{
		"name": "Body",
		"type": "RECORD",
		"mode" : "NULLABLE",
		"fields": [
			{
				"name": "deviceId",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "messageId",
		        "mode" : "NULLABLE",
				"type": "INTEGER"
			},
			{
				"name": "temperature",
		        "mode" : "NULLABLE",
				"type": "FLOAT"
			},
			{
				"name": "humidity",
		        "mode" : "NULLABLE",
				"type": "FLOAT"
			}
		]
	}
]

"""


task_load_into_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id="create_big_table_table",
    bucket=working_bucket,
    source_objects=['bigtable/{{ ds }}/{{ ds }}.json'],
    destination_project_dataset_table="camp2019-pani-236011.e2e_testdata.airflow_{{ ds.replace('-','_') }}",
    schema_fields=json.loads(bigquery_schema),
    dag=dag,
    source_format='NEWLINE_DELIMITED_JSON'
)


# task_store_every_paresed_in_azure = PythonOperator(
#     task_id="store_single_messages_in_azure",
#     provide_context=True,
#     depends_on_past=True,
#     python_callable=store_every_msg_in_azure,
#     dag=dag,
# )

task_create_bq_input = PythonOperator(
    task_id="create_big_query_input",
    provide_context=True,
    depends_on_past=True,
    python_callable=create_big_query_input,
    dag=dag,
)

task_flatten_json_files = PythonOperator(
    task_id="flatten_json_files",
    provide_context=True,
    depends_on_past=True,
    python_callable=flatten_files,
    dag=dag
)

#task_insert_into_sql = CloudSqlInstanceImportOperator(
#    dag=dag,
#    task_id="import_into_sql",
#    instance='powerbi-source',
#    body
#)

task_copy_azure_gcp >> task_store_paresed_in_azure  #>> task_store_every_paresed_in_azure
task_copy_azure_gcp >> task_create_bq_input >> task_load_into_bigquery
task_copy_azure_gcp >> task_flatten_json_files
