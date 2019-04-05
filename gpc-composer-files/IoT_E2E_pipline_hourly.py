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
import dateutil.parser
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

dag = DAG('new-IoT-e2e-pipeline_hourly', schedule_interval='@hourly', default_args=default_args)

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


def copy_files_from_azure_to_google(ds, ts, **context):

    dt = dateutil.parser.parse(ts)


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
    file_name_regex = re.compile(r'[A-z0-9]+/[0-9]{2}/%04i/%02i/%02i/%02i/[0-5][0-9]' % (dt.year, dt.month, dt.day, dt.hour))


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
        blob = bucket.blob("raw_hourly/%s/%02i/%s.json" % (ds, dt.hour, ds) )
        blob.upload_from_file(f)



def stored_parsed_data_in_azure (ds, ts, **context):
    dt = dateutil.parser.parse(ts)

    fd, filename = mkstemp(suffix=".json")
    logging.info("start downloading")
    in_fn = "raw_hourly/%s/%02i/%s.json" % (ds, dt.hour, ds)
    out_fn = "json_hourly/%s/%02i/%s.json" % (ds, dt.hour, ds)
    downloadGcSBlob(working_bucket, in_fn, filename, remove_old=True)

    azure_conn_id = "custom_azure_credentials"
    connection = BaseHook.get_connection(azure_conn_id)

    block_blob_service = BlockBlobService(account_name=connection.login, account_key=connection.password)
    block_blob_service.create_blob_from_path(
        container_name="e2emessgageconverted",
        blob_name=out_fn,
        file_path=filename)

    removeFile(filename)


def parseMessage(msg):
    try:
        m = {}
        time = msg["EnqueuedTimeUtc"]
        body = msg["Body"]
        props = msg["SystemProperties"]

        m["EnqueuedTimeUtc"] = time

        b = {}

        b["dateTime"] = body["dateTime"]
        b["ip"] = str(body["ip"])
        b["vibration"] =float(body["vibration"])
        b["voltage"] =float(body["voltage"])
        b["temperature"] =float(body["temperature"])
        b["messageId"] =str(body["messageId"])

        m["EnqueuedTimeUtc"] = time
        m["Body"] = b
        m["SystemProperties"] = props

        return m


    except Exception as e:
        #pprint(e)
        #pprint(msg)
        return None


def create_big_query_input(ds, ts,**context):

    print("\nstart create_big_query_input")

    dt = dateutil.parser.parse(ts)
    #downloadGcSBlob(working_bucket, name, dest)

    fd, filename = mkstemp(suffix=".json")
    logging.info("start downloading")
    in_fn = "raw_hourly/%s/%02i/%s.json" % (ds, dt.hour, ds)
    out_fn = "big_query_hourly/%s/%02i/%s.json" % (ds, dt.hour, ds)
    downloadGcSBlob(working_bucket, in_fn, filename, remove_old=True)

    outputfile, outputfile_name = mkstemp(suffix=".json")
    with open(filename, 'r') as inputfile:
        with open( outputfile_name, "a+") as outputfile:
            logging.info("parsing json file")
            content = json.load(inputfile)
            logging.info("start processing messages")
            counter=0
            for msg in content:
                parsed_msg = parseMessage(msg)
                if parsed_msg == None:
                    continue

                outputfile.write(json.dumps(parsed_msg))
                outputfile.write('\n')
                counter +=1

            logging.info("start uploading")
            print('\n number of entries: %i' % counter)
            uploadGcSBlob(working_bucket, outputfile_name, out_fn)


    removeFile(filename)
    removeFile(outputfile_name)



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
				"name": "id",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "timestamp",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "deviceId",
		        "mode" : "NULLABLE",
				"type": "STRING"
			},
			{
				"name": "messageId",
		        "mode" : "NULLABLE",
				"type": "STRING"
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
			},
            {
            "name": "dateTime",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {
                    "name": "date",
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": [
                        {
                            "name": "year",
                            "mode": "NULLABLE",
                            "type": "INTEGER"
                        },
                        {
                            "name": "month",
                            "mode": "NULLABLE",
                            "type": "INTEGER"
                        },
                        {
                            "name": "day",
                            "mode": "NULLABLE",
                            "type": "INTEGER"
                        }
                    ]
                },
                {
                    "name": "time",
                    "mode": "NULLABLE",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "hour",
                            "mode": "NULLABLE",
                            "type": "INTEGER"
                        },
                        {
                            "name": "minute",
                            "mode": "NULLABLE",
                            "type": "INTEGER"
                        },
                        {
                            "name": "second",
                            "mode": "NULLABLE",
                            "type": "INTEGER"
                        },
                        {
                            "name": "nano",
                            "mode": "NULLABLE",
                            "type": "INTEGER"
                        }
                    ]
                }
            ]
        },
        {
            "name": "ip",
            "mode": "NULLABLE",
            "type": "STRING"
        },
        {
            "name": "vibration",
            "mode": "NULLABLE",
            "type": "FLOAT"
        },
        {
            "name": "voltage",
            "mode": "NULLABLE",
            "type": "FLOAT"
        }
		]
	}
]

"""

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

task_create_bq_input = PythonOperator(
    task_id="create_big_query_input",
    provide_context=True,
    depends_on_past=False,
    python_callable=create_big_query_input,
    dag=dag,
)


task_store_parsed_in_azure = PythonOperator(
    task_id="stored_parsed_data_in_azure",
    provide_context=True,
    depends_on_past=True,
    python_callable=stored_parsed_data_in_azure,
    dag=dag,
)

task_copy_azure_gcp >> task_create_bq_input # >> task_load_into_bigquery
task_copy_azure_gcp >> task_store_parsed_in_azure
