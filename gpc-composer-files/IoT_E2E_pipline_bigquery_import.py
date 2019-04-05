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
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator, BigQueryValueCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

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

dag = DAG('new-IoT-e2e-pipeline_bq_import', schedule_interval='@daily', default_args=default_args)

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


def create_big_query_input(ds, ts,**context):

    print("\nstart create_big_query_input")
    import re
    dt = dateutil.parser.parse(ts)

    file_name_regex = re.compile(r'big_query_hourly/%04i-%02i-%02i/.*' % (dt.year, dt.month, dt.day))

    from google.cloud import storage
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(working_bucket)
    blobs = bucket.list_blobs(prefix="big_query_hourly/")
    #blobs = bucket.list_blobs()


    #downloadGcSBlob(working_bucket, name, dest)

    logging.info("start downloading")

    filenames = []
    fd, outfile_name = mkstemp(suffix=".json")
    out_desc = open(outfile_name, 'a+')
    for b in  blobs:
        if file_name_regex.match(b.name):
            fd, filename = mkstemp(suffix=".json")
            filenames.append(filename)
            downloadGcSBlob(working_bucket, b.name, filename)

            bytes_written = 0
            with open(filename, 'r') as f:
                for l in f.readlines():
                    n_bytes = out_desc.write(l)
                    bytes_written += n_bytes

            if bytes_written > 0:
                out_desc.write("\n")

    out_desc.close()



    uploadGcSBlob(working_bucket, outfile_name, "big_query_daily/%04i_%02i_%02i.json" %(dt.year, dt.month, dt.day))


    removeFile(outfile_name)
    for f in filenames:
        removeFile(f)


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

def determine_branch(ts,**context):

    #return "delete_dataset"
    return "join_branches"


############################################################################
# dag construction
############################################################################


task_load_into_bigquery = GoogleCloudStorageToBigQueryOperator(
     task_id="create_big_table_table",
     bucket=working_bucket,
     source_objects=['big_query_daily/{{ ds.replace("-", "_") }}.json'],
     destination_project_dataset_table="camp2019-pani-236011.e2e_testdata.merged_{{ ts_nodash[0:8] }}",
     schema_fields=json.loads(bigquery_schema),
     depends_on_past=True,
     trigger_rule="all_done",
     dag=dag,
     source_format='NEWLINE_DELIMITED_JSON'
)

from airflow.contrib.operators.bigquery_operator import BigQueryDeleteDatasetOperator
task_delete_bq_table = BigQueryDeleteDatasetOperator(
    dataset_id = 'e2e_testdata.merged_{{ ts_nodash[0:8] }}',
    project_id = 'camp2019-pani-236011',
    task_id='delete_dataset',
    retries=0,
    dag=dag)


#task_check_for_existing_dataset = BigQueryCheckOperator(
#    sql='SELECT count(1) FROM `camp2019-pani-236011.e2e_testdata.__TABLES__` where table_id = "merged_{{ ts_nodash[0:8] }}"',
#    task_id="check_for_dataset",
#    provide_context=True,
#    depends_on_past=True,
#    retries=0,
#    dag=dag,
#)


# task_determine_branch       = BranchPythonOperator(
#     task_id="trigger_table_deletion",
#     provide_context=True,
#     depends_on_past=True,
#     python_callable=determine_branch,
#     dag=dag,
# )

task_create_bq_input = PythonOperator(
    task_id="create_big_query_input",
    provide_context=True,
    depends_on_past=True,
    python_callable=create_big_query_input,
    dag=dag,
)

#ask_join = DummyOperator(
#    dag=dag,
#    task="join_branches"
#)

task_create_bq_input  >> task_delete_bq_table   >> task_load_into_bigquery

#task_create_bq_input  >> task_check_for_existing_dataset
#task_check_for_existing_dataset >> task_determine_branch >> task_delete_bq_table >> task_join
#task_determine_branch >> task_join
#task_join >> task_load_into_bigquery
