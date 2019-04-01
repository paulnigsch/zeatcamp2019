"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator


from pprint import pprint

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

dag = DAG('fron-gpc-v2', schedule_interval='@daily', default_args=default_args)



def print_bucket_content(ds, **context): 
    pprint(context)
    print("---------------------------------------")
    return_value_of_prev_task = context['task_instance'].xcom_pull(task_ids='GCS_List_Files')
    for i in  return_value_of_prev_task :
        print(i)

    print("---------------------------------------")
    list_bucket_content = GoogleCloudStorageListOperator(
        task_id='GCS_Files',
        bucket='landingarea',
        prefix='datalanding/fronius/2018_1/',
        delimiter='.csv',
        google_cloud_storage_conn_id="google_cloud_storage_default",
    )

    print("conents of list_bucket_content:" )
    print(list_bucket_content)
    print("---------------------------------------")

    print("print df")
    print(type(ds))
    print(ds)

    return 'Whatever you return gets printed in the logs'

print_context_to_log = PythonOperator(
    task_id="print_bucket_content",
    provide_context=True,
    python_callable=print_bucket_content,
    dag=dag,

    start_date= datetime(2018, 1, 29),
    )

copy_to_local=GoogleCloudStorageDownloadOperator(
    dag=dag,
    task_id='GCS_copy_file',
    bucket='landingarea',
    google_cloud_storage_conn_id="google_cloud_storage_default",
    object='datalanding/fronius/{{ ds.replace("-","_")[0:7] }}/{{ ds.replace("-","_") }}.csv', 
    filename='{{ ds.replace("-","_") }}.csv'
    #store_to_xcom_key="fileOutput" # file too large
) 

list_bucket_content = GoogleCloudStorageListOperator(
    dag=dag,
    task_id='GCS_List_Files',
    bucket='landingarea',
    prefix='datalanding/fronius/',
    delimiter='.csv',
    google_cloud_storage_conn_id="google_cloud_storage_default",
)


def list_local_file(ds, **context):
    print("==============================================")
    return_value_of_prev_task = context['task_instance'].xcom_pull(task_ids='GCS_copy_file')
    pprint(return_value_of_prev_task)
    print("==============================================")
    import os
    pprint(os.listdir())

task_list_local_file = PythonOperator(
    task_id="list_local_file",
    provide_context=True,
    python_callable=list_local_file,
    dag=dag,
    start_date= datetime(2018, 1, 29),
    )

def use_python_sdk(ds, **context):
    print("==============================================")
    # Imports the Google Cloud client library
    from google.cloud import storage
    storage_client = storage.Client()

   
    bucket = storage_client.get_bucket("landingarea")
    blob = bucket.blob('datalanding/fronius/2018_01/2018_01_29.csv')

    blob.download_to_filename("test.csv")

    print('Blob {} downloaded to {}.'.format(
        'datalanding/fronius/2018_01/2018_01_29.csv',
        "test.csv"))
    print("==============================================")
    import os
    pprint(os.listdir())
    print("==============================================")


task_use_python_sdk = PythonOperator(
    task_id="use_python_sdk",
    provide_context=True,
    python_callable=use_python_sdk,
    dag=dag,
    start_date= datetime(2018, 1, 29),
    )

list_bucket_content >> print_context_to_log >>  copy_to_local >> task_list_local_file >>  task_use_python_sdk 


