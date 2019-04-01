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

dag = DAG('fron-gpc-v1', schedule_interval='@daily', default_args=default_args)



def print_bucket_content(ds, **context): 
    from pprint import pprint
    pprint(context)
    print("---------------------------------------")
    return_value_of_prev_task = context['task_instance'].xcom_pull(task_ids='GCS_List_Files')
    for i in  return_value_of_prev_task :
        print(i)

    print("---------------------------------------")
    list_bucket_content = GoogleCloudStorageListOperator(
        task_id='GCS_Files',
        bucket='landingarea',
        prefix='datalanding/fronius/',
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

list_bucket_content = GoogleCloudStorageListOperator(
    dag=dag,
    task_id='GCS_List_Files',
    bucket='landingarea',
    prefix='datalanding/fronius/',
    delimiter='.csv',
    google_cloud_storage_conn_id="google_cloud_storage_default",
)

list_bucket_content >> print_context_to_log


