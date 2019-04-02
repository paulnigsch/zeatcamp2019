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

in_bucket = "landingarea"

############################################################################
# utility functions
############################################################################

def removeFile(path):
    if os.path.isfile(path):
        os.remove(path)

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
    return True

def uploadGcSBlob(bucket, source_file_name, destination_blob_name):

    from google.cloud import storage
    import os
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



#ef ensure_folder_exits(folder):
#   from google.cloud import storage
#   storage_client = storage.Client()
#   bucket = storage_client.get_bucket(in_bucket)
#
#   logging.info("trying to get %s", inpath)
#
#   ret = bucket.get_blob(inpath)
#   if ret is None:
#       logging.info("blob not found")
#       import sys
#       sys.exit(-1)




############################################################################
# task logic
############################################################################


def input_is_present(ds, **context):

    path = "datalanding/fronius"

    logging.info("processing bucket: %s, path %s", in_bucket, path)
    blob_path = get_curent_file_path(ds, path, suffix="csv")

    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(in_bucket)

    logging.info("trying to get %s", blob_path)

    ret = bucket.get_blob(blob_path)
    if ret is None:
        logging.info("blob not found")
        import sys
        sys.exit(-1)
        return False
        raise AirflowException("blob not found")

    return True


def add_timestamps(ds, **context):
    import pandas as pd

    inpath, outpath = get_in_out_paths(ds, prev_stage='datalanding', cur_stage='w_timestamps')
    inpath += ".csv"
    outpath += ".msgpack"

    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(in_bucket)

    logging.info("trying to get %s", inpath)

    ret = bucket.get_blob(inpath)
    if ret is None:
        logging.info("blob not found")
        import sys
        sys.exit(-1)


    ## copy data


    from tempfile import mkstemp
    fd, filename =  mkstemp()
    if not downloadGcSBlob(in_bucket, inpath, filename, remove_old=True):
        raise AirflowException("error while downloading blob")



    data = pd.read_csv(filename, header=None)
    column_names = ("Time", 'IP', 'Vibration', 'v2', 'Temperature')

    data.columns =  column_names

    data["Timestamp"] = data['Time'].apply( lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timestamp() )

    invalid_temperature = data["Temperature"] < 0

    data["Temperature"][invalid_temperature] = None

    import os
    fd, out_filename =  mkstemp(suffix=".msgpack")
    data.to_msgpack(out_filename)

    print(os.listdir())

    logging.info("start uploading")

    uploadGcSBlob(in_bucket, out_filename, outpath)
    return True

def remove_duplicates(ds, **context):
    import pandas as pd
    import numpy as np
    import os

    infile, outfile = get_in_out_paths(ds, 'w_timestamps', cur_stage='duplicates_removed')
    infile += ".msgpack"
    outfile += ".msgpack"
    print('reading file from: %s' % (infile + '.msgpack'))

    from tempfile import mkstemp
    fd, filename =  mkstemp(suffix="msgpack")
    if not downloadGcSBlob(in_bucket, infile, filename, remove_old=True):
        raise AirflowException("error while downloading blob")


    d = pd.read_msgpack(filename)

    def get_duplicates(data):
        assert (data["Timestamp"].is_monotonic)
        data_shifted = ((data["Timestamp"].shift(1) - data["Timestamp"]) == 0)
        dups = data_shifted | data_shifted.shift(-1)
        return dups

    data_out = d.copy().set_index(["Timestamp", "IP"])
    for grp, d in d.groupby("IP"):
        print(grp)
        duplicated_vals = d[get_duplicates(d)]
        print('dups: ', len(duplicated_vals))
        dd = duplicated_vals.groupby(by="Timestamp", sort=False, as_index=False)
        ddd = dd.apply(lambda x: x.agg(
            {'Time': lambda x: x.iloc[0], "IP": lambda x: x.iloc[0], "Vibration": np.mean, "v2": np.mean,
             "Temperature": np.mean, "Timestamp": min}))
        dddd = ddd.set_index(["Timestamp", "IP"]).sort_index()
        data_out.loc[dddd.index] = dddd

    data_out= data_out.reset_index()
    no_dups = data_out.drop_duplicates()



    import os
    fd, out_filename =  mkstemp(suffix=".msgpack")
    no_dups.to_msgpack(out_filename)

    print(os.listdir())

    logging.info("start uploading")
    uploadGcSBlob(in_bucket, out_filename, outfile)

    return True


def reindex_data(ds, **kwargs):
    import pandas as pd
    import numpy as np
    import os.path
    import os

    infile, outfile = get_in_out_paths(ds, prev_stage='duplicates_removed', cur_stage='reindex')
    print('reading file from: %s' % (infile + '.msgpack'))

    infile += ".msgpack"
    outfile += ".msgpack"
    print('reading file from: %s' % (infile + '.msgpack'))

    from tempfile import mkstemp
    fd, filename =  mkstemp(suffix="msgpack")
    if not downloadGcSBlob(in_bucket, infile, filename, remove_old=True):
        raise AirflowException("error while downloading blob")

    data = pd.read_msgpack(filename)

    ##############################################
    # logic

    ip_index = ['192.168.0.101', '192.168.0.103', '192.168.0.105']
    good_sensor_vals = data[ data['IP'].apply( lambda x : x in ip_index)]

    ds_split = [int(i) for i in ds.split('-')]
    from datetime import datetime

    start_date = datetime(*ds_split, 0,0,0)
    end_date = datetime(*ds_split, 23, 59, 59)

    #dt_index = range(int(np.round(good_sensor_vals["Timestamp"].min())), int(np.round(good_sensor_vals["DT"].max())) + 1)
    dt_index = range(int(start_date.timestamp()), int(end_date.timestamp()) + 1)
    ar = list(map(lambda x: [(x, i) for i in ip_index], dt_index))

    new_index = [item for sublist in ar for item in sublist]
    d_indexed = good_sensor_vals.set_index(['Timestamp', "IP"])

    d_reindexed = d_indexed.reindex(new_index)
    data_out = d_reindexed.reset_index()

    ##############################################
    # storing

    import os
    fd, out_filename =  mkstemp(suffix=".msgpack")
    data_out.to_msgpack(out_filename)

    print(os.listdir())

    logging.info("start uploading")
    uploadGcSBlob(in_bucket, out_filename, outfile)

    return True




############################################################################
# dag construction
############################################################################

task_input_check = PythonOperator(
    task_id='check_input_availability',
    provide_context=True,
    python_callable=input_is_present,
    #depends_on_past = True,
    #inlets={"datasets" :[infile]},
    #outlets={"datasets" :[outfile]},
    dag=dag)

task_add_timestamps = PythonOperator(
    task_id='add_timestamps',
    provide_context=True,
    python_callable=add_timestamps,
    #depends_on_past = True,
    #inlets={"datasets" :[infile]},
    #outlets={"datasets" :[outfile]},
    dag=dag)


task_remove_duplicates = PythonOperator(
    task_id='remove_duplicates',
    provide_context=True,
    python_callable=remove_duplicates,
    #depends_on_past = True,
    #inlets={"datasets" :[infile]},
    #outlets={"datasets" :[outfile]},
    dag=dag)

task_reindex_data = PythonOperator(
    task_id='reindex_data',
    provide_context=True,
    python_callable=reindex_data,
    #depends_on_past = True,
    #inlets={"datasets" :[infile]},
    #outlets={"datasets" :[outfile]},
    dag=dag)

task_input_check >> task_add_timestamps >> task_remove_duplicates >> task_reindex_data

