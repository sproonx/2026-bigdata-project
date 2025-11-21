# -*- coding: utf-8 -*-
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_folder_operations import UnzipFolderOperator
from airflow.operators.hdfs_operations import (
    HdfsPutFilesOperator,
    HdfsGetFileOperator,
    HdfsMkdirsFileOperator,
)
from airflow.operators.filesystem_operations import (
    CreateDirectoryOperator,
    ClearDirectoryOperator,
)

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from utils.file_name_formatting import *

args = {
    "owner": "airflow"
}

# DAG definition
dag = DAG(
    "BikeSharing",
    default_args=args,
    description="BikeSharing",
    schedule_interval="56 18 * * *",
    start_date=datetime(2019, 10, 16),
    catchup=False,
    max_active_runs=1,
)

# Tasks
create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow",
    directory="bikesharing_input",
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/bikesharing_input",
    pattern="*",
    dag=dag,
)

create_local_output_dir = CreateDirectoryOperator(
    task_id="create_output_dir",
    path="/home/airflow",
    directory="bikesharing_output",
    dag=dag,
)

clear_local_output_dir = ClearDirectoryOperator(
    task_id="clear_output_dir",
    directory="/home/airflow/bikesharing_output",
    pattern="*",
    dag=dag,
)

download_hubway_data = HttpDownloadOperator(
    task_id="download_hubway_data",
    download_uri="https://www.kaggle.com/api/v1/datasets/download/acmeyer/hubway-data",
    save_to="/home/airflow/bikesharing_input/hubway_data.zip",
    dag=dag,
)

unzip_hubway_data = UnzipFolderOperator(
    task_id="unzip_hubway_data",
    zip_file="/home/airflow/bikesharing_input/hubway_data.zip",
    extract_to="/home/airflow/bikesharing_input/hubway_data/",
    dag=dag,
)

combine_split_years_old_format = BashOperator(
    task_id="combine_old_format_split_years",
    bash_command='''
        cd /home/airflow/bikesharing_input/hubway_data/
        for year in $(ls hubway_Trips_*_*.csv | sed -E 's/.*hubway_Trips_([0-9]{4})_.*/\\1/' | sort -u); do
            count=$(ls hubway_Trips_${year}_*.csv 2>/dev/null | wc -l)
            if [ "$count" -gt 0 ]; then
                cat hubway_Trips_${year}_*.csv > hubway_Trips_${year}.csv
                rm hubway_Trips_${year}_*.csv
            fi
        done
    ''',
    dag=dag,
)

get_yyyy_old_format = PythonOperator(
    task_id="get_yyyy_old_format",
    python_callable=get_yyyy_old_format,
    op_kwargs={"parent_dir_path": "/home/airflow/bikesharing_input/hubway_data/"},
    dag=dag,
)

create_raw_hdfs_dir_old_format = HdfsMkdirsFileOperator(
    task_id="create_raw_hdfs_dir_old_format",
    parent_directory="/data/bikesharing/raw/",
    folder_names="{{ task_instance.xcom_pull(task_ids='get_yyyy_old_format') }}",
    hdfs_conn_id="hdfs",
    dag=dag,
)
create_raw_hdfs_dir_old_format.set_upstream(get_yyyy_old_format)

get_yyyyMM_new_format = PythonOperator(
    task_id="get_yyyyMM_new_format",
    python_callable=get_yyyyMM_new_format,
    op_kwargs={"parent_dir_path": "/home/airflow/bikesharing_input/hubway_data/"},
    dag=dag,
)

create_raw_hdfs_dir_new_format = HdfsMkdirsFileOperator(
    task_id="create_raw_hdfs_dir_new_format",
    parent_directory="/data/bikesharing/raw/",
    folder_names="{{ task_instance.xcom_pull(task_ids='get_yyyyMM_new_format') }}",
    hdfs_conn_id="hdfs",
    dag=dag,
)
create_raw_hdfs_dir_new_format.set_upstream(get_yyyyMM_new_format)

get_mv_import_raw_pairs = PythonOperator(
    task_id="get_mv_import_raw_pairs",
    python_callable=get_mv_import_raw_pairs,
    op_kwargs={"parent_dir_path": "/home/airflow/bikesharing_input/hubway_data/"},
    dag=dag,
)

hdfs_put_files = HdfsPutFilesOperator(
    task_id="hdfs_put_files",
    local_remote_pairs="{{ task_instance.xcom_pull(task_ids='get_mv_import_raw_pairs') }}",
    hdfs_conn_id="hdfs",
    dag=dag,
)
hdfs_put_files.set_upstream(get_mv_import_raw_pairs)

hdfs_put_files_station = HdfsPutFilesOperator(
    task_id="hdfs_put_files_station",
    local_remote_pairs="[('/home/airflow/bikesharing_input/hubway_data/Hubway_Stations_2011_2016.csv','/data/bikesharing/raw/Hubway_Stations_2011_2016.csv')]",
    hdfs_conn_id="hdfs",
    dag=dag,
)

run_clean_new_format = SparkSubmitOperator(
    task_id="run_clean_new_format",
    application="/home/airflow/airflow/spark/cleanNewFormat.py",
    name="clean_new_format",
    conn_id="spark",
    total_executor_cores="2",
    executor_cores="2",
    executor_memory="2g",
    num_executors="2",
    application_args=[
        "--base-path", "/data/bikesharing/raw",
        "--output-path", "/data/bikesharing/final"
    ],
    conf={"spark.sql.sources.partitionOverwriteMode": "static"},
    dag=dag,
)

run_clean_old_format = SparkSubmitOperator(
    task_id="run_clean_old_format",
    application="/home/airflow/airflow/spark/cleanOldFormat.py",
    name="clean_old_format",
    conn_id="spark",
    total_executor_cores="2",
    executor_cores="2",
    executor_memory="2g",
    num_executors="2",
    application_args=[
        "--base-path", "/data/bikesharing/raw",
        "--output-path", "/data/bikesharing/final",
        "--stations-path", "/data/bikesharing/raw/Hubway_Stations_2011_2016.csv"
    ],
    conf={"spark.sql.sources.partitionOverwriteMode": "static"},
    dag=dag,
)

run_calculate_kpis = SparkSubmitOperator(
    task_id="run_calculate_kpis",
    application="/home/airflow/airflow/spark/calculateKPIs.py",
    name="calculate_kpis",
    conn_id="spark",
    total_executor_cores="2",
    executor_cores="2",
    executor_memory="2g",
    num_executors="2",
    application_args=[
        "--input-path", "/data/bikesharing/final",
        "--output-path", "/home/airflow/bikesharing_output/kpis.xlsx"
    ],
    dag=dag,
)

# Dummies
data_import = DummyOperator(task_id="data_import", dag=dag)
hdfs_process_raw = DummyOperator(task_id="hdfs_process_raw", dag=dag)
hdfs_process_final = DummyOperator(task_id="hdfs_process_final", dag=dag)
kpis_calculated = DummyOperator(task_id="kpis_calculated", dag=dag)

# Dependencies
data_import >> create_local_import_dir >> clear_local_import_dir >> download_hubway_data >> unzip_hubway_data >> hdfs_process_raw
data_import >> create_local_output_dir >> clear_local_output_dir >> download_hubway_data

hdfs_process_raw >> combine_split_years_old_format >> get_yyyy_old_format >> create_raw_hdfs_dir_old_format >> get_mv_import_raw_pairs
hdfs_process_raw >> get_yyyyMM_new_format >> create_raw_hdfs_dir_new_format >> get_mv_import_raw_pairs

get_mv_import_raw_pairs >> hdfs_put_files
get_mv_import_raw_pairs >> hdfs_put_files_station

hdfs_put_files >> run_clean_new_format
hdfs_put_files >> run_clean_old_format

hdfs_put_files_station >> run_clean_old_format

run_clean_new_format >> hdfs_process_final
run_clean_old_format >> hdfs_process_final

hdfs_process_final >> run_calculate_kpis >> kpis_calculated