
# -*- coding: utf-8 -*-
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_folder_operations import UnzipFolderOperator
from airflow.operators.hdfs_operations import (
    HdfsPutFileOperator,
    HdfsGetFileOperator,
    HdfsMkdirFileOperator,
)
from airflow.operators.filesystem_operations import (
    CreateDirectoryOperator,
    ClearDirectoryOperator,
)
from airflow.operators.hive_operator import HiveOperator

args = {"owner": "airflow"}

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
    directory="bikesharing",
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/bikesharing",
    pattern="*",
    dag=dag,
)

download_hubway_data = HttpDownloadOperator(
    task_id="download_hubway_data",
    download_uri="https://www.kaggle.com/api/v1/datasets/download/acmeyer/hubway-data",
    save_to="/home/airflow/bikesharing/hubway_data_{{ ds }}.zip",
    dag=dag,
)

unzip_hubway_data = UnzipFolderOperator(
    task_id="unzip_hubway_data",
    zip_file="/home/airflow/bikesharing/hubway_data_{{ ds }}.zip",
    extract_to="/home/airflow/bikesharing/hubway_data_{{ ds }}/",
    dag=dag,
)

# Dependencies
create_local_import_dir >> clear_local_import_dir >> download_hubway_data >> unzip_hubway_data
