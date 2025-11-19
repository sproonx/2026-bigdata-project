
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
    HdfsMkdirsFileOperator,
)
from airflow.operators.filesystem_operations import (
    CreateDirectoryOperator,
    ClearDirectoryOperator,
)
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator

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
    save_to="/home/airflow/bikesharing/hubway_data.zip",
    dag=dag,
)

unzip_hubway_data = UnzipFolderOperator(
    task_id="unzip_hubway_data",
    zip_file="/home/airflow/bikesharing/hubway_data.zip",
    extract_to="/home/airflow/bikesharing/hubway_data/",
    dag=dag,
)

combine_old_format_split_years = BashOperator(
    task_id="combine_old_format_split_years",
    bash_command='''
        cd /home/airflow/bikesharing/hubway_data/
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

# Dependencies
create_local_import_dir >> clear_local_import_dir >> download_hubway_data >> unzip_hubway_data >> combine_old_format_split_years
