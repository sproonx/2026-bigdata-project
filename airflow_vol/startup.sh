#!/bin/bash

# Setup PostgreSQL and Initialize Airflow on first run
CONTAINER_ALREADY_INITIALIZED="CONTAINER_ALREADY_INITIALIZED"
AIRFLOW_CONNECTIONS_ALREADY_INITIALIZED="AIRFLOW_CONNECTIONS_ALREADY_INITIALIZED"

if [ ! -e $CONTAINER_ALREADY_INITIALIZED ]; then
    touch $CONTAINER_ALREADY_INITIALIZED
    
    # Start PostgreSQL and create Airflow database
    sudo service postgresql start
    sudo -u postgres psql -U postgres -c 'CREATE DATABASE airflow;'
    sudo -u postgres psql -U postgres -c "CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';"
    sudo -u postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow to airflow;"
    
    # Initialize Airflow
    sudo -u airflow -H sh -c "/home/airflow/.local/bin/airflow initdb"
    
    # Install required Python packages
    pip3 install openpyxl
fi

# Start PostgreSQL
sudo service postgresql restart

# Set environment variables and start Airflow
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
export HADOOP_USER_NAME=hadoop
export SPARK_HOME=/home/airflow/spark
export HADOOP_HOME=/home/airflow/hadoop
export PYSPARK_PYTHON=python3
export PATH=$PATH:/home/airflow/.local/bin:/home/airflow/hadoop/bin:/home/airflow/hadoop/sbin

sudo -u airflow -H sh -c "export JAVA_HOME=$JAVA_HOME; export JRE_HOME=$JRE_HOME; export HADOOP_USER_NAME=$HADOOP_USER_NAME; export SPARK_HOME=$SPARK_HOME; export HADOOP_HOME=$HADOOP_HOME; export PYSPARK_PYTHON=$PYSPARK_PYTHON; PATH=$PATH /home/airflow/.local/bin/airflow scheduler > /home/airflow/airflow_scheduler.log &"
sudo -u airflow -H sh -c "export JAVA_HOME=$JAVA_HOME; export JRE_HOME=$JRE_HOME; export HADOOP_USER_NAME=$HADOOP_USER_NAME; export SPARK_HOME=$SPARK_HOME; export HADOOP_HOME=$HADOOP_HOME; export PYSPARK_PYTHON=$PYSPARK_PYTHON; PATH=$PATH /home/airflow/.local/bin/airflow webserver -p 8080 --pid /home/airflow/airflow/airflow-webserver.pid > /home/airflow/airflow_webservice.log &"

# Create Airflow connections on first run
if [ ! -e $AIRFLOW_CONNECTIONS_ALREADY_INITIALIZED ]; then
    touch $AIRFLOW_CONNECTIONS_ALREADY_INITIALIZED
    sleep 10  # Wait for Airflow to be ready
    
    sudo -u airflow -H sh -c "PATH=$PATH /home/airflow/.local/bin/airflow connections --add --conn_id hdfs --conn_host hadoop --conn_type hdfs --conn_port 9000 --conn_login hadoop"
    sudo -u airflow -H sh -c "PATH=$PATH /home/airflow/.local/bin/airflow connections --add --conn_id spark --conn_host yarn --conn_type spark --conn_extra '{\"queue\": \"default\"}'"
fi

# Keep container running
while sleep 60; do
    sleep 1
done
