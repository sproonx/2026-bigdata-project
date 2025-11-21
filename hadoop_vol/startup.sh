#!/bin/bash

# Start SSH (required for Hadoop)
service ssh restart

# Format HDFS on first run
CONTAINER_ALREADY_INITIALIZED="CONTAINER_ALREADY_INITIALIZED"
if [ ! -e $CONTAINER_ALREADY_INITIALIZED ]; then
    touch $CONTAINER_ALREADY_INITIALIZED
    sudo -u hadoop -H sh -c "/home/hadoop/hadoop/bin/hdfs namenode -format"
fi

# Start Hadoop (HDFS + YARN)
sudo -u hadoop -H sh -c "/home/hadoop/hadoop/sbin/start-all.sh"

# Keep container running
while sleep 60; do
    sleep 1
done
