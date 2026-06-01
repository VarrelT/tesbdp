#!/bin/bash
set -e

echo "Waiting for NameNode to be ready..."
sleep 10

echo "Starting HDFS DataNode..."

# Initialize datanode directories
mkdir -p "$HADOOP_HOME/data/dataNode"

# Start datanode
hdfs datanode
