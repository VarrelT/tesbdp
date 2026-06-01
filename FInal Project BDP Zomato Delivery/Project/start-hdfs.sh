#!/bin/bash
set -e

echo "Starting HDFS NameNode..."

# Check if NameNode is already formatted (by checking for VERSION file)
if [ ! -f "$HADOOP_HOME/data/nameNode/current/VERSION" ]; then
    echo "Formatting NameNode for the first time..."
    hdfs namenode -format -force
fi

# Start namenode
hdfs namenode
