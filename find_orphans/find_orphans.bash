#!/bin/bash

source /scratch/work/public/apps/pyspark/3.4.2/scripts/spark-setup-slurm.bash
echo "run bash"
start_all
printenv
spark-submit \
--master=${SPARK_URL} \
--executor-memory=${MEMORY} \
--driver-memory=${MEMORY} \
--conf spark.executor.memoryOverhead=600 \
--conf spark.port.maxRetries=50 \
--conf spark.rpc.message.maxSize=2047 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.network.timeout=500s \
--conf spark.rpc.askTimeout=500s \
--conf spark.executor.heartbeatInterval=60s \
--conf spark.yarn.maxAppAttempts=5 \
--conf spark.task.maxFailures=10 \
--jars=${VASTDB_CONNECTOR_JARS} \
find_orphans.py

stop_all

