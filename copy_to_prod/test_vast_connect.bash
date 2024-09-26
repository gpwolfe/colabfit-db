#!/bin/bash

source /scratch/work/public/apps/pyspark/3.4.2/scripts/spark-setup-slurm.bash
echo "run bash"
start_all
printenv
spark-submit \
--master=${SPARK_URL} \
--executor-memory=${MEMORY} \
--driver-memory=${MEMORY} \
--conf spark.executor.extraJavaOptions=-Xss160m \
--conf spark.driver.extraJavaOptions=-Xss16m \
--jars=${VASTDB_CONNECTOR_JARS} \
--py-files="test_vast_connect.py" \
test_vast_connect.py

stop_all

