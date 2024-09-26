#!/bin/bash

source /scratch/work/public/apps/pyspark/3.4.2/scripts/spark-setup-slurm.bash
echo "run-spark-mtpu.bash"
start_all
printenv
spark-submit \
--master=${SPARK_URL} \
--executor-memory=${MEMORY} \
--driver-memory=${MEMORY} \
--jars=${VASTDB_CONNECTOR_JARS} \
--py-files="move_metadata_path.py" \
move_metadata_path.py

stop_all
