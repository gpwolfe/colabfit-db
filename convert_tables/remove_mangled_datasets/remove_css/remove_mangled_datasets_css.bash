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
--py-files="remove_mangled_datasets_css.py" \
remove_mangled_datasets_css.py

stop_all
