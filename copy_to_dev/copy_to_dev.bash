#!/bin/bash

source /scratch/work/public/apps/pyspark/3.4.2/scripts/spark-setup-slurm.bash
echo "run-spark-mtpu.bash"
start_all
printenv
spark-submit \
--master=${SPARK_URL} \
--executor-memory=${MEMORY} \
--conf spark.driver.maxResultSize=10g \
--driver-memory=${MEMORY} \
--jars=${VASTDB_CONNECTOR_JARS} \
--py-files="/scratch/gw2338/vast/data-lake-main/spark/scripts/convert_tables/copy_to_dev/copy_to_dev.py" \
/scratch/gw2338/vast/data-lake-main/spark/scripts/convert_tables/copy_to_dev/copy_to_dev.py

stop_all