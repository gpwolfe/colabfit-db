#!/bin/bash

source /scratch/work/public/apps/pyspark/3.4.2/scripts/spark-setup-slurm.bash
echo "run_copy_tables.bash"
start_all
# printenv
spark-submit \
--master=${SPARK_URL} \
--executor-memory=${MEMORY} \
--driver-memory=${MEMORY} \
--jars=${VASTDB_CONNECTOR_JARS} \
--py-files="/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/copy_table_from_ingest.py" \
/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/copy_table_from_ingest.py

stop_all