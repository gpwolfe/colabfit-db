#!/bin/bash
#SBATCH --job-name=md_col
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=60GB
#SBATCH --output=logs_%A/%a.out
#SBATCH --array=1-1215%20
#SBATCH --time=8:00:00

export MY_SPARK_AUX_DIR=$(pwd)/spark-aux-${SLURM_JOB_ID}
function get_free_port()
{
  local port=
  for port in $(shuf -i 30000-65500 -n 10000); do
    if [[ $(netstat -tupln 2>&1 | grep ${port} | wc -l) -eq 0 ]]; then
      echo ${port}
      break
    fi
  done
}
export SPARK_CONF_DIR="/home/gw2338/spark-conf/"
export __SPARK_UI_PORT=$(get_free_port)
export CUSTOM_CONDA_ENV="/scratch/gw2338/vast/scripts/conda-env2.ext3"
export SPARK_SCRIPTS_DIR="/scratch/work/public/apps/pyspark/3.5.1/scripts"
/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/run-spark-singularity-no-vdev.bash \
  /bin/bash -c "
  unset -f which
  source /ext3/env.sh
  python metadata_to_column.py
"