#!/bin/bash

#SBATCH --job-name=pos_to_prod
#SBATCH --nodes=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=160GB
#SBATCH --time=10:00:00

export SPARK_SCRIPTS_DIR=/scratch/work/public/apps/pyspark/3.4.2/scripts

# Optional: link a custom conda environment with additional dependencies
export CUSTOM_CONDA_ENV="/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/conda-env.ext3"
export MY_SPARK_AUX_DIR=$(pwd)/spark-aux-${SLURM_JOB_ID}
mkdir -p ${MY_SPARK_AUX_DIR}

export SSH_PORT=$(shuf -i 10000-65500 -n 1)

export NODELIST=$(scontrol show hostname ${SLURM_NODELIST})

ssh_dir=${MY_SPARK_AUX_DIR}/ssh; mkdir -p ${ssh_dir}
for node in ${NODELIST}; do
  /usr/bin/ssh -x ${node} \
  CUSTOM_CONDA_ENV=${CUSTOM_CONDA_ENV} \
  /scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/setup-sshd-no-vdev.bash \
  --dir ${ssh_dir} --port ${SSH_PORT}
done
echo "run-spark-singularity-no-vdev.bash"
/scratch/gw2338/vast/data-lake-main/spark/scripts/gw_scripts/run-spark-singularity-no-vdev.bash \
  bash copy_to_dev_pos.bash
