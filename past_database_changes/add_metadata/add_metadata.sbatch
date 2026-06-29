#!/bin/bash

#SBATCH --job-name=md_pyarr
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=""
#SBATCH --nodes=1
#SBATCH --account=""
#SBATCH --cpus-per-task=3
#SBATCH --output=logs_%A/%a.out
#SBATCH --mem=5GB
#SBATCH --array=0
#SBATCH --time=04:00:00

export CUSTOM_CONDA_ENV=""
singularity exec --overlay $CUSTOM_CONDA_ENV:ro \
/share/apps/images/ubuntu-22.04.4.sif \
    /bin/bash -c "
    unset -f which
    source /ext3/env.sh
    python3 add_metadata.py
    "
