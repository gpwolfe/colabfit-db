#!/bin/bash

#SBATCH --job-name=xyzexp
#SBATCH --mail-type=END
#SBATCH --mail-user=gw2338@nyu.edu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --output=logs_%A/%a.out
#SBATCH --mem=30GB
#SBATCH --array=400-449%100
#SBATCH --time=10:00:00

function get_free_port()
{
  local port=
  for port in $(shuf -i 30000-65500 -n 100); do
    if [[ $(netstat -tupln 2>&1 | grep ${port} | wc -l) -eq 0 ]]; then
      echo ${port}
      break
    fi
  done
}
export CUSTOM_CONDA_ENV="/scratch/gw2338/vast/scripts/conda-env2.ext3"
singularity exec --overlay /scratch/gw2338/vast/scripts/conda-env2.ext3:ro \
/scratch/work/public/singularity/ubuntu-22.04.4.sif \
    /bin/bash -c "
    unset -f which
    source /ext3/env.sh
    python3 xyz_export.py
    "

