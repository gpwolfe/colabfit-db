#!/bin/bash

#SBATCH --job-name=md_pyarr
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=gw2338@nyu.edu
#SBATCH --nodes=1
#SBATCH --account=torch_pr_148_general
#SBATCH --cpus-per-task=3
#SBATCH --output=logs_%A/%a.out
#SBATCH --mem=5GB
#SBATCH --array=0
#SBATCH --time=04:00:00

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
export CUSTOM_CONDA_ENV="/scratch/gw2338/on_demand_overlay.ext3"
singularity exec --overlay $CUSTOM_CONDA_ENV:ro \
/share/apps/images/ubuntu-22.04.4.sif \
    /bin/bash -c "
    unset -f which
    source /ext3/env.sh
    python3 add_metadata_pyarrow.py
    "
