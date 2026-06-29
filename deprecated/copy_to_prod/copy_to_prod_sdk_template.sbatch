#!/bin/bash

#SBATCH --job-name=copy_to_prod
#SBATCH --nodes=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=20GB
#SBATCH --time=08:00:00

# singularity exec \
#     --overlay /scratch/work/public/apps/pyspark/3.4.2/default-env.ext3:ro \
#     /scratch/work/public/singularity/ubuntu-22.04.3.sif \
#     /bin/bash -c "
#     source /ext3/env.sh
#     python3 copy_to_prod.py colabfit.ingest.po_new_schema colabfit-prod.prod.po_20240820
#     "
# singularity exec \
#     --overlay /scratch/work/public/apps/pyspark/3.4.2/default-env.ext3:ro \
#     /scratch/work/public/singularity/ubuntu-22.04.3.sif \
#     /bin/bash -c "
#     source /ext3/env.sh
#     python3 copy_to_prod.py colabfit.dev.co_test colabfit-prod.prod.co_20240820
#     "
# singularity exec \
#     --overlay /scratch/work/public/apps/pyspark/3.4.2/default-env.ext3:ro \
#     /scratch/work/public/singularity/ubuntu-22.04.3.sif \
#     /bin/bash -c "
#     source /ext3/env.sh
#     python3 copy_to_prod.py colabfit.ingest.ds_new_schema colabfit-prod.prod.ds_20240820
#     "

singularity exec \
    --overlay /scratch/work/public/apps/pyspark/3.4.2/default-env.ext3:ro \
    /scratch/work/public/singularity/ubuntu-22.04.3.sif \
    /bin/bash -c "
    source /ext3/env.sh
    python3 copy_to_prod.py colabfit-prod.prod.co colabfit.dev.co_remove_ds_ids
    "