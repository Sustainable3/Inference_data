#!/bin/bash
#SBATCH -J splt_ortos
#SBATCH --partition=batch
#SBATCH --nodes=1
#SBATCH --mail-type=END
#SBATCH --time 24:00:00

module load trytonp/apptainer/1.3.0
singularity exec --nv docker://ultralytics/ultralytics:latest python3 img_preprocessing.py
