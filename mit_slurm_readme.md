## How to use bootstrap-fmriprep on slurm

#### 1) Clone the repo

- naviagte to openmind_slurm branch to use bootstrap-fmriprep on slurm as described below

#### 2) navigate to TheWay/scripts/cubic/config_examples/bootstrap-fmriprep and 

- edit env_setup.sh so that it will activated an environment with datalad installed
- edit slurm_opt.txt so that it has your required slurm job specifications
- edit fmriprep_opt.txt so that it has your required fmriprep parameters
**if you are not using a bids filter file, remove the line "--bids-filter-file code/fmriprep_filter.json"*
**don't need to change the freesurfer license file path, this can be specified later*


#### 3) navigate to /TheWay/scripts/cubic and using an environment with datalad installed, run the sbatch_fmriprep.sh script, for example:

    ./bootstrap-fmriprep.sh \
    -i /om/scratch/Tue/jsmentch/merlin_fmriprep/ds001110 \
    -t /om/scratch/Tue/jsmentch/merlin_fmriprep/working \
    -v 21.0.1 \
    -e /om/scratch/Tue/jsmentch/merlin_fmriprep/TheWay/scripts/cubic/config_examples/bootstrap-fmriprep/env_setup.sh \
    -f /om/scratch/Tue/jsmentch/merlin_fmriprep/TheWay/scripts/cubic/config_examples/bootstrap-fmriprep/fmriprep_opt.txt \
    -w /om/scratch/Tue/jsmentch/merlin_fmriprep/TheWay/scripts/cubic/config_examples/bootstrap-fmriprep/slurm_opt.txt \
    -p /om/scratch/Tue/jsmentch/merlin_fmriprep/project_dir \
    -s sub-* \
    -l /om2/user/jsmentch/data/freesurfer_license.txt

  **the project_dir (-p) should not exist, it will be created as a new dataset*
  **the working directory (-t) should be an existing path*
#### 4) navigate to your new project_dir/analysis directory
#### 5) submit your job array to run fmriprep
  **chmod u+x code/merge_outputs.sh if it is not executable*
  **run from the analysis directory eg:*
  
      sbatch code/sbatch_array.sh
#### 6) after the jobs finish, run ./code/merge_outputs.sh to merge the outputs
