## How to use bootstrap-fmriprep on slurm

#### Repository structure

In [scripts/fmriprep/](https://github.com/sensein/TheScript/tree/main/scripts/fmriprep) you will find:
- `bootstrap-fmriprep.py` -  the main script that create the project structure and generate slurm and bash scripts to run the simulation
- `config_examples` directory with the examples of configuration files that should be adjusted to the specific use case:
    - `env_setup.sh` - loads singularity and activates an environment with datalad installed
    - `slurm_opt.txt` - contains slurm job specifications
    - `fmriprep_opt.txt` - contains fmriprep specific parameters


#### Running the main script

In order to run the script you need to have `Python 3.7+` with `datalad` and `click`.

When you run the script with `--help` you will have the list of all arguments that are supported:

    Options:
    -i, --bidsinput TEXT           path to the input dataset  [required]
    -p, --projectroot TEXT         path to the project dir  [required]
    -t, --job_tmpdir TEXT          path to the job workdir  [required]
    -v, --version [23.1.4|21.0.2|22.1.0]  fmriprep_version  [required]
    -s, --subjects_subset TEXT     optional, pattern for subjects subset, e.g.
                                 sub-0*
    -f, --fmriprep_opt_file TEXT   path to the fmriprep options  [required]
    -e, --env_script TEXT          path to teh script for env setup  [required]
    -w, --slurm_opt_file TEXT      path to the job workdir  [required]
    -l, --freesurfer_license TEXT  path to the freesurfer license  [required]
    -c, --copy_dir TEXT            optional, path to the directory that will be
                                 copied to the code directory
    --max_job TEXT                 optional, maximal number of jobs run on slurm
    --sessions TEXT                optional, name of sessions if fmriprep is run
                                 per session, multiple sessions allowed
    --reconstruction [unco]        optional, type of reconstructions
    --help                         Show this message and exit.

Example of running the script on `OpenMind`:

    python bootstrap-fmriprep.py \
    -i /om2/scratch/Wed/djarecka/data_test/mmc_datalad1 \
    -t /om2/scratch/Wed/djarecka/deb_21_jan2  
    -v 21.0.2 \ 
    -e /om2/user/djarecka/bootstrap/env_setup.sh \
    -f /om2/user/djarecka/bootstrap/fmriprep_opt_debbie.txt \
    -w /om2/user/djarecka/bootstrap/slurm_opt_debbie.txt \
    -p /om2/user/djarecka/bootstrap/deb_21_jan2 \
    -s sub-MM31*  \
    -l /om2/user/jsmentch/data/freesurfer_license.txt

Both, `projectroot` (-p) and `job_tmpdir` (-t) will be created.


#### Running the fmriprep workflow

- Navigate to `<projectroot>/analysis`
- Run sbatch script (or scripts) that were created in the `code` directory using `sbatch` command, e.g.:
```
sbatch code/sbatch_array.sh
```
#### Merging the output

In order to merge the output at the end of all runs, run the merging code:
 
    ./code/merge_outputs.sh
    
The script will create a new directory  `<projectroot>/merge_ds`, where you can find directories with results (`derivatives`) and inputs. In order to check the results you need to use `datalad` to get the files, e.g. `datalad get derivatives`


## Provenance
The code was updated from the original code from the PennLINC/TheWay repository, that was based on [Wagner at al., 2022: FAIRly big: A framework for computationally reproducible processing of large-scale data](https://www.nature.com/articles/s41597-022-01163-2)

