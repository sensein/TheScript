## NOTE ##
# This workflow is derived from the Datalad Handbook

## Ensure the environment is ready to bootstrap the analysis workspace
# Check that we have conda installed
#conda activate
#if [ $? -gt 0 ]; then
#    echo "Error initializing conda. Exiting"
#    exit $?
#fi


DATALAD_VERSION=$(datalad --version)

if [ $? -gt 0 ]; then
    echo "No datalad available in your conda environment."
    echo "Try pip install datalad"
    # exit 1
fi

echo USING DATALAD VERSION ${DATALAD_VERSION}


while getopts i:t:v:p:f:o:s:l: flag
do
    case "${flag}" in
        i) BIDSINPUT=${OPTARG};;
        t) JOB_TMPDIR=${OPTARG};;
        v) VERSION=${OPTARG};;
        p) PRESCRIPT=${OPTARG};;
        f) FMRIPREP_OPT_FILE=${OPTARG};;
        o) PROJECTROOT=${OPTARG};;
        s) SLURM_OPT_FILE=${OPTARG};;
	l) FREESURFER_LICENSE=${OPTARG};;
    esac
done

## Set up the directory that will contain the necessary directories
if [[ -z ${PROJECTROOT} ]]
then
    PROJECTROOT=${PWD}/fmriprep
    echo PROJECTROOT set to ${PROJECTROOT}
fi

if [[ -d ${PROJECTROOT} ]]
then
    echo ${PROJECTROOT} already exists
    exit 1
fi

if [[ ! -w $(dirname ${PROJECTROOT}) ]]
then
    echo Unable to write to ${PROJECTROOT}\'s parent. Change permissions and retry
    exit 1
fi

## Check the BIDS input
if [[ -z ${BIDSINPUT} ]]
then
    echo "BIDS source is required, use -i flag"
    exit 1
fi

## Check the JOB_TMPDIR
if [[ -z ${JOB_TMPDIR} ]]
then
    echo "JOB TMPDIR argument is required, use -t flag"
    exit 1
fi
echo "JOB_TMPDIR $JOB_TMPDIR"
## Check the fmriprep version
if [[ -z ${VERSION} ]]
then
    echo "fmriprep version is required, use -v flag"
    exit 1
fi
echo "fmriprep version:  $VERSION"

# check the file with fmriprep options
if [[ -z ${FMRIPREP_OPT_FILE} ]]
then
    echo "file with fmriprep options is required, use -f flag"
    exit 1
fi
echo "file with fmriprep options:  $FMRIPREP_OPT_FILE"

# check the file with SLURM options
if [[ -z ${SLURM_OPT_FILE} ]]
then
    echo "file with SLURM options is required, use -s flag"
    exit 1
fi
echo "file with SLURM options:  $SLURM_OPT_FILE"

# setting freesurface license path
# if license is not provided, checking for FREESURFER_HOME
if [[ -z ${FREESURFER_LICENSE} ]]
then
  if [[ -z ${FREESURFER_HOME} ]]
  then
    echo "FREESURFER_HOME is not set, so license path required, use -l flag"
    exit 1
  else
    FREESURFER_LICENSE=${FREESURFER_HOME}/license.txt
  fi
fi
echo "freesurfer license file: $FREESURFER_LICENSE"


set -e -u

# Is it a directory on the filesystem?
BIDS_INPUT_METHOD=clone
if [[ -d "${BIDSINPUT}" ]]
then
    # Check if it's datalad
    BIDS_DATALAD_ID=$(datalad -f '{infos[dataset][id]}' wtf -S \
                      dataset -d ${BIDSINPUT} 2> /dev/null || true)
    [ "${BIDS_DATALAD_ID}" = 'N/A' ] && BIDS_INPUT_METHOD=copy
fi


## Start making things
mkdir -p ${PROJECTROOT}
cd ${PROJECTROOT}

# Jobs are set up to not require a shared filesystem (except for the lockfile)
# ------------------------------------------------------------------------------
# RIA-URL to a different RIA store from which the dataset will be cloned from.
# Both RIA stores will be created
input_store="ria+file://${PROJECTROOT}/input_ria"
output_store="ria+file://${PROJECTROOT}/output_ria"

# Create a source dataset with all analysis components as an analysis access
# point.
datalad create -c yoda analysis
cd analysis

# create dedicated input and output locations. Results will be pushed into the
# output sibling and the analysis will start with a clone from the input sibling.
datalad create-sibling-ria -s output "${output_store}"
# dj: not used
pushremote=$(git remote get-url --push output)
datalad create-sibling-ria -s input --storage-sibling off "${input_store}"

# register the input dataset
if [[ "${BIDS_INPUT_METHOD}" == "clone" ]]
then
    echo "Cloning input dataset into analysis dataset"
    datalad clone -d . ${BIDSINPUT} inputs/data
    # amend the previous commit with a nicer commit message
    git commit --amend -m 'Register input data dataset as a subdataset'
else
    echo "WARNING: copying input data into repository"
    mkdir -p inputs/data
    cp -r ${BIDSINPUT}/* inputs/data
    datalad save -r -m "added input data"
fi

# dj: using subset for testing
SUBJECTS=$(find inputs/data -type d -name 'sub-NDARZZ*' | cut -d '/' -f 3 )
if [ -z "${SUBJECTS}" ]
then
    echo "No subjects found in input data"
    # exit 1
fi

#dj: should we change this part?
CONTAINERDS=///repronim/containers
datalad install -d . --source ${CONTAINERDS}
datalad get containers/images/bids/bids-fmriprep--${VERSION}.sing

cd ${PROJECTROOT}/analysis

## the actual compute job specification
echo '#!/bin/bash' > code/participant_job.sh

cat $PRESCRIPT >> code/participant_job.sh

cat >> code/participant_job.sh << EOT

echo I\'m in \$PWD using `which python`
# fail whenever something is fishy, use -x to get verbose logfiles
set -e -u -x
# Set up the remotes and get the subject id from the call
args=($@)
dssource="\$1"
pushgitremote="\$2"
subid="\$3"
job_id="\$4"
echo SUBID: \${subid}
echo TMPDIR: \${TMPDIR}
echo JOB_TMPDIR: \${JOB_TMPDIR}
echo fmriprep_version: ${VERSION}
# change into the cluster-assigned temp directory. Not done by default in SGE
cd \${JOB_TMPDIR}
# OR Run it on a shared network drive
# cd /cbica/comp_space/$(basename $HOME)
# Used for the branch names and the temp dir
BRANCH="job-\${job_id}-\${subid}"
mkdir \${BRANCH}
cd \${BRANCH}

# get the analysis dataset, which includes the inputs as well
# importantly, we do not clone from the lcoation that we want to push the
# results to, in order to avoid too many jobs blocking access to
# the same location and creating a throughput bottleneck
datalad clone "\${dssource}" ds

# all following actions are performed in the context of the superdataset
cd ds

# in order to avoid accumulation temporary git-annex availability information
# and to avoid a syncronization bottleneck by having to consolidate the
# git-annex branch across jobs, we will only push the main tracking branch
# back to the output store (plus the actual file content). Final availability
# information can be establish via an eventual `git-annex fsck -f joc-storage`.
# this remote is never fetched, it accumulates a larger number of branches
# and we want to avoid progressive slowdown. Instead we only ever push
# a unique branch per each job (subject AND process specific name)
git remote add outputstore "\$pushgitremote"

# all results of this job will be put into a dedicated branch
git checkout -b "\${BRANCH}"

# we pull down the input subject manually in order to discover relevant
# files. We do this outside the recorded call, because on a potential
# re-run we want to be able to do fine-grained recomputing of individual
# outputs. The recorded calls will have specific paths that will enable
# recomputation outside the scope of the original setup
datalad get -n "inputs/data/\${subid}"

# Reomve all subjects we're not working on
(cd inputs/data && rm -rf `find . -type d -name 'sub*' | grep -v \$subid`)

# ------------------------------------------------------------------------------
# Do the run!
# TODO: Be sure the actual path to the fmriprep container is correct
# TODO FIX path!!
echo Before running datalad run
datalad run \
    -i code/fmriprep_run.sh \
    -i inputs/data/\${subid} \
    -i inputs/data/*json \
    -i containers/images/bids/bids-fmriprep--${VERSION}.sing \
    --explicit \
    -o \${subid}_fmriprep-${VERSION} \
    -o \${subid}_freesurfer-${VERSION} \
    -m "fmriprep:${VERSION} \${subid}" \
    "bash code/fmriprep_run.sh \${subid} ${VERSION}"

# file content first -- does not need a lock, no interaction with Git
datalad push --to output-storage
# and the output branch
flock \${DSLOCKFILE} git push outputstore

echo TMPDIR TO DELETE
echo \${BRANCH}

datalad uninstall -r --nocheck --if-dirty ignore inputs/data
datalad drop -r . --nocheck
git annex dead here
cd ../..
rm -rf \${BRANCH}

echo SUCCESS
# job handler should clean up workspace
EOT

chmod +x code/participant_job.sh

cat > code/fmriprep_run.sh << "EOT"
#!/bin/bash
set -e -u -x
subid="$1"
fmriprep_version="$2"
mkdir -p ${PWD}/.git/tmp/wdir
# TODO: fix path to singularity image
echo FMRIPREP_VER: ${fmriprep_version}
echo SUBID: ${subid}
echo PWD: ${PWD}
echo In fmriprep_run before singularity;
singularity run --cleanenv -B ${PWD} \
    containers/images/bids/bids-fmriprep--${fmriprep_version}.sing \
    inputs/data \
    prep \
    participant \
    -w ${PWD}/.git/tmp/wkdir \
EOT

cat ${FMRIPREP_OPT_FILE} >> code/fmriprep_run.sh

cat >> code/fmriprep_run.sh << "EOT"
cd prep
mv fmriprep ../${subid}_fmriprep-${fmriprep_version}
mv freesurfer  ../${subid}_freesurfer-${fmriprep_version}
rm -rf prep .git/tmp/wkdir
EOT

chmod +x code/fmriprep_run.sh
cp ${FREESURFER_LICENSE} code/license.txt

mkdir logs
echo .SLURM_datalad_lock >> .gitignore
echo logs >> .gitignore

datalad save -m "Participant compute job implementation"

# Add a script for merging outputs
# dj: not used
MERGE_POSTSCRIPT=https://raw.githubusercontent.com/PennLINC/TheWay/main/scripts/cubic/merge_outputs_postscript.sh
cat > code/merge_outputs.sh << "EOT"
#!/bin/bash
set -e -u -x
EOT
echo "outputsource=${output_store}#$(datalad -f '{infos[dataset][id]}' wtf -S dataset)" \
    >> code/merge_outputs.sh
echo "cd ${PROJECTROOT}" >> code/merge_outputs.sh
wget -qO- ${MERGE_POSTSCRIPT} >> code/merge_outputs.sh

dssource="${input_store}#$(datalad -f '{infos[dataset][id]}' wtf -S dataset)"
pushgitremote=$(git remote get-url --push output)


################################################################################
# SLURM SETUP START - remove or adjust to your needs
################################################################################
env_flags="--export=DSLOCKFILE=${PWD}/.SLURM_datalad_lock"

# checking the length of the subjects list
subjects_list=(${SUBJECTS}) 
subjects_len=${#subjects_list[@]}
 
echo '#!/bin/bash' > code/sbatch_array.sh

cat ${SLURM_OPT_FILE} >> code/sbatch_array.sh

cat >> code/sbatch_array.sh <<EOF

#SBATCH --output=logs/array_%A_%a.out
#SBATCH --error=logs/array_%A_%a.err

#SBATCH --export=DSLOCKFILE=${PROJECTROOT}/analysis/.SLURM_datalad_lock,JOB_TMPDIR=${JOB_TMPDIR}

#SBATCH --array=0-$((subjects_len-1))

subjects=(${SUBJECTS})
sub=\${subjects[\$SLURM_ARRAY_TASK_ID]}

${PROJECTROOT}/analysis/code/participant_job.sh ${dssource} ${pushgitremote} \$sub \$SLURM_JOB_ID

EOF


dssource="${input_store}#$(datalad -f '{infos[dataset][id]}' wtf -S dataset)"
pushgitremote=$(git remote get-url --push output)
eo_args="-e ${PWD}/logs -o ${PWD}/logs"

datalad save -m "SLURM submission setup" code/ .gitignore

################################################################################
# SLURM SETUP END
################################################################################

# cleanup - we have generated the job definitions, we do not need to keep a
# massive input dataset around. Having it around wastes resources and makes many
# git operations needlessly slow
if [ "${BIDS_INPUT_METHOD}" = "clone" ]
then
    datalad uninstall -r --nocheck inputs/data
fi

# make sure the fully configured output dataset is available from the designated
# store for initial cloning and pushing the results.
datalad push --to input
datalad push --to output

# Add an alias to the data in the RIA store
RIA_DIR=$(find $PROJECTROOT/output_ria/???/ -maxdepth 1 -type d | sort | tail -n 1)
mkdir -p ${PROJECTROOT}/output_ria/alias
ln -s ${RIA_DIR} ${PROJECTROOT}/output_ria/alias/data

# if we get here, we are happy
echo SUCCESS
