import json

import click
import datalad
import datalad.api as dl
import glob
import os
from pathlib import Path
import subprocess as sb
import shutil
from urllib.request import urlopen

# Add a script for merging outputs
MERGE_POSTSCRIPT = "https://raw.githubusercontent.com/sensein/TheWay/main/scripts/fmriprep/merge_outputs_postscript.sh"
# location of the fake container TODO
FAKE_CONTAINER_PATH = "/om2/user/djarecka/bootstrap/fake/fake_fmriprep_test_amd_latest.sif"
#FAKE_CONTAINER_PATH = "/Users/dorota/tmp/fake_fmriprep_test_amd_latest.test"

class BootstrapScript:

    def __init__(self, bidsinput, projectroot, job_tmpdir, version, subjects_subset,
                 fmriprep_opt_file, env_script, slurm_opt_file, freesurfer_license,
                 copy_dir, max_job, sessions):
        self.bidsinput = bidsinput
        self.projectroot = Path(projectroot)
        self.analysis_dir = self.projectroot / "analysis"
        self.job_tmpdir = Path(job_tmpdir)
        self.version = version
        self.subjects_subset = subjects_subset
        self.fmriprep_opt_file = fmriprep_opt_file
        self.env_script = env_script
        self.slurm_opt_file = slurm_opt_file
        self.freesurfer_license = freesurfer_license
        self.copy_dir = copy_dir
        if max_job is None:
            self.max_job = max_job
        else:
            self.max_job = int(max_job)
        self.sessions = sessions

    def settup_and_script(self):
        # assuming bids_input_method = "clone"
        bids_dataset_id = sb.check_output(
            ["datalad", "-f", '{infos[dataset][id]}', "wtf", "-S", "dataset", "-d", f"{self.bidsinput}"]
        ).decode('utf-8').strip()
        print("BIDS_DATALAD_ID: ", bids_dataset_id)
        self.job_tmpdir.mkdir(parents=True, exist_ok=True)
        self.projectroot.mkdir(parents=True)
        os.chdir(self.projectroot)
        # analysis_dir = projectroot / "analysis"
        dl.create(self.analysis_dir, cfg_proc=['yoda'])
        os.chdir(self.analysis_dir)

        self.input_store = f"ria+file://{self.projectroot}/input_ria"
        self.output_store = f"ria+file://{self.projectroot}/output_ria"
        dl.create_sibling_ria(self.output_store, name="output", new_store_ok=True)
        dl.create_sibling_ria(self.input_store, name="input", storage_sibling=False, new_store_ok=True)

        print("Cloning input dataset into analysis dataset")
        dl.clone(source=self.bidsinput, path=self.analysis_dir / "inputs/data",
                 dataset=self.analysis_dir)
        sb.run(['git', 'commit', '--amend', '-m', 'Register input data dataset as a subdataset'])
        if Path(self.subjects_subset).is_file():
            raise NotImplementedError("subjects_subset from file not implemented")
        else:
            selected_dirs = (glob.glob(f'{self.analysis_dir}/inputs/data/{self.subjects_subset}'))
            self.subjects = [el.split("/")[-1] for el in selected_dirs]

        print("!!!PWD ", os.getcwd())
        print("subject subsets ", self.subjects)

        if not self.subjects:
            raise Exception("No subjects found in input data")

        print("LIST OF SUBJECTS: ", self.subjects)
        containers_ds = "///repronim/containers"
        dl.install(dataset=self.analysis_dir, source=containers_ds)
        # amend the previous commit with a nicer commit message
        sb.run(['git', 'commit', '--amend', '-m', 'Register containers repo as a subdataset'])

        if self.version == "fake":
            # TODO
            shutil.copy(FAKE_CONTAINER_PATH,
                        f"{self.analysis_dir}/containers/images/bids/bids-fmriprep--fake.sing")
            dl.save(path=f"containers/images/bids/bids-fmriprep--fake.sing", message="added a fake image")
            print("added a fake container")
        else:
            dl.get(f"containers/images/bids/bids-fmriprep--{self.version}.sing")

        self.containers_repo = f"{self.projectroot}/analysis/containers"

        self._write_participant_scripts()

        shutil.copy(self.freesurfer_license, self.analysis_dir / "code/license.txt")

        if not self.copy_dir:
            print("No COPY_DIR set, nothing is copied to code/")
        else:
            shutil.copytree(self.copy_dir, self.analysis_dir / "code", dirs_exist_ok=True)
            print(f"content of {self.copy_dir} is copied to code/")

        (self.analysis_dir / "logs").mkdir()

        with (self.analysis_dir / ".gitignore").open("w") as f:
            f.write(".SLURM_datalad_lock\n")
            f.write("logs\n")

        dl.save(message="Participant compute job implementation")
        self.dataset_id = sb.check_output(
            ["datalad", "-f", '{infos[dataset][id]}', "wtf", "-S", "dataset"]
        ).decode('utf-8').strip()
        print("D_id", self.dataset_id)

        self._write_merge_script()

        self.dssource = f"{self.input_store}#{self.dataset_id}"
        self.pushgitremote = sb.check_output(["git", "remote", "get-url", "--push", "output"]).decode('utf-8').strip()

        if self.max_job and len(self.subjects) > self.max_job:
            nn = len(self.subjects) // self.max_job
            if len(self.subjects) % self.max_job:
                nn += 1
            for ii in range(nn):
                subjects_part = self.subjects[ii*self.max_job:(ii+1)*self.max_job]
                self._write_slurm_script(subjects=subjects_part, slurm_filename=f"sbatch_array_{ii}.sh")
        else: #TODO: polaczyc dwa ify
            if self.sessions:
                for ses in self.sessions:
                    self._write_slurm_script(subjects=self.subjects, session=ses, slurm_filename=f"sbatch_array_ses-{ses}.sh")
            else:
                self._write_slurm_script(subjects=self.subjects)

        # I believe this is not needed: path=["code/", ".gitignore"],
        dl.save(message="SLURM submission setup")

        dl.drop(path="inputs/data", reckless="availability", recursive=True)
        dl.push(to="input")
        dl.push(to="output")

        ria_dir_l = glob.glob(f"{self.projectroot}/output_ria/???/*")
        if len(ria_dir_l) != 1:
            raise Exception("ria_dir finding has to be fixed")
        ria_dir = ria_dir_l[0]
        (self.projectroot / "output_ria/alias").mkdir(parents=True)
        (self.projectroot / "output_ria/alias/data").symlink_to(ria_dir)
        print("Success")


    def _write_participant_scripts(self):
        remove_all_text = """#!/bin/bash

set -eu
data="${1:?Usage FOLDER SUBJ}"; shift
subid="${1:?Usage FOLDER SUBJ}"; shift

(cd "$data" && /bin/ls -1d sub-* | grep -v "${subid}\$" | xargs rm -rf .heudiconv sourcedata rawdata derivatives)
"$@"
"""

        with (self.analysis_dir / "code/remove-all-other-subjects-first.sh").open("w") as f:
            f.write(remove_all_text)

        (self.analysis_dir / "code/remove-all-other-subjects-first.sh").chmod(0o775)

        for ses in self.sessions:
            self._create_session_filter(ses)

        part_session_text = "$5" if self.sessions else ""
        fmri_session_text = "$3" if self.sessions else "none"


        main_participant_text \
            = f"""echo I\\'m in $PWD using {sb.check_output(["which", "python"]).decode('utf-8').strip()}
# fail whenever something is fishy, use -x to get verbose logfiles
PS4=+
set -e -u -x
# Set up the remotes and get the subject id from the call
args=($@)
dssource="$1"
pushgitremote="$2"
subid="$3"
session={part_session_text}
CONTAINERS_REPO="$4"
echo SUBID: ${{subid}}
echo TMPDIR: ${{TMPDIR}}
echo JOB_TMPDIR: ${{JOB_TMPDIR}}
echo fmriprep_version: {self.version}
echo dssource: ${{dssource}}
echo CONTAINERS_REPO: ${{CONTAINERS_REPO}}
echo pushgitremote: ${{pushgitremote}}
# change into the cluster-assigned temp directory. Not done by default in SGE
cd ${{JOB_TMPDIR}}
# OR Run it on a shared network drive
# cd /cbica/comp_space/{Path(os.environ['HOME']).name}
# Used for the branch names and the temp dir
BRANCH="${{subid}}${{session}}"
if [ ! -f ${{BRANCH}}.exists ]; then
    rm -rf ${{BRANCH}}

    mkdir -p ${{BRANCH}}
    cd ${{BRANCH}}

# get the analysis dataset, which includes the inputs as well
# importantly, we do not clone from the lcoation that we want to push the
# results to, in order to avoid too many jobs blocking access to
# the same location and creating a throughput bottleneck
echo inside particpant_job, before cloning "${{dssource}}", PWD: "${{PWD}}"
datalad clone "${{dssource}}" ds

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
    git remote add outputstore "$pushgitremote"

# clonning local containers repo
    datalad clone --reckless ephemeral "${{CONTAINERS_REPO}}" containers/
# this probably can be skipped
    cd containers
    git remote remove datasets.datalad.org
    cd ..

# all results of this job will be put into a dedicated branch
    git checkout -b "${{BRANCH}}"

# we pull down the input subject manually in order to discover relevant
# files. We do this outside the recorded call, because on a potential
# re-run we want to be able to do fine-grained recomputing of individual
# outputs. The recorded calls will have specific paths that will enable
# recomputation outside the scope of the original setup
    datalad get -n "inputs/data/${{subid}}"

#setup before fmriprep run complete, make a file for requeuing
    touch ../../${{BRANCH}}.exists
else
    cd ${{BRANCH}}/ds
fi

# ------------------------------------------------------------------------------
# Do the run!
echo Before running datalad run
if [[ -d prep/sourcedata/freesurfer ]]; then
    find prep/sourcedata/freesurfer -name "*IsRunning*" -delete
fi
echo I am in ${{PWD}}
datalad run \
    -i code/fmriprep_run.sh \
    -i inputs/data/${{subid}} \
    -i "inputs/data/*json" \
    -i containers/images/bids/bids-fmriprep--{self.version}.sing \
    --explicit \
    -o \\derivatives \
    -m "fmriprep:{self.version} ${{subid}}" \
    code/remove-all-other-subjects-first.sh inputs/data "${{subid}}" code/fmriprep_run.sh ${{subid}} {self.version} "${{session}}"

# file content first -- does not need a lock, no interaction with Git
datalad push --to output-storage
# and the output branch
flock ${{DSLOCKFILE}} git push outputstore

echo TMPDIR TO DELETE
echo ${{BRANCH}}

#datalad uninstall -r --nocheck --if-dirty ignore inputs/data
datalad drop -r . --reckless kill
git annex dead here
cd ../..
#TODO: for now I will just move it instead of removing
# rm -rf ${{BRANCH}}

echo SUCCESS
# job handler should clean up workspace
"""

        with (self.analysis_dir / "code/participant_job.sh").open("w") as f:
            f.write('#!/bin/bash\n')
            f.write(Path(self.env_script).read_text())
            f.write(main_participant_text)

        (self.analysis_dir / "code/participant_job.sh").chmod(0o755)

        fmripreprun_beg_text = f"""#!/bin/bash
PS4=+
set -e -u -x
subid="$1"
fmriprep_version="$2"
session={fmri_session_text}
mkdir -p ${{PWD}}/.git/tmp/wkdir
echo FMRIPREP_VER: {self.version}
echo SUBID: ${{subid}}
echo SESSION: ${{session}}
echo PWD: ${{PWD}}
echo In fmriprep_run before singularity;
singularity run --cleanenv -B ${{PWD}}:/pwd \
    containers/images/bids/bids-fmriprep--{self.version}.sing \
    /pwd/inputs/data \
    /pwd/prep \
    participant \
    -w /pwd/.git/tmp/wkdir \
"""
        if self.sessions: #TODO
            fmripreprun_beg_text += "--bids-filter-file code/filter_${session}.json"


        fmripreprun_end_text = f"""cd prep
if [ -d ../fmriprep-{self.version} ]; then
    rm -rf ../fmriprep-{self.version}
fi
mkdir ../derivatives

mv ${{subid}} ../derivatives/
if [ -f ${{subid}}.html ]; then
    mv ${{subid}}.html ../derivatives/
fi
if [ -d ../freesurfer-{self.version}  ]; then
    rm -rf ../freesurfer-{self.version}
fi

mv sourcedata  ../derivatives/
cd ..
rm -rf prep #.git/tmp/wkdir
"""

        with Path(self.fmriprep_opt_file).open() as f:
            fmriprep_opt_text = f.read()

        with (self.analysis_dir / "code/fmriprep_run.sh").open("w") as f:
            f.write(fmripreprun_beg_text)
            f.write(fmriprep_opt_text)
            f.write(fmripreprun_end_text)

        (self.analysis_dir / "code/fmriprep_run.sh").chmod(0o775)


    def _write_merge_script(self):
        merge_text_start = f"""#!/bin/bash
PS4=+
set -e -u -x
outputsource={self.output_store}#{self.dataset_id}
cd {self.projectroot}
"""
        with urlopen(MERGE_POSTSCRIPT) as f:
            merge_text_file = f.read().decode('utf-8')

        with (self.analysis_dir / "code/merge_outputs.sh").open("w") as f:
            f.write(merge_text_start)
            f.write(merge_text_file)

        (self.analysis_dir / "code/merge_outputs.sh").chmod(0o775)


    def _create_session_filter(self, session):
        filter_dict = {}
        for (suf, tp) in [("bold", "func"), ("t1w", "anat"), ("t2w", "anat")]:
            filter_dict[suf] = {"datatype": tp, "suffix": suf, "session": session}
            with (self.analysis_dir / f"code/filter_{session}.json").open("w") as f:
                f.write(json.dumps(filter_dict))

    def _write_slurm_script(self, subjects, session=None, slurm_filename="sbatch_array.sh"):
        with Path(self.slurm_opt_file).open() as f:
            slurm_opt_text = f.read()

        slurm_session_text = session if session else ""

        slurm_main_text = f"""#SBATCH --output=logs/array_%A_%a.out
#SBATCH --error=logs/array_%A_%a.err

#SBATCH --export=DSLOCKFILE={self.projectroot}/analysis/.SLURM_datalad_lock,JOB_TMPDIR={self.job_tmpdir}

#SBATCH --array=0-{len(subjects) - 1}

subjects=({' '.join(subjects)})
sub=${{subjects[$SLURM_ARRAY_TASK_ID]}}

{self.projectroot}/analysis/code/participant_job.sh {self.dssource} {self.pushgitremote} $sub {self.containers_repo} {slurm_session_text}
"""

        with (self.analysis_dir / f"code/{slurm_filename}").open("w") as f:
            f.write("#!/bin/bash\n")
            f.write(slurm_opt_text)
            f.write(slurm_main_text)



@click.command(help="Search TODO")
@click.option(
    "-i",
    "--bidsinput",
    required=True,
    help="path to the input dataset"
)
@click.option(
    "-p",
    "--projectroot",
    required=True,
    help="path to the project dir"
)
@click.option(
    "-t",
    "--job_tmpdir",
    required=True,
    help="path to the job workdir"
)
@click.option(
    "-v",
    "--version",
    required=True,
    type=click.Choice(["fake", "21.0.2"]),
    help="fmriprep_version"
)
@click.option(
    "-s",
    "--subjects_subset",
    help="optional, pattern for subjects subset, e.g. sub-0*"
)
@click.option(
    "-f",
    "--fmriprep_opt_file",
    required=True,
    help="path to the fmriprep options"
)
@click.option(
    "-e",
    "--env_script",
    required=True,
    help="path to teh script for env setup"
)
@click.option(
    "-w",
    "--slurm_opt_file",
    required=True,
    help="path to the job workdir"
)
@click.option(
    "-l",
    "--freesurfer_license",
    required=True,
    help="path to the freesurfer license"
)
@click.option(
    "-c",
    "--copy_dir",
    help="optional, path to the directory that will be copied to the code directory"
)
@click.option(
    "--max_job",
    help="optional, maximal number of jobs run on slurm"
)
@click.option(
    "--sessions",
    multiple=True,
    help="optional, name of sessions if fmriprep is run per session"
)
def main(bidsinput, projectroot, job_tmpdir, version, subjects_subset, fmriprep_opt_file,
              env_script, slurm_opt_file, freesurfer_license, copy_dir, max_job, sessions):
    print("bidsinput", bidsinput)
    print("job_tmpdir", job_tmpdir)
    print(f"copy_dir", copy_dir)
    print(f"Using datalad version: {datalad.__version__}")
    print(f"fmriprep version: {version}")
    print(f"file with fmriprep options: {fmriprep_opt_file}")
    print(f"file with SLURM options: {slurm_opt_file}")
    print(f"freesurfer license file: {freesurfer_license}")
    print(f"sessions = {sessions}")

    bs = BootstrapScript(bidsinput=bidsinput, projectroot=projectroot, job_tmpdir=job_tmpdir,
                         version=version, subjects_subset=subjects_subset,
                         fmriprep_opt_file=fmriprep_opt_file, env_script=env_script,
                         slurm_opt_file=slurm_opt_file, freesurfer_license=freesurfer_license,
                         copy_dir=copy_dir, max_job=max_job, sessions=sessions)
    bs.settup_and_script()


if __name__ == '__main__':
    main()
