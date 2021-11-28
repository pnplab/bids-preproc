import math
from os import set_inheritable
from src.executor import TaskConfig
from src.cli import VMEngine
from src.path import PathPlaceHolder, InputFile, InputDir, OutputFile, \
                     OutputDir

# This task is mostly used to copy singularity executable on compute node.
# @warning
# This won't work with dir in singularity/docker, as VM engine will be
# configured to map file, not dir (cf. decorators).
COPY_FILE = TaskConfig(
    raw_executable='rsync',
    singularity_image=None,
    docker_image=None,
    cmd='''
        {0}
            -avz --no-g --no-p
            "{sourcePath}"
            "{destPath}"
    ''',
    decorators={
        "sourcePath": InputFile,
        "destPath": OutputFile
    }
)

COPY_DIR = TaskConfig(
    raw_executable='rsync',
    singularity_image=None,
    docker_image=None,
    cmd='''
        {0}
            -avz --no-g --no-p
            "{sourcePath}/"
            "{destPath}/"
    ''',
    decorators={
        "sourcePath": InputDir,
        "destPath": OutputDir
    }
)

REMOVE_FILE = TaskConfig(
    raw_executable='rm',
    singularity_image=None,
    docker_image=None,
    cmd='''
        {0}
            "{filePath}"
    ''',
    decorators={
        "filePath": InputFile
    }
)

REMOVE_DIR = TaskConfig(
    raw_executable='rm',
    singularity_image=None,
    docker_image=None,
    cmd='''
        {0}
            -r
            "{dirPath}"
    ''',
    decorators={
        "dirPath": InputDir
    }
)

ARCHIVE_DATASET = TaskConfig(
    raw_executable='dar',
    singularity_image=None,  # @TODO !!! map to NONE
    docker_image=None,  # @TODO !!! map to NONE
    cmd='''
        {0} -R "{datasetDir}" -v -s 1024M -w -c "{archiveDir}/{archiveName}"
    ''',
    decorators={
        "datasetDir": InputDir,
        "archiveDir": OutputDir
        # @note archive name is just a prefix, thus not an OutputFile.
    }
)

# @warning when outputDir is a local ssd, these can't be extracted to local
# node ssd on ComputeCanada@Beluga (mutualised 480GO ssd) when dataset ~>
# 300GO. Also, ssd space allocation is mutualised and not guaranteed.
EXTRACT_DATASET = TaskConfig(
    raw_executable='dar',
    singularity_image=None,  # @TODO !!! map to NONE
    docker_image=None,  # @TODO !!! map to NONE
    cmd='''
        {0} -R "{outputDir}" -O -w -x "{archiveDir}/{archiveName}" -v -am
    ''',
    decorators={
        "outputDir": OutputDir,
        "archiveDir": InputDir
        # @note archive name is just a prefix, thus not an InputFile.
    }
)

EXTRACT_DATASET_SUBJECT = TaskConfig(
    raw_executable='dar',
    singularity_image=None,  # @TODO !!! map to NONE
    docker_image=None,  # @TODO !!! map to NONE
    cmd='''
        {0} -R "{outputDir}" -O -w -x "{archiveDir}/{archiveName}" -v -am -P "sub-*" -g "sub-{subjectId}"
    ''',
    decorators={
        "outputDir": OutputDir,
        "archiveDir": InputDir
        # @note archive name is just a prefix, thus not an InputFile.
    }
)

EXTRACT_DATASET_SESSION = TaskConfig(
    raw_executable='dar',
    singularity_image=None,  # @TODO !!! map to NONE
    docker_image=None,  # @TODO !!! map to NONE
    # @TODO double check if multiple session extraction / file override doesn't cause trouble.
    # @note --overwriting-policy=pP seems not required to do multiple extraction.
    cmd='''
        {0} -R "{outputDir}" -O -w
            -x "{archiveDir}/{archiveName}" -v -am
            -P "sub-*"
            -g "sub-{subjectId}"
            -P "sub-{subjectId}/ses-*" 
            -g "sub-{subjectId}/ses-{sessionId}"
    ''',
    decorators={
        "outputDir": OutputDir,
        "archiveDir": InputDir
        # @note archive name is just a prefix, thus not an InputFile.
    }
)

LIST_ARCHIVE_SESSIONS = TaskConfig(
    raw_executable='dar',
    singularity_image=None,  # @TODO !!! map to NONE
    docker_image=None,  # @TODO !!! map to NONE
    # Will output with this format:
    # 01    -- sub
    # 01,01 -- sub,ses
    # 01,01,/anat/sub-01_ses-01_run-01_T1w.nii.gz -- sub,ses,anat
    # 01,02
    # 02
    # ...
    # @note Double brakes are present `{{ }}` to prevent python interpolation,
    # `\\`` and `\$` within `"` to prevent bash interpolation within 'HEREDOC'
    # syntax.
    # @note We truncate sub- / ses- prefixes to be consistant with pybids
    # output.
    # @note We store anat paths in order to be able to process anat sessions
    # with smriprep on large longitudinal dataset.
    cmd='''
        {0} -l "{archiveDir}/{archiveName}" --list-format=normal
            | awk "NR > 2 {{ print \$NF; }}"
            | grep 'sub-'
            | sed -E "s;^sub-([^/]+)(/ses-([^/]+))?(/anat/[^.]+_T1w.nii(.gz)?)?.*;\\1,\\3,\\4;g"
            | sed -E "s/,+\$//g"
            | sort | uniq
    ''',
    decorators={
        "datasetDir": InputDir,
        # @note archive name is just a prefix, thus not an OutputFile.
    }
)

BIDS_VALIDATOR = TaskConfig(
    raw_executable='bids-validator',
    singularity_image='../singularity-images/bids-validator-1.8.5.simg',
    docker_image='bids/validator:v1.8.5',
    cmd='''
        {0} "{datasetDir}" --verbose
    ''',
    # Map paths to vm volumes using argument decorators.
    decorators={
        "datasetDir": InputDir
    }
)

MRIQC_SUBJECT = TaskConfig(
    raw_executable='mriqc',
    singularity_image='../singularity-images/mriqc-21.0.0rc2.simg',
    docker_image='nipreps/mriqc:21.0.0rc2',
    cmd='''
        {0}
            --no-sub
            --nprocs {nproc}
            --mem_gb {memGB}
            -vvv
            -w "{workDir}"
            "{datasetDir}"
            "{outputDir}"
            participant
            --participant_label "{subjectId}"
    ''',
    # Map paths to vm volumes using argument decorators.
    decorators={
        "workDir": OutputDir,
        "datasetDir": InputDir,
        "outputDir": OutputDir,
        "templateflowDataDir": InputDir,
        "memGB": math.floor
    }
)

MRIQC_GROUP = TaskConfig(
    raw_executable='mriqc',
    singularity_image='../singularity-images/mriqc-21.0.0rc2.simg',
    docker_image='poldracklab/mriqc:21.0.0rc2',
    cmd='''
        {0}
            --no-sub
            --nprocs {nproc}
            --mem_gb {memGB}
            -vvv
            -w "{workDir}"
            "{datasetDir}"
            "{outputDir}"
            group
    ''',
    # Map paths to vm volumes using argument decorators.
    decorators={
        "workDir": OutputDir,
        "datasetDir": InputDir,
        "outputDir": OutputDir,
        "templateflowDataDir": InputDir,
        "memGB": math.floor
    }
)

# @warning 6000mb crashes on docker. (? was it ram ?)
# @note some additional output spaces seems required later on by fmriprep for
#       intermediate processing (do they?).
# @warning potential templateflow compat issue.
# cf. https://neurostars.org/t/tips-for-getting-a-bare-metal-installation-of-fmriprep-1-4-1-working/4660/2
SMRIPREP_SUBJECT = TaskConfig(
    raw_executable='smriprep',
    singularity_image='../singularity-images/smriprep-0.8.1.simg',
    docker_image='nipreps/smriprep:0.8.1',
    cmd='''
        {0}
            --participant-label "{subjectId}"
            --notrack
            --output-spaces
                MNI152NLin6Asym MNI152NLin2009cAsym OASIS30ANTs
            --nprocs {nproc}
            --mem-gb {memGB}
            --fs-no-reconall
            -vvv
            --fs-license "{freesurferLicenseFile}"
            -w "{workDir}"
            "{datasetDir}"
            "{outputDir}"
            participant
            --low-mem
    ''',
    # Map paths to vm volumes using argument decorators.
    decorators={
        "datasetDir": InputDir,
        "workDir": OutputDir,
        "outputDir": OutputDir,
        "templateflowDataDir": InputDir,
        "freesurferLicenseFile": InputFile,
        "memGB": math.floor
    }
)

FMRIPREP_SUBJECT = TaskConfig(
    raw_executable='fmriprep',
    singularity_image='../singularity-images/fmriprep-20.2.6.simg',
    docker_image='nipreps/fmriprep:20.2.6',
    cmd='''
        {0}
            --notrack
            --skip-bids-validation
            --ignore slicetiming
            --nprocs {nproc}
            --mem-mb {memMB}
            -vvv
            --fs-no-reconall
            --fs-license "{freesurferLicenseFile}"
            -w "{workDir}"
            "{datasetDir}"
            "{outputDir}"
            participant
            --participant-label "{subjectId}"
            --low-mem
    ''',
    # Map paths to vm volumes using argument decorators.
    decorators={
        "datasetDir": InputDir,
        "workDir": OutputDir,
        "outputDir": OutputDir,
        "templateflowDataDir": InputDir,
        "freesurferLicenseFile": InputFile,
        "memMB": math.floor
    }
)

# Used to generate fmriprep bids file selection filter for session level
# granularity.
#
# @note we've removed the anat data from the filter, as fmriprep seems to
# reprocess the anatomical t1 even though we've used anat fast-track.

FMRIPREP_SESSION_FILTER = TaskConfig(
    raw_executable='printf',
    singularity_image=None,  # @TODO !!! map to NONE
    docker_image=None,  # @TODO !!! map to NONE
    cmd='''
        {0} '%s' '{{
            "fmap": {{
                "datatype": "fmap"
            }},
            "bold": {{
                "datatype": "func",
                "suffix": "bold",
                "session": "{sessionId}"
            }},
            "sbref": {{
                "datatype": "func",
                "suffix": "sbref",
                "session": "{sessionId}"
            }},
            "flair": {{"datatype": "anat", "suffix": "FLAIR"}},
            "t2w": {{"datatype": "anat", "suffix": "T2w"}},
            "t1w": {{"datatype": "anat", "suffix": "T1w"}},
            "roi": {{"datatype": "anat", "suffix": "roi"}}
        }}' > "{bidsFilterFile}"
    ''',
    # Map paths to vm volumes using argument decorators.
    decorators={
        "bidsFilterFile": OutputFile
    }
)

# @note on some workers, some of the fmriprep processes (ie. mcflirt/fslsplit)
# hangs with 0% CPU and htop D flag (uninterruptible process). Might be due to
# memory overload, cf. https://github.com/nipreps/fmriprep/issues/1045, even
# though weird has we already inject 4GO per cpu, which is twice what 
# fmriprep recommands (although no info about swap), and this is inner per-task
# processing,
# We thus use the --low-mem flag which swaps work files to disk - should work
# nice with ssds local processing. We've also reduced logging to minimun
# because it might be a bottleneck has it's recorded not on the local ssd but
# the network file system, even though mcflirt/fslsplit don't seem to pipe to
# this log.
FMRIPREP_SESSION = TaskConfig(
    raw_executable='fmriprep',
    singularity_image='../singularity-images/fmriprep-20.2.6.simg',
    docker_image='nipreps/fmriprep:20.2.6',
    cmd='''
        {0}
            --notrack
            --skip-bids-validation
            --output-spaces
                MNI152NLin2009cAsym
            --ignore slicetiming
            --nprocs {nproc}
            --mem-mb {memMB}
            -vvv
            --fs-no-reconall
            --fs-license "{freesurferLicenseFile}"
            --anat-derivatives "{anatsDerivativesDir}"
            -w "{workDir}"
            "{datasetDir}"
            "{outputDir}"
            participant
            --participant-label "{subjectId}"
            --bids-filter-file "{bidsFilterFile}"
            --low-mem
    ''',
    # Map paths to vm volumes using argument decorators.
    decorators={
        "datasetDir": InputDir,
        "anatsDerivativesDir": InputDir,
        "workDir": OutputDir,
        "outputDir": OutputDir,
        "templateflowDataDir": InputDir,
        "freesurferLicenseFile": InputFile,
        "bidsFilterFile": InputFile,
        "memMB": math.floor,
        "fasttrackFixDir": InputDir
    }
)
