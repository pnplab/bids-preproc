import math
from os import set_inheritable
from .src.cli import VMEngine
from .src.path import PathPlaceHolder, InputFile, InputDir, OutputFile, \
    OutputDir


# # Setup pipeline tasks.
availableExecutables = {
    'bidsValidator': {
        VMEngine.NONE: 'bids-validator',
        VMEngine.SINGULARITY: '../singularity-images/bids-validator-1.5.2.simg', # @todo
        VMEngine.DOCKER: 'bids/validator:v1.5.6',
    },
    'mriqc': {
        VMEngine.NONE: 'mriqc',
        VMEngine.SINGULARITY: '../singularity-images/mriqc-0.15.2.simg',
        VMEngine.DOCKER: 'poldracklab/mriqc:0.15.2'
    },
    'smriprep': {
        VMEngine.NONE: 'smriprep',
        VMEngine.SINGULARITY: '../singularity-images/smriprep-0.7.0.simg', # @todo
        VMEngine.DOCKER: 'nipreps/smriprep:0.7.0'
    },
    'fmriprep': {
        VMEngine.NONE: 'fmriprep',
        VMEngine.SINGULARITY: '../singularity-images/fmriprep-20.2.0.simg', # @todo
        VMEngine.DOCKER: 'nipreps/fmriprep:20.2.0'
    },
    # Used to generate fmriprep bids file selection filter for session
    # level granularity.
    'printf': {
        VMEngine.NONE: 'printf',
        VMEngine.SINGULARITY: None,
        VMEngine.DOCKER: None
    }
}


# BIDS_VALIDATOR = {
#     VMEngine.NONE: 'bids-validator',
#     VMEngine.SINGULARITY: '../singularity-images/bids-validator-1.5.2.simg',
#     VMEngine.DOCKER: 'bids/validator:v1.5.6',
#     "cmd": '''
#         {0} "{datasetDir}"
#     ''',
#     # Map paths to vm volumes using argument decorators.
#     "decorators": {
#         "datasetDir": InputDir
#     }
# }

# MRIQC_SUBJECT = {
#     VMEngine.NONE: 'mriqc',
#     VMEngine.SINGULARITY: '../singularity-images/mriqc-0.15.2.simg',
#     VMEngine.DOCKER: 'poldracklab/mriqc:0.15.2',
#     "cmd": '''
#         {0}
#             --no-sub
#             --nprocs {nproc}
#             --mem_gb {memGb}
#             -vvvv
#             -w "{workDir}"
#             "{datasetDir}"
#             "{outputDir}"
#             participant
#             --participant_label "{subjectId}"
#     ''',
#     # Map paths to vm volumes using argument decorators.
#     "decorators": {
#         "workDir": OutputDir,
#         "datasetDir": InputDir,
#         "outputDir": OutputDir,
#         "memGb": math.floor
#     }
# }

# MRIQC_GROUP = {
#     VMEngine.NONE: 'mriqc',
#     VMEngine.SINGULARITY: '../singularity-images/mriqc-0.15.2.simg',
#     VMEngine.DOCKER: 'poldracklab/mriqc:0.15.2',
#     "cmd": '''
#         {0}
#             --no-sub
#             --nprocs {nproc}
#             --mem_gb {memGb}
#             -vvvv
#             -w "{workDir}"
#             "{datasetDir}"
#             "{outputDir}"
#             group
#     ''',
#     # Map paths to vm volumes using argument decorators.
#     "decorators": {
#         "workDir": OutputDir,
#         "datasetDir": InputDir,
#         "outputDir": OutputDir,
#         "memGb": math.floor
#     }
# }

# FMRIPREP_SUBJECT = {
#     VMEngine.NONE: 'fmriprep',
#     VMEngine.SINGULARITY: '../singularity-images/fmriprep-20.2.0.simg',
#     VMEngine.DOCKER: 'nipreps/fmriprep:20.2.0',
#     "cmd": '''
#         {0}
#             --notrack
#             --skip-bids-validation
#             --ignore slicetiming
#             --nprocs {nproc}
#             --mem-mb {memMb}
#             -vvvv
#             --fs-no-reconall
#             --fs-license "{freesurferLicenseFile}"
#             -w "{workDir}"
#             "{datasetDir}"
#             "{outputDir}"
#             participant
#             --participant-label "{subjectId}"
#     ''',
#     # Map paths to vm volumes using argument decorators.
#     "decorators": {
#         "datasetDir": InputDir,
#         "workDir": OutputDir,
#         "outputDir": OutputDir,
#         "templateflowDataDir": InputDir,
#         "freesurferLicenseFile": InputFile,
#         "memMb": math.floor
#     }
# }

# SMRIPREP_SUBJECT = {
#     VMEngine.NONE: 'smriprep',
#     VMEngine.SINGULARITY: '../singularity-images/smriprep-0.7.0.simg',
#     VMEngine.DOCKER: 'nipreps/smriprep:0.7.0',
#     "cmd": '''
#         {0}
#             --participant-label "{subjectId}"
#             --notrack
#             --output-spaces
#                 MNI152NLin6Asym MNI152NLin2009cAsym OASIS30ANTs
#             --nprocs {nproc}
#             --mem-gb {memGb}
#             --fs-no-reconall
#             -vvvv
#             --fs-license "{freesurferLicenseFile}"
#             -w "{workDir}"
#             "{datasetDir}"
#             "{outputDir}"
#             participant
#     ''',
#     # Map paths to vm volumes using argument decorators.
#     "decorators": {
#         "datasetDir": InputDir,
#         "workDir": OutputDir,
#         "outputDir": OutputDir,
#         "templateflowDataDir": InputDir,
#         "freesurferLicenseFile": InputFile,
#         "memGb": math.floor
#     }
# }

# # Used to generate fmriprep bids file selection filter for session level
# # granularity.
# FMRIPREP_SESSION_FILTER = {
#     VMEngine.NONE: 'printf',
#     VMEngine.SINGULARITY: None,  # @TODO !!! map to NONE
#     VMEngine.DOCKER: None,  # @TODO !!! map to NONE
#     "cmd": '''
#         {0} '%s' '{{
#             "fmap": {{
#                 "datatype": "fmap"
#             }},
#             "bold": {{
#                 "datatype": "func",
#                 "suffix": "bold",
#                 "session": "{sessionId}"
#             }},
#             "sbref": {{
#                 "datatype": "func",
#                 "suffix": "sbref",
#                 "session": "{sessionId}"
#             }},
#             "flair": {{"datatype": "anat", "suffix": "FLAIR"}},
#             "t2w": {{"datatype": "anat", "suffix": "T2w"}},
#             "t1w": {{"datatype": "anat", "suffix": "T1w"}},
#             "roi": {{"datatype": "anat", "suffix": "roi"}}
#         }}' > "{bidsFilterFile}"
#     ''',
#     # Map paths to vm volumes using argument decorators.
#     "decorators": {
#         "bidsFilterFile": OutputFile
#     }
# }

# FMRIPREP_SESSION = {
#     VMEngine.NONE: 'fmriprep',
#     VMEngine.SINGULARITY: '../singularity-images/fmriprep-20.2.0.simg',
#     VMEngine.DOCKER: 'nipreps/fmriprep:20.2.0',
#     "cmd": '''
#         {0}
#             --notrack
#             --skip-bids-validation
#             --output-spaces
#                 MNI152NLin2009cAsym
#             --ignore slicetiming
#             --nprocs {nproc}
#             --mem-mb {memMb}
#             -vvvv
#             --fs-no-reconall
#             --fs-license "{freesurferLicenseFile}"
#             --anat-derivatives "{anatsDerivativesDir}"
#             -w "{workDir}"
#             "{datasetDir}"
#             "{outputDir}"
#             participant
#             --participant-label "{subjectId}"
#             --bids-filter-file "{bidsFilterFile}"
#     ''',
#     # Map paths to vm volumes using argument decorators.
#     "decorators": {
#         "datasetDir": InputDir,
#         "anatsDerivativesDir": InputDir,
#         "workDir": OutputDir,
#         "outputDir": OutputDir,
#         "templateflowDataDir": InputDir,
#         "freesurferLicenseFile": InputFile,
#         "bidsFilterFile": InputFile,
#         "memMb": math.floor
#     }
# }
