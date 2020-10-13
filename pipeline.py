from typing import Callable, Dict
from src.cmd_helpers import InputDir, InputFile, OutputDir, OutputFile, \
    createTaskForCmd


class Pipeline:
    validateBids: Callable
    generateMriQcSubjectReport: Callable
    generateMriQcGroupReport: Callable
    preprocessSMRiPrepAnatBySubject: Callable
    generateFMRiPrepSessionFilter: Callable
    preprocessFMRiPrepFuncBySession: Callable

    # @warning docker requires manual app modification.
    # https://stackoverflow.com/questions/44533319/how-to-assign-more-memory-to-docker-container/44533437#44533437
    def __init__(self, bidsValidator: Dict[str, str], mriqc: Dict[str, str],
                 smriprep: Dict[str, str], fmriprep: Dict[str, str],
                 printf: Dict[str, str]) -> None:

        self.validateBids = createTaskForCmd(
            bidsValidator['vmEngine'],
            bidsValidator['executable'],
            '''
                {0} "{datasetDir}"
            ''',
            # Map paths to vm volumes using argument decorators.
            datasetDir=InputDir
        )

        self.generateMriQcSubjectReport = createTaskForCmd(
            mriqc['vmEngine'],
            mriqc['executable'],
            '''
                {0}
                    --no-sub
                    --nprocs 2
                    --mem_gb 6
                    -vv
                    -w "{workDir}"
                    "{datasetDir}"
                    "{outputDir}"
                    participant
                    --participant_label "{subjectId}"
            ''',
            # Map paths to vm volumes using argument decorators.
            workDir=OutputDir,
            datasetDir=InputDir,
            outputDir=OutputDir
        )

        self.generateMriQcGroupReport = createTaskForCmd(
            mriqc['vmEngine'],
            mriqc['executable'],
            '''
                {0}
                    --no-sub
                    --nprocs 2
                    --mem_gb 6
                    -vv
                    -w "{workDir}"
                    "{datasetDir}"
                    "{outputDir}"
                    group
            ''',
            # Map paths to vm volumes using argument decorators.
            datasetDir=InputDir,
            workDir=OutputDir,
            outputDir=OutputDir
        )

        # @warning 6000mb crashes on docker.
        #   MNI152NLin2009cAsym OASIS30ANTs
        # @warning potential templateflow compat issue.
        #   https://neurostars.org/t/tips-for-getting-a-bare-metal-installation-of-fmriprep-1-4-1-working/4660/2
        # might be   --sloppy
        # TEMPLATEFLOW_HOME="{templateflowDataDir}"
        self.preprocessSMRiPrepAnatBySubject = createTaskForCmd(
            smriprep['vmEngine'],
            smriprep['executable'],
            '''
                {0}
                    --participant-label "{subjectId}"
                    --notrack
                    --fs-no-reconall
                    --output-spaces
                        MNI152NLin6Asym MNI152NLin2009cAsym OASIS30ANTs
                    --nprocs 2
                    --mem-gb 8
                    -vv
                    --fs-license "{freesurferLicenseFile}"
                    -w "{workDir}"
                    "{datasetDir}"
                    "{outputDir}"
                    participant
            ''',
            # Map paths to vm volumes using argument decorators.
            datasetDir=InputDir,
            workDir=OutputDir,
            outputDir=OutputDir,
            templateflowDataDir=InputDir,
            freesurferLicenseFile=InputFile
        )

        self.generateFMRiPrepSessionFilter = createTaskForCmd(
            printf['vmEngine'],
            printf['executable'],
            '''
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
            bidsFilterFile=OutputFile
        )
# --output-spaces MNI152NLin6Asym MNI152NLin2009cAsym OASIS30ANTs
        self.preprocessFMRiPrepFuncBySession = createTaskForCmd(
            fmriprep['vmEngine'],
            fmriprep['executable'],
            '''
                TEMPLATEFLOW_HOME="{templateflowDataDir}"
                {0}
                    --notrack
                    --skip-bids-validation
                    --fs-no-reconall
                    --ignore slicetiming
                    --nprocs 2
                    --mem-mb 8000
                    -vv
                    --fs-license "{freesurferLicenseFile}"
                    --anat-derivatives "{anatsDerivativesDir}"
                    -w "{workDir}"
                    "{datasetDir}"
                    "{outputDir}"
                    participant
                    --participant-label "{subjectId}"
                    --bids-filter-file "{bidsFilterFile}"
            ''',
            # Map paths to vm volumes using argument decorators.
            datasetDir=InputDir,
            anatsDerivativesDir=InputDir,
            workDir=OutputDir,
            outputDir=OutputDir,
            templateflowDataDir=InputDir,
            freesurferLicenseFile=InputFile,
            bidsFilterFile=InputFile
        )
