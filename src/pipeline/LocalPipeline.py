from typing import Callable, Dict
import math
from ..cli import VMEngine
from ..path import PathPlaceHolder, InputFile, InputDir, OutputFile, \
                     OutputDir
from ..executor import TaskFactory


class Pipeline:
    _validateBids: Callable
    _generateMriQcSubjectReport: Callable
    _generateMriQcGroupReport: Callable
    _preprocessSMRiPrepAnatBySubject: Callable
    _generateFMRiPrepSessionFilter: Callable
    _preprocessFMRiPrepFuncBySession: Callable
    _preprocessFMRiPrepBySubject: Callable

    def validateBids(self, *args, **kargs):
        return self._validateBids(*args, **kargs)
    def generateMriQcSubjectReport(self, *args, **kargs):
        return self._generateMriQcSubjectReport(*args, **kargs)
    def generateMriQcGroupReport(self, *args, **kargs):
        return self._generateMriQcGroupReport(*args, **kargs)
    def preprocessSMRiPrepAnatBySubject(self, *args, **kargs):
        return self._preprocessSMRiPrepAnatBySubject(*args, **kargs)
    def generateFMRiPrepSessionFilter(self, *args, **kargs):
        return self._generateFMRiPrepSessionFilter(*args, **kargs)
    def preprocessFMRiPrepFuncBySession(self, *args, **kargs):
        return self._preprocessFMRiPrepFuncBySession(*args, **kargs)
    def preprocessFMRiPrepBySubject(self, *args, **kargs):
        return self._preprocessFMRiPrepBySubject(*args, **kargs)

    # def __init__(self, vmEngine: VMEngine, availableExecutables) -> None:

    # @warning docker requires manual app modification.
    # https://stackoverflow.com/questions/44533319/how-to-assign-more-memory-to-docker-container/44533437#44533437
    def __init__(self, bidsValidator: Dict[str, str], mriqc: Dict[str, str],
                 smriprep: Dict[str, str], fmriprep: Dict[str, str],
                 printf: Dict[str, str]) -> None:
        self._validateBids = self._createTaskForCmd(
            bidsValidator['vmEngine'],
            bidsValidator['executable'],
            '''
                {0} "{datasetDir}"
            ''',
            # Map paths to vm volumes using argument decorators.
            datasetDir=InputDir
        )

        self._generateMriQcSubjectReport = self._createTaskForCmd(
            mriqc['vmEngine'],
            mriqc['executable'],
            '''
                {0}
                    --no-sub
                    --nprocs {nproc}
                    --mem_gb {memGb}
                    -vvvv
                    -w "{workDir}"
                    "{datasetDir}"
                    "{outputDir}"
                    participant
                    --participant_label "{subjectId}"
            ''',
            # Map paths to vm volumes using argument decorators.
            workDir=OutputDir,
            datasetDir=InputDir,
            outputDir=OutputDir,
            memGb=math.floor
        )

        self._generateMriQcGroupReport = self._createTaskForCmd(
            mriqc['vmEngine'],
            mriqc['executable'],
            '''
                {0}
                    --no-sub
                    --nprocs {nproc}
                    --mem_gb {memGb}
                    -vvvv
                    -w "{workDir}"
                    "{datasetDir}"
                    "{outputDir}"
                    group
            ''',
            # Map paths to vm volumes using argument decorators.
            datasetDir=InputDir,
            workDir=OutputDir,
            outputDir=OutputDir,
            memGb=math.floor
        )

        # @warning 6000mb crashes on docker.
        #   MNI152NLin2009cAsym OASIS30ANTs
        # @warning potential templateflow compat issue.
        #   https://neurostars.org/t/tips-for-getting-a-bare-metal-installation-of-fmriprep-1-4-1-working/4660/2
        # might be   --sloppy
        # TEMPLATEFLOW_HOME="{templateflowDataDir}"
        self._preprocessSMRiPrepAnatBySubject = self._createTaskForCmd(
            smriprep['vmEngine'],
            smriprep['executable'],
            '''
                {0}
                    --participant-label "{subjectId}"
                    --notrack
                    --output-spaces
                        MNI152NLin6Asym MNI152NLin2009cAsym OASIS30ANTs
                    --nprocs {nproc}
                    --mem-gb {memGb}
                    --fs-no-reconall
                    -vvvv
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
            freesurferLicenseFile=InputFile,
            memGb=math.floor
        )

        self._generateFMRiPrepSessionFilter = self._createTaskForCmd(
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
        #                TEMPLATEFLOW_HOME="{templateflowDataDir}"
        self._preprocessFMRiPrepFuncBySession = self._createTaskForCmd(
            fmriprep['vmEngine'],
            fmriprep['executable'],
            '''
                {0}
                    --notrack
                    --skip-bids-validation
                    --output-spaces
                        MNI152NLin2009cAsym
                    --ignore slicetiming
                    --nprocs {nproc}
                    --mem-mb {memMb}
                    -vvvv
                    --fs-no-reconall
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
            bidsFilterFile=InputFile,
            memMb=math.floor
        )

        self._preprocessFMRiPrepBySubject = self._createTaskForCmd(
            fmriprep['vmEngine'],
            fmriprep['executable'],
            '''
                {0}
                    --notrack
                    --skip-bids-validation
                    --ignore slicetiming
                    --nprocs {nproc}
                    --mem-mb {memMb}
                    -vvvv
                    --fs-no-reconall
                    --fs-license "{freesurferLicenseFile}"
                    -w "{workDir}"
                    "{datasetDir}"
                    "{outputDir}"
                    participant
                    --participant-label "{subjectId}"
            ''',
            # Map paths to vm volumes using argument decorators.
            datasetDir=InputDir,
            workDir=OutputDir,
            outputDir=OutputDir,
            templateflowDataDir=InputDir,
            freesurferLicenseFile=InputFile,
            memMb=math.floor
        )

    def _createTaskForCmd(self, vmType: VMEngine, executable: str,
                          cmdTemplate: str, **argsDecorators: Dict[str, PathPlaceHolder]):
        return TaskFactory.generate(vmType, executable, cmdTemplate,
            **argsDecorators)

