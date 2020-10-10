import sys  # for python version check + sys.exit
import os  # for cache file delete
import shutil  # for cache dir delete
import dask.distributed  # for MT
import dask_jobqueue
import dask_mpi
from pipeline import Pipeline
from cli import Executor, readCLIArgs, Executor
from cmd_helpers import VMEngine
from daskrunner import DaskRunner
from runner import Runner
from dataset import Dataset


# Run the pipeline.
if __name__ == '__main__':
    # 0. Check python version.
    # @note f-strings require python 3.6, will throw syntax error instead
    # though.
    assert sys.version_info >= (3, 6)

    # Retrieve args
    args = readCLIArgs()
    datasetDir = args.datasetPath
    outputDir = args.outputDir
    vmEngine = args.vmEngine
    executor = args.executor
    reset = args.reset
    enableBidsValidator = args.enableBidsValidator
    enableMRIQC = args.enableMRIQC
    enableSMRiPrep = args.enableSMRiPrep
    enableFMRiPrep = args.enableFMRiPrep

    # Reset cache files/folders.
    if reset:
        shutil.rmtree('./__pycache__', ignore_errors=True)
        shutil.rmtree(f'./{outputDir}', ignore_errors=True)

    # Setup task executor.
    runner = None
    if executor == Executor.NONE:
        runner = Runner(f'{outputDir}/.task_cache.csv')
    elif executor == Executor.LOCAL:
        cluster = dask.distributed.LocalCluster()
        cluster.scale(4)
        client = dask.distributed.Client(cluster)
        runner = DaskRunner(f'{outputDir}/.task_cache.csv', client)
        print(client)
    elif executor == Executor.SLURM:
        cluster = dask_jobqueue.SLURMCluster()
        cluster.scale(4)
        client = dask.distributed.Client(cluster)
        runner = DaskRunner(f'{outputDir}/.task_cache.csv', client)
        print(client)
    elif executor == Executor.MPI:
        dask_mpi.initialize()
        client = dask.distributed.Client()
        runner = DaskRunner(f'{outputDir}/.task_cache.csv', client)
        print(client)

    # Retrieve dataset info.
    dataset = Dataset(datasetDir, f'{outputDir}/.bids_cache')

    # Setup pipeline tasks.
    bidsValidator = {
        VMEngine.NONE: 'bids-validator',
        VMEngine.SINGULARITY: '../singularity-images/bids-validator-1.5.6.sif',
        VMEngine.DOCKER: 'bids/validator:v1.5.6'
    }
    mriqc = {
        VMEngine.NONE: 'mriqc',
        VMEngine.SINGULARITY: '../singularity-images/mriqc-0.15.2.sif',
        VMEngine.DOCKER: 'poldracklab/mriqc:0.15.2'
    }
    smriprep = {
        VMEngine.NONE: 'smriprep',
        VMEngine.SINGULARITY: '../singularity-images/smriprep-0.7.0.sif',
        VMEngine.DOCKER: 'nipreps/smriprep:0.7.0'
    }
    fmriprep = {
        VMEngine.NONE: 'fmriprep',
        VMEngine.SINGULARITY: '../singularity-images/fmriprep-20.2.0.sif',
        VMEngine.DOCKER: 'nipreps/fmriprep:20.2.0'
    }
    printf = {
        VMEngine.NONE: 'printf'
    }
    pipeline = Pipeline(
        bidsValidator={
            'vmEngine': vmEngine,
            'executable': bidsValidator[vmEngine]
        },
        mriqc={
            'vmEngine': vmEngine,
            'executable': mriqc[vmEngine]
        },
        smriprep={
            'vmEngine': vmEngine,
            'executable': smriprep[vmEngine]
        },
        fmriprep={
            'vmEngine': vmEngine,
            'executable': fmriprep[vmEngine]
        },
        # @note no vm for printf.
        printf={
            'vmEngine': VMEngine.NONE,
            'executable': printf[VMEngine.NONE]
        }
    )

    # Execute pipeline.
    if enableBidsValidator:
        # 1. BidsValidator.
        didSucceed = runner.runTask(
            'validate_bids',
            lambda: pipeline.validateBids(
                datasetDir=datasetDir,
                logFile=f'{outputDir}/log/validate-bids.txt'
            )
        )
        if not didSucceed:
            sys.exit(1)

    subjectIds = dataset.getSubjectIds()
    if enableMRIQC:
        # 2. MRIQC: qc by subjects.
        successfulSubjectIds, failedSubjectIds = runner.batchTask(
            'mriqc_subj',
            lambda subjectId: pipeline.generateMriQcSubjectReport(
                datasetDir=datasetDir,
                workDir=f'{outputDir}/tmp/mriqc/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives/mriqc',
                logFile=f'{outputDir}/log/mriqc/sub-{subjectId}.txt',
                subjectId=subjectId
            ),
            subjectIds
        )
        if len(successfulSubjectIds) == 0:
            sys.exit(2)

        # 3. MRIQC: group qc.
        didSucceed = runner.runTask(
            'mriqc_group',
            lambda: pipeline.generateMriQcGroupReport(
                datasetDir=datasetDir,
                workDir=f'{outputDir}/tmp/mriqc/group',
                outputDir=f'{outputDir}/derivatives/mriqc',
                logFile=f'{outputDir}/log/mriqc/group.txt'
            )
        )
        if not didSucceed:
            sys.exit(3)

        # Limit next step's subject ids to the one that succeeded MRIQC.
        subjectIds = successfulSubjectIds

    if enableSMRiPrep:
        # 4. SMRiPrep: anat by subjects.
        successfulSubjectIds, failedSubjectIds = runner.batchTask(
            'smriprep_anat',
            lambda subjectId: pipeline.preprocessSMRiPrepAnatBySubject(
                datasetDir=datasetDir,
                workDir=f'{outputDir}/tmp/smriprep/anat/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives',  # /smriprep will be add by the cmd.
                logFile=f'{outputDir}/log/smriprep/anat/sub-{subjectId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                # templateflowDataDir='./templateflow',
                subjectId=subjectId
            ),
            subjectIds
        )
        if len(successfulSubjectIds) == 0:
            sys.exit(4)

        # Limit next step's subject ids to the one that succeeded MRIQC.
        subjectIds = successfulSubjectIds

    sessionIds = [
        (subjectId, sessionId)
        for subjectId in subjectIds
        for sessionId in dataset.getSessionIdsBySubjectId(subjectId)
    ]
    if enableFMRiPrep:
        # 5 FMRiPrep: generate sessions' func file filters.
        successfulSessionIds, failedSessionIds = runner.batchTask(
            'fmriprep_filter',
            lambda subjectId, sessionId: pipeline.generateFMRiPrepSessionFilter(
                logFile=f'{outputDir}/log/fmriprep/filters/sub-{subjectId}.txt',
                bidsFilterFile=f'{outputDir}/tmp/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',
                sessionId=sessionId
            ),
            sessionIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(5)

        # 6. FMRiPrep: func by subjects.
        successfulSessionIds, failedSessionIds = runner.batchTask(
            'fmriprep_func',
            lambda subjectId, sessionId: pipeline.preprocessFMRiPrepFuncBySession(
                datasetDir=datasetDir,
                anatsDerivativesDir=f'{outputDir}/derivatives/smriprep',
                workDir=f'{outputDir}/tmp/fmriprep/func/sub-{subjectId}/ses-{sessionId}',
                outputDir=f'{outputDir}/derivatives',  # /fmriprep will be add by the cmd.
                logFile=f'{outputDir}/log/fmriprep/func/sub-{subjectId}/ses-{sessionId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                templateflowDataDir='./templateflow',
                bidsFilterFile=f'{outputDir}/tmp/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',
                subjectId=subjectId,
                sessionId=sessionId
            ),
            successfulSessionIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(6)

        # Limit next step's subject/session ids to the one that succeeded
        # MRIQC.
        sessionIds = successfulSessionIds
