from src.executor.TaskFactory import TaskFactory
import sys  # for python version check + sys.exit
import os  # for cache file delete
import shutil  # for cache dir delete
import dask.distributed  # for MT
import dask_jobqueue
import dask_mpi
from .config import availableExecutables
from .src.pipeline import LocalPipeline, DistributedPipeline
from .src.cli import readCLIArgs, Executor, Granularity, VMEngine
from .src.scheduler import LocalScheduler, DaskScheduler
from .src.dataset import LocalDataset

# Run the pipeline.
if __name__ == '__main__':
    # 0. Check python version.
    # @note f-strings require python 3.6, will throw syntax error instead
    # though.
    assert sys.version_info >= (3, 6)

    # Retrieve args
    args = readCLIArgs()
    granularity = args.granularity
    datasetDir = args.datasetPath
    outputDir = args.outputDir
    vmEngine = args.vmEngine
    executor = args.executor
    reset = args.reset
    enableBidsValidator = args.enableBidsValidator
    enableMRIQC = args.enableMRIQC
    enableSMRiPrep = args.enableSMRiPrep
    enableFMRiPrep = args.enableFMRiPrep
    fasttrackFixDir = os.path.dirname(os.path.realpath(__file__)) + '/smriprep-fasttrack-fix'
    workerCount = args.workerCount
    nproc = args.workerCpuCount
    memGb = args.workerMemoryGb
    workerWallTime = args.workerWallTime
    workerLocalDir = args.workerLocalDir  # can be None
    workerSharedDir = args.workerSharedDir  # can be None
    isPipelineDistributed = False if workerLocalDir is None else True
    workDir = f'{outputDir}/work/' if workerLocalDir is None else f'{workerLocalDir}/work/'  # @warning can only be used within dask task
    print(f'nproc: {nproc}')
    print(f'memGb: {memGb}')

    # Reset cache files/folders.
    if reset:
        shutil.rmtree('./__pycache__', ignore_errors=True)
        shutil.rmtree(f'./{outputDir}', ignore_errors=True)

    # Setup task executor.
    scheduler = None
    client = None
    cluster = None
    if executor == Executor.NONE:
        scheduler = LocalScheduler(f'{outputDir}/.task_cache.csv')
    elif executor == Executor.LOCAL:
        cluster = dask.distributed.LocalCluster()
        cluster.scale(1)  # Only one worker if local, fmriprep etc. should use multiple cpus!
        client = dask.distributed.Client(cluster)
        scheduler = DaskScheduler(f'{outputDir}/.task_cache.csv', client)
    elif executor == Executor.SLURM:
        cluster = dask_jobqueue.SLURMCluster(
            # @warning
            # worker job stealing failing on slurmcluster with resources when
            # using multiple cores.
            # cf. https://github.com/dask/dask-jobqueue/issues/206
            # Thus with resources, only one job work at a time.
            # As a fix, we dont mind specifiyng only one core for dask, as
            # subprocesses wont be restrained by this constraint.
            # cores=nproc,
            cores=1,
            job_cpu=nproc,  # job_cpu default to `cores` parameter, see above.
            # @warning resources are applied per worker's process, not per worker!
            processes=1,  # (probably false:) one job per worker ? cf. https://github.com/dask/dask-jobqueue/issues/365 - doc is unclear -- removed because fear of deadlock due to locking sarge/subprocess while loop
            # Limit to max 1 job per worker through passing arbitraty resources
            # limit variables to worker launch
            # cf. https://jobqueue.dask.org/en/latest/examples.html#slurm-deployment-providing-additional-arguments-to-the-dask-workers
            # 
            # @warning
            # `The resources keyword only affects the final result tasks by default.
            # There isn't a great way to restrict the entire computation today.` (2019)
            # cf. https://github.com/dask/distributed/issues/2832#issuecomment-510668723
            # Weird since resources in #compute allow to specify global or per-task
            # resource allocation.
            extra=['--resources job=1'],  
            project="def-porban",
            death_timeout=0,  # disable worker kill when scheduler is not accessible for 60 seconds.
            memory=f'{memGb} GB',
            walltime=workerWallTime,
            local_directory=f'{workDir}/dask',  # @warning does it work with '$' embedded ????
            log_directory=f'{outputDir}/log/dask',  # @warning not to copied to local hd first, instead use shared file system.
            # death_timeout=120,
            job_extra=['--tmp="240G"' if granularity is Granularity.SESSION else '--tmp="300G"'],  # requires at least 200G available (~half of 480GB beluga)
            env_extra=['module load singularity']
        )
        cluster.scale(1)  # at least one worker required in order to be able to
                          # fetch dataset information.
        client = dask.distributed.Client(cluster)
        scheduler = DaskScheduler(f'{outputDir}/.task_cache.csv', client)
    elif executor == Executor.MPI:
        dask_mpi.initialize()
        client = dask.distributed.Client()
        scheduler = DaskScheduler(f'{outputDir}/.task_cache.csv', client)
    print(client)

    # Generate executable config.
    # validateBids = TaskFactory.generate(vmEngine, BIDS_VALIDATOR[vmEngine], BIDS_VALIDATOR['cmd'], BIDS_VALIDATOR['decorators'])
    executables = {
        'bidsValidator': {
            'vmEngine': vmEngine,
            'executable': availableExecutables['bidsValidator'][vmEngine]
        },
        'mriqc': {
            'vmEngine': vmEngine,
            'executable': availableExecutables['mriqc'][vmEngine]
        },
        'smriprep': {
            'vmEngine': vmEngine,
            'executable': availableExecutables['smriprep'][vmEngine]
        },
        'fmriprep': {
            'vmEngine': vmEngine,
            'executable': availableExecutables['fmriprep'][vmEngine]
        },
        # @note no vm for printf.
        'printf': {
            'vmEngine': VMEngine.NONE,
            'executable': availableExecutables['printf'][VMEngine.NONE]
        }
    }

    # Generate pipeline.
    pipeline = None
    if not isPipelineDistributed:
        pipeline = LocalPipeline(vmEngine, availableExecutables)
    # Override pipeline file management in order to optimize for distributed
    # file systems.
    elif isPipelineDistributed:

        ## @TODO !!!!!!!!
        pipeline = DistributedPipeline(
            **executables,
            archiveDir=f'{outputDir}/archives/',
            archiveName=os.path.basename(os.path.dirname(datasetDir)),
            workerLocalDir=workerLocalDir,
            workerSharedDir=workerSharedDir
        )

    # Execute pipeline.
    if isPipelineDistributed:
        # 0. Archive dataset for faster IO when pipeline is distributed.
        # @todo remove dual archiveName/Dir with pipeline
        didSucceed = scheduler.runTask(
            'archive_dataset',
            lambda: pipeline.archiveDataset(
                datasetDir=datasetDir,
                # logFile=f'{outputDir}/log/archive-dataset.txt'
            )
        )
        if not didSucceed:
            sys.exit(-1)

    # Retrieve dataset info.
    dataset = None
    if not isPipelineDistributed:
        dataset = LocalDataset(datasetDir, f'{outputDir}/.bids_cache')
    # Wrap inside distributed pipeline, in order to prevent issues due to
    # distributed file system (missing subject ids, etc).
    elif isPipelineDistributed:
        print('Fetching dataset informations..')
        prefetchDatasetInfoDelayed = dask.delayed(pipeline.prefetchDatasetInfo)
        datasetInfoDelayed = prefetchDatasetInfoDelayed()
        datasetInfoComputation = client.compute(datasetInfoDelayed, resources={'job': 1})
        dataset = datasetInfoComputation.result()
        if dataset is None:
            print('error: couldn\'t extract dataset from archive to retrieve info')
            sys.exit(-2)

    # Set workerCount to number of subject if == -1 and scale worker on/if
    # slurm cluster scheduler is used accordingly.
    if workerCount == -1:
        workerCount = len(dataset.getSubjectIds())
    if executor == Executor.SLURM:
        cluster.scale(workerCount)

    if enableBidsValidator:
        # 1. BidsValidator.
        didSucceed = scheduler.runTask(
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
        successfulSubjectIds, failedSubjectIds = scheduler.batchTask(
            'mriqc_subj',
            lambda subjectId: pipeline.generateMriQcSubjectReport(
                datasetDir=datasetDir,
                workDir=f'{workDir}/mriqc/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives/mriqc',
                logFile=f'{outputDir}/log/mriqc/sub-{subjectId}.txt',
                nproc=nproc,
                memGb=memGb,
                subjectId=subjectId
            ),
            subjectIds
        )
        if len(successfulSubjectIds) == 0:
            sys.exit(2)

        # 3. MRIQC: group qc.
        didSucceed = scheduler.runTask(
            'mriqc_group',
            lambda: pipeline.generateMriQcGroupReport(
                datasetDir=datasetDir,
                workDir=f'{workDir}/mriqc/group',
                outputDir=f'{outputDir}/derivatives/mriqc',
                logFile=f'{outputDir}/log/mriqc/group.txt',
                nproc=nproc,
                memGb=memGb,
            )
        )
        if not didSucceed:
            sys.exit(3)

        # Limit next step's subject ids to the one that succeeded MRIQC.
        subjectIds = successfulSubjectIds
    
    if enableSMRiPrep and granularity is Granularity.SESSION:
        # 4. SMRiPrep: anat by subjects.
        successfulSubjectIds, failedSubjectIds = scheduler.batchTask(
            'smriprep_anat',
            lambda subjectId: pipeline.preprocessSMRiPrepAnatBySubject(
                datasetDir=datasetDir,
                workDir=f'{workDir}/smriprep/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives',  # /smriprep will be add by the cmd.
                logFile=f'{outputDir}/log/smriprep/sub-{subjectId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                nproc=nproc,
                memGb=memGb,
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
    if enableFMRiPrep and granularity is Granularity.SESSION:
        # 5 FMRiPrep: generate sessions' func file filters.
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_filter',
            lambda subjectId, sessionId: pipeline.generateFMRiPrepSessionFilter(
                logFile=f'{outputDir}/log/fmriprep/filters/sub-{subjectId}.txt',
                bidsFilterFile=f'{outputDir}/filefilters/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',  # @todo remove func
                sessionId=sessionId
            ),
            sessionIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(5)

        # 6. FMRiPrep: func by subjects.
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_func',
            lambda subjectId, sessionId: pipeline.preprocessFMRiPrepFuncBySession(
                datasetDir=datasetDir,
                anatsDerivativesDir=f'{outputDir}/derivatives/smriprep',
                workDir=f'{workDir}/fmriprep/sub-{subjectId}/ses-{sessionId}',
                outputDir=f'{outputDir}/derivatives',  # /fmriprep will be add by the cmd.
                logFile=f'{outputDir}/log/fmriprep/sub-{subjectId}/ses-{sessionId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                templateflowDataDir='./templateflow',
                bidsFilterFile=f'{outputDir}/filefilters/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',  # @todo remove func
                nproc=nproc,
                memMb=memGb*1024,
                subjectId=subjectId,
                sessionId=sessionId,
                fasttrackFixDir=fasttrackFixDir
            ),
            successfulSessionIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(6)

        # Limit next step's subject/session ids to the one that succeeded
        # MRIQC.
        sessionIds = successfulSessionIds

    if granularity is Granularity.DATASET or granularity is Granularity.SUBJECT:
        # 7. FMRiPrep: all by subjects.
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_all',
            lambda subjectId: pipeline.preprocessFMRiPrepBySubject(
                datasetDir=datasetDir,
                workDir=f'{workDir}/fmriprep/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives',  # /fmriprep will be add by the cmd.
                logFile=f'{outputDir}/log/fmriprep/sub-{subjectId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                templateflowDataDir='./templateflow',
                nproc=nproc,
                memMb=memGb*1024,
                subjectId=subjectId,
            ),
            subjectIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(7)
