import sys  # for python version check + sys.exit
import os  # for cache file delete
import shutil  # for cache dir delete
import dask.distributed  # for MT
import dask_jobqueue
import dask_mpi
from typing import Set
from config import REMOVE_FILE, REMOVE_DIR, COPY_FILE, \
                   ARCHIVE_DATASET, EXTRACT_DATASET, EXTRACT_DATASET_SUBJECT, \
                   EXTRACT_DATASET_SESSION, \
                   LIST_ARCHIVE_SESSIONS, BIDS_VALIDATOR, MRIQC_SUBJECT, \
                   MRIQC_GROUP, SMRIPREP_SUBJECT, FMRIPREP_SUBJECT, \
                   FMRIPREP_SESSION, FMRIPREP_SESSION_FILTER
from src.cli import readCLIArgs, Executor, Granularity, VMEngine
from src.scheduler import LocalScheduler, DaskScheduler
from src.executor import TaskFactory, TaskConfig
from src.dataset import LocalDataset, DistributedDataset
# from src.pipeline import LocalPipeline, DistributedPipeline

# Run the pipeline.
# @warning the pipeline currently doesn't cleanup the work dir when task fails.
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
    if executor is Executor.NONE:
        scheduler = LocalScheduler(f'{outputDir}/.task_cache.csv')
    elif executor is Executor.LOCAL:
        cluster = None
        # Setup max job per worker, through worker resource limitation
        # cf. https://distributed.dask.org/en/latest/resources.html#specifying-resources
        # Mainly used to prevent disk usage overload on distributed, compute
        # canada, local SSD (edge case), though we need to setup the resources
        # for every dask config, including local one, otherwise our task will
        # get stuck, waiting for allocation (due to DaskScheduler code).
        with dask.config.set({"distributed.worker.resources.job": 1}):
            cluster = dask.distributed.LocalCluster()
        cluster.scale(1)  # Only one worker if local, fmriprep etc. should use multiple cpus!
        client = dask.distributed.Client(cluster)
        scheduler = DaskScheduler(f'{outputDir}/.task_cache.csv', client)
    elif executor is Executor.SLURM:
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
            # job_cpu default to `cores` parameter, see above.
            job_cpu=nproc,
            # @warning resources are applied per worker's process, not per
            # worker!
            # (probably false:) one job per worker ? cf.
            # https://github.com/dask/dask-jobqueue/issues/365 - doc is unclear
            # -- removed because fear of deadlock due to locking
            # sarge/subprocess while loop
            processes=1,
            # Limit to max 1 job per worker through passing arbitraty resources
            # limit variables to worker launch and task scheduling.
            # cf. https://jobqueue.dask.org/en/latest/examples.html#slurm-deployment-providing-additional-arguments-to-the-dask-workers
            # 
            # @warning
            # `The resources keyword only affects the final result tasks by
            # default. There isn't a great way to restrict the entire
            # computation today.` (2019)
            # cf. https://github.com/dask/distributed/issues/2832#issuecomment-510668723
            # Weird since resources in #compute allow to specify global or
            # per-task resource allocation.
            extra=['--resources job=1'],  
            project="def-porban",
            # Disable worker kill when scheduler is not accessible for > 60
            # seconds.
            death_timeout=0,
            memory=f'{memGb} GB',
            walltime=workerWallTime,
            # @warning does it work with '$' embedded ???? -- seems to!!
            local_directory=f'{workDir}/dask', 
            # @warning not to copied to local hd first, instead use shared file
            # system.
            log_directory=f'{outputDir}/log/dask',
            # death_timeout=120,
            # Requires at least 200G available (~half of 480GB beluga).
            job_extra=['--tmp="240G"' if granularity is Granularity.SESSION \
                else '--tmp="300G"'],
            env_extra=['module load singularity']
        )
        cluster.scale(1)  # at least one worker required in order to be able to
                          # fetch dataset information.
        client = dask.distributed.Client(cluster)
        scheduler = DaskScheduler(
            f'{outputDir}/.task_cache.csv', client, forceResourceRequest=True)
    elif executor is Executor.MPI:
        # Setup max job per worker, through worker resource limitation
        # cf. https://distributed.dask.org/en/latest/resources.html#specifying-resources
        # Mainly used to prevent disk usage overload on distributed, compute
        # canada, local SSD (edge case), though we need to setup the resources
        # for every dask config, otherwise our task will get stuck, waiting for
        # allocation (due to DaskScheduler code).
        with dask.config.set({"distributed.worker.resources.job": 1}):
            dask_mpi.initialize()
            client = dask.distributed.Client()
        scheduler = DaskScheduler(f'{outputDir}/.task_cache.csv', client)
    print(client)

    # Generate tasks.
    remove_file = TaskFactory.generate(VMEngine.NONE, REMOVE_FILE)
    remove_dir = TaskFactory.generate(VMEngine.NONE, REMOVE_DIR)
    copy_file = TaskFactory.generate(VMEngine.NONE, COPY_FILE)
    archive_dataset = TaskFactory.generate(VMEngine.NONE, ARCHIVE_DATASET)  # distributed only / no singularity/docker image available.
    extract_dataset = TaskFactory.generate(VMEngine.NONE, EXTRACT_DATASET)  # distributed only / no singularity/docker image available.
    extract_dataset_subject = TaskFactory.generate(VMEngine.NONE, EXTRACT_DATASET_SUBJECT)  # distributed only / no singularity/docker image available.
    extract_dataset_session = TaskFactory.generate(VMEngine.NONE, EXTRACT_DATASET_SESSION)  # distributed only / no singularity/docker image available.
    list_archive_sessions = TaskFactory.generate(VMEngine.NONE, LIST_ARCHIVE_SESSIONS) # distributed only / no singularity/docker image available.
    bids_validator = TaskFactory.generate(vmEngine, BIDS_VALIDATOR)
    mriqc_subject = TaskFactory.generate(vmEngine, MRIQC_SUBJECT)
    mriqc_group = TaskFactory.generate(vmEngine, MRIQC_GROUP)
    smriprep_subject = TaskFactory.generate(vmEngine, SMRIPREP_SUBJECT)
    fmriprep_subject = TaskFactory.generate(vmEngine, FMRIPREP_SUBJECT)
    fmriprep_session_filter = TaskFactory.generate(VMEngine.NONE, FMRIPREP_SESSION_FILTER)  # no singularity/docker image available for printf.
    fmriprep_session = TaskFactory.generate(vmEngine, FMRIPREP_SESSION)

    # Archive dataset for faster IO if pipeline is distributed (+ prevent files
    # from being distributed across multiple LUSTRE slaves and fragmented,
    # which I suspect to cause random bugs + cope with Compute Canada file 
    # cap).
    # @todo remove dual archiveName/Dir with pipeline.
    if isPipelineDistributed:
        archiveDir = f'{outputDir}/archives/'
        archiveName = os.path.basename(datasetDir)

        didSucceed = scheduler.runTask(
            'archive_dataset',
            lambda: archive_dataset(
                datasetDir=datasetDir,
                archiveDir=archiveDir,
                archiveName=archiveName,
                logFile=f'{outputDir}/log/archive-dataset.txt'
            ),
            lambda didSucceed: None
        )
        if not didSucceed:
            sys.exit(-1)

    # Analyse dataset in order to be able to orchestrate parallel processing
    # across subjects / sessions.
    dataset = None
    if not isPipelineDistributed:
        dataset = LocalDataset(datasetDir, f'{outputDir}/.bids_cache')
    # Wrap inside distributed pipeline, in order to prevent issues due to
    # distributed file system (missing subject ids, etc). These issues are
    # speculated, although they seems to have been appearing randomly, until I
    # stopped relying on LUSTRE.
    elif isPipelineDistributed:
        # Retrieve dataset info on local node.
        # @warning @todo doesn't work on dataset > 300GO as they can't be
        # extracted from local node...
        # > need to write a drive
        archiveDir = f'{outputDir}/archives/'
        archiveName = os.path.basename(datasetDir)
        # dataset = DistributedDataset.loadFromArchiveWithPyBids(client,
        #                                                        extract_dataset,
        #                                                        archiveDir,
        #                                                        archiveName,
        #                                                        workerLocalDir)

        dataset = DistributedDataset.loadFromArchiveWithDar(client,
                                                            list_archive_sessions,
                                                            archiveDir,
                                                            archiveName)

        # Exit in case of dataset info extraction failure.
        if dataset is None:
            print('error: couldn\'t extract dataset from archive to retrieve info')
            sys.exit(-2)

    # Set workerCount to number of subject if == -1 and scale worker on/if
    # slurm cluster scheduler is used accordingly.
    if workerCount == -1:
        workerCount = len(dataset.getSubjectIds())
    if executor is Executor.SLURM:
        cluster.scale(workerCount)

    # Setup dataset retrieval method (either path, or archive extraction).
    fetch_dataset = None
    if not isPipelineDistributed:
        def fetch_dataset1(subjectId: str = None, sessionIds: Set[str] = None):
            return datasetDir
        def cleanup1(subjectId: str = None, sessionIds: Set[str] = None):
            # Nothing to cleanup.
            pass
        
        fetch_dataset = fetch_dataset1
        fetch_dataset.cleanup = cleanup1
    else:
        def fetch_dataset2(subjectId: str = None, sessionIds: Set[str] = None):
            archiveDir=f'{outputDir}/archives/'
            archiveName=os.path.basename(datasetDir)
            localOutputDir=None  # conditionally defined.

            # Arg check / Edge case.
            if subjectId is None and sessionIds is not None:
                err = 'dataset session extraction requires subject id.'
                raise Exception(err)
            # Extract the whole dataset if subject is not defined.
            elif subjectId is None:
                localOutputDir = f'{workerLocalDir}/dataset'
                extract_dataset(archiveDir=archiveDir,
                                archiveName=archiveName, outputDir=localOutputDir)
            # Extract by subject if session is not defined.
            elif sessionIds is None:
                localOutputDir=f'{workerLocalDir}/dataset-{subjectId}'
                # @todo check result!
                extract_dataset_subject(archiveDir=archiveDir,
                                        archiveName=archiveName,
                                        outputDir=localOutputDir,
                                        subjectId=subjectId)
            # Extract by session if both subject and session are defined.
            else:
                localOutputDir=f'{workerLocalDir}/dataset-{subjectId}-{".".join(sessionIds)}'
                for sessionId in sessionIds:
                    extract_dataset_session(archiveDir=archiveDir,
                                            archiveName=archiveName,
                                            outputDir=localOutputDir,
                                            subjectId=subjectId,
                                            sessionId=sessionId)
            return localOutputDir

        def cleanup2(subjectId: str = None, sessionIds: Set[str] = None):
            # Arg check / Edge case.
            if subjectId is None and sessionIds is not None:
                err = 'dataset session cleanup requires subject id.'
                raise Exception(err)
            # Cleanup the whole dataset if subject is not defined.
            elif subjectId is None:
                localOutputDir = f'{workerLocalDir}/dataset'
                remove_dir(dirPath=localOutputDir)
            # Cleanup by subject if session is not defined.
            elif sessionIds is None:
                localOutputDir = f'{workerLocalDir}/dataset-{subjectId}'
                remove_dir(dirPath=localOutputDir)
            # Cleanup by session if both subject and session are defined.
            else:
                localOutputDir = f'{workerLocalDir}/dataset-{subjectId}-{".".join(sessionIds)}'
                remove_dir(dirPath=localOutputDir)
            pass

        fetch_dataset = fetch_dataset2
        fetch_dataset.cleanup = cleanup2
    
    # Setup executable retrieval method (either direct or copy).
    # @note
    # We have to override this process although this is already coded within
    # the TaskFactory in order to change the singularity path in case we decide
    # to copy it on computational node, and thus don't have the final path
    # until the task has started and we've copied it.
    def fetch_executable(taskConfig: TaskConfig):
        if vmEngine is VMEngine.NONE:
            return taskConfig.raw_executable
        elif vmEngine is VMEngine.DOCKER \
             and taskConfig.docker_image is not None:
            return taskConfig.docker_image
        elif vmEngine is VMEngine.SINGULARITY and \
             taskConfig.singularity_image is not None and \
             not isPipelineDistributed:
            return taskConfig.singularity_image
        elif vmEngine is VMEngine.SINGULARITY and \
             taskConfig.singularity_image is not None and \
             isPipelineDistributed:
            # Copy singularity image file to the local folder.
            origImagePath = taskConfig.singularity_image
            imageFilename = os.path.basename(origImagePath)
            destImagePath = f'{workerLocalDir}/{imageFilename}'

            # @warning
            # Can't recursively fetch/copy 'singularity' executable for
            # COPY_FILE task.
            copy_file(sourcePath=origImagePath, destPath=destImagePath)

            # Return the new path.
            return destImagePath
        else:
            # Return raw executable, for tasks that don't have contenerized
            # image.
            return taskConfig.raw_executable
    def fetch_executable_cleanup(taskConfig: TaskConfig):
        if vmEngine is VMEngine.SINGULARITY and \
             taskConfig.singularity_image is not None and \
             isPipelineDistributed:
            origImagePath = taskConfig.singularity_image
            imageFilename = os.path.basename(origImagePath)
            tmpImagePath = f'{workerLocalDir}/{imageFilename}'
            remove_file(dirPath=tmpImagePath)
    fetch_executable.cleanup = fetch_executable_cleanup
        
    
    # - BidsValidator.
    # @todo allow per subject bids validation when dataset > available disk
    # space.
    if enableBidsValidator and granularity is Granularity.DATASET:
        didSucceed = scheduler.runTask(
            'validate_bids',
            lambda: bids_validator(
                fetch_executable(BIDS_VALIDATOR),
                datasetDir=fetch_dataset(),
                logFile=f'{outputDir}/log/validate-bids.txt'
            ),
            lambda didSucceed: (
                fetch_executable.cleanup(BIDS_VALIDATOR),
                fetch_dataset.cleanup()
            )
        )
        if not didSucceed:
            sys.exit(1)

    # MRIQC: qc by subjects.
    subjectIds = dataset.getSubjectIds()
    if enableMRIQC and granularity is not Granularity.SESSION:
        successfulSubjectIds, failedSubjectIds = scheduler.batchTask(
            'mriqc_subj',
            lambda subjectId: mriqc_subject(
                fetch_executable(MRIQC_SUBJECT),
                datasetDir=fetch_dataset(subjectId),
                workDir=f'{workDir}/mriqc/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives/mriqc',
                logFile=f'{outputDir}/log/mriqc/sub-{subjectId}.txt',
                nproc=nproc,
                memGb=memGb,
                subjectId=subjectId
            ),
            lambda didSucceed, subjectId: (
                fetch_executable.cleanup(MRIQC_SUBJECT),
                fetch_dataset.cleanup(subjectId),
                didSucceed and remove_dir(f'{workDir}/mriqc/sub-{subjectId}')
            ),
            # lambda subjectId: fetch_dataset.cleanup(subjectId=subjectId),
            subjectIds
        )
        if len(successfulSubjectIds) == 0:
            sys.exit(2)

        # Limit next step's subject ids to the one that succeeded MRIQC.
        subjectIds = successfulSubjectIds

    # MRIQC: group qc.
    if enableMRIQC and granularity is Granularity.DATASET:
        didSucceed = scheduler.runTask(
            'mriqc_group',
            lambda: mriqc_group(
                fetch_executable(MRIQC_GROUP),
                datasetDir=fetch_dataset(),
                workDir=f'{workDir}/mriqc/group',
                outputDir=f'{outputDir}/derivatives/mriqc',
                logFile=f'{outputDir}/log/mriqc/group.txt',
                nproc=nproc,
                memGb=memGb,
            ),
            lambda didSucceed: (
                fetch_executable.cleanup(MRIQC_GROUP),
                fetch_dataset.cleanup(),
                didSucceed and remove_dir(f'{workDir}/mriqc/group')
            )
        )
        if not didSucceed:
            sys.exit(3)

    # SMRiPrep: anat by subjects (only when granularity is session, otherwise
    # we use fmriprep instead of smriprep) [case A].
    if enableSMRiPrep and granularity is Granularity.SESSION:
        successfulSubjectIds, failedSubjectIds = scheduler.batchTask(
            'smriprep_anat',
            lambda subjectId: smriprep_subject(
                fetch_executable(SMRIPREP_SUBJECT),
                datasetDir=fetch_dataset(
                    subjectId,
                    # Extract only the sessions containing anats.
                    # Limit to max two sessions in case there is T1 in every
                    # sessions (fmriprep will limit to one or two anat anyway).
                    # cf. https://fmriprep.org/en/0.6.3/workflows.html#longitudinal-processing
                    dataset.getAnatSessionIdsBySubjectId(subjectId)[:2]
                ),
                workDir=f'{workDir}/smriprep/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives',  # /smriprep will be add by the cmd.
                logFile=f'{outputDir}/log/smriprep/sub-{subjectId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                nproc=nproc,
                memGb=memGb,
                # templateflowDataDir='./templateflow',
                subjectId=subjectId
            ),
            lambda didSucceed, subjectId: (
                fetch_executable.cleanup(SMRIPREP_SUBJECT),
                fetch_dataset.cleanup(subjectId),
                didSucceed and remove_dir(f'{workDir}/smriprep/sub-{subjectId}')
            ),
            subjectIds
        )
        if len(successfulSubjectIds) == 0:
            sys.exit(4)

        # Limit next step's subject ids to the one that succeeded MRIQC.
        subjectIds = successfulSubjectIds

    # List all sessions as (subj, ses) pairs.
    sessionIds = [
        (subjectId, sessionId)
        for subjectId in subjectIds
        for sessionId in dataset.getSessionIdsBySubjectId(subjectId)
    ]
    
    # FMRiPrep: generate sessions' func file filters [case A].
    if enableFMRiPrep and granularity is Granularity.SESSION:
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_filter',
            lambda subjectId, sessionId: fmriprep_session_filter(
                logFile=f'{outputDir}/log/fmriprep/filters/sub-{subjectId}.txt',
                bidsFilterFile=f'{outputDir}/filefilters/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',  # @todo remove func
                sessionId=sessionId
            ),
            lambda didSucceed, subjectId, sessionId: None,
            sessionIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(5)

    # FMRiPrep: func by subjects [case A].
    if enableFMRiPrep and granularity is Granularity.SESSION:
        # @todo copy smriprep's subject derivatives to the compute node etc.
        # @todo copy templateflow data to the compute node etc.
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_func',
            lambda subjectId, sessionId: fmriprep_session(
                fetch_executable(FMRIPREP_SESSION),
                datasetDir=fetch_dataset(subjectId, [sessionId]),
                anatsDerivativesDir=f'{outputDir}/derivatives/smriprep',
                workDir=f'{workDir}/fmriprep/sub-{subjectId}/ses-{sessionId}',
                outputDir=f'{outputDir}/derivatives',  # /fmriprep will be add by the cmd.
                logFile=f'{outputDir}/log/fmriprep/sub-{subjectId}/ses-{sessionId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                templateflowDataDir='./templateflow',
                bidsFilterFile=f'{outputDir}/filefilters/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',  # @todo remove func -- ? why?
                nproc=nproc,
                memMb=memGb*1024,
                subjectId=subjectId,
                sessionId=sessionId,
                # fasttrackFixDir=fasttrackFixDir
            ),
            lambda didSucceed, subjectId, sessionId: (
                fetch_executable.cleanup(FMRIPREP_SESSION),
                fetch_dataset.cleanup(subjectId, [sessionId]),
                didSucceed and remove_dir(
                    f'{workDir}/fmriprep/sub-{subjectId}/ses-{sessionId}')
            ),
            successfulSessionIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(6)

        # Limit next step's subject/session ids to the successful ones.
        sessionIds = successfulSessionIds

    # FMRiPrep: all by subjects [case B].
    if granularity is Granularity.DATASET or granularity is Granularity.SUBJECT:
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_all',
            lambda subjectId: fmriprep_subject(
                fetch_executable(FMRIPREP_SUBJECT),
                datasetDir=fetch_dataset(subjectId),
                workDir=f'{workDir}/fmriprep/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives',  # /fmriprep will be add by the cmd.
                logFile=f'{outputDir}/log/fmriprep/sub-{subjectId}.txt',
                freesurferLicenseFile='./licenses/freesurfer.txt',
                templateflowDataDir='./templateflow',
                nproc=nproc,
                memMb=memGb*1024,
                subjectId=subjectId,
            ),
            lambda didSucceed, subjectId: (
                fetch_executable.cleanup(FMRIPREP_SUBJECT),
                fetch_dataset.cleanup(subjectId),
                didSucceed and remove_dir(
                    f'{workDir}/fmriprep/sub-{subjectId}')
            ),
            subjectIds
        )
        if len(successfulSessionIds) == 0:
            sys.exit(7)
