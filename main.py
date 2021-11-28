import sys  # for python version check + sys.exit
import os  # for cache file delete
import shutil  # for cache dir delete
import threading  # for dask stalled workers periodic cleanup
import time  # for dask stalled workers timestamp comparison
import dask.distributed  # for MT
import dask_jobqueue
import dask_mpi
from typing import Set
from config import COPY_FILE, COPY_DIR, REMOVE_FILE, REMOVE_DIR, \
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
# @warning docker requires manual app modification.
# https://stackoverflow.com/questions/44533319/how-to-assign-more-memory-to-docker-container/44533437#44533437
if __name__ == '__main__':
    # 0. Check python version.
    # @note f-strings require python 3.6, will throw syntax error instead
    # though.
    assert sys.version_info >= (3, 6)

    # Retrieve args
    args = readCLIArgs()
    granularity = args.granularity
    datasetDir = args.datasetPath.rstrip('/')  # strip trailing '/' otherwise os.path.basename will return null and break dar
    outputDir = args.outputDir
    vmEngine = args.vmEngine
    executor = args.executor
    reset = args.reset
    enableBidsValidator = args.enableBidsValidator
    enableMRIQC = args.enableMRIQC
    enableSMRiPrep = args.enableSMRiPrep
    enableFMRiPrep = args.enableFMRiPrep
    enablePybidsCache = args.enablePybidsCache
    fasttrackFixDir = os.path.dirname(os.path.realpath(__file__)) + '/smriprep-fasttrack-fix'
    workerCount = args.workerCount
    nproc = args.workerCpuCount
    memGB = args.workerMemoryGB
    workerWallTime = args.workerWallTime
    workerLocalDir = args.workerLocalDir  # can be None
    workerSharedDir = args.workerSharedDir  # can be None
    isPipelineDistributed = False if workerLocalDir is None else True
    workDir = f'{outputDir}/work/' if workerLocalDir is None else f'{workerLocalDir}/work/'  # @warning can only be used within dask task
    # @todo copy on local node 
    templateflowDataDir = './templateflow'
    templateflowDataDirWithinVM = '/v_templateflowDataDir'  # check _volume_mapping.py file
    print(f'nproc: {nproc}')
    print(f'memGB: {memGB}')
    print('warning: if processes are hanging (D-state uninterruptible sleep) consider increasing ram and CPU.')

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
        # Convert walltime to seconds in order to restart worker adterwards if
        # needed.
        workerWallTimeArray = workerWallTime.split(sep='-')
        workerWallTimeAsSec = 0
        workerWallTimeAsSec += int(workerWallTimeArray[0]) * 24 * 3600 if len(workerWallTimeArray) == 2 else 0
        workerWallTimeArray = workerWallTimeArray[1 if len(workerWallTimeArray) == 2 else 0].split(sep=':')
        workerWallTimeAsSec += int(workerWallTimeArray[0]) * 3600
        workerWallTimeAsSec += int(workerWallTimeArray[1]) * 60
        if len(workerWallTimeArray) == 3:
            workerWallTimeAsSec += int(workerWallTimeArray[2])
        elif len(workerWallTimeArray) != 2:
            raise Exception("unexpected walltime format.")
        workerLifetime = max(3*3600, workerWallTimeAsSec - 3600)
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
            extra=[
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
                '--resources job=1',
                # Restart workers until they're not needed anymore (most do
                # timeout within one day on our compute canada / beluga system
                # for some reason).
                # cf. https://github.com/dask/dask-jobqueue/issues/122#issuecomment-626333697
                f'--lifetime {workerLifetime}s',
                '--lifetime-stagger 5m',
                '--lifetime-restart'
            ],
            project='def-porban',
            # Disable worker kill (60 seconds by default).
            # Edit: seems actually disabled by default.
            # Edit: killing death worker is mandatory, because living-death
            # workers prevent task stealing, thus prevent their queued task
            # from being ever processed.
            death_timeout=60,
            memory=f'{memGB} GB',
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
            env_extra=[
                # @warning this requires to download templateflow files.
                # @todo `module load singularity` out !
                f'export SINGULARITYENV_TEMPLATEFLOW_HOME="{templateflowDataDirWithinVM}"',
                'module load singularity'
            ],
            # Control each worker by an intermediate nanny / monitoring process.
            # @note found erratic claim disabling nanny process may solve
            # worker timeout issues we have. cf. https://github.com/dask/dask-jobqueue/issues/20
            # I tested and this is not the case + .
            nanny=True
        )
        # Reduces scripts memory allocation from 3 gigs, just allowing a
        # buffer to prevent memory overhead (singularity+fmriprep stopping
        # consuming memory) or worker error such as:
        # `slurmstepd: error: Detected 593060 oom-kill event(s) in step
        # 18446833.batch cgroup. Some of your processes may have been killed by
        # the cgroup out-of-memory handler.``
        print('memGB has been reduced by 3 GBs, in order to leave a buffer.')
        memGB = memGB-3

        # At least one worker required in order to be able to fetch dataset
        # information.
        cluster.scale(1)

        # Setup scheduler.
        client = dask.distributed.Client(cluster)
        scheduler = DaskScheduler(
            f'{outputDir}/.task_cache.csv', client)

        # Periodically check worker are still alive, and restart them if not.
        # @note death_timeout arg doesn't work, as it restart worker from
        # within the worker, if it can't access the scheduler. Here we kill the
        # worker from the scheduler if we don't get any heartbeat. This prevents
        # stalled worker from accumulating the job requests while preventing the
        # other workers to steal the jobs.
        # @warning supposedly thread friendly, shouldn't cause issue.
        def loopThroughStalledWorkersAndKill():
            # Recursively execute in 2 minutes.
            # @warning #retire_workers might timeout, better set this
            # function's recursion at the beggining of its definition rather
            # than at the end.
            checkupTiming = 120  # in sec.
            threading.Timer(checkupTiming, loopThroughStalledWorkersAndKill).start()

            # Retrieve stalled workers.
            deathTimeout = 60  # in sec
            stalledWorkerAddresses = []
            workers = client.scheduler_info()['workers']
            for key, worker in workers.items():
                workerAddress = key
                # @note timestamp as real number in seconds.
                lastSeen = worker['last_seen']
                # @note most heartbeat are available within a second.
                if time.time() - lastSeen > deathTimeout:
                    stalledWorkerAddresses.append(workerAddress)

            # Log stalled workers
            if len(stalledWorkerAddresses) > 0:
                print('killing stalled workers:')
                print(stalledWorkerAddresses)

            # Kill the stalled workers.
            # @note remove: bool - Whether or not to remove the worker metadata
            # immediately or else wait for the worker to contact us.
            # cf. https://distributed.dask.org/en/latest/_modules/distributed/scheduler.html#Scheduler.retire_workers
            # We set to false as we have had instances where worker is not
            # effectively killed, and still visible within the dask dashboard
            # while #scheduler_info metadata are no longer there, so no new
            # attempt are done.
            # Even though remove is set to false, remove will still be called
            # and awaited when close workers is called at the end.
            if len(stalledWorkerAddresses) > 0:
                client.retire_workers(workers=stalledWorkerAddresses,
                                      remove=False, close_workers=True)

            # # Kill worker through CLI slurm instead of dask.
            # workerIp = '' # @todo parse IP from workerAddress
            # result = execute_cmd(f'''nslookup {workerIp} | awk '{{ print $4 }}' | sed -E 's/^([^.]+).*/\1/' ''')
            # workerNode = result.stdout
            # execute_cmd(f'''sq | grep {workerNode} | awk '{{ print $1 }}' | xargs scancel''')
        loopThroughStalledWorkersAndKill()
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
    bids_validator = TaskFactory.generate(vmEngine, BIDS_VALIDATOR)
    mriqc_subject = TaskFactory.generate(vmEngine, MRIQC_SUBJECT)
    mriqc_group = TaskFactory.generate(vmEngine, MRIQC_GROUP)
    smriprep_subject = TaskFactory.generate(vmEngine, SMRIPREP_SUBJECT)
    fmriprep_subject = TaskFactory.generate(vmEngine, FMRIPREP_SUBJECT)
    fmriprep_session_filter = TaskFactory.generate(VMEngine.NONE, FMRIPREP_SESSION_FILTER)  # no singularity/docker image available for printf.
    fmriprep_session = TaskFactory.generate(vmEngine, FMRIPREP_SESSION)
    # @note distributed pipeline only / no singularity/docker image available.
    copy_file = TaskFactory.generate(VMEngine.NONE, COPY_FILE)
    copy_dir = TaskFactory.generate(VMEngine.NONE, COPY_DIR)
    remove_file = TaskFactory.generate(VMEngine.NONE, REMOVE_FILE)
    remove_dir = TaskFactory.generate(VMEngine.NONE, REMOVE_DIR)
    archive_dataset = TaskFactory.generate(VMEngine.NONE, ARCHIVE_DATASET)
    extract_dataset = TaskFactory.generate(VMEngine.NONE, EXTRACT_DATASET)
    extract_dataset_subject = TaskFactory.generate(VMEngine.NONE, EXTRACT_DATASET_SUBJECT)
    extract_dataset_session = TaskFactory.generate(VMEngine.NONE, EXTRACT_DATASET_SESSION)
    list_archive_sessions = TaskFactory.generate(VMEngine.NONE, LIST_ARCHIVE_SESSIONS)

    # Archive dataset for faster IO if pipeline is distributed (+ prevent files
    # from being distributed across multiple LUSTRE slaves and fragmented,
    # which I suspect to cause random bugs + cope with Compute Canada file 
    # cap).
    if isPipelineDistributed:
        archiveDir = f'{outputDir}/archives'
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
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
            sys.exit(-1)

    # Analyse dataset in order to be able to orchestrate parallel processing
    # across subjects / sessions.
    dataset = None
    if not isPipelineDistributed:
        pybidsCache = f'{outputDir}/.bids_cache' if enablePybidsCache else None
        dataset = LocalDataset(datasetDir, pybidsCache)
    # Wrap inside distributed pipeline, in order to prevent issues due to
    # distributed file system (missing subject ids, etc). These issues are
    # speculated, although they seems to have appeared randomly until I stopped
    # relying on LUSTRE.
    elif isPipelineDistributed:
        # Retrieve dataset info on local node.
        archiveDir = f'{outputDir}/archives'
        archiveName = os.path.basename(datasetDir)
        # @warning @todo doesn't work on dataset > 300GO as they can't be
        # extracted from local node...
        # dataset = DistributedDataset.loadFromArchiveWithPyBids(client,
        #                                                        extract_dataset,
        #                                                        archiveDir,
        #                                                        archiveName,
        #                                                        workerLocalDir)

        # @note this works from scheduling node (no dataset copy/extraction).
        dataset = DistributedDataset.loadFromArchiveWithDar(client,
                                                            list_archive_sessions,
                                                            archiveDir,
                                                            archiveName)

        # Exit in case of dataset info extraction failure.
        if dataset is None:
            print('error: couldn\'t extract dataset from archive to retrieve info')
            sys.exit(-2)

    # Set workerCount to number of subject if == -1 and scale worker count
    # accordingly slurm cluster scheduler is used.
    effectiveWorkerCount = workerCount
    if workerCount == -1:
        effectiveWorkerCount = len(dataset.getSubjectIds())
    # Scale slurm worker count.
    # Do not launch more worker than what we can use at the moment (thus max
    # one per subject). This might be upscaled later on.
    # @note `adapt` instead of scale should allow to automatically restart dead
    # workers. cf. https://stackoverflow.com/a/61295019/939741
    if executor is Executor.SLURM:
        cluster.adapt(minimum_jobs=1, maximum_jobs=effectiveWorkerCount)

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
            # Check sessions are not empty, has pipeline has not been
            # developed to use session granularity when bids dataset
            # doesn't contain session.
            elif len(sessionIds) == 0:
                err="subject granularity shall be used when there is no session."
                raise Exception(err)
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
            # Check sessions are not empty, has pipeline has not been
            # developed to use session granularity when bids dataset
            # doesn't contain session.
            elif len(sessionIds) == 0:
                err="subject granularity shall be used when there is no session."
                raise Exception(err)
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
            remove_file(filePath=tmpImagePath)
    fetch_executable.cleanup = fetch_executable_cleanup

    # Setup freesurfer license retrieval method.
    def fetch_freesurfer_license(suffix: str = ''):
        if not isPipelineDistributed:
            return './licenses/freesurfer.txt'
        else:
            origFilePath = './licenses/freesurfer.txt'
            origFileName = os.path.basename(origFilePath)
            destFilePath = f'{workerLocalDir}/{origFileName}{suffix}'
            copy_file(sourcePath=origFilePath, destPath=destFilePath)
            return destFilePath
    def fetch_freesurfer_license_cleanup(suffix: str = ''):
        if isPipelineDistributed:
            origFilePath = './licenses/freesurfer.txt'
            origFileName = os.path.basename(origFilePath)
            destFilePath = f'{workerLocalDir}/{origFileName}{suffix}'
            remove_file(filePath=destFilePath)
    fetch_freesurfer_license.cleanup = fetch_freesurfer_license_cleanup

    # Setup fastrack fix source code retrival method.
    # @waning we suspect this might be the cause of state D (uninteruptible
    # sleep) of fmriprep child processes such as mcflirt, which also cause
    # `htop` and `ps aux` to be failing.
    def fetch_fastrack_fix_dir(suffix: str = ''):
        if not isPipelineDistributed:
            return fasttrackFixDir
        else:
            origDirPath = fasttrackFixDir
            origDirName = os.path.basename(origDirPath)
            destDirPath = f'{workerLocalDir}/{origDirName}{suffix}'
            copy_dir(sourcePath=origDirPath, destPath=destDirPath)
            return destDirPath
    def fetch_fastrack_fix_dir_cleanup(suffix: str = ''):
        origDirPath = fasttrackFixDir
        origDirName = os.path.basename(origDirPath)
        destDirPath = f'{workerLocalDir}/{origDirName}{suffix}'
        remove_dir(dirPath=destDirPath)
    fetch_fastrack_fix_dir.cleanup = fetch_fastrack_fix_dir_cleanup

    # Setup T1 template retrieval method.
    # @warning ensure output paths are all different if you run pipeline as
    # distributed.
    def fetch_mri_templates(suffix: str = ''):
        if not isPipelineDistributed:
            return templateflowDataDir
        else:
            origDirPath = templateflowDataDir
            origDirName = os.path.basename(origDirPath)
            destDirPath = f'{workerLocalDir}/{origDirName}{suffix}'
            copy_dir(sourcePath=origDirPath, destPath=destDirPath)
            return destDirPath
    def fetch_mri_templates_cleanup(suffix: str = ''):
        if isPipelineDistributed:
            origDirPath = templateflowDataDir
            origDirName = os.path.basename(origDirPath)
            destDirPath = f'{workerLocalDir}/{origDirName}{suffix}'
            remove_dir(dirPath=destDirPath)
    fetch_mri_templates.cleanup = fetch_mri_templates_cleanup

    # BidsValidator.
    # @todo allow per subject bids validation when dataset > available disk
    # space.
    if enableBidsValidator and granularity is not Granularity.DATASET:
        print("warning: bids validation input and output streams will occur through shared filesystem (lustre?).")
        didSucceed = scheduler.runTask(
            'validate_bids',
            lambda: bids_validator(
                fetch_executable(BIDS_VALIDATOR),
                datasetDir=datasetDir,
                logFile=f'{outputDir}/log/validate-bids.txt'
            ),
            lambda didSucceed: (
                fetch_executable.cleanup(BIDS_VALIDATOR)
            )
        )
        if not didSucceed:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
            sys.exit(1)
    elif enableBidsValidator and granularity is Granularity.DATASET:
        print("warning: bids validation output stream will be sustained over shared filesystem (lustre?).")
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
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
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
                templateflowDataDir=fetch_mri_templates(suffix=f'_mriqc_subj_{subjectId}'),
                logFile=f'{outputDir}/log/mriqc/sub-{subjectId}.txt',
                nproc=nproc,
                memGB=memGB,
                subjectId=subjectId
            ),
            lambda didSucceed, subjectId: (
                fetch_executable.cleanup(MRIQC_SUBJECT),
                fetch_dataset.cleanup(subjectId),
                fetch_mri_templates.cleanup(suffix=f'_mriqc_subj_{subjectId}'),
                didSucceed and remove_dir(dirPath=f'{workDir}/mriqc/sub-{subjectId}')
            ),
            # lambda subjectId: fetch_dataset.cleanup(subjectId=subjectId),
            subjectIds
        )
        if len(successfulSubjectIds) == 0:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
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
                templateflowDataDir=templateflowDataDir,  # probably not used
                logFile=f'{outputDir}/log/mriqc/group.txt',
                nproc=nproc,
                memGB=memGB,
            ),
            lambda didSucceed: (
                fetch_executable.cleanup(MRIQC_GROUP),
                fetch_dataset.cleanup(),
                didSucceed and remove_dir(dirPath=f'{workDir}/mriqc/group')
            )
        )
        if not didSucceed:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
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
                    dataset.getAnatSessionIdsBySubjectId(subjectId)[:2]  # @todo @warning dev func for non-dar dataset
                ),
                workDir=f'{workDir}/smriprep/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives',  # /smriprep will be add by the cmd.
                logFile=f'{workDir}/log/smriprep/sub-{subjectId}.txt',
                freesurferLicenseFile=fetch_freesurfer_license(suffix=f'_smriprep_anat_{subjectId}'),
                templateflowDataDir=fetch_mri_templates(suffix=f'_smriprep_anat_{subjectId}'),
                nproc=nproc,
                memGB=memGB,
                subjectId=subjectId
            ),
            lambda didSucceed, subjectId: (
                fetch_executable.cleanup(SMRIPREP_SUBJECT),
                fetch_dataset.cleanup(
                    subjectId,
                    dataset.getAnatSessionIdsBySubjectId(subjectId)[:2]
                ),
                isPipelineDistributed and copy_file(
                    sourcePath=f'{workDir}/log/smriprep/sub-{subjectId}.txt',
                    destPath=f'{outputDir}/log/smriprep/sub-{subjectId}.txt'),
                fetch_freesurfer_license.cleanup(suffix=f'_smriprep_anat_{subjectId}'),
                fetch_mri_templates.cleanup(suffix=f'_smriprep_anat_{subjectId}'),
                didSucceed and remove_dir(
                    dirPath=f'{workDir}/smriprep/sub-{subjectId}')
            ),
            subjectIds
        )
        if len(successfulSubjectIds) == 0:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
            sys.exit(4)

        # Limit next step's subject ids to the one that succeeded MRIQC.
        subjectIds = successfulSubjectIds

    # Setup default smriprep derivatives retrieval method (either path, or
    # archive extraction).
    fetch_smriprep_derivatives = None
    if not isPipelineDistributed or granularity is not Granularity.SESSION:
        def fetch_smriprep_derivatives1(subjectId: str = None, sessionIds: Set[str] = None):
            return f'{outputDir}/derivatives/smriprep'
        def fetch_smriprep_derivatives_cleanup1(subjectId: str = None, sessionIds: Set[str] = None):
            # Nothing to cleanup.
            pass
        
        fetch_smriprep_derivatives = fetch_smriprep_derivatives1
        fetch_smriprep_derivatives.cleanup = fetch_smriprep_derivatives_cleanup1
    # Archive preprocessed anat for faster IO if pipeline is distributed (+ 
    # prevent files from being distributed across multiple LUSTRE slaves and
    # fragmented, which I suspect to cause random bugs + cope with Compute 
    # Canada file cap).
    # Setup default smriprep derivatives retrieval method for archive
    # extraction.
    elif isPipelineDistributed and granularity is Granularity.SESSION:
        archiveDir = f'{outputDir}/archives/smriprep'
        archiveName = f'{os.path.basename(datasetDir)}.smriprep'
        derivativesDir=f'{outputDir}/derivatives/smriprep'

        didSucceed = scheduler.runTask(
            'archive_smriprep',
            lambda: archive_dataset(
                datasetDir=derivativesDir,
                archiveDir=archiveDir,
                archiveName=archiveName,
                logFile=f'{outputDir}/log/archive-smriprep.txt'
            ),
            lambda didSucceed: None
        )
        if not didSucceed:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
            sys.exit(-1)

        def fetch_smriprep_derivatives2(subjectId: str = None, sessionIds: Set[str] = None):
            archiveDir = f'{outputDir}/archives/smriprep'
            archiveName = f'{os.path.basename(datasetDir)}.smriprep'
            localOutputDir=None  # conditionally defined.

            # Arg check / Edge case.
            if subjectId is None and sessionIds is not None:
                err = 'dataset session extraction requires subject id.'
                raise Exception(err)
            # Extract the whole dataset if subject is not defined.
            elif subjectId is None:
                localOutputDir = f'{workerLocalDir}/smriprep'
                extract_dataset(archiveDir=archiveDir,
                                archiveName=archiveName, outputDir=localOutputDir)
            # Extract by subject if session is not defined.
            elif sessionIds is None:
                localOutputDir=f'{workerLocalDir}/smriprep-{subjectId}'
                extract_dataset_subject(archiveDir=archiveDir,
                                        archiveName=archiveName,
                                        outputDir=localOutputDir,
                                        subjectId=subjectId)
            # Check sessions are not empty, has pipeline has not been
            # developed to use session granularity when bids dataset
            # doesn't contain session.
            elif len(sessionIds) == 0:
                err="subject granularity shall be used when there is no session."
                raise Exception(err)
            # Extract by session if both subject and session are defined.
            # @note this provides the global subject's anat/ folder as well!
            else:
                localOutputDir=f'{workerLocalDir}/smriprep-{subjectId}-{".".join(sessionIds)}'
                for sessionId in sessionIds:
                    extract_dataset_session(archiveDir=archiveDir,
                                            archiveName=archiveName,
                                            outputDir=localOutputDir,
                                            subjectId=subjectId,
                                            sessionId=sessionId)
            return localOutputDir

        def fetch_smriprep_derivatives_cleanup2(subjectId: str = None, sessionIds: Set[str] = None):
            # Arg check / Edge case.
            if subjectId is None and sessionIds is not None:
                err = 'smriprep session cleanup requires subject id.'
                raise Exception(err)
            # Cleanup the whole dataset if subject is not defined.
            elif subjectId is None:
                localOutputDir = f'{workerLocalDir}/smriprep'
                remove_dir(dirPath=localOutputDir)
            # Cleanup by subject if session is not defined.
            elif sessionIds is None:
                localOutputDir = f'{workerLocalDir}/smriprep-{subjectId}'
                remove_dir(dirPath=localOutputDir)
            # Check sessions are not empty, has pipeline has not been
            # developed to use session granularity when bids dataset
            # doesn't contain session.
            elif len(sessionIds) == 0:
                err="subject granularity shall be used when there is no session."
                raise Exception(err)
            # Cleanup by session if both subject and session are defined.
            else:
                localOutputDir = f'{workerLocalDir}/smriprep-{subjectId}-{".".join(sessionIds)}'
                remove_dir(dirPath=localOutputDir)

        fetch_smriprep_derivatives = fetch_smriprep_derivatives2
        fetch_smriprep_derivatives.cleanup = fetch_smriprep_derivatives_cleanup2

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
                # @warning logFile would break the output, as the logs are the
                # output!
                # logFile=f'{outputDir}/log/fmriprep/filters/sub-{subjectId}-{sessionId}.txt',
                bidsFilterFile=f'{outputDir}/filefilters/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',  # @todo remove func
                sessionId=sessionId
            ),
            lambda didSucceed, subjectId, sessionId: None,
            sessionIds
        )
        if len(successfulSessionIds) == 0:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
            sys.exit(5)

    # FMRiPrep: func by subjects [case A].
    if enableFMRiPrep and granularity is Granularity.SESSION:
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_func',
            lambda subjectId, sessionId: fmriprep_session(
                fetch_executable(FMRIPREP_SESSION),
                datasetDir=fetch_dataset(subjectId, [sessionId]),
                # @note fetch_smriprep_derivatives provides the global
                # subject's anat/, folder as well! In case the subject has only
                # acquired a single T1 , fmriprep doesn't generate a global
                # anat/ folder, as there is no merged template being processed.
                # Thus we have to provide the only anat session available
                # instead, which is not necessarily the same session as the one
                # being currently processing for func.
                anatsDerivativesDir=fetch_smriprep_derivatives(subjectId,
                    dataset.getAnatSessionIdsBySubjectId(subjectId)[:1]),
                workDir=f'{workDir}/fmriprep/sub-{subjectId}/ses-{sessionId}',
                outputDir=f'{outputDir}/derivatives',  # /fmriprep will be add by the cmd.
                # @warning long-running file descriptor, better not handle on network file system.
                logFile=f'{workDir}/log/fmriprep/sub-{subjectId}/ses-{sessionId}.txt',
                freesurferLicenseFile=fetch_freesurfer_license(suffix=f'_fmriprep_func_{subjectId}_{sessionId}'),
                templateflowDataDir=fetch_mri_templates(suffix=f'_fmriprep_func_{subjectId}_{sessionId}'),
                bidsFilterFile=f'{outputDir}/filefilters/fmriprep/func/sub-{subjectId}/ses-{sessionId}/filter.json',  # @todo remove func -- ? why?
                nproc=nproc,
                memMB=memGB*1000, # not 1024 / GiB
                subjectId=subjectId,
                sessionId=sessionId,
                fasttrackFixDir=fetch_fastrack_fix_dir(suffix=f'_fmriprep_func_{subjectId}_{sessionId}')
            ),
            lambda didSucceed, subjectId, sessionId: (
                fetch_executable.cleanup(FMRIPREP_SESSION),
                fetch_dataset.cleanup(subjectId, [sessionId]),
                fetch_smriprep_derivatives.cleanup(subjectId,
                    dataset.getAnatSessionIdsBySubjectId(subjectId)[:1]),
                isPipelineDistributed and copy_file(
                    sourcePath=f'{workDir}/log/fmriprep/sub-{subjectId}/ses-{sessionId}.txt',
                    destPath=f'{outputDir}/log/fmriprep/sub-{subjectId}/ses-{sessionId}.txt'),
                # remove_file(filePath=f'{workDir}/log/fmriprep/sub-{subjectId}/ses-{sessionId}.txt'),  # commented, as might be done parallely to copy_file
                fetch_freesurfer_license.cleanup(suffix=f'_fmriprep_func_{subjectId}_{sessionId}'),
                fetch_mri_templates.cleanup(suffix=f'_fmriprep_func_{subjectId}_{sessionId}'),
                fetch_fastrack_fix_dir.cleanup(suffix=f'_fmriprep_func_{subjectId}_{sessionId}'),
                didSucceed and remove_dir(
                    dirPath=f'{workDir}/fmriprep/sub-{subjectId}/ses-{sessionId}')
            ),
            successfulSessionIds
        )
        if len(successfulSessionIds) == 0:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
            sys.exit(6)

        # Limit next step's subject/session ids to the successful ones.
        sessionIds = successfulSessionIds

    # FMRiPrep: all by subjects [case B].
    if enableFMRiPrep and granularity is Granularity.DATASET:
        print("warning: dataset granularity for fmriprep is not implemented. use subject granularity instead.")
    elif enableFMRiPrep and granularity is Granularity.SUBJECT:
        successfulSessionIds, failedSessionIds = scheduler.batchTask(
            'fmriprep_all',
            lambda subjectId: fmriprep_subject(
                fetch_executable(FMRIPREP_SUBJECT),
                datasetDir=fetch_dataset(subjectId),
                workDir=f'{workDir}/fmriprep/sub-{subjectId}',
                outputDir=f'{outputDir}/derivatives',  # /fmriprep will be add by the cmd.
                logFile=f'{workDir}/log/fmriprep/sub-{subjectId}.txt',
                freesurferLicenseFile=fetch_freesurfer_license(suffix=f'_fmriprep_all_{subjectId}'),
                templateflowDataDir=fetch_mri_templates(suffix=f'_fmriprep_all_{subjectId}'),
                nproc=nproc,
                memMB=memGB*1000, # not 1024 / GiB
                subjectId=subjectId
            ),
            lambda didSucceed, subjectId: (
                fetch_executable.cleanup(FMRIPREP_SUBJECT),
                fetch_dataset.cleanup(subjectId),
                isPipelineDistributed and copy_file(
                    sourcePath=f'{workDir}/log/fmriprep/sub-{subjectId}.txt',
                    destPath=f'{outputDir}/log/fmriprep/sub-{subjectId}.txt'),
                fetch_freesurfer_license.cleanup(suffix=f'_fmriprep_all_{subjectId}'),
                fetch_mri_templates.cleanup(suffix=f'_fmriprep_all_{subjectId}'),
                didSucceed and remove_dir(
                    dirPath=f'{workDir}/fmriprep/sub-{subjectId}')
            ),
            subjectIds
        )
        if len(successfulSessionIds) == 0:
            # Close dask slurm scheduler + workers.
            if executor is Executor.SLURM:
                client.shutdown()
            sys.exit(7)

    # Close dask slurm scheduler + workers.
    if executor is Executor.SLURM:
        client.shutdown()