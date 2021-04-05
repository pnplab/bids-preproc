from typing import Callable, Dict
import math
from cli import VMEngine
from src.path import PathPlaceHolder, InputFile, InputDir, OutputFile, \
                    OutputDir
from src.executor import TaskFactory, execute, TaskResult
from src.dataset import DistributedDataset
from pipeline import Pipeline

class DistributedPipeline(Pipeline):
    _archiveDir: str
    _archiveName: str
    _workerLocalDir: str
    _workerSharedDir: str
    _enableSingularityContainerLocalCopy: bool

    # @warning docker requires manual app modification.
    # https://stackoverflow.com/questions/44533319/how-to-assign-more-memory-to-docker-container/44533437#44533437
    def __init__(self, archiveDir: str, archiveName: str,
                 workerLocalDir: str, workerSharedDir: str,
                 bidsValidator: Dict[str, str], mriqc: Dict[str, str],
                 smriprep: Dict[str, str], fmriprep: Dict[str, str],
                 printf: Dict[str, str]) -> None:

        self._archiveDir = archiveDir
        self._archiveName = archiveName
        self._workerLocalDir = workerLocalDir
        self._workerSharedDir = workerSharedDir
        self._enableSingularityContainerLocalCopy = True

        super().__init__(bidsValidator, mriqc, smriprep, fmriprep, printf)

    # Wrap/overload task generation to copy singularity executable on local
    # hard drive when singularity is used and option activated.
    def _createTaskForCmd(self, vmType: VMEngine, executable: str,
                          cmdTemplate: str,
                          **argsDecorators: Dict[str, PathPlaceHolder]):
        # Replace singularity executable path to local hd, one. New executable
        # is to be copied within overloaded task method later one.
        executableOrigin = None
        isExecutableCopiedLocally = self._enableSingularityContainerLocalCopy \
                                    and vmType == VMEngine.SINGULARITY
        if isExecutableCopiedLocally:
            executableOrigin = executable
            executable = f'{self._workerLocalDir}/executable.simg'

        # Generate task.
        task = TaskFactory.generate(vmType, executable, cmdTemplate,
            **argsDecorators)

        # Wrap task with copy of executable to local host + cleanup.
        if isExecutableCopiedLocally:
            taskOriginFn = task
            copyFileFn = self.copyFile
            cleanupFn = self.cleanup
            def taskWrappedWithLocalExecutableCopy(*args, **kargs):
                # @TODO USE LOCAL COPY + CYCLING LOG. Remove logging for now.
                if 'logFile' in kargs:
                    kargs['logFile'] = f'{self._workerLocalDir}/logs/{kargs["logFile"]}'
                
                # Copy executable locally.
                result = copyFileFn(executableOrigin, executable)
                if not result.didSucceed:
                    print(f'error: failed to copy executable from {executableOrigin} to {executable}.')
                    return result
                
                # Execute task.
                taskResult = taskOriginFn(*args, **kargs)

                # Clean up previously copied executable.
                # cleanupFn(executable)  # @todo cleanup with InputFile instead of InputDir

                # Return task result
                return taskResult
            task = taskWrappedWithLocalExecutableCopy

        # Return newly generated task.
        return task

    def archive(self, inputDir, outputFile):
        inputDir = InputDir(inputDir)
        outputFile = OutputFile(outputFile)

        cmd = f'dar -R "{inputDir}" -v -s 1024M -w -c "{outputFile}"'
        # @todo log..

        return execute_strcmd(cmd)

    def archiveDataset(self, datasetDir):
        datasetDir = InputDir(datasetDir)
        archiveDir = OutputDir(self._archiveDir)
        archiveName = self._archiveName

        cmd = f'dar -R "{datasetDir}" -v -s 1024M -w -c "{archiveDir}/{archiveName}"'  # -g {archiveName} not req. as all by default

        # @todo log..

        return execute_strcmd(cmd)

    def prefetchDatasetInfo(self):
        # Decompress datasetDir on local cluster.
        archiveDir = self._archiveDir
        archiveName = self._archiveName
        workDir = f'{self._workerLocalDir}/prefetch'
        extractedDatasetDir = f'{workDir}/dataset'
        result = self.extractDataset(archiveDir, archiveName, extractedDatasetDir)

        # Stop here if extraction failed.
        if not result.didSucceed:
            return None

        # Prefetch dataset.
        dataset = DistributedDataset(extractedDatasetDir, None)
        dataset.prefetch()

        # Cleanup.
        self.cleanup(workDir)

        return dataset

    def extractDataset(self, archiveDir, archiveName, outputDir):
        archiveDir = InputDir(archiveDir)
        outputDir = OutputDir(outputDir)
        
        cmd = f'dar -R "{outputDir}" -O -w -x "{archiveDir}/{archiveName}" -v -am'
        return execute_strcmd(cmd)

    def extractDatasetForSubjectId(self, archiveDir, archiveName, outputDir, subjectId):
        archiveDir = InputDir(archiveDir)
        outputDir = OutputDir(outputDir)

        cmd = f'dar -R "{outputDir}" -O -w -x "{archiveDir}/{archiveName}" -v -am -P "sub-*" -g "sub-{subjectId}"'
        return execute_strcmd(cmd)

    def cleanup(self, dir):
        dir = InputDir(dir)
        # @todo cleanup! We do not at the moment to make debugging easier
        # cmd = f'rm -rf "{dir}"'
        # return execute_strcmd(cmd)
        return TaskResult(
            didSucceed=True,
            returnCode=0,
            stdout=None, # result.stdout.text,
            stderr=None  # result.stderr.text
        )

    def copyDir(self, sourceDir, destDir):
        sourceDir = InputDir(sourceDir)
        destDir = OutputDir(destDir)
        cmd = f'rsync -avz --no-g --no-p {sourceDir} {destDir}'
        return execute_strcmd(cmd)

    def copyFile(self, sourceFile, destFile):
        sourceFile = InputFile(sourceFile)
        destFile = OutputFile(destFile)
        cmd = f'rsync -avz --no-g --no-p {sourceFile} {destFile}'
        return execute_strcmd(cmd)

    def validateBids(self, *args, **kargs):
        # @todo add checkup.
        # Decompress datasetDir on local cluster.
        archiveDir = self._archiveDir
        archiveName = self._archiveName
        workDir = f'{self._workerLocalDir}/bids-validator'
        extractedDatasetDir = f'{workDir}/dataset'
        result = self.extractDataset(archiveDir, archiveName, extractedDatasetDir)

        # Stop here if extraction failed.
        if not result.didSucceed:
            return result

        # Replace input datasetDir.
        kargs['datasetDir'] = extractedDatasetDir

        # Run super's task.
        result = super().validateBids(*args, **kargs)

        # Cleanup.
        self.cleanup(workDir)

        return result

    def generateMriQcSubjectReport(self, *args, **kargs):
        # @todo add checkup.

        # Decompress datasetDir on local cluster.
        archiveDir = self._archiveDir
        archiveName = self._archiveName
        workDir = kargs['workDir']
        extractedDatasetDir = f'{workDir}/dataset'
        result = self.extractDatasetForSubjectId(archiveDir, archiveName, extractedDatasetDir, kargs['subjectId'])
        
        # Stop here if extraction failed.
        if not result.didSucceed:
            return result
        
        # Replace input datasetDir and workdir.
        kargs['datasetDir'] = extractedDatasetDir

        # Run super's task.
        result = super().generateMriQcSubjectReport(*args, **kargs)

        # Cleanup.
        self.cleanup(workDir)

        return result
    
    def generateMriQcGroupReport(self, *args, **kargs):
        # @todo add checkup.

        # Decompress datasetDir on local cluster.
        archiveDir = self._archiveDir
        archiveName = self._archiveName
        workDir = kargs['workDir']
        extractedDatasetDir = f'{workDir}/dataset'
        result = self.extractDataset(archiveDir, archiveName, extractedDatasetDir)

        # Stop here if extraction failed.
        if not result.didSucceed:
            return result

        # Replace input datasetDir and workdir.
        kargs['datasetDir'] = extractedDatasetDir

        # Run super's task.
        result = super().generateMriQcGroupReport(*args, **kargs)

        # Cleanup.
        self.cleanup(workDir)

        return result
    

    def preprocessSMRiPrepAnatBySubject(self, *args, **kargs):
        # @todo add checkup.

        # Decompress datasetDir on local cluster.
        archiveDir = self._archiveDir
        archiveName = self._archiveName
        workDir = kargs['workDir']
        extractedDatasetDir = f'{workDir}/dataset'
        result = self.extractDatasetForSubjectId(archiveDir, archiveName, extractedDatasetDir, kargs['subjectId'])

        # Stop here if extraction failed.
        if not result.didSucceed:
            self.cleanup(workDir)
            return result

        # Change (log) and output dir.
        # outputFinalDir = kargs['outputDir']
        outputSharedDir = f'{self._workerSharedDir}/smriprep/sub-{kargs["subjectId"]}'
        # outputSharedArchive = f'{self._workerSharedDir}/smriprep/sub-{kargs["subjectId"]}'
        outputLocalDir = f'{workDir}/output'
        # outputLocalArchive = f'{self._workerLocalDir}/smriprep/sub-{kargs["subjectId"]}'
        kargs['outputDir'] = outputLocalDir

        # Replace input datasetDir and workdir.
        kargs['datasetDir'] = extractedDatasetDir

        # Run super's task.
        result = super().preprocessSMRiPrepAnatBySubject(*args, **kargs)

        # Transfer output back.
        if result.didSucceed:
            result2 = self.copyDir(outputLocalDir, outputSharedDir)  # @todo append '/' so it doesnt create a new dir!
            if not result2.didSucceed:
                self.cleanup(workDir)
                return result2

        # Cleanup.
        self.cleanup(workDir)

        return result

    def generateFMRiPrepSessionFilter(self, *args, **kargs):
        return super().generateFMRiPrepSessionFilter(args, **kargs)

    def preprocessFMRiPrepFuncBySession(self, *args, **kargs):
        # @todo add checkup.

        # Decompress datasetDir on local cluster.
        archiveDir = self._archiveDir
        archiveName = self._archiveName
        workDir = kargs['workDir']
        extractedDatasetDir = f'{workDir}/dataset'
        result = self.extractDatasetForSubjectId(archiveDir, archiveName, extractedDatasetDir, kargs['subjectId'])

        # Stop here if extraction failed.
        if not result.didSucceed:
            self.cleanup(workDir)
            return result

        # Copy anats on local cluster.
        kargs['anatsDerivativesDir'] = f'{self._workerSharedDir}/smriprep/sub-{kargs["subjectId"]}/output/smriprep'  # @todo fix, here retrieved from shared dir. -- and remove output!
        result = self.copyDir(kargs['anatsDerivativesDir'], f'{workDir}/sub-{kargs["subjectId"]}/anats')
        kargs['anatsDerivativesDir'] = f'{workDir}/sub-{kargs["subjectId"]}/anats'

        # Stop here if copy failed.
        if not result.didSucceed:
            self.cleanup(workDir)
            return result

        # Change (log) and output dir.
        # outputFinalDir = kargs['outputDir']
        outputSharedDir = f'{self._workerSharedDir}/fmriprep/sub-{kargs["subjectId"]}/ses-{kargs["sessionId"]}'
        # outputSharedArchive = f'{self._workerSharedDir}/smriprep/sub-{kargs["subjectId"]}'
        outputLocalDir = f'{workDir}/sub-{kargs["subjectId"]}/output'
        # outputLocalArchive = f'{self._workerLocalDir}/smriprep/sub-{kargs["subjectId"]}'
        kargs['outputDir'] = outputLocalDir

        # Replace input datasetDir and workdir.
        kargs['datasetDir'] = extractedDatasetDir

        # Run super's task.
        result = super().preprocessFMRiPrepFuncBySession(*args, **kargs)

        # Transfer output back.
        if result.didSucceed:
            result2 = self.copyDir(outputLocalDir, outputSharedDir)
            if not result2.didSucceed:
                self.cleanup(workDir)
                return result2
        
        # Cleanup.
        self.cleanup(workDir)

        return result

    def preprocessFMRiPrepBySubject(self, *args, **kargs):
        # @todo add checkup.

        # Decompress datasetDir on local cluster.
        archiveDir = self._archiveDir
        archiveName = self._archiveName
        workDir = kargs['workDir']
        extractedDatasetDir = f'{workDir}/dataset'
        result = self.extractDatasetForSubjectId(archiveDir, archiveName, extractedDatasetDir, kargs['subjectId'])

        # Stop here if extraction failed.
        if not result.didSucceed:
            self.cleanup(workDir)
            return result

        # Change (log) and output dir.
        # outputFinalDir = kargs['outputDir']
        outputSharedDir = f'{self._workerSharedDir}/fmriprep/sub-{kargs["subjectId"]}'
        # outputSharedArchive = f'{self._workerSharedDir}/smriprep/sub-{kargs["subjectId"]}'
        outputLocalDir = f'{workDir}/sub-{kargs["subjectId"]}/output'
        # outputLocalArchive = f'{self._workerLocalDir}/smriprep/sub-{kargs["subjectId"]}'
        kargs['outputDir'] = outputLocalDir

        # Replace input datasetDir and workdir.
        kargs['datasetDir'] = extractedDatasetDir

        # Run super's task.
        result = super().preprocessFMRiPrepBySubject(*args, **kargs)

        # Transfer output back.
        if result.didSucceed:
            result2 = self.copyDir(outputLocalDir, outputSharedDir)
            if not result2.didSucceed:
                self.cleanup(workDir)
                return result2
        
        # Cleanup.
        self.cleanup(workDir)

        return result
