import dask.distributed  # for MT
from typing import Type, Set, Dict, Tuple

from . import LocalDataset


# @warning DistributedDataset breaks bids caching.
class DistributedDataset(LocalDataset):
    _subjectIds: Set[str]
    _sessionIds: Dict[str, str]
    _sessionsWithAnatIds: Set[Tuple[str, str]]

    def __init__(self, subjectIds: Set[str], sessionIds: Dict[str, str],
                 sessionsWithAnatIds: Set[Tuple[str, str]]):
        self._subjectIds = subjectIds
        self._sessionIds = sessionIds
        self._sessionsWithAnatIds = sessionsWithAnatIds

    # NEW WAY : dar list (default: list-format=tar-mode) + awk $6? -> rel path

    # @warning when workerLocalDir is a local ssd, these can't be extracted to
    # local node ssd on ComputeCanada@Beluga (mutualised 480GO ssd) when
    # dataset ~> 300GO. Also, ssd space allocation is mutualised and not
    # guaranteed.
    @classmethod
    def loadFromArchiveWithPyBids(cls: Type, daskClient: dask.distributed.Client, 
                                  extractDatasetFn, archiveDir: str,
                                  archiveName: str, workerLocalDir: str):
        # Generate task.
        def taskFn():
            # Decompress datasetDir on local cluster.
            extractedDatasetDir = f'{workerLocalDir}/prefetch/dataset'

            result = extractDatasetFn(
                outputDir=extractedDatasetDir,
                archiveDir=archiveDir,
                archiveName=archiveName,
                # logFile=f'{outputDir}/log/prefetch-extract-dataset.txt'
            )

            # Stop here if extraction failed.
            if not result.didSucceed:
                return None

            # Prefetch dataset.
            dataset = LocalDataset(extractedDatasetDir, None)

            # Retrieve subjects list.
            subjectIds = dataset.getSubjectIds()

            # Retrieve session list
            sessionIds = {}
            for subjectId in subjectIds:
                sessionIds[subjectId] = dataset.getSessionIdsBySubjectId(subjectId)

            # Create prefetched dataset.
            # @todo add sessionWithAnatList
            dataset = DistributedDataset(subjectIds, sessionIds)

            # Cleanup.
            # self.cleanup(workDir) # @todo !!! cleanup !!!

            return dataset

        dataset = None
        # Run task locally.
        if daskClient == None:
            dataset = taskFn()
        # Run task remotely.
        else:
            taskDelayed = dask.delayed(taskFn)
            taskComputation = daskClient.submit(taskFn, resources={'job': 1})
            dataset = taskComputation.result()

        return dataset

    @classmethod
    def loadFromArchiveWithDar(cls: Type, daskClient: dask.distributed.Client,
                               listArchiveSessionsFn, archiveDir: str,
                               archiveName: str):
        # Generate task.
        def taskFn():
            # Decompress datasetDir on local cluster.
            result = listArchiveSessionsFn(
                archiveDir=archiveDir,
                archiveName=archiveName
            )

            # Stop here if retrieval has failed.
            if not result.didSucceed:
                return None

            # Convert result output to suitable array.
            sessionPerSubjectsDict = {}
            sessionsWithAnatList = []
            rawSubSesStrListStr = result.stdout
            rawSubSesStrList = rawSubSesStrListStr.splitlines()
            for rawSubSesStr in rawSubSesStrList:
                subSesPair = rawSubSesStr.split(',')
                subject = subSesPair[0]
                session = subSesPair[1] if len(subSesPair) >= 2 else None
                hasAnat = len(subSesPair) >= 3

                if subject not in sessionPerSubjectsDict:
                    sessionPerSubjectsDict[subject] = []
                
                if session is not None and session not in \
                   sessionPerSubjectsDict[subject]:
                    sessionPerSubjectsDict[subject].append(session)
                
                if hasAnat:
                    tuple = (subject, session)
                    sessionsWithAnatList.append(tuple)
                
            # Create prefetched dataset.
            subjectIds = list(sessionPerSubjectsDict.keys())
            sessionIds = sessionPerSubjectsDict
            print(subjectIds, sessionIds, sessionsWithAnatList)
            dataset = DistributedDataset(
                subjectIds, sessionIds, sessionsWithAnatList)

            return dataset

        dataset = None
        # Run task locally.
        if daskClient == None:
            dataset = taskFn()
        # Run task remotely.
        else:
            taskDelayed = dask.delayed(taskFn)
            taskComputation = daskClient.submit(taskFn, resources={'job': 1})
            dataset = taskComputation.result()

        return dataset

    # def prefetch(self):
    #     dataset = LocalDataset(self._bidsDatasetPath, self._cachePath)

    #     # Retrieve subjects list.
    #     subjectIds = dataset.getSubjectIds()

    #     # Retrieve session list
    #     sessionIds = {}
    #     for subjectId in subjectIds:
    #         sessionIds[subjectId] = dataset.getSessionIdsBySubjectId(subjectId)

    #     self._subjectIds = subjectIds
    #     self._sessionIds = sessionIds

    def getSubjectIds(self) -> Set[str]:
        return self._subjectIds

    def getSessionIdsBySubjectId(self, subjectId: str) -> Set[str]:
        return self._sessionIds[subjectId]

    # @deprecated (not used?)
    def doesSessionContainAnat(self, subjectId: str, sessionId: str) -> bool:
        tuple = (subjectId, sessionId)
        return tuple in self._sessionsWithAnatIds
    
    def getAnatSessionIdsBySubjectId(self, subjectId: str) -> Set[str]:
        return [
            sessionId
            for sessionId in self._sessionIds[subjectId]
            if (subjectId, sessionId) in self._sessionsWithAnatIds
        ]



    # import dask
    # 
    # 
    # def prefetch(self, daskClient: dask.distributed.Client):
    #     dataset = Dataset(self._bidsDatasetPath, self._cachePath)

    #     # Retrieve subjects list.
    #     getSubjectIdsDelayed = dask.delayed(Dataset.getSubjectIds)
    #     subjectIdsDelayed = getSubjectIdsDelayed(dataset)
    #     subjectIdsComputation = daskClient.compute(subjectIdsDelayed, resources={'job': 1})
    #     subjectIds = subjectIdsComputation.result()

    #     # Retrieve session list
    #     getSessionIdsDelayed = dask.delayed(Dataset.getSessionIdsBySubjectId)
    #     sessionIds = []
    #     for subjectId in subjectIds:
    #         sessionIdsDelayed = getSessionIdsDelayed(dataset, subjectId)
    #         sessionIdsComputation = daskClient.compute(sessionIdsDelayed, resources={'job': 1})
    #         sessionIds[subjectId] = sessionIdsComputation.result()

    #     self._subjectIds = subjectIds
    #     self._sessionIds = sessionIds
