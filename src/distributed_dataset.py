from src.dataset import Dataset
from typing import Set, Dict
import dask


# @warning DistributedDataset breaks bids caching.
class DistributedDataset(Dataset):
    _subjectIds: Set[str]
    _sessionIds: Dict[str, str]

    def __init__(self, bidsDatasetPath: str, cachePath: str):
        self._bidsDatasetPath = bidsDatasetPath
        self._cachePath = cachePath
    
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

    def prefetch(self):
        dataset = Dataset(self._bidsDatasetPath, self._cachePath)

        # Retrieve subjects list.
        subjectIds = dataset.getSubjectIds()

        # Retrieve session list
        sessionIds = {}
        for subjectId in subjectIds:
            sessionIds[subjectId] = dataset.getSessionIdsBySubjectId(subjectId)
        
        self._subjectIds = subjectIds
        self._sessionIds = sessionIds
        
    def getSubjectIds(self) -> Set[str]:
        return self._subjectIds

    def getSessionIdsBySubjectId(self, subjectId: str) -> Set[str]:
        return self._sessionIds[subjectId]
