import os
from bids import BIDSLayout, config
from typing import Set

# Allow leading dot in cache path.
config.set_option('extension_initial_dot', True)


class Dataset:
    def __init__(self, bidsDatasetPath: str, cachePath: str = 'bids_cache'):
        # @todo serialize dataset path within name
        bidsDatasetPath = os.path.normpath(bidsDatasetPath)
        cachePath = os.path.normpath(cachePath)

        # @todo set adaptive db cache path.
        # @note cache load time >5s
        self._datasetLayout = BIDSLayout(
            bidsDatasetPath,
            database_path=cachePath,
            reset_database=False
        )

    def getSubjectIds(self) -> Set[str]:
        layout = self._datasetLayout
        subjectIds = layout.get(
            return_type='id',
            target='subject',
            suffix='T1w'
        )
        return subjectIds

    def getSessionIdsBySubjectId(self, subjectId: str) -> Set[str]:
        layout = self._datasetLayout
        subjectIds = layout.get(
            return_type='id',
            target='session',
            suffix='bold',
            subject=f'{subjectId}'
        )
        return subjectIds
