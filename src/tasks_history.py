import os
import pandas
from typing import List


# @warning not safe for parallel write.
class TasksHistory:
    _cachePath: str
    _cache: pandas.DataFrame
    _columnNames: List[str]

    def __init__(self, cachePath):
        cachePath = os.path.normpath(cachePath)
        parentDir = os.path.dirname(cachePath)
        columnNames = ['task_name', 'did_succeed', 'return_code']

        # Generate dir if not exists.
        if not os.path.exists(parentDir):
            os.makedirs(parentDir, exist_ok=True)

        # Generate cache if not exists.
        if not os.path.exists(cachePath):
            cache = pandas.DataFrame(None, columns=columnNames)
            cache.to_csv(cachePath, index=False)

        # Read cache
        cache = pandas.read_csv(
            cachePath,
            index_col=None,
            dtype={
                'task_name': str,
                'did_succeed': bool,
                'return_code': int
            }
        )

        self._cachePath = cachePath
        self._cache = cache
        self._columnNames = columnNames

    def writeTaskCache(self, taskName: str, didSucceed: bool, returnCode: int):
        cache = self._cache

        # Remove previous indentical task record.
        items = cache.loc[cache['task_name'] == taskName]
        cache = cache.drop(items.index)

        # Add new task record.
        newRecord = {
            'task_name': taskName,
            'did_succeed': didSucceed,
            'return_code': returnCode
        }
        cache = cache.append(newRecord, ignore_index=True)

        # Store new cache file.
        cache.to_csv(self._cachePath, index=False)
        self._cache = cache

    def readTaskCache(self, taskName: str) -> pandas.DataFrame:
        cache = self._cache

        # Get task record.
        items = cache.loc[cache['task_name'] == taskName]
        assert len(items) < 2
        item = items.iloc[0] if len(items) > 0 else None

        return item
