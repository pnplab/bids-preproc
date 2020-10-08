import collections  # for sequence type unpacking in higher order functions
import os  # for path
import sarge
import time  # for sleep to avoid infinite loop while fetching run log
from task_history import TaskHistory
from typing import Set
import dask
import dask_jobqueue

from runner import Runner, run
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster


class DaskRunner(Runner):
    def __init__(self, historyCachePath: str):
        super().__init__(historyCachePath)

        cluster = LocalCluster()
        cluster.scale(4)
        client = Client(cluster)

        self._client = client

    def runTask(self, taskName: str, taskFn, *args, **kwargs) -> bool:
        client = self._client

        # Wrap task to dask.
        def blockingTaskFn(*args, **kwargs):
            delayedTaskFn = dask.delayed(taskFn)
            delayedResult = delayedTaskFn(*args, **kwargs)
            returnValues = delayedResult.compute()
            return returnValues

        # Run task.
        return super().runTask(taskName, blockingTaskFn, *args, **kwargs)

    def batchTask(self, taskName: str, taskFn, itemIds: Set[any]):
        client = self._client
        cache = self._history

        print(f'Batch {taskName} {str(itemIds)} starting.')

        # Gather task batch.
        delayedTaskFn = dask.delayed(taskFn)
        delayedResults = {}

        for itemId in itemIds:
            # Expand list itemId as argument for the function.
            if isinstance(itemId, collections.Sequence) and not \
               isinstance(itemId, str):
                didSucceedDelayed = delayedTaskFn(*itemId)
                delayedResults[itemId] = didSucceedDelayed
            # Expand dictionnary itemId as argument for the function.
            elif isinstance(itemId, dict):
                didSucceedDelayed = delayedTaskFn(**itemId)
                delayedResults[itemId] = didSucceedDelayed
            # Send itemId as argument for the function.
            else:
                didSucceedDelayed = delayedTaskFn(itemId)
                delayedResults[itemId] = didSucceedDelayed

        # Compute batch.
        computations = client.compute(delayedResults)
        results = computations.result()

        # Generate a list of successful itemIds.
        successfulItemIds = [
            itemId for itemId, returnValues in results.items() if returnValues[0] == True
        ]

        # Generate a list of unsuccessful itemIds.
        failedItemIds = [
            item for item in itemIds if item not in successfulItemIds
        ]

        # Update cache.
        for itemId, returnValues in results.items():
            taskItemName = f'{taskName}_{str(itemId)}'
            didSucceed, returnCode = returnValues
            cache.writeTaskCache(taskItemName, didSucceed, returnCode)

        print(f'Batch {taskName} {str(successfulItemIds)} succeeded.')
        print(f'Batch {taskName} {str(failedItemIds)} failed.'.replace('    ', ''))

        return successfulItemIds, failedItemIds