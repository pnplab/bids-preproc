import collections  # for sequence type unpacking in higher order functions
# import sarge
# import time  # for sleep to avoid infinite loop while fetching run log
from typing import Set
from tqdm import tqdm
from src.tasks_history import TasksHistory


class LocalScheduler:
    def __init__(self, historyCachePath: str):
        self._history = TasksHistory(historyCachePath)

    def runTask(self, taskName: str, taskFn, cleanupFn, *args, **kwargs) -> bool:
        cache = self._history

        # Bypass task if it has already run successfully.
        if self.didTaskSucceed(taskName):
            # Log.
            tqdm.write(f'Task {taskName} already successfully executed.')
            return True
        # Run task for the first time or again if it failed last time.
        else:
            # Log.
            tqdm.write(f'Task {taskName} starting.')

            # Run task.
            taskResult = taskFn(*args, **kwargs)
            didSucceed = taskResult.didSucceed
            returnCode = taskResult.returnCode

            # Record result.
            cache.writeTaskCache(taskName, didSucceed, returnCode)

            # Log.
            doneMsg = f'Task {taskName} {"succeeded" if didSucceed else "failed"} \
            (exit code {returnCode}).'.replace('    ', '')
            tqdm.write(doneMsg)

            # Run cleanup.
            if cleanupFn is not None:
                # @todo Do something with the clean up output.
                cleanupFn(didSucceed, *args, **kwargs)

            # Return success/failure
            return didSucceed

    def batchTask(self, taskName: str, taskFn, cleanupFn, itemIds: Set[any]):
        print(f'Batch {taskName} {str(itemIds)} starting.')

        successfulItemIds = []
        failedItemIds = []
        for itemId in tqdm(itemIds):
            didSucceed = None
            # Expand list itemId as argument for the function.
            if isinstance(itemId, collections.Sequence) and not \
               isinstance(itemId, str):
                didSucceed = self.runTask(f'{taskName}_{str(itemId)}', taskFn,
                                          cleanupFn, *itemId)
            # Expand dictionnary itemId as argument for the function.
            elif isinstance(itemId, dict):
                didSucceed = self.runTask(f'{taskName}_{str(itemId)}', taskFn,
                                          cleanupFn, **itemId)
            # Send itemId as argument for the function.
            else:
                didSucceed = self.runTask(f'{taskName}_{itemId}', taskFn,
                                          cleanupFn, itemId)
            # Store itemId if successful.
            if didSucceed:
                successfulItemIds.append(itemId)

        # Generate a list of unsuccessful itemIds.
        failedItemIds = [
            item for item in itemIds if item not in successfulItemIds
        ]

        print(f'Batch {taskName} {str(successfulItemIds)} succeeded.')
        print(f'Batch {taskName} {str(failedItemIds)} failed.'
              .replace('    ', ''))

        return successfulItemIds, failedItemIds

    def didTaskSucceed(self, taskName: str) -> bool:
        cache = self._history

        taskRecord = cache.readTaskCache(taskName)
        didSucceed = taskRecord is not None \
            and taskRecord['did_succeed'] == True  # noqa: E712
        return didSucceed
