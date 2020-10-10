import collections  # for sequence type unpacking in higher order functions
# import sarge
# import time  # for sleep to avoid infinite loop while fetching run log
from task_history import TaskHistory
from typing import Set


# def run(command: str, oneliner: bool = True) -> bool:
#     # Strip command from newlines / tabs.
#     if oneliner:
#         command = command.replace('\n', ' ').replace('    ', '')

#     # Launch command and capture stdout/stderr stream.
#     stream = sarge.Capture()
#     result = sarge.run(command, async_=True, stdout=stream, stderr=stream,
#                        shell=True)

#     # Wait for result object to be fully instantiated (especially
#     # returncode property, due to async_ property which creates
#     # it in another thread).
#     result.commands[0].poll()

#     # Loop until command has finished
#     while (result.returncode is None):
#         # Write received stdout/stderr output from stream.
#         while (line := stream.readline(timeout=1)):
#             print('> ' + line.decode('utf-8'), end='')

#         # Delay next iteration in order to avoid 100% CPU usage due to
#         # infinite loop.
#         time.sleep(0.05)

#         # Update returncode.
#         result.commands[0].poll()

#     # Wait for app end (optional since we loop on returncode existance).
#     result.wait()
#     # print('return code: %s' % result.returncode)

#     # Close output stream.
#     stream.close()

#     didSucceed = True if result.returncode == 0 else False
#     return didSucceed, result.returncode, result.stdout.text, \
#         result.stderr.text


class Runner:
    def __init__(self, historyCachePath: str):
        self._history = TaskHistory(historyCachePath)

    def runTask(self, taskName: str, taskFn, *args, **kwargs) -> bool:
        cache = self._history

        # Bypass task if it has already run successfully.
        if self.didTaskSucceed(taskName):
            # Log.
            print(f'Task {taskName} already successfully executed.')
            return True
        # Run task for the first time or again if it failed last time.
        else:
            # Log.
            print(f'Task {taskName} starting.')

            # Run task.
            didSucceed, returnCode = taskFn(*args, **kwargs)

            # Record result.
            cache.writeTaskCache(taskName, didSucceed, returnCode)

            # Log.
            doneMsg = f'Task {taskName} {"succeeded" if didSucceed else "failed"} \
            (exit code {returnCode}).'.replace('    ', '')
            print(doneMsg)

            # Return success/failure
            return didSucceed

    def batchTask(self, taskName: str, taskFn, itemIds: Set[any]):
        print(f'Batch {taskName} {str(itemIds)} starting.')

        successfulItemIds = []
        failedItemIds = []
        for itemId in itemIds:
            didSucceed = None
            # Expand list itemId as argument for the function.
            if isinstance(itemId, collections.Sequence) and not \
               isinstance(itemId, str):
                didSucceed = self.runTask(f'{taskName}_{str(itemId)}', taskFn,
                                          *itemId)
            # Expand dictionnary itemId as argument for the function.
            elif isinstance(itemId, dict):
                didSucceed = self.runTask(f'{taskName}_{str(itemId)}', taskFn,
                                          **itemId)
            # Send itemId as argument for the function.
            else:
                didSucceed = self.runTask(f'{taskName}_{itemId}', taskFn,
                                          itemId)
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
