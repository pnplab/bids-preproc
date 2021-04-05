import sarge
from . import TaskResult


# Execute a shell command.
# Deadlock safe hopefully, stream disabled.
def execute_strcmd(command: str) -> bool:
    # Launch command and capture stdout/stderr stream.
    stream = sarge.Capture()
    result = sarge.run(command, async_=False, shell=True)

    didSucceed = True if result.returncode == 0 else False
    return TaskResult(
        didSucceed=didSucceed,
        returnCode=result.returncode,
        stdout=None,  # stdout=result.stdout.text,
        stderr=None  # result.stderr.text
    )


# import time  # for sleep to avoid infinite loop while fetching run log
# 
# 
# def run(command: str) -> bool:
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
#         while True:
#             line = stream.readline(timeout=1)
#             if not line:
#                 break
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
#     return TaskResult(
#         didSucceed=didSucceed,
#         returnCode=result.returncode,
#         stdout=None, # stdout=result.stdout.text,
#         stderr=None  # result.stderr.text
#     )
