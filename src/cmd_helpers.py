import itertools
from pathlib import Path
from typing import Dict
from enum import Enum
import sarge
import time  # for sleep to avoid infinite loop while fetching run log


class VMEngine(Enum):
    NONE = 'none'
    DOCKER = 'docker'
    SINGULARITY = 'singularity'

    # Used by argparse to provide user CLI input arg values.
    def __str__(self):
        return self.value


# @note it is not possible to inherith Path due to misimplementation, cf.
# https://bugs.python.org/issue24132
class MyPath():
    _path: str
    name: str

    def __init__(self, path: str):
        path = Path(path)
        self._path = path
        self.name = path.name
        self.parent = path.parent

    def __str__(self) -> str:
        return str(self._path)


class InputFile(MyPath):
    def __init__(self, path: str) -> None:
        path = Path(path)

        # Do checkup.
        if not path.exists():
            raise FileNotFoundError(f'file not found: {path}')
        if not path.is_file():
            raise FileNotFoundError(f'not a file: {path}')

        # Convert to abs path + resolve symlinks.
        path = path.resolve()

        # Call super constructor.
        path = str(path)
        super().__init__(path)


class InputDir(MyPath):
    def __init__(self, path: str) -> None:
        path = Path(path)

        # Do checkup.
        if not path.exists():
            raise FileNotFoundError(f'file not found: {path}')
        if not path.is_dir():
            raise FileNotFoundError(f'not a dir: {path}')

        # Convert to abs path + resolve symlinks.
        path = path.resolve()

        # Call super constructor.
        path = str(path)
        super().__init__(path)


class OutputFile(MyPath):
    def __init__(self, path: str) -> None:
        path = Path(path)

        # Convert to abs path + resolve symlinks.
        path = path.resolve(strict=False)

        # Generate parent folder.
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

        # Check path type.
        if path.exists() and not path.is_file():
            raise FileNotFoundError('path is not a file: {path}')

        # Call super constructor.
        path = str(path)
        super().__init__(path)


class OutputDir(MyPath):
    def __init__(self, path: str) -> None:
        path = Path(path)

        # Convert to abs path + resolve symlinks.
        path = path.resolve(strict=False)

        # Generate folder.
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        # Check path type.
        if not path.is_dir():
            raise FileNotFoundError('path is not a directory: {path}')

        # Call super constructor.
        path = str(path)
        super().__init__(path)


def run(command: str) -> bool:
    # Launch command and capture stdout/stderr stream.
    stream = sarge.Capture()
    result = sarge.run(command, async_=True, stdout=stream, stderr=stream,
                       shell=True)

    # Wait for result object to be fully instantiated (especially
    # returncode property, due to async_ property which creates
    # it in another thread).
    result.commands[0].poll()

    # Loop until command has finished
    while (result.returncode is None):
        # Write received stdout/stderr output from stream.
        while (line := stream.readline(timeout=1)):
            print('> ' + line.decode('utf-8'), end='')

        # Delay next iteration in order to avoid 100% CPU usage due to
        # infinite loop.
        time.sleep(0.05)

        # Update returncode.
        result.commands[0].poll()

    # Wait for app end (optional since we loop on returncode existance).
    result.wait()
    # print('return code: %s' % result.returncode)

    # Close output stream.
    stream.close()

    didSucceed = True if result.returncode == 0 else False
    return didSucceed, result.returncode, result.stdout.text, \
        result.stderr.text


def _mapPathsToVolumesInArgs(*args, **kargs):
    # Map path args to volumes.
    inputVolumes = {}
    outputVolumes = {}
    newKargs = {}

    for arg, val in itertools.chain(enumerate(args), kargs.items()):
        # Copy the argument as is if it is not a path.
        if isinstance(val, InputFile):
            hostPath = val
            mountPath = f'/v_{arg}'

            # Map volume dir to local file's parent dir path.
            fileName = hostPath.name
            inputVolumes[mountPath] = hostPath.parent

            # Replace command's arg path with mapped one.
            newKargs[arg] = f'{mountPath}/{fileName}'
        elif isinstance(val, InputDir):
            hostPath = val
            mountPath = f'/v_{arg}'

            # Map volume dir to local dir path.
            inputVolumes[mountPath] = hostPath

            # Replace command's arg path with mapped one.
            newKargs[arg] = mountPath
        elif isinstance(val, OutputFile):
            hostPath = val
            mountPath = f'/v_{arg}'

            # Map volume dir to local file's parent dir path.
            fileName = hostPath.name
            outputVolumes[mountPath] = hostPath.parent

            # Replace command's arg path with mapped one.
            newKargs[arg] = f'{mountPath}/{fileName}'
        elif isinstance(val, OutputDir):
            hostPath = val
            mountPath = f'/v_{arg}'

            # Map volume dir to local dir path.
            outputVolumes[mountPath] = hostPath

            # Replace command's arg path with mapped one.
            newKargs[arg] = mountPath
        else:
            newKargs[arg] = val

    return inputVolumes, outputVolumes, newKargs


def dockerize(cmd: str, image: str, *args, **kargs) -> str:
    inputVolumes, outputVolumes, newKargs = _mapPathsToVolumesInArgs(*args,
                                                                     **kargs)
    parsedCmd = cmd.format(
        # -m 6G
        oneliner(f'''
            docker run -ti --rm
                {' '.join([
                    f'-v "{mountDir}:{originalDir}:ro"'
                    for originalDir, mountDir in inputVolumes.items()
                ])}
                {' '.join([
                    f'-v "{mountDir}:{originalDir}"'
                    for originalDir, mountDir in outputVolumes.items()
                ])}
                --cpus 2.0
                -m 8G
                {image}
        '''),
        **newKargs
    )
    return parsedCmd


def singularize(cmd: str, imagePath: str, *args, **kargs) -> str:
    inputVolumes, outputVolumes, newKargs = _mapPathsToVolumesInArgs(*args,
                                                                     **kargs)
    parsedCmd = cmd.format(
        oneliner(f'''
            singularity run
                --no-home
                --cleanenv
                {' '.join([
                    f'-B "{mountDir}:{originalDir}:ro"'
                    for originalDir, mountDir in inputVolumes.items()
                ])}
                {' '.join([
                    f'-B "{mountDir}:{originalDir}"'
                    for originalDir, mountDir in outputVolumes.items()
                ])}
                "{imagePath}"
        '''),
        **newKargs
    )
    return parsedCmd


def logify(cmd, logPath):
    return f'''(set -o pipefail && {cmd} 2>&1 | tee "{logPath}")'''


# Strip command from newlines / tabs.
def oneliner(cmd):
    return cmd.replace('\n', ' ').replace('    ', '')


def createTaskForCmd(vmType: VMEngine, executable: str, cmdTemplate: str,
                     **argsDecorators: Dict[str, MyPath]):
    # Create task.
    def task(*args, **kargs):
        # Decorate command args (ie. path type placeholders, which can be used
        # by vm tool wrapper to setup the vm volumes, and performs path
        # checking and directory creation).
        newKargs = {}
        for arg, val in itertools.chain(enumerate(args), kargs.items()):
            if arg in argsDecorators:
                wrapper = argsDecorators[arg]
                val = wrapper(val)
            newKargs[arg] = val

        # Inject command args within command template, and wrap with vm tool
        # when relevant.
        cmd = None
        if vmType == VMEngine.NONE:
            cmd = str.format(
                cmdTemplate,
                executable,
                **newKargs
            )
        elif vmType == VMEngine.DOCKER:
            cmd = dockerize(
                cmdTemplate,
                executable,
                **newKargs
            )
        elif vmType == VMEngine.SINGULARITY:
            cmd = singularize(
                cmdTemplate,
                executable,
                **newKargs
            )

        # Inject logs capture to the command.
        if 'logFile' in kargs:
            cmd = logify(cmd, OutputFile(kargs['logFile']))

        # Merge the multiline command string as a single line, so the command
        # execution doesn't crash due to seperate, unwinllingly splitted
        # instructions.
        cmd = oneliner(cmd)

        # Print command.
        print(f"cmd: {cmd}")

        # Execute command.
        didSucceed, returnCode, stdout, stderr = run(cmd)

        # Return command results.
        return didSucceed, returnCode

    return task
