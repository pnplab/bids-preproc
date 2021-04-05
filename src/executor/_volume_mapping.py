import itertools
from ..path import InputFile, InputDir, OutputFile, OutputDir


def mapPathsToVolumesInArgs(*args, **kargs):
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
