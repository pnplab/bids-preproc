from ._volume_mapping import mapPathsToVolumesInArgs
from . import oneliner


def dockerize(cmd: str, image: str, *args, **kargs) -> str:
    inputVolumes, outputVolumes, newKargs = mapPathsToVolumesInArgs(*args,
                                                                    **kargs)

    parsedCmd = cmd.format(
        oneliner(f'''
            docker run --rm
                {' '.join([
                    f'-v "{mountDir}:{originalDir}:ro"'
                    for originalDir, mountDir in inputVolumes.items()
                ])}
                {' '.join([
                    f'-v "{mountDir}:{originalDir}"'
                    for originalDir, mountDir in outputVolumes.items()
                ])}
                {image}
        '''),
        **newKargs
    )
    return parsedCmd
