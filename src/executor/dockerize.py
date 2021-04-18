from ._volume_mapping import mapPathsToVolumesInArgs
from . import oneliner


def dockerize(cmd: str, image: str, *args, **kargs) -> str:
    inputVolumes, outputVolumes, newKargs = mapPathsToVolumesInArgs(*args,
                                                                    **kargs)
    # @todo cleanup once smriprep is fixed!
    if 'fasttrackFixDir' in kargs:
        inputVolumes['/usr/local/miniconda/lib/python3.7/site-packages/smriprep/utils'] = kargs['fasttrackFixDir']

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
