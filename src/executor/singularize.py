from ._volume_mapping import mapPathsToVolumesInArgs
from . import oneliner


def singularize(cmd: str, imagePath: str, *args, **kargs) -> str:
    inputVolumes, outputVolumes, newKargs = mapPathsToVolumesInArgs(*args,
                                                                    **kargs)
    # @todo cleanup once smriprep is fixed!
    if 'fasttrackFixDir' in kargs:
        inputVolumes['/usr/local/miniconda/lib/python3.7/site-packages/smriprep/utils'] = kargs['fasttrackFixDir']

    parsedCmd = cmd.format(
        # @note singularity `--no-home` removed since:
        #     cf. https://github.com/poldracklab/mriqc/issues/853
        #     cf. https://github.com/nipreps/fmriprep/pull/1830
        oneliner(f'''
            singularity run
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
        # Inject other args.
        **newKargs
    )
    return parsedCmd
