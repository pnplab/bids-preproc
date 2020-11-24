# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
"""Utilities to handle BIDS inputs."""
from collections import defaultdict
from pathlib import Path
from json import loads
from pkg_resources import resource_filename as pkgrf
from bids.layout.writing import build_path
from bids import BIDSLayout
from .. import config


def get_outputnode_spec():
    """
    Generate outputnode's fields from I/O spec file.

    Examples
    --------
    >>> get_outputnode_spec()  # doctest: +NORMALIZE_WHITESPACE
    ['t1w_preproc', 't1w_mask', 't1w_dseg', 't1w_tpms',
    'std_preproc', 'std_mask', 'std_dseg', 'std_tpms',
    'anat2std_xfm', 'std2anat_xfm',
    't1w_aseg', 't1w_aparc',
    't1w2fsnative_xfm', 'fsnative2t1w_xfm',
    'surfaces']

    """
    spec = loads(Path(pkgrf('smriprep', 'data/io_spec.json')).read_text())["queries"]
    fields = ['_'.join((m, s)) for m in ('t1w', 'std') for s in spec["baseline"].keys()]
    fields += [s for s in spec["std_xfms"].keys()]
    fields += [s for s in spec["surfaces"].keys()]
    return fields


def collect_derivatives(derivatives_dir, subject_id, std_spaces, freesurfer,
                        spec=None, patterns=None):
    """Gather existing derivatives and compose a cache."""
    layout = BIDSLayout(
        derivatives_dir,
        validate=False,
        config=['bids', 'derivatives']
    )

    if spec is None or patterns is None:
        _spec, _patterns = tuple(
            loads(Path(pkgrf('smriprep', 'data/io_spec.json')).read_text()).values())

        if spec is None:
            spec = _spec
        if patterns is None:
            patterns = _patterns

    derivs_cache = defaultdict(list, {})
    derivatives_dir = Path(derivatives_dir)

    def _check_item(item):
        if not item:
            return None

        if isinstance(item, str):
            item = [item]

        result = []
        for i in item:
            if not (derivatives_dir / i).exists():
                i = i.rstrip('.gz')
                if not (derivatives_dir / i).exists():
                    config.loggers.workflow.warning('derivatives file not found for item: %s, %s' %
                                                    (str(i), str(derivatives_dir / i)))
                    return None
            result.append(str(derivatives_dir / i))

        return result

    for space in [None] + std_spaces:
        for k, q in spec['baseline'].items():
            q['subject'] = subject_id
            if space is not None:
                q['space'] = space

            # Retrieve paths.
            paths = layout.get(
                return_type='filename',
                subject=q['subject'],
                suffix=q['suffix'],  # loaded from iospec file == T1w
                extension=['nii', 'nii.gz'],  # json files must not be included.
                desc=q['desc'] if 'desc' in q else None,  # loaded from iospec file == preproc
                label=q['label'] if 'label' in q else None,  # loaded from iospec file == preproc
                absolute_paths=True
            )

            # Filter files by space manually, as it can't be used as a pybids
            # filter.
            if space is None:
                paths = list(filter(lambda path: f'_space-' not in path, paths))
            else:
                paths = list(filter(lambda path: f'_space-{space}' in path, paths))

            # Log if no path found.
            if not paths or len(paths) == 0:
                config.loggers.workflow.warning(
                    'space item not found for baseline: %s, %s, %s' % space, k, q)
                return None

            if space:
                derivs_cache["std_%s" % k] += paths if len(paths) == 1 else [paths]
            else:
                derivs_cache["t1w_%s" % k] = paths[0] if len(paths) == 1 else paths

    for space in std_spaces:
        for k, q in spec['std_xfms'].items():
            q['subject'] = subject_id
            q['from'] = q['from'] or space
            q['to'] = q['to'] or space

            # Retrieve paths.
            paths = layout.get(
                return_type='filename',
                subject=q['subject'],
                suffix=q['suffix'],  # loaded from iospec file == T1w
                # sometimes loaded from iospec file as 'T1w'
                extension=q['extension'] if 'extension' in q else None,
                desc=q['desc'] if 'desc' in q else None,  # loaded from iospec file == preproc
                absolute_paths=True
            )

            # Filter manually by mode, from and to, and hemi, as these can't be
            # used as pybids filter.
            if 'mode' in q:
                mode = q['mode']
                paths = list(filter(lambda path: f'_mode-{mode}' in path, paths))
            if 'from' in q:
                from_ = q['from'] or space
                paths = list(filter(lambda path: f'_from-{from_}' in path, paths))
            if 'to' in q:
                to = q['to'] or space
                paths = list(filter(lambda path: f'_to-{to}' in path, paths))
            if 'hemi' in q:
                hemis = q['hemi']  # expected to == L or R
                paths = list(filter(lambda path: any(
                    f'_hemi-{hemi}' in path for hemi in hemis), paths))

            # Log if no path found.
            if not paths or len(paths) == 0:
                config.loggers.workflow.warning(
                    'space item not found for std_xfms: %s, %s, %s' % space, k, q)
                return None

            derivs_cache[k] += paths

    derivs_cache = dict(derivs_cache)  # Back to a standard dictionary

    if freesurfer:
        for k, q in spec['surfaces'].items():
            q['subject'] = subject_id
            item = _check_item(build_path(q, patterns))
            if not item:
                config.loggers.workflow.warning(
                    'freesurfer item not found for surfaces: %s, %s' % k, q)
                return None

            if len(item) == 1:
                item = item[0]
            derivs_cache[k] = item

    derivs_cache['template'] = std_spaces
    return derivs_cache


def write_bidsignore(deriv_dir):
    bids_ignore = [
        "*.html", "logs/", "figures/",  # Reports
        "*_xfm.*",  # Unspecified transform files
        "*.surf.gii",  # Unspecified structural outputs
    ]
    ignore_file = Path(deriv_dir) / ".bidsignore"

    ignore_file.write_text("\n".join(bids_ignore) + "\n")


def write_derivative_description(bids_dir, deriv_dir):
    """
    Write a ``dataset_description.json`` for the derivatives folder.

    .. testsetup::

    >>> from pkg_resources import resource_filename
    >>> from pathlib import Path
    >>> from tempfile import TemporaryDirectory
    >>> tmpdir = TemporaryDirectory()
    >>> bids_dir = resource_filename('smriprep', 'data/tests')
    >>> deriv_desc = Path(tmpdir.name) / 'dataset_description.json'

    .. doctest::

    >>> write_derivative_description(bids_dir, deriv_desc.parent)
    >>> deriv_desc.is_file()
    True

    .. testcleanup::

    >>> tmpdir.cleanup()


    """
    import os
    from pathlib import Path
    import json
    from ..__about__ import __version__, DOWNLOAD_URL

    bids_dir = Path(bids_dir)
    deriv_dir = Path(deriv_dir)
    desc = {
        'Name': 'sMRIPrep - Structural MRI PREProcessing workflow',
        'BIDSVersion': '1.4.0',
        'DatasetType': 'derivative',
        'GeneratedBy': [{
            'Name': 'sMRIPrep',
            'Version': __version__,
            'CodeURL': DOWNLOAD_URL,
        }],
        'HowToAcknowledge':
            'Please cite our paper (https://doi.org/10.1101/306951), and '
            'include the generated citation boilerplate within the Methods '
            'section of the text.',
    }

    # Keys that can only be set by environment
    if 'SMRIPREP_DOCKER_TAG' in os.environ:
        desc['GeneratedBy'][0]['Container'] = {
            "Type": "docker",
            "Tag": f"poldracklab/smriprep:{os.environ['SMRIPREP_DOCKER_TAG']}"
        }
    if 'SMRIPREP_SINGULARITY_URL' in os.environ:
        desc['GeneratedBy'][0]['Container'] = {
            "Type": "singularity",
            "URI": os.environ['SMRIPREP_SINGULARITY_URL'],
        }

    # Keys deriving from source dataset
    orig_desc = {}
    fname = bids_dir / 'dataset_description.json'
    if fname.exists():
        orig_desc = json.loads(fname.read_text())

    if 'DatasetDOI' in orig_desc:
        doi = orig_desc["DatasetDOI"]
        desc['SourceDatasets'] = [{
            'URL': f"https://doi.org/{doi}",
            'DOI': doi,
        }]
    if 'License' in orig_desc:
        desc['License'] = orig_desc['License']

    Path.write_text(deriv_dir / 'dataset_description.json', json.dumps(desc, indent=4))
