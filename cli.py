import argparse
from enum import Enum
from src.cmd_helpers import VMEngine


class Executor(Enum):
    NONE = 'none'
    LOCAL = 'local'
    SLURM = 'slurm'
    MPI = 'mpi'

    # Used by argparse to provide user CLI input arg values.
    def __str__(self):
        return self.value


def readCLIArgs():
    parser = argparse.ArgumentParser(
        description='Preprocess fmri bids dataset.'
    )
    parser.add_argument(
        'datasetPath',  # cf. https://code.google.com/archive/p/argparse/issues/65
        type=str,
        help='path of the input bids dataset',
        metavar='dataset-path'
    )
    parser.add_argument(
        'outputDir',
        type=str,
        help='output dir',
        metavar='output-dir'
    )
    parser.add_argument(
        '--vm-engine',
        type=VMEngine,
        choices=list(VMEngine),
        required=True,
        help='virtual machine engine used to run the command.',
        dest='vmEngine'
    )
    parser.add_argument(
        '--executor',
        type=Executor,
        choices=list(Executor),
        required=True,
        help='executor used to orchestrate the tasks.',
        dest='executor'
    )
    parser.add_argument(
        '--disable-bids-validator',
        action='store_false',
        dest='enableBidsValidator'
    )
    parser.add_argument(
        '--disable-mriqc',
        action='store_false',
        dest='enableMRIQC'
    )
    parser.add_argument(
        '--disable-smriprep',
        action='store_false',
        dest='enableSMRiPrep'
    )
    parser.add_argument(
        '--disable-fmriprep',
        action='store_false',
        dest='enableFMRiPrep'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        dest='reset'
    )

    args = parser.parse_args()
    return args
