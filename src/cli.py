import argparse
import psutil
import sys
from enum import Enum

class VMEngine(Enum):
    NONE = 'none'
    DOCKER = 'docker'
    SINGULARITY = 'singularity'

    # Used by argparse to provide user CLI input arg values.
    def __str__(self):
        return self.value

class Executor(Enum):
    NONE = 'none'
    LOCAL = 'local'
    SLURM = 'slurm'
    MPI = 'mpi'

    # Used by argparse to provide user CLI input arg values.
    def __str__(self):
        return self.value

class Granularity(Enum):
    SUBJECT = 'subject'
    SESSION = 'session'
    DATASET = 'dataset'

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
        '--granularity',
        type=Granularity,
        choices=list(Granularity),
        default=Granularity.SESSION,
        help='fmriprep granularity.',
        dest='granularity'
    )
    parser.add_argument( # @todo rename to scheduler instead
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
        '--enable-pybids-cache',
        action='store_true',
        dest='enablePybidsCache'
    )
    parser.add_argument(
        '--reset',
        action='store_true',
        dest='reset'
    )

    # @warning these are only used with slurm.
    
    defaultMemoryGb = psutil.virtual_memory().total / 1024 / 1024 / 1024
    parser.add_argument(
        '--worker-memory-gb',
        dest='workerMemoryGB',
        type=int,
        default=defaultMemoryGb
    )
    cpuCount = psutil.cpu_count()
    parser.add_argument(
        '--worker-cpu-count',
        dest='workerCpuCount',
        type=int,
        default=cpuCount
    )
    parser.add_argument( # @todo throw warning if != 1 and executor is not slurm
        '--worker-count',
        dest='workerCount',
        type=int,
        default=1,
        required='slurm' in sys.argv
    )
    # @todo throw warning if executor is not slurm
    # @todo check as time (type==func)
    parser.add_argument(
        '--worker-walltime',
        dest='workerWallTime',
        type=str,
        default='01:00:00',
        required='slurm' in sys.argv
    )
    parser.add_argument(
        '--worker-local-dir',
        dest='workerLocalDir',
        type=str,
        help='archive dataset on remote file system and extract on worker node\'s local file system to avoid overwhelming i/o',
        default=None
    )
    parser.add_argument(
        '--worker-shared-dir',
        dest='workerSharedDir',
        type=str,
        help='store compressed shared output on remote shared file system to avoid overwhelming i/o and locking parent dir on write (lustre)',
        default=None
    )

    args = parser.parse_args()
    return args
