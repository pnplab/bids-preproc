from dataclasses import dataclass


@dataclass
class TaskResult:
    didSucceed: bool
    returnCode: int
    # @warning
    # Capturing stdout/stderr might produce heavy amount of transfer between
    # computational nodes and scheduling node when using dask, as well as hefty
    # memory usage (fmriprep verbose logs often are > several hundreds megs
    # each).
    # @note
    # Captured result is used by some task (eg. dar archive content listing, to
    # retrieve subjects and sessions).
    stdout: str
    stderr: str
