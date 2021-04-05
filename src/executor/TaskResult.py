from dataclasses import dataclass


@dataclass
class TaskResult:
    didSucceed: bool
    returnCode: int
    stdout: str
    stderr: str
