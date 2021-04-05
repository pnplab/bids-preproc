from pathlib import Path

# For os.path.expandvars (no pathlib equivalent cf.
# https://bugs.python.org/issue21301)
import os

# Import abstract placeholder class.
from . import PathPlaceHolder


class InputFile(PathPlaceHolder):
    def __init__(self, path: str) -> None:
        # Expand environment variables within context (eg. $SLURM_TMPDIR within
        # hcp compute node).
        path = os.path.expandvars(path)

        # Process path using pathlib.
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
