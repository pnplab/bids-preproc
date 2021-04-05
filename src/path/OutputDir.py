from pathlib import Path

# For os.path.expandvars (no pathlib equivalent cf.
# https://bugs.python.org/issue21301)
import os

# Import abstract placeholder class.
from . import PathPlaceHolder


class OutputDir(PathPlaceHolder):
    def __init__(self, path: str) -> None:
        # Expand environment variables within context (eg. $SLURM_TMPDIR within
        # hcp compute node).
        path = os.path.expandvars(path)

        # Process path using pathlib.
        path = Path(path)

        # Convert to abs path + resolve symlinks.
        path = path.resolve(strict=False)

        # Generate folder.
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        # Check path type.
        if not path.is_dir():
            raise FileNotFoundError('path is not a directory: {path}')

        # Call super constructor.
        path = str(path)
        super().__init__(path)
