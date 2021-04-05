# Abstract Path Placeholder Object Class.
#
# These objects specify additional attribute to a path:
# - is the path intended to be used as an input or an output ?
# - is the path either a file or a directory ?
# Providing these additional attributes let the underlying file processors
# decide how to load the file. ie. if file is loaded by a docker image and is
# an input file, then docker will configure the file's directory as a read only
# volume, and remap the underlying file path within the container's volume.
# Additionally, it provides runtime path validation and folder creation when
# relevant.
#
# @note
# It is not possible to inherith Path due to misimplementation, cf.
# https://bugs.python.org/issue24132
# Thus we create our own class instead, in order to be able to rely on
# polymorphism. This is often prominent for placeholder objects, as these are
# used for their intrinsic object type, rather than their implementation.

from pathlib import Path

class PathPlaceHolder():
    _path: str
    name: str

    def __init__(self, path: str):
        path = Path(path)
        self._path = path
        self.name = path.name
        self.parent = path.parent

    def __str__(self) -> str:
        return str(self._path)
