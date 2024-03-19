from __future__ import annotations
from dataclasses import dataclass
from fsspec.implementations.arrow import AbstractFileSystem


@dataclass
class Artefact:

    _path: str
    _filesystem: AbstractFileSystem

    @classmethod
    def from_path(cls, path: str, filesystem: AbstractFileSystem) -> Artefact:
        if not filesystem.exists(path=path):
            raise FileNotFoundError(path)
        return cls(_path=path, _filesystem=filesystem)

    @property
    def path(self) -> str:
        return self._path

    @property
    def filesystem(self) -> AbstractFileSystem:
        return self._filesystem

    @property
    def exists(self) -> bool:
        return self._filesystem.exists(self._path)


class Dataset(Artefact): ...


# Pull dataset
#
