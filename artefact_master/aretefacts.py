from dataclasses import dataclass


@dataclass
class Artefact:

    _path: str

    @property
    def path(self) -> str:
        return self._path


class Dataset(Artefact):
    ...


# Pull dataset
# Name as property ?
# Fs ? 
# Exists ? 
# From path ?
# 