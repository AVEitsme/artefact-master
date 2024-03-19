from __future__ import annotations
from pathlib import PurePath
from typing import Dict, List, Union
from fsspec.implementations.arrow import AbstractFileSystem

from artefact_master.aretefacts import Artefact, Dataset
from artefact_master.exceptions import (
    AlreadyRegisteredError,
    ArtefactNotFoundError,
    NotRegisteredError,
)


class ArtefactMaster:
    def __init__(
        self,
        root_path: str,
        registered_artefacts: Dict[str, Artefact],
    ) -> None:
        self._root_path = root_path
        self._registered_artefacts = registered_artefacts

    @property
    def root_path(self) -> str:
        return self._root_path

    @property
    def registered_objects(self) -> Dict[str, Artefact]:
        return self._registered_artefacts

    @property
    def is_registered(self, artefact_name: str) -> bool:
        return self._is_registered(artefact_name=artefact_name)

    def _is_registered(self, artefact_name: str) -> bool:
        return artefact_name in self._registered_artefacts

    def _list_artefacts(
        self, as_artefacts: bool = False
    ) -> Union[List[str], List[Artefact]]:
        if not as_artefacts:
            return list(self._registered_artefacts.keys())
        return list(self._registered_artefacts.values())

    def _validate_registrered(self, artefact_name: str) -> None:
        if not self._is_registered(artefact_name=artefact_name):
            raise NotRegisteredError(
                artefact_name=artefact_name, registered_artefacts=self._list_artefacts()
            )

    def _register_artefact(self, artefact_name: str, artefact: Artefact) -> None:
        if self._is_registered(artefact_name=artefact_name):
            raise AlreadyRegisteredError(artefact_name=artefact_name)
        if not artefact.exists:
            raise ArtefactNotFoundError(artefact_path=artefact.path)
        self._registered_artefacts.update({artefact_name: artefact})

    def _get_artefact(self, artefact_name: str) -> Artefact:
        self._validate_registrered(artefact_name=artefact_name)
        return self._registered_artefacts[artefact_name]

    def _delete_artefact(self, artefact_name: str) -> None:
        artefact = self._get_artefact(artefact_name)
        artefact.filesystem.rm(artefact.path, recursive=True)
        del self._registered_artefacts[artefact_name]


class DatasetMaster(ArtefactMaster):
    def __init__(self, root_path: str, filesystem: AbstractFileSystem) -> None:
        registered_artefacts = {
            PurePath(dataset_path).name: Dataset(
                path=dataset_path, filesystem=filesystem
            )
            for dataset_path in filesystem.ls(root_path)
            if filesystem.isdir(dataset_path)
        }

        super().__init__(root_path=root_path, registered_artefacts=registered_artefacts)

    def get_dataset(self, dataset_name: str) -> Dataset:
        return self._get_artefact(artefact_name=dataset_name)

    def list_datasets(
        self, as_datasets: bool = False
    ) -> Union[List[str], List[Dataset]]:
        return self._list_artefacts(as_artefacts=as_datasets)

    def register_dataset(self, dataset_name: str):
        dataset = Dataset(path=f"{self._root_path}/{dataset_name}")
        self._register_artefact(artefact_name=dataset_name, artefact=dataset)

    def delete_dataset(self, dataset_name: str) -> None:
        return self._delete_artefact(artefact_name=dataset_name)
