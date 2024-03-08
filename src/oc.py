from __future__ import annotations
from fsspec.implementations.arrow import AbstractFileSystem
from pathlib import Path
from typing import Union, List, Dict, Any, Callable, Optional

import pyarrow as pa
import pyarrow.dataset as ds


class NotRegisteredError(KeyError):
    ...


class AlreadyRegisteredError(KeyError): # <-- probably should be ValueError
    ...


class ObjectNotFoundError(FileNotFoundError):
    ...


class LakeObject:

    def __init__(self, path: str) -> None:
        
        self._path = path

    @property 
    def path(self) -> str:

        return self._path

    # def from_name(cls, object_name: str, root_path: str) -> LakeObject:



class Dataset(LakeObject): 
    ...
    # partitioning: Optional[List[str]] # <-- always HIVE (root_path/column=value/)
    # partitions_path: Optional[List[str]]
     
    # def pull_dataset(self) -> Any:
    #     return ds.dataset(self._path)
    # def initialize(cls, root_path: str, dataset_name: str) -> Dataset:
        
    #     dataset_path = f"{root_path}/{dataset_name}"
    #     return cls(path=dataset_path)


class ObjectMaster: 
    
    def __init__(
        self, 
        root_path: str, 
        fs: AbstractFileSystem,
        registered_objects: Dict[str, LakeObject]
    ) -> None:
        
        self._root_path = root_path
        self._fs = fs
        self._registered_objects = registered_objects

    @property
    def root_path(self) -> str:
        
        return self._root_path

    @property
    def fs(self) -> AbstractFileSystem:
        
        return self._fs

    @property
    def registered_objects(self) -> Dict[str, LakeObject]:
        
        return self._registered_objects

    def _is_registered(self, object_name) -> bool:
        
        return object_name in self._registered_objects

    def _list_objects(self, as_object: bool=False) -> Union[List[str], List[LakeObject]]:
        
        if not as_object:
            return list(self._registered_objects.keys())
        return list(self._registered_objects.values())
    
    def _call_registrered(self, object_name: str) -> None:
        
        if not self._is_registered(object_name=object_name):
            raise NotRegisteredError(
                f"Object {object_name} doesn't exists in registered objects: {self._list_objects()}"
            )

    def _register_object(self, object_name: str, object: LakeObject) -> None:
        
        if self._is_registered(object_name=object_name):
            raise AlreadyRegisteredError(f"Object {object_name} already exists in registered objects.")
        # check is exists
        # raise object not found error in that case
        self._registered_objects.update({object_name: object})

    def _get_object(self, object_name: str) -> LakeObject:
        
        self._call_registrered(object_name=object_name)
        return self._registered_objects[object_name]
    
    def _delete_object(self, object_name: str) -> None:
        
        object = self._get_object(object_name)
        self._fs.rm(object._path, recursive=True)
        del self._registered_objects[object_name]


class DatasetMaster(ObjectMaster):

    def __init__(self, root_path: str, fs: AbstractFileSystem) -> None:
        registered_objects = {
            dataset_path.split('/')[-1]: Dataset(path=dataset_path)
            for dataset_path in fs.ls(root_path)
        }
        super().__init__(
            root_path=root_path, fs=fs, registered_objects=registered_objects
        )

    def get_dataset(self, dataset_name: str) -> Dataset:

        return self._get_object(object_name=dataset_name)    

    def list_datasets(self, as_datasets: bool=False) -> Union[List[str], List[Dataset]]:
        
        return self._list_objects(as_object=as_datasets)

    def register_dataset(self, dataset_name: str):

        dataset = Dataset(path=f"{self._root_path}/{dataset_name}")
        self._register_object(object_name=dataset_name, object=dataset)

    def delete_dataset(self, dataset_name: str) -> None:
        
        return self._delete_object(object_name=dataset_name)
    
# Is there any difference between ModelMaster & OtherMaster?

class ModelMaster(ObjectMaster):
    ...

class OtherMaster(ObjectMaster):   
    ...

class LakeMaster:

    def initialize(
        cls, 
        root_path: Union[str, Path], # <-- Get from env ? 
        fs: AbstractFileSystem,
        bucket_name: str # <-- rename   
    ) -> LakeMaster:
        
        # Initialize from existing bucket
        # OR
        # Create new bucket
        # bucket/
        #   datasets/
        #   models/
        #   others/ <-- metrics, statistics, etc...
        # 

        return cls(root_path, fs)
    
    def _refresh(self) -> None: 
            ...

