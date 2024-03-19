import tempfile
import pytest

from fsspec.implementations.arrow import ArrowFSWrapper
from pyarrow.fs import LocalFileSystem

from artefact_master.artefacts import Artefact

filesystem = ArrowFSWrapper(LocalFileSystem())


@pytest.fixture
def temporary_file():
    file = tempfile.NamedTemporaryFile()
    yield file
    if not file.closed:
        file.close()


def test_file_not_found_error():

    with pytest.raises(FileNotFoundError):
        Artefact.from_path(path="path/to/file", filesystem=filesystem)


def test_path_property(temporary_file):

    temporary_file.write(b"existing test case")
    artefact = Artefact.from_path(path=temporary_file.name, filesystem=filesystem)
    assert artefact.path == temporary_file.name


def test_filesystem_property(temporary_file):

    temporary_file.write(b"existing test case")
    artefact = Artefact.from_path(path=temporary_file.name, filesystem=filesystem)
    assert artefact.filesystem == filesystem


def test_exists_property_true(temporary_file):

    temporary_file.write(b"existing test case")
    artefact = Artefact.from_path(path=temporary_file.name, filesystem=filesystem)
    assert artefact.exists == True


def test_exists_property_false(temporary_file):

    temporary_file.write(b"existing test case")
    artefact = Artefact.from_path(path=temporary_file.name, filesystem=filesystem)
    temporary_file.close()
    assert artefact.exists == False
