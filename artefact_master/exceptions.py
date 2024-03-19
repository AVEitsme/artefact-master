from typing import Dict

from artefact_master.artefacts import Artefact


class NotRegisteredError(Exception):
    """Raised when an artefact doesn't exists in the registered artefacts."""

    def __init__(
        self, artefact_name: str, registered_artefacts: Dict[str, Artefact]
    ) -> None:
        self.artefact_name = artefact_name
        self.registered_artefacts = registered_artefacts
        self.message = f"Artefact {artefact_name} doesn't exists in registered artefacts: {registered_artefacts}."


class AlreadyRegisteredError(Exception):
    """Raised when an artefact already exists in the registered artefacts."""

    def __init__(self, artefact_name: str) -> None:
        self.artefact_name = artefact_name
        self.message = (
            f"Artefact {artefact_name} already exists in registered artefacts."
        )


class ArtefactNotFoundError(Exception):
    """Raised when an artefact doesn't exists in the filesystem."""

    def __init__(self, artefact_path: str) -> None:
        self.artefact_path = artefact_path
        self.message = f"Artefact doesn't exists at given path: {artefact_path}."
