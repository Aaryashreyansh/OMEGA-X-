from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .storage import ContentAddressedStorage


@dataclass(frozen=True)
class CASBlob:
    """A lazy reference to a shared CAS object; never materializes the payload."""
    digest: str
    storage: ContentAddressedStorage

    def iter_chunks(self) -> Iterator[bytes]:
        yield from self.storage.get(self.digest)


def resolve_cas_references(value, storage: ContentAddressedStorage):
    """Resolve the exact {$cas: digest} marker into a lazy CASBlob."""
    if isinstance(value, dict):
        if set(value) == {"$cas"}:
            digest = value["$cas"]
            if not isinstance(digest, str):
                raise ValueError("CAS reference digest must be a string")
            # Validate existence before dispatch; integrity is checked while consumed.
            path = storage._path(digest)
            if not path.is_file():
                raise FileNotFoundError(f"CAS object not found: {digest}")
            return CASBlob(digest, storage)
        return {key: resolve_cas_references(item, storage) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_cas_references(item, storage) for item in value]
    return value
