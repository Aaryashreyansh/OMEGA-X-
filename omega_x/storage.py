from __future__ import annotations
import hashlib
import json
import os
import sqlite3
import tempfile
import threading
from pathlib import Path
from typing import Any, Iterable, Iterator

class ContentAddressedStorage:
    """Disk CAS. SQLite stores only object keys and digests, never payload bytes.
    get() verifies the digest after the iterator is completely consumed.
    """
    def __init__(self, root: str | Path = "omega_data", chunk_size: int = 1024 * 1024):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.root = Path(root)
        self.blobs = self.root / "blobs"
        self.blobs.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.root / "metadata.db", check_same_thread=False)
        with self._lock:
            self._db.execute("""CREATE TABLE IF NOT EXISTS objects (
                object_key TEXT PRIMARY KEY, digest TEXT NOT NULL)""")
            self._db.commit()

    @staticmethod
    def _validate_digest(digest: str) -> None:
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Invalid SHA-256 digest")

    def _path(self, digest: str) -> Path:
        self._validate_digest(digest)
        return self.blobs / digest[:2] / digest[2:]

    def put(self, chunks: Iterable[bytes]) -> str:
        hasher = hashlib.sha256()
        fd, name = tempfile.mkstemp(prefix=".omega-", dir=self.blobs)
        temp = Path(name)
        try:
            with os.fdopen(fd, "wb") as stream:
                for chunk in chunks:
                    if not isinstance(chunk, bytes):
                        raise TypeError("CAS accepts bytes chunks only")
                    hasher.update(chunk)
                    stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            digest = hasher.hexdigest()
            target = self._path(digest)
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(temp, target)
            except FileExistsError:
                pass
            return digest
        finally:
            temp.unlink(missing_ok=True)

    def put_bytes(self, payload: bytes) -> str:
        return self.put((payload,))

    def get(self, digest: str) -> Iterator[bytes]:
        path = self._path(digest)
        hasher = hashlib.sha256()
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(self.chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
                yield chunk
        if hasher.hexdigest() != digest:
            raise ValueError("Blob integrity verification failed")

    def bind(self, object_key: str, digest: str) -> None:
        self._validate_digest(digest)
        if not self._path(digest).is_file():
            raise FileNotFoundError(digest)
        with self._lock:
            self._db.execute("""INSERT INTO objects(object_key,digest) VALUES (?,?)
                ON CONFLICT(object_key) DO UPDATE SET digest=excluded.digest""",
                (object_key, digest))
            self._db.commit()

    def lookup(self, object_key: str) -> str | None:
        with self._lock:
            row = self._db.execute("SELECT digest FROM objects WHERE object_key=?",
                                   (object_key,)).fetchone()
        return row[0] if row else None

    def put_json(self, value: Any) -> str:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"),
                             allow_nan=False).encode("utf-8")
        return self.put_bytes(payload)

    def get_json(self, digest: str) -> Any:
        # Small JSON values only; large payloads must use get() as a stream.
        return json.loads(b"".join(self.get(digest)))

    def close(self) -> None:
        with self._lock:
            self._db.close()
