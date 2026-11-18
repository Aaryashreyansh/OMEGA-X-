import hashlib
import pytest

from omega_x.storage import ContentAddressedStorage


def test_streaming_put_get_does_not_require_joining_payload(tmp_path):
    cas = ContentAddressedStorage(tmp_path / "cas", chunk_size=4096)
    chunks = [b"a" * 4096, b"b" * 4096, b"c" * 17]
    digest = cas.put(iter(chunks))
    received = bytearray()
    for chunk in cas.get(digest):
        received.extend(chunk)
    assert bytes(received) == b"".join(chunks)
    assert hashlib.sha256(received).hexdigest() == digest
    cas.close()


def test_corrupt_blob_fails_integrity_check_after_stream_consumption(tmp_path):
    cas = ContentAddressedStorage(tmp_path / "cas", chunk_size=1024)
    digest = cas.put_bytes(b"integrity-check-payload")
    path = cas._path(digest)
    path.write_bytes(b"tampered-data")
    iterator = cas.get(digest)
    assert b"".join(iterator) == b"tampered-data"
    # Generator raises when fully consumed and final digest comparison runs.
    with pytest.raises(ValueError, match="integrity"):
        next(iterator)
    cas.close()
