import hashlib
import pytest
from omega_x.storage import ContentAddressedStorage

def test_stream_roundtrip(tmp_path):
    cas = ContentAddressedStorage(tmp_path, chunk_size=3)
    payload = b"abcdefghijk"
    digest = cas.put([payload[:2], payload[2:7], payload[7:]])
    assert digest == hashlib.sha256(payload).hexdigest()
    assert b"".join(cas.get(digest)) == payload
    cas.close()

def test_sqlite_stores_digest_not_payload(tmp_path):
    cas = ContentAddressedStorage(tmp_path)
    digest = cas.put_bytes(b"payload")
    cas.bind("task:input", digest)
    assert cas.lookup("task:input") == digest
    columns = [r[1] for r in cas._db.execute("PRAGMA table_info(objects)")]
    assert "payload" not in columns
    cas.close()

def test_corruption_detected_after_full_consumption(tmp_path):
    cas = ContentAddressedStorage(tmp_path)
    digest = cas.put_bytes(b"original")
    cas._path(digest).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="integrity"):
        list(cas.get(digest))
    cas.close()

def test_invalid_digest_rejected(tmp_path):
    cas = ContentAddressedStorage(tmp_path)
    with pytest.raises(ValueError):
        list(cas.get("../bad"))
    cas.close()
