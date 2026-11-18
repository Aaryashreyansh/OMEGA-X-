import hashlib
import pytest

from omega_x.cas_protocol import CASBlob
from omega_x.dag import Task
from omega_x.distributed import RemoteWorkerPool, WorkerEndpoint
from omega_x.engine import OmegaEngine, OperationRegistry
from omega_x.storage import ContentAddressedStorage
from omega_x.worker import serve_worker

TOKEN = "test-secret-token-with-at-least-24-chars"

@pytest.mark.asyncio
async def test_remote_worker_streams_shared_cas_blob_without_rpc_payload(tmp_path):
    # In production these paths are the same shared mount on coordinator and worker.
    coordinator_cas = ContentAddressedStorage(tmp_path / "shared")
    payload = (b"OMEGA-X-TITAN-" * 500_000)  # 7 MB, intentionally larger than RPC limit.
    digest = coordinator_cas.put((payload[i:i+131071] for i in range(0, len(payload), 131071)))

    registry = OperationRegistry()
    def hash_stream(blob: CASBlob):
        h = hashlib.sha256()
        total = 0
        for chunk in blob.iter_chunks():
            h.update(chunk)
            total += len(chunk)
        return {"sha256": h.hexdigest(), "bytes": total}
    registry.register("hash_stream", hash_stream)

    server = await serve_worker(registry, TOKEN, "127.0.0.1", 0, 2,
                                cas_root=tmp_path / "shared")
    port = server.sockets[0].getsockname()[1]
    async with server:
        pool = RemoteWorkerPool([WorkerEndpoint("127.0.0.1", port, TOKEN)])
        # Only a short digest crosses the wire, not the 7 MB payload.
        result = await pool.execute("hash_stream", {"blob": {"$cas": digest}})
        assert result == {"sha256": hashlib.sha256(payload).hexdigest(),
                          "bytes": len(payload)}
    coordinator_cas.close()
    server._omega_storage.close()

@pytest.mark.asyncio
async def test_worker_rejects_cas_reference_without_cas_mount():
    registry = OperationRegistry()
    registry.register("identity", lambda value: value)
    server = await serve_worker(registry, TOKEN, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        pool = RemoteWorkerPool([WorkerEndpoint("127.0.0.1", port, TOKEN)])
        with pytest.raises(RuntimeError, match="no CAS mount"):
            await pool.execute("identity", {"value": {"$cas": "a" * 64}})
