import asyncio
import pytest

from omega_x.dag import Task
from omega_x.distributed import RemoteWorkerPool, WorkerEndpoint
from omega_x.engine import OmegaEngine, OperationRegistry
from omega_x.worker import serve_worker

TOKEN = "test-secret-token-with-at-least-24-chars"

@pytest.mark.asyncio
async def test_remote_worker_executes_registered_operation():
    registry = OperationRegistry()
    registry.register("sum", lambda a, b: a + b)
    server = await serve_worker(registry, TOKEN, "127.0.0.1", 0, 2)
    port = server.sockets[0].getsockname()[1]
    async with server:
        pool = RemoteWorkerPool([WorkerEndpoint("127.0.0.1", port, TOKEN)])
        assert await pool.execute("sum", {"a": 19, "b": 23}) == 42

@pytest.mark.asyncio
async def test_remote_worker_rejects_bad_token():
    registry = OperationRegistry()
    registry.register("sum", lambda a, b: a + b)
    server = await serve_worker(registry, TOKEN, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        pool = RemoteWorkerPool([WorkerEndpoint("127.0.0.1", port,
                                                "wrong-secret-token-which-is-long")])
        with pytest.raises(RuntimeError, match="Unauthorized"):
            await pool.execute("sum", {"a": 1, "b": 2})

@pytest.mark.asyncio
async def test_engine_can_dispatch_tasks_to_remote_worker():
    registry = OperationRegistry()
    registry.register("sum", lambda a, b: a + b)
    registry.register("double", lambda value: value * 2)
    server = await serve_worker(registry, TOKEN, "127.0.0.1", 0, 2)
    port = server.sockets[0].getsockname()[1]
    async with server:
        pool = RemoteWorkerPool([WorkerEndpoint("127.0.0.1", port, TOKEN)])
        tasks = [
            Task("A", "sum", {"a": 20, "b": 1}),
            Task("B", "double", {"value": {"from": "A"}}, ("A",)),
        ]
        engine = OmegaEngine(tasks, concurrency=2, worker_pool=pool)
        report = await engine.execute()
        assert report["results"]["B"]["value"] == 42
        assert report["summary"]["completed"] == 2


@pytest.mark.asyncio
async def test_engine_retries_failed_worker_and_fails_over_to_next_worker():
    import socket

    # Reserve then release a local port to create a deterministic unavailable endpoint.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        unavailable_port = sock.getsockname()[1]

    registry = OperationRegistry()
    registry.register("sum", lambda a, b: a + b)
    server = await serve_worker(registry, TOKEN, "127.0.0.1", 0, 2)
    healthy_port = server.sockets[0].getsockname()[1]

    async with server:
        pool = RemoteWorkerPool([
            WorkerEndpoint("127.0.0.1", unavailable_port, TOKEN),
            WorkerEndpoint("127.0.0.1", healthy_port, TOKEN),
        ])
        task = Task("retry", "sum", {"a": 20, "b": 22}, retries=1)
        engine = OmegaEngine([task], concurrency=1, worker_pool=pool)
        report = await engine.execute()

        assert report["results"]["retry"]["status"] == "SUCCESS"
        assert report["results"]["retry"]["value"] == 42
        assert report["results"]["retry"]["attempts"] == 2
        assert report["summary"]["completed"] == 1

        health = await pool.health_snapshot()
        assert health[0]["healthy"] is False
        assert health[0]["last_error"] is not None
        assert health[1]["healthy"] is True
