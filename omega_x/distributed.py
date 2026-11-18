from __future__ import annotations

import asyncio
import itertools
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerEndpoint:
    host: str
    port: int
    token: str


class RemoteWorkerPool:
    """Round-robin RPC client for independently running OMEGA-X workers."""
    def __init__(self, workers: list[WorkerEndpoint], timeout: float = 30.0):
        if not workers:
            raise ValueError("At least one remote worker is required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        for worker in workers:
            if not worker.host or not (1 <= worker.port <= 65535):
                raise ValueError("Invalid worker endpoint")
            if not worker.token or len(worker.token) < 24:
                raise ValueError("Worker token must contain at least 24 characters")
        self.workers = tuple(workers)
        self.timeout = timeout
        self._cycle = itertools.cycle(self.workers)
        self._lock = asyncio.Lock()

    async def execute(self, operation: str, inputs: dict):
        async with self._lock:
            worker = next(self._cycle)
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(worker.host, worker.port), timeout=self.timeout)
        try:
            request = {"token": worker.token, "operation": operation, "inputs": inputs}
            payload = (json.dumps(request, separators=(",", ":"), allow_nan=False) + "\n").encode()
            if len(payload) > 1_048_576:
                raise ValueError("Remote task request exceeds 1 MiB; pass a CAS reference instead")
            writer.write(payload)
            await asyncio.wait_for(writer.drain(), timeout=self.timeout)
            raw = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
            if not raw or len(raw) > 1_048_576:
                raise ConnectionError("Worker returned an invalid response")
            response = json.loads(raw)
            if not response.get("ok"):
                raise RuntimeError(response.get("error", "Remote worker failed"))
            return response.get("value")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
