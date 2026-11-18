from __future__ import annotations

import asyncio
import itertools
import json
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerEndpoint:
    host: str
    port: int
    token: str


class RemoteWorkerPool:
    """Round-robin RPC client with temporary health-based worker exclusion.

    Transport failures put a worker into cooldown. Application-level errors
    (for example an unknown operation or rejected token) do not affect health.
    Workers become eligible again automatically when their cooldown expires.
    """

    def __init__(
        self,
        workers: list[WorkerEndpoint],
        timeout: float = 30.0,
        failure_cooldown: float = 5.0,
    ):
        if not workers:
            raise ValueError("At least one remote worker is required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if failure_cooldown < 0:
            raise ValueError("failure_cooldown must be >= 0")
        for worker in workers:
            if not worker.host or not (1 <= worker.port <= 65535):
                raise ValueError("Invalid worker endpoint")
            if not worker.token or len(worker.token) < 24:
                raise ValueError("Worker token must contain at least 24 characters")
        self.workers = tuple(workers)
        self.timeout = timeout
        self.failure_cooldown = failure_cooldown
        self._cycle = itertools.cycle(self.workers)
        self._lock = asyncio.Lock()
        self._unhealthy_until: dict[WorkerEndpoint, float] = {}
        self._last_error: dict[WorkerEndpoint, str] = {}
        self._last_seen: dict[WorkerEndpoint, float] = {}

    async def _select_worker(self) -> WorkerEndpoint:
        async with self._lock:
            now = time.monotonic()
            eligible = [
                worker for worker in self.workers
                if self._unhealthy_until.get(worker, 0.0) <= now
            ]
            if not eligible:
                # Do not deadlock when every worker is cooling down. Retry the
                # worker whose cooldown expires first; its connection may recover.
                earliest = min(self._unhealthy_until.get(w, 0.0) for w in self.workers)
                eligible = [
                    worker for worker in self.workers
                    if self._unhealthy_until.get(worker, 0.0) == earliest
                ]
            eligible_ids = {id(worker) for worker in eligible}
            for _ in range(len(self.workers)):
                candidate = next(self._cycle)
                if id(candidate) in eligible_ids:
                    return candidate
            return eligible[0]

    async def _mark_unhealthy(self, worker: WorkerEndpoint, exc: Exception) -> None:
        async with self._lock:
            self._unhealthy_until[worker] = time.monotonic() + self.failure_cooldown
            self._last_error[worker] = f"{type(exc).__name__}: {exc}"

    async def health_snapshot(self) -> list[dict]:
        """Return current worker health without exposing authentication tokens."""
        async with self._lock:
            now = time.monotonic()
            return [
                {
                    "host": worker.host,
                    "port": worker.port,
                    "healthy": self._unhealthy_until.get(worker, 0.0) <= now,
                    "cooldown_remaining": max(
                        0.0, self._unhealthy_until.get(worker, 0.0) - now
                    ),
                    "last_error": self._last_error.get(worker),
                    "last_seen": self._last_seen.get(worker),
                }
                for worker in self.workers
            ]

    async def heartbeat(self, worker: WorkerEndpoint) -> bool:
        """Ping one worker using the authenticated health RPC."""
        try:
            result = await self._execute_on(worker, "__omega_health__", {})
            if not isinstance(result, dict) or result.get("status") != "ok":
                raise ConnectionError("Worker returned an invalid heartbeat")
            async with self._lock:
                self._unhealthy_until.pop(worker, None)
                self._last_error.pop(worker, None)
                self._last_seen[worker] = time.time()
            return True
        except (OSError, asyncio.TimeoutError, ConnectionError, json.JSONDecodeError, RuntimeError) as exc:
            await self._mark_unhealthy(worker, exc)
            return False

    async def check_workers(self) -> list[dict]:
        """Heartbeat every configured worker and return a fresh health snapshot."""
        await asyncio.gather(*(self.heartbeat(worker) for worker in self.workers))
        return await self.health_snapshot()

    async def _execute_on(self, worker: WorkerEndpoint, operation: str, inputs: dict):
        writer = None
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(worker.host, worker.port), timeout=self.timeout)
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
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
    async def execute(self, operation: str, inputs: dict):
        worker = await self._select_worker()
        try:
            result = await self._execute_on(worker, operation, inputs)
            async with self._lock:
                self._last_seen[worker] = time.time()
                self._last_error.pop(worker, None)
                self._unhealthy_until.pop(worker, None)
            return result
        except (OSError, asyncio.TimeoutError, ConnectionError, json.JSONDecodeError) as exc:
            await self._mark_unhealthy(worker, exc)
            raise
