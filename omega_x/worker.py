from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import os
from pathlib import Path

from .cas_protocol import resolve_cas_references
from .engine import OperationRegistry, demo_registry
from .storage import ContentAddressedStorage

MAX_MESSAGE_BYTES = 1_048_576


async def serve_worker(
    registry: OperationRegistry,
    token: str,
    host: str = "127.0.0.1",
    port: int = 8765,
    max_concurrency: int = 4,
    cas_root: str | Path | None = None,
):
    """Serve bounded JSON RPC; CAS references are resolved lazily from shared storage.

    The coordinator and worker must mount the same CAS directory. The RPC sends
    only the digest, never the blob bytes. Use a trusted private network/VPN or TLS.
    """
    if not token or len(token) < 24:
        raise ValueError("Worker token must contain at least 24 characters")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")
    storage = ContentAddressedStorage(cas_root) if cas_root is not None else None
    semaphore = asyncio.Semaphore(max_concurrency)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=10)
            if not raw or len(raw) > MAX_MESSAGE_BYTES or not raw.endswith(b"\n"):
                raise ValueError("Invalid or oversized request")
            request = json.loads(raw)
            supplied = request.get("token", "")
            if not isinstance(supplied, str) or not hmac.compare_digest(supplied, token):
                response = {"ok": False, "error": "Unauthorized"}
            else:
                operation = request.get("operation")
                inputs = request.get("inputs")
                if not isinstance(operation, str) or not isinstance(inputs, dict):
                    raise ValueError("Request requires operation string and inputs object")
                if operation == "__omega_health__":
                    response = {"ok": True, "value": {"status": "ok", "protocol": 1}}
                else:
                    async with semaphore:
                        try:
                            if storage is not None:
                                inputs = resolve_cas_references(inputs, storage)
                            elif any(isinstance(v, dict) and "$cas" in v for v in inputs.values()):
                                raise ValueError("Worker has no CAS mount configured")
                            value = await asyncio.to_thread(registry.execute, operation, inputs)
                            encoded = json.dumps({"ok": True, "value": value},
                                                 separators=(",", ":"), allow_nan=False)
                            response = json.loads(encoded)
                        except Exception as exc:
                            response = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
            writer.write((json.dumps(response, separators=(",", ":"), allow_nan=False) + "\n").encode())
            await writer.drain()
        except (ValueError, json.JSONDecodeError, asyncio.TimeoutError):
            try:
                writer.write(b'{"ok":false,"error":"Bad request"}\n')
                await writer.drain()
            except (ConnectionError, OSError):
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    server = await asyncio.start_server(handle, host, port, limit=MAX_MESSAGE_BYTES + 1)
    if storage is not None:
        server._omega_storage = storage
    return server


async def _run_cli():
    parser = argparse.ArgumentParser(description="OMEGA-X TITAN remote worker")
    parser.add_argument("--host", default=os.getenv("OMEGA_WORKER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("OMEGA_WORKER_PORT", "8765")))
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--cas-root", default=os.getenv("OMEGA_CAS_ROOT", ".omega-cas"))
    args = parser.parse_args()
    token = os.getenv("OMEGA_WORKER_TOKEN")
    if not token:
        raise SystemExit("Set OMEGA_WORKER_TOKEN to a random secret (at least 24 characters).")
    registry = demo_registry()
    server = await serve_worker(registry, token, args.host, args.port,
                                args.concurrency, args.cas_root)
    addresses = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    print(f"OMEGA-X worker listening on {addresses}; CAS root={args.cas_root}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(_run_cli())
