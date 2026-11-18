from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import os
from typing import Any

from .engine import OperationRegistry, demo_registry

MAX_MESSAGE_BYTES = 1_048_576


async def serve_worker(
    registry: OperationRegistry,
    token: str,
    host: str = "127.0.0.1",
    port: int = 8765,
    max_concurrency: int = 4,
):
    """Serve allowlisted operations over a bounded newline-delimited JSON RPC.

    This protocol uses a bearer token over TCP and is intended for a trusted
    private network/VPN or a TLS-terminating proxy. Do not expose it to the
    public internet as plain TCP.
    """
    if not token or len(token) < 24:
        raise ValueError("Worker token must contain at least 24 characters")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")
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
                async with semaphore:
                    try:
                        value = await asyncio.to_thread(registry.execute, operation, inputs)
                        # Enforce the wire contract before sending a success response.
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

    return await asyncio.start_server(handle, host, port, limit=MAX_MESSAGE_BYTES + 1)


async def _run_cli():
    parser = argparse.ArgumentParser(description="OMEGA-X TITAN remote worker")
    parser.add_argument("--host", default=os.getenv("OMEGA_WORKER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("OMEGA_WORKER_PORT", "8765")))
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    token = os.getenv("OMEGA_WORKER_TOKEN")
    if not token:
        raise SystemExit("Set OMEGA_WORKER_TOKEN to a random secret (at least 24 characters).")
    server = await serve_worker(demo_registry(), token, args.host, args.port, args.concurrency)
    addresses = ", ".join(str(sock.getsockname()) for sock in (server.sockets or []))
    print(f"OMEGA-X worker listening on {addresses}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(_run_cli())
