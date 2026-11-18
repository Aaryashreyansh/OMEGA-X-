from __future__ import annotations

import argparse
import asyncio
import os
import time
from datetime import datetime, timezone

from .distributed import RemoteWorkerPool, WorkerEndpoint


def load_workers() -> list[WorkerEndpoint]:
    token = os.getenv("OMEGA_WORKER_TOKEN", "")
    if len(token) < 24:
        raise SystemExit("Set OMEGA_WORKER_TOKEN (minimum 24 characters).")
    raw = os.getenv("OMEGA_WORKERS", "")
    if not raw.strip():
        raise SystemExit("Set OMEGA_WORKERS, e.g. 127.0.0.1:8765,10.0.0.12:8765.")
    workers = []
    for item in raw.split(","):
        host, separator, port = item.strip().rpartition(":")
        if not separator or not host:
            raise SystemExit(f"Invalid worker endpoint: {item!r}")
        workers.append(WorkerEndpoint(host, int(port), token))
    return workers


async def monitor(interval: float, once: bool) -> None:
    pool = RemoteWorkerPool(load_workers(), timeout=3.0, failure_cooldown=interval)
    while True:
        rows = await pool.check_workers()
        print("\033[2J\033[H", end="")
        print("OMEGA-X TITAN | WORKER HEALTH MONITOR")
        print("Updated:", datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"))
        print("-" * 92)
        print(f"{'WORKER':32} {'STATUS':12} {'LAST HEARTBEAT':22} LAST ERROR")
        print("-" * 92)
        for row in rows:
            endpoint = f"{row['host']}:{row['port']}"
            status = "HEALTHY" if row["healthy"] else "UNHEALTHY"
            seen = row["last_seen"]
            seen_text = datetime.fromtimestamp(seen).astimezone().strftime("%H:%M:%S") if seen else "never"
            error = row["last_error"] or "-"
            print(f"{endpoint:32.32} {status:12} {seen_text:22} {error[:45]}")
        print("-" * 92)
        if once:
            return
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="OMEGA-X live worker health dashboard")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true", help="Run one heartbeat scan and exit")
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")
    try:
        asyncio.run(monitor(args.interval, args.once))
    except KeyboardInterrupt:
        print("\\nDashboard stopped.")


if __name__ == "__main__":
    main()
