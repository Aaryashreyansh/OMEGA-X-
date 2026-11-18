# OMEGA-X TITAN

DAG orchestration with critical-chain scheduling, disk-backed content-addressed storage, persistent task recovery, authenticated remote workers, and lazy shared-CAS streaming.

> Engineering prototype. Remote worker RPC is bounded JSON over TCP with bearer-token authentication. Use a trusted private network/VPN or TLS termination. The shared CAS mode requires the coordinator and each worker to mount the same durable filesystem path.

## Run tests
```bash
python -m pip install -e ".[dev]"
pytest -q
```

## Local demo
```bash
python -m omega_x --state-dir .omega-state
```

## Shared CAS worker
The coordinator and worker must both see the same shared directory (for example, a correctly configured private NFS mount). Set the same path on both machines.

Worker:
```bash
export OMEGA_WORKER_TOKEN='replace-with-a-random-secret-of-at-least-24-characters'
export OMEGA_CAS_ROOT='/mnt/omega-shared/cas'
python -m omega_x.worker --host 10.0.0.12 --port 8765 --cas-root "$OMEGA_CAS_ROOT"
```

Coordinator example:
```python
import asyncio
from omega_x.distributed import RemoteWorkerPool, WorkerEndpoint

async def main():
    pool = RemoteWorkerPool([
        WorkerEndpoint("10.0.0.12", 8765, "replace-with-the-same-secret")
    ])
    # The task payload crossing RPC is a digest reference, not the blob.
    result = await pool.execute("hash_stream", {
        "blob": {"$cas": "PUT_THE_64_CHARACTER_SHA256_DIGEST_HERE"}
    })
    print(result)

asyncio.run(main())
```

A worker operation can receive a lazy `CASBlob` object and iterate `blob.iter_chunks()`. The iterator validates SHA-256 only when fully exhausted. Register the operation in the worker's allowlisted `OperationRegistry`; it is not enabled by default in the demo registry.

## CAS reference protocol
- The wire marker is exactly `{"$cas": "<sha256-digest>"}`.
- The worker resolves it to a lazy `CASBlob`; payload bytes are not serialized into RPC.
- The coordinator must place the blob in shared CAS before dispatch.
- The shared filesystem must provide durable, coherent file visibility across hosts. Local disk paths on different machines are not shared storage.
- Current RPC messages remain limited to 1 MiB. Large data stays in CAS and only small JSON metadata/digests cross the network.
- Stream consumers must exhaust the iterator to complete digest verification.

## Security and reliability limits
- Bearer-token TCP is not encrypted. Use VPN/private networking or TLS.
- Worker operation registry is an allowlist; do not register arbitrary code execution.
- Side effects may repeat after a lost response; design operations to be idempotent.
- The task journal is coordinator-local. Distributed leases, consensus, and multi-coordinator recovery are not implemented.
- This prototype does not claim production readiness or validated 1GB throughput. Benchmark on the target shared filesystem before setting capacity expectations.

## CAS streaming benchmark
Run a synthetic 1 GiB benchmark (requires enough free disk space; generated incrementally, not held in RAM):
```bash
python benchmarks/cas_stream_benchmark.py --size-mib 1024 --chunks-kib 64 256 1024 4096
```
For a quick smoke benchmark use `--size-mib 32 --chunks-kib 64 256`. The script reports write/read MiB/s, Python-traced peak allocation, and digest verification. Results are hardware/filesystem-specific; do not treat one run as a production capacity guarantee. Benchmark cases use separate temporary CAS directories and clean them up afterward.


## Live worker health dashboard

Workers expose an authenticated `__omega_health__` heartbeat RPC. The coordinator records the last successful heartbeat, marks transport failures unhealthy, and automatically makes a worker eligible again after its cooldown.

Configure a shared worker token and comma-separated endpoints:
```bash
export OMEGA_WORKER_TOKEN='replace-with-a-random-secret-of-at-least-24-characters'
export OMEGA_WORKERS='127.0.0.1:8765,10.0.0.12:8765'
python -m omega_x.monitor --interval 5
```

Run one scan and exit:
```bash
python -m omega_x.monitor --once
```

The dashboard is a terminal monitor, not a web UI. It never prints authentication tokens. Keep worker RPC on a trusted private network or behind TLS/VPN.
