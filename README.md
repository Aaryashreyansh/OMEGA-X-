# OMEGA-X TITAN

DAG orchestration with critical-chain scheduling, disk-backed content-addressed storage, persistent task recovery, and an initial remote-worker RPC layer.

> Engineering prototype. Remote workers use a bearer token over newline-delimited JSON/TCP. Deploy only on a trusted private network/VPN or behind a TLS-terminating proxy. This is not a production-hardened distributed scheduler.

## Run tests
```bash
python -m pip install -e ".[dev]"
pytest -q
```

## Run the local demo with persistent state
```bash
python -m omega_x --state-dir .omega-state
```

## Start a remote worker
On a worker machine, install the repository and set a unique random token (at least 24 characters):
```bash
export OMEGA_WORKER_TOKEN='replace-with-a-long-random-secret'
python -m omega_x.worker --host 0.0.0.0 --port 8765 --concurrency 4
```
Bind only to an address reachable inside your private network. Do not expose plain TCP bearer-token traffic to the public internet.

## Connect an engine to remote workers
```python
import asyncio
from omega_x.dag import Task
from omega_x.distributed import RemoteWorkerPool, WorkerEndpoint
from omega_x.engine import OmegaEngine

async def main():
    pool = RemoteWorkerPool([
        WorkerEndpoint("10.0.0.12", 8765, "replace-with-the-same-long-random-secret"),
        WorkerEndpoint("10.0.0.13", 8765, "replace-with-the-same-long-random-secret"),
    ])
    tasks = [Task("sum", "add", {"a": 20, "b": 22})]
    engine = OmegaEngine(tasks, concurrency=8, worker_pool=pool)
    print(await engine.execute())

asyncio.run(main())
```

The example assumes the worker's registry has the requested operation registered. The built-in CLI worker exposes only the demo operations: `add`, `multiply`, and `sha256_text`.

## Architecture and limits
- `omega_x/dag.py`: dependency validation and critical-chain priority.
- `omega_x/storage.py`: chunked SHA-256 CAS. Fully consume `get()` to complete integrity verification.
- `omega_x/persistence.py`: SQLite task journal; outputs stored in CAS by digest.
- `omega_x/worker.py`: bounded authenticated worker server with an explicit operation registry.
- `omega_x/distributed.py`: round-robin remote worker client.
- `omega_x/engine.py`: local engine with optional remote worker dispatch.
- RPC inputs and outputs are JSON-serializable and limited to 1 MiB per request/response line. Large payloads must be staged in shared/object storage and passed as digest references; the current worker protocol does not transfer 1GB payloads.
- Workers must have compatible operation implementations and access to any referenced shared data.
- Remote task retries can repeat side effects if a worker completed a task but the response was lost. Make side-effecting operations idempotent.
- Persistent journal state is local to the coordinator; multi-coordinator leases and distributed consensus are not implemented.
- Use TLS/VPN for network confidentiality. The bearer token alone does not encrypt traffic.
- A thread-backed operation may continue after its asyncio timeout.
