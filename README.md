# OMEGA-X TITAN

DAG task orchestration with critical-chain scheduling, disk-backed SHA-256 content-addressed storage, and an optional persistent execution journal.

> Engineering prototype. Persistent single-process recovery and JSON-compatible output caching are implemented as an initial V2 milestone. Distributed workers, multi-process locking, end-to-end streaming task transforms, and production-scale benchmarks are not implemented or claimed.

## Requirements
- Python 3.11+
- Runtime uses the Python standard library
- Tests use pytest and pytest-asyncio

## Run in GitHub Codespaces
```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m omega_x --state-dir .omega-state
pytest -q
```

The default state directory is `.omega-state`. Re-running the same unchanged task graph restores successful task outputs from CAS and skips those tasks. Use a fresh state directory when intentionally changing task definitions, for example `--state-dir .omega-state-new`.

## Modules
- `omega_x/storage.py`: disk CAS; SQLite stores object keys and digests, not payload bytes.
- `omega_x/persistence.py`: SQLite task journal; successful JSON-compatible outputs are stored in CAS and referenced by digest.
- `omega_x/dag.py`: DAG validation, longest downstream chain priority, deterministic `asyncio.PriorityQueue`.
- `omega_x/engine.py`: bounded async execution, retries, dependency failure propagation, and optional persistent recovery.
- `omega_x/verify.py`: independent report consistency checks.

## Recovery contract
- A task is marked `RUNNING` before its worker starts.
- A successful output is written to CAS before its journal row is committed as `SUCCESS`.
- On restart, `SUCCESS` outputs are restored from CAS and are not re-executed.
- A task left `RUNNING` by an interrupted process is reset to `PENDING` and may run again.
- Task definitions are fingerprinted. Changing a task while reusing the same state directory raises an error instead of silently reusing stale output.
- Successful outputs in persistent mode must be JSON-serializable.
- Exactly-once side effects are not guaranteed: an interrupted task may have completed an external side effect before its success was journaled. Operations with external side effects should be idempotent.

## Streaming integrity contract
`ContentAddressedStorage.put(chunks)` consumes byte chunks. `get(digest)` yields chunks and verifies SHA-256 only after the iterator is fully exhausted. A consumer must finish iteration before treating the payload as verified. Use `get()` for streaming; `get_json()` is only for small structured values.

## Current limitations
- The journal's SQLite lock is process-local; multi-process and distributed coordination need a separate locking/lease design.
- A timed-out thread-backed Python operation may continue running because Python cannot forcibly stop that thread.
- The engine currently passes dependency values as Python objects; end-to-end streaming task transforms are a planned extension.
- No claim of distributed execution, 1GB+ end-to-end processing, or production readiness is made without dedicated tests and measurements.
