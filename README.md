# OMEGA-X

Autonomous DAG task orchestration with SHA-256 content-addressed storage, streaming blob I/O, critical-path priority scheduling, and independent report verification.

> Initial engineering scaffold, not production-ready. Persistent crash recovery, safe legacy DB migration, multi-process coordination, distributed execution, and large-scale benchmark evidence are not implemented yet.

## Requirements
- Python 3.11+
- Runtime uses the Python standard library
- Tests use pytest and pytest-asyncio

## Run in GitHub Codespaces
```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
python -m omega_x
pytest -q
```

## Modules
- `omega_x/storage.py`: disk-backed CAS; SQLite stores object keys and digests, not payload bytes.
- `omega_x/dag.py`: DAG validation, longest remaining chain priority, deterministic asyncio.PriorityQueue.
- `omega_x/engine.py`: bounded async execution, retries, dependency failure propagation.
- `omega_x/verify.py`: independent report consistency checks.

## Streaming integrity contract
`ContentAddressedStorage.put(chunks)` consumes byte chunks. `get(digest)` yields chunks and verifies SHA-256 only after the iterator is fully exhausted. A consumer must finish iteration before treating the payload as verified. Use `get()` for streaming; `get_json()` is only for small structured values.

## Important limitations
- The SQLite connection lock is process-local; multi-process coordination needs more work.
- A timed-out thread-backed Python operation may continue running because Python cannot forcibly stop that thread.
- The engine currently passes dependency values as Python objects; end-to-end streaming task transforms are a planned extension.
- No claim of 1GB+ end-to-end processing, crash recovery, or production readiness is made without tests and measurements.
