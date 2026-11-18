from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
import tracemalloc
from pathlib import Path

from omega_x.storage import ContentAddressedStorage


def generated_chunks(total_bytes: int, chunk_size: int):
    remaining = total_bytes
    seed = b"OMEGA-X-TITAN-BENCHMARK-"
    while remaining:
        size = min(chunk_size, remaining)
        repeats = (size + len(seed) - 1) // len(seed)
        yield (seed * repeats)[:size]
        remaining -= size


def run_case(root: Path, total_bytes: int, chunk_size: int):
    cas = ContentAddressedStorage(root, chunk_size=chunk_size)
    tracemalloc.start()
    started = time.perf_counter()
    digest = cas.put(generated_chunks(total_bytes, chunk_size))
    write_seconds = time.perf_counter() - started

    started = time.perf_counter()
    actual = hashlib.sha256()
    read_bytes = 0
    for chunk in cas.get(digest):
        actual.update(chunk)
        read_bytes += len(chunk)
    read_seconds = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    cas.close()

    if read_bytes != total_bytes or actual.hexdigest() != digest:
        raise RuntimeError("Streaming verification failed")

    return {
        "bytes": total_bytes,
        "chunk_size": chunk_size,
        "sha256": digest,
        "write_mib_s": round(total_bytes / max(write_seconds, 1e-9) / 2**20, 2),
        "read_mib_s": round(total_bytes / max(read_seconds, 1e-9) / 2**20, 2),
        "python_peak_mib": round(peak / 2**20, 2),
        "integrity": "PASS",
    }


def main():
    parser = argparse.ArgumentParser(description="OMEGA-X CAS streaming benchmark")
    parser.add_argument("--size-mib", type=int, default=1024,
                        help="Synthetic payload size in MiB (default: 1024)")
    parser.add_argument("--chunks-kib", type=int, nargs="+", default=[64, 256, 1024, 4096])
    parser.add_argument("--directory", default=None,
                        help="Optional benchmark directory; defaults to a temporary directory")
    args = parser.parse_args()
    if args.size_mib < 1 or any(value < 1 for value in args.chunks_kib):
        parser.error("size and chunk sizes must be positive")
    total_bytes = args.size_mib * 2**20
    results = []
    with tempfile.TemporaryDirectory(dir=args.directory) as temp:
        for chunk_kib in args.chunks_kib:
            case_root = Path(temp) / f"chunk-{chunk_kib}"
            results.append(run_case(case_root, total_bytes, chunk_kib * 1024))
    print(json.dumps({"benchmark": "omega-x-cas-streaming", "results": results}, indent=2))


if __name__ == "__main__":
    main()
