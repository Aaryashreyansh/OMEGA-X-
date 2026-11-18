from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass, asdict
from typing import Any, Callable
from .dag import Task, DependencyGraph, AdvancedDAGScheduler, ReadyQueue
from .persistence import ExecutionJournal

class OperationRegistry:
    def __init__(self):
        self.operations: dict[str, Callable[..., Any]] = {}
    def register(self, name: str, function: Callable[..., Any]):
        if name in self.operations:
            raise ValueError(f"Duplicate operation: {name}")
        self.operations[name] = function
    def execute(self, name: str, inputs: dict):
        if name not in self.operations:
            raise ValueError(f"Unknown operation: {name}")
        return self.operations[name](**inputs)

@dataclass
class TaskResult:
    task_id: str
    status: str
    value: Any = None
    error: str | None = None
    attempts: int = 0
    duration: float = 0.0

class OmegaEngine:
    def __init__(self, tasks: list[Task], registry: OperationRegistry | None = None,
                 concurrency: int = 4, state_dir: str | None = None, worker_pool=None):
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.graph = DependencyGraph(tasks)
        self.scheduler = AdvancedDAGScheduler(self.graph)
        self.registry = registry or OperationRegistry()
        self.concurrency = concurrency
        self.worker_pool = worker_pool
        self.results: dict[str, TaskResult] = {}
        self.journal = ExecutionJournal(state_dir) if state_dir else None

    def _resolve(self, inputs: dict) -> dict:
        resolved = {}
        for key, value in inputs.items():
            if isinstance(value, dict) and set(value) == {"from"}:
                source = value["from"]
                result = self.results[source]
                if result.status != "SUCCESS":
                    raise RuntimeError(f"Dependency {source} failed")
                resolved[key] = result.value
            else:
                resolved[key] = value
        return resolved

    async def _execute_one(self, task: Task) -> TaskResult:
        start = time.perf_counter()
        error = None
        for attempt in range(1, task.retries + 2):
            try:
                args = self._resolve(task.inputs)
                if self.worker_pool is not None:
                    value = await asyncio.wait_for(
                        self.worker_pool.execute(task.operation, args), timeout=task.timeout)
                else:
                    value = await asyncio.wait_for(
                        asyncio.to_thread(self.registry.execute, task.operation, args),
                        timeout=task.timeout)
                return TaskResult(task.id, "SUCCESS", value=value, attempts=attempt,
                                  duration=time.perf_counter() - start)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                if attempt <= task.retries:
                    await asyncio.sleep(min(0.1 * 2 ** (attempt - 1), 2.0))
        return TaskResult(task.id, "FAILED", error=error, attempts=task.retries + 1,
                          duration=time.perf_counter() - start)

    async def execute(self):
        if self.journal:
            self.results.update(self.journal.restore(self.graph.tasks))
        pending = set(self.graph.tasks) - set(self.results)
        failed = {tid for tid, result in self.results.items()
                  if result.status in {"FAILED", "BLOCKED"}}
        running = {}
        ready = ReadyQueue(self.scheduler)
        while pending or running:
            changed = True
            while changed:
                changed = False
                for tid in list(pending):
                    task = self.graph.tasks[tid]
                    if any(dep in failed for dep in task.dependencies):
                        result = TaskResult(tid, "BLOCKED", error="Dependency failed")
                        self.results[tid] = result
                        if self.journal:
                            self.journal.save_result(task, result)
                        pending.remove(tid)
                        failed.add(tid)
                        changed = True
            for tid in sorted(pending):
                task = self.graph.tasks[tid]
                if all(dep in self.results and self.results[dep].status == "SUCCESS"
                       for dep in task.dependencies):
                    if await ready.add(task):
                        pending.remove(tid)
            while ready.qsize() and len(running) < self.concurrency:
                task = await ready.get()
                if self.journal:
                    self.journal.mark_running(task)
                future = asyncio.create_task(self._execute_one(task))
                running[future] = task.id
                ready.task_done()
            if not running:
                if pending:
                    raise RuntimeError("Scheduler stalled with unresolved tasks")
                break
            done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for future in done:
                tid = running.pop(future)
                result = future.result()
                self.results[tid] = result
                if self.journal:
                    self.journal.save_result(self.graph.tasks[tid], result)
                if result.status != "SUCCESS":
                    failed.add(tid)
        return {
            "summary": {
                "total": len(self.graph.tasks),
                "completed": sum(r.status == "SUCCESS" for r in self.results.values()),
                "failed": sum(r.status == "FAILED" for r in self.results.values()),
                "blocked": sum(r.status == "BLOCKED" for r in self.results.values())},
            "results": {key: asdict(value) for key, value in self.results.items()}}

    def close(self):
        if self.journal:
            self.journal.close()

def demo_registry():
    registry = OperationRegistry()
    registry.register("add", lambda a, b: a + b)
    registry.register("multiply", lambda a, b: a * b)
    registry.register("sha256_text", lambda value: __import__("hashlib").sha256(
        str(value).encode()).hexdigest())
    return registry

async def demo(state_dir: str | None = None):
    tasks = [
        Task("A", "add", {"a": 100, "b": 50}),
        Task("B", "multiply", {"a": {"from": "A"}, "b": 2}, ("A",)),
        Task("C", "add", {"a": 7, "b": 9}),
        Task("D", "add", {"a": {"from": "B"}, "b": {"from": "C"}}, ("B", "C")),
        Task("E", "sha256_text", {"value": {"from": "D"}}, ("D",))]
    engine = OmegaEngine(tasks, demo_registry(), concurrency=3, state_dir=state_dir)
    try:
        return await engine.execute()
    finally:
        engine.close()

if __name__ == "__main__":
    import json
    print(json.dumps(asyncio.run(demo()), indent=2))
