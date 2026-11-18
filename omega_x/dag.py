from __future__ import annotations
import asyncio
import heapq
from dataclasses import dataclass, field

@dataclass(frozen=True)
class Task:
    id: str
    operation: str
    inputs: dict = field(default_factory=dict)
    dependencies: tuple[str, ...] = ()
    priority: int = 0
    retries: int = 0
    timeout: float = 30.0

class DependencyGraph:
    def __init__(self, tasks: list[Task]):
        self.tasks = {t.id: t for t in tasks}
        if len(self.tasks) != len(tasks):
            raise ValueError("Duplicate task IDs")
        self.children = {key: [] for key in self.tasks}
        for task in tasks:
            if task.timeout <= 0 or task.retries < 0:
                raise ValueError(f"Invalid timeout/retries for {task.id}")
            if len(set(task.dependencies)) != len(task.dependencies):
                raise ValueError(f"Duplicate dependencies for {task.id}")
            for dep in task.dependencies:
                if dep not in self.tasks:
                    raise ValueError(f"Unknown dependency {dep!r}")
                if dep == task.id:
                    raise ValueError("Self dependency")
                self.children[dep].append(task.id)
        self.topological_order = self._topological_sort()

    def _topological_sort(self):
        degree = {key: len(t.dependencies) for key, t in self.tasks.items()}
        ready = [key for key, count in degree.items() if count == 0]
        heapq.heapify(ready)
        order = []
        while ready:
            current = heapq.heappop(ready)
            order.append(current)
            for child in self.children[current]:
                degree[child] -= 1
                if degree[child] == 0:
                    heapq.heappush(ready, child)
        if len(order) != len(self.tasks):
            raise ValueError("Dependency cycle detected")
        return order

class AdvancedDAGScheduler:
    def __init__(self, graph: DependencyGraph, critical_weight: int = 10):
        self.graph = graph
        self.critical_weight = critical_weight
        self.remaining_depth = {}
        for task_id in reversed(graph.topological_order):
            self.remaining_depth[task_id] = 1 + max(
                (self.remaining_depth[c] for c in graph.children[task_id]), default=0)

    def get_execution_priority(self, task_id: str) -> int:
        task = self.graph.tasks[task_id]
        return task.priority + self.critical_weight * self.remaining_depth[task_id]

class ReadyQueue:
    """Queue only tasks already known to be dependency-ready."""
    def __init__(self, scheduler: AdvancedDAGScheduler):
        self.scheduler = scheduler
        self.queue = asyncio.PriorityQueue()
        self.scheduled = set()

    async def add(self, task: Task) -> bool:
        if task.id in self.scheduled:
            return False
        score = self.scheduler.get_execution_priority(task.id)
        await self.queue.put((-score, task.id, task))
        self.scheduled.add(task.id)
        return True

    async def get(self) -> Task:
        _, _, task = await self.queue.get()
        return task

    def task_done(self):
        self.queue.task_done()

    async def join(self):
        await self.queue.join()

    def qsize(self):
        return self.queue.qsize()
