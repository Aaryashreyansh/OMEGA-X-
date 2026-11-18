import pytest
from omega_x.dag import Task
from omega_x.engine import OmegaEngine, demo_registry
from omega_x.verify import verify_report

@pytest.mark.asyncio
async def test_engine_executes_dependencies():
    tasks = [Task("A","add",{"a":2,"b":3}),
             Task("B","multiply",{"a":{"from":"A"},"b":4},("A",))]
    report = await OmegaEngine(tasks, demo_registry(), concurrency=2).execute()
    assert report["results"]["B"]["value"] == 20
    assert verify_report(report, {"A","B"})["valid"]

@pytest.mark.asyncio
async def test_failure_blocks_descendants():
    tasks = [Task("A","missing"), Task("B","add",{"a":1,"b":2},("A",))]
    report = await OmegaEngine(tasks, demo_registry()).execute()
    assert report["results"]["A"]["status"] == "FAILED"
    assert report["results"]["B"]["status"] == "BLOCKED"
