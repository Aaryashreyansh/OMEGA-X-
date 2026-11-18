import pytest
from omega_x.dag import Task
from omega_x.engine import OmegaEngine, OperationRegistry

@pytest.mark.asyncio
async def test_persistent_engine_reuses_successful_outputs(tmp_path):
    calls = {"count": 0}
    registry = OperationRegistry()
    def counted(value):
        calls["count"] += 1
        return {"answer": value + 1}
    registry.register("counted", counted)
    tasks = [Task("A", "counted", {"value": 41})]

    first = OmegaEngine(tasks, registry, state_dir=str(tmp_path))
    report1 = await first.execute()
    assert report1["results"]["A"]["value"] == {"answer": 42}
    first.close()
    assert calls["count"] == 1

    second_registry = OperationRegistry()
    second_registry.register("counted", counted)
    second = OmegaEngine(tasks, second_registry, state_dir=str(tmp_path))
    report2 = await second.execute()
    assert report2["results"]["A"]["value"] == {"answer": 42}
    second.close()
    assert calls["count"] == 1

@pytest.mark.asyncio
async def test_changed_task_definition_rejected_for_existing_state(tmp_path):
    registry = OperationRegistry()
    registry.register("identity", lambda value: value)
    first = OmegaEngine([Task("A", "identity", {"value": 1})], registry,
                        state_dir=str(tmp_path))
    await first.execute()
    first.close()

    second_registry = OperationRegistry()
    second_registry.register("identity", lambda value: value)
    second = OmegaEngine([Task("A", "identity", {"value": 2})], second_registry,
                         state_dir=str(tmp_path))
    with pytest.raises(ValueError, match="Task definition changed"):
        await second.execute()
    second.close()
