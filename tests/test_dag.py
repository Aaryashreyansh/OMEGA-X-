import pytest
from omega_x.dag import Task, DependencyGraph, AdvancedDAGScheduler

def test_cycle_rejected():
    with pytest.raises(ValueError, match="cycle"):
        DependencyGraph([
            Task("A", "noop", dependencies=("B",)),
            Task("B", "noop", dependencies=("A",)),
        ])

def test_long_critical_chain_gets_priority():
    graph = DependencyGraph([
        Task("A", "noop", dependencies=("B",)),
        Task("B", "noop", dependencies=("C",)),
        Task("C", "noop"),
        Task("X", "noop"),
    ])
    scheduler = AdvancedDAGScheduler(graph)
    # C is the currently-ready ancestor that unlocks the longest downstream chain.
    assert scheduler.get_execution_priority("C") > scheduler.get_execution_priority("X")

def test_unknown_dependency_rejected():
    with pytest.raises(ValueError, match="Unknown dependency"):
        DependencyGraph([Task("A", "noop", dependencies=("missing",))])
