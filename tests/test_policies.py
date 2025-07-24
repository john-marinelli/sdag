from thomas.state import TaskState, RunPolicy, POLICIES


def test_all_success() -> None:
    deps = []
    assert POLICIES[RunPolicy.ALL_SUCCESS](deps)

    deps = [TaskState.SUCCESS, TaskState.FAILED]
    assert not POLICIES[RunPolicy.ALL_SUCCESS](deps)

    deps = [TaskState.SUCCESS, TaskState.SUCCESS]
    assert POLICIES[RunPolicy.ALL_SUCCESS](deps)
