from enum import Enum
from typing import Callable

class TaskState(Enum):
    """States that a task can inhabit.

    BUILDING: str
        Task has been created, but DAG not finalized.
        Tasks should only be in this state when building a DAG.
    AWAITING_UPSTREAM: str
        Waiting on one or more upstream tasks to complete.        
    READY: str
        Ready to execute.
    RUNNING: str
        Currently executing.
    FAILED: str
        Attempted execution, then failed.
    SUCCESS: str
        Successfully executed.
    SKIPPED: str
        Task was skipped.
    """
    
    BUILDING = "building"
    AWAITING_UPSTREAM = "awaiting_upstream"
    READY = "ready"
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "successful"
    SKIPPED = "skipped"


class RunPolicy(Enum):
    """Enum representing the run policy for tasks.
    
    Mirrors trigger rules from Airflow.

    Attributes
    ----------

    ALL_SUCCESS: str
        All upstream tasks have succeeded.
    ALL_FAILED: str
        All upstream tasks have failed.
    ALL_DONE: str
        All upstream tasks have finished, whether failed or succeeded.
    ONE_SUCCESS: str
        At least one upstream task was successful.
    ONE_FAILED: str
        At least one upstream task failed.
    ALWAYS: str
        Always execute.
    NEVER: str
        Never execute.
    """
    
    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_successful"
    ONE_FAILED = "one_failed"
    ALWAYS = "always"
    NEVER = "never"


def _all_success(states: list[TaskState]) -> bool:
    return all([i == TaskState.SUCCESS for i in states])

def _all_failed(states: list[TaskState]) -> bool:
    return all([i == TaskState.FAILED for i in states])
    
def _all_done(states: list[TaskState]) -> bool:
    return all([i in [TaskState.FAILED, TaskState.SUCCESS] for i in states])

def _one_success(states: list[TaskState]) -> bool:
    return len([i for i in states if i == TaskState.SUCCESS]) > 0

def _one_failed(states: list[TaskState]) -> bool:
    return len([i for i in states if i == TaskState.FAILED]) > 0

def _always(_: list[TaskState]) -> bool:
    return True

def _never(_: list[TaskState]) -> bool:
    return False

POLICIES: dict[RunPolicy, Callable[[list[TaskState]], bool]] = {
    RunPolicy.ALL_SUCCESS: _all_success,
    RunPolicy.ALL_FAILED: _all_failed,
    RunPolicy.ALL_DONE: _all_done,
    RunPolicy.ONE_SUCCESS: _one_success,
    RunPolicy.ONE_FAILED: _one_failed,
    RunPolicy.ALWAYS: _always,
    RunPolicy.NEVER: _never
}
