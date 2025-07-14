from __future__ import annotations
from thomas.state import TaskState
from thomas.node import _Node, Task
import threading
from uuid import UUID
from numba import types
import logging

JITInputValue = types.UnionType([types.float64, types.int64, types.unicode_type])

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(threadName)s - %(message)s'
)

class DAG:
    _adj: dict[UUID, list[_Node]]
    _tasks: dict[UUID, _Node]
    _states: dict[UUID, TaskState]
    _roots: list[_Node]
    _lock: threading.RLock
    
    def __init__(self) -> None:
        self._adj = {}
        self._roots = []
        self._lock = threading.RLock()
        self._tasks = {}

    def _add_root(self, task: Task) -> None:
        if task.id in self._adj:
            raise ValueError(
                f"Task {task.name} is already present in DAG"
            )
        self._adj[task.id] = []
        self._roots.append(task)
        self._tasks[task.id] = task
    
    def _add_upstream(self, upstream: _Node, downstream: _Node) -> None:
        if upstream.id not in self._adj:
            self._adj[upstream.id] = []
        self._adj[upstream.id].append(downstream)
        self._tasks[downstream.id] = downstream

    def get_downstream(self, task_id: UUID) -> list[_Node]:
        return self._adj[task_id]
    
    @property
    def roots(self) -> list[_Node]:
        return self._roots
    
    @property
    def tasks(self) -> dict[UUID, _Node]:
        return self._tasks

    def initialize_tasks(self) -> None:
        for t in self._tasks:
            self._tasks[t].state = TaskState.AWAITING_UPSTREAM
            
        for r in self._roots:
            r.state = TaskState.READY
        
    def validate_dag(self) -> None:
        pass

