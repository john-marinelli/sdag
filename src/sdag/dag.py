from __future__ import annotations
from sdag.state import TaskState
from sdag.node import _Node, Task
from sdag.executors import Executor
from sdag.result import TaskResult
from collections import deque
from uuid import UUID
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(threadName)s - %(message)s'
)

class DAG:
    _adj: dict[UUID, list[UUID]]
    _tasks: dict[UUID, _Node]
    _states: dict[UUID, TaskState]
    _roots: list[UUID]
    _executor: Executor
    _queue: deque = deque()
    
    def __init__(self, executor: Executor) -> None:
        self._adj = {}
        self._roots = []
        self._tasks = {}
        self._executor = executor

    def _add_root(self, task: Task) -> None:
        if task.id in self._adj:
            raise ValueError(
                f"Task {task.name} is already present in DAG"
            )
        self._adj[task.id] = []
        self._roots.append(task.id)
        self._tasks[task.id] = task
    
    def _add_upstream(self, upstream: _Node, downstream: _Node) -> None:
        self._adj[upstream.id].append(downstream.id)
        if downstream.id not in self._adj:
            self._adj[downstream.id] = []
        self._tasks[downstream.id] = downstream

    def _initialize_tasks(self) -> None:
        for t in self._tasks:
            self._tasks[t].state = TaskState.AWAITING_UPSTREAM
            
        for r in self._roots:
            self._tasks[r].state = TaskState.READY

    def run(self) -> None:
        self._initialize_tasks()
        self._queue.extend(self._roots)
        self._bf_exec()
                
    def _bf_exec(self) -> None:
        while self._queue:
            self._submit_tasks()
            self._poll_finished()

            
    def _submit_tasks(self) -> None:
        exec_q = [
            t for t in self._queue
            if self._can_run(t)
        ]
        for t in exec_q:
            self._executor.submit(
                self._tasks[t].run
            )
            self._tasks[t].state = TaskState.RUNNING

    def _can_run(self, task: UUID) -> bool:
        return (
            self._tasks[task].state == TaskState.READY and
            self._tasks[task].policy(
                [self._tasks[t].state for t in self._tasks[task].deps]
            )
        )
    
    def _poll_finished(self) -> None:
        finished = self._executor.poll()

        for f in finished:
            self._tasks[f.id].output = f
            if f.error is not None:
                self._tasks[f.id].state = TaskState.FAILED
            else:
                self._tasks[f.id].state = TaskState.SUCCESS

            for t in self._adj[f.id]:
                if isinstance(f, TaskResult):
                    self._tasks[t].input = f
                if self._tasks[f.id].state == TaskState.SUCCESS:
                    self._tasks[t].state = TaskState.READY
                    self._queue.extend(self._adj[f.id])
                else:
                    self._tasks[t].state = TaskState.SKIPPED

            self._queue.remove(f.id)
            if (
                self._tasks[f.id].state == TaskState.FAILED 
                and self._tasks[f.id].has_error_callback
            ):
                self._executor.submit(self._tasks[f.id].on_error)
            elif self._tasks[f.id].has_success_callback:
                self._executor.submit(self._tasks[f.id].on_success)
