from __future__ import annotations
from sdag.state import TaskState
from sdag.node import _Node, Task
from sdag.executors import Executor
from sdag.result import TaskResult, BranchResult, Result
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
        """
            Top level function to run the DAG.
            This will finish once all tasks have
            executed or failed based on execution policy.
        """
        while self._queue:
            self._submit_tasks()
            self._poll_finished()

            
    def _submit_tasks(self) -> None:
        """
            Grabs ready tasks from the queue
            and submits them to the executor.
        """
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
        """
            Checks a tasks state and checks its 
            dependencies against the policy it was
            assigned.
        """
        return (
            self._tasks[task].policy(
                [self._tasks[t].state for t in self._tasks[task].deps]
            )
        )
    
    def _poll_finished(self) -> None:
        """
            Polls executor for finished tasks then 
            queues downstream tasks.
        """
        finished = self._executor.poll()

        for f in finished:
            self._tasks[f.id].output = f
            if f.error is not None:
                self._tasks[f.id].state = TaskState.FAILED
            else:
                self._tasks[f.id].state = TaskState.SUCCESS

            if isinstance(f, TaskResult):
                self._handle_task_result(f)
            elif isinstance(f, BranchResult):
                self._handle_branch_result(f)

            self._queue.remove(f.id)
            if (
                self._tasks[f.id].state == TaskState.FAILED 
                and self._tasks[f.id].has_error_callback
            ):
                self._executor.submit(self._tasks[f.id].on_error)
            elif self._tasks[f.id].has_success_callback:
                self._executor.submit(self._tasks[f.id].on_success)
    
    def _handle_task_result(self, res: TaskResult) -> None:
        """
            Handles a finished task, queueing up all
            tasks that are downstream.
        """
        for t in self._adj[res.id]:
            self._tasks[t].input = res
            if self._tasks[res.id].state == TaskState.SUCCESS:
                self._tasks[t].state = TaskState.READY
            else:
                self._tasks[t].state = TaskState.SKIPPED
            self._queue.append(t)

    def _handle_branch_result(self, res: BranchResult) -> None:
        """
            Handles a finished branch task, queuing up
            the task with a name matching the branch's
            returned string.
        """
        if self._tasks[res.id].state == TaskState.SUCCESS:
            # Just playing it safe in case there are
            # multiple tasks with the same name
            picked = [
                t for t in self._adj[res.id] 
                if self._tasks[t].name == res.value
            ]
            for t in picked:
                self._tasks[t].input = self._tasks[res.id].input
                self._tasks[t].state = TaskState.READY
                self._queue.append(t)
        else:
            for t in self._adj[res.id]:
                self._tasks[t].state = TaskState.SKIPPED

