from thomas.dag import DAG
from thomas.executors import PathosExecutor, Executor
from typing import Any
from uuid import UUID

class DAGRunner:
    _dag: DAG
    _args: dict[UUID, Any]
    _executor: Executor

    def __init__(self, dag: DAG) -> None:
        self._dag = dag
        self._executor = PathosExecutor()
        self._args = {}
        for t in self._dag.tasks:
            self._args[t] = {}

    def run(self) -> None:
        curr = self._dag.roots
        while curr:
            for i in curr:
                self._executor.submit(
                    i.run, 
                    self._args[i.id] if i.id in self._args else {}
                )

