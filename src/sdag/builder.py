from __future__ import annotations
from sdag.dag import DAG
from sdag.node import _Node, Task, Branch

class DAGBuilder:
    dag: DAG
    prev: _Node | None
    
    def __init__(self, dag: DAG, prev: _Node | None = None) -> None:
        self.dag = dag
        self.prev = prev

    def add_root(self, task: Task) -> DAGBuilder:
        self.dag._add_root(task)

        return DAGBuilder(dag=self.dag, prev=task)
        
    def add_task(self, task: Task) -> DAGBuilder:
        if self.prev is None:
            raise ValueError(
                "No previous task is defined, please use add_root"
            )

        if task.placed:
            raise ValueError(
                "This task has already been placed somewhere else in the same DAG."
            )
            
        self.dag._add_upstream(self.prev, task)
        
        task.deps = [*self.prev.deps, self.prev.id]
        task.place()

        return DAGBuilder(dag=self.dag, prev=task)

    def branch(self, condition: Branch, n_branches: int) -> list[DAGBuilder]:
        if self.prev is None:
            raise ValueError("No previous task is defined, please use add_root")
            
        if condition.placed:
            raise ValueError(
                "This condition has already been placed somewhere else in the same DAG."
            )

        self.dag._add_upstream(self.prev, condition)

        condition.deps = [*self.prev.deps, self.prev.id]
        condition.place()
        
        return [
            DAGBuilder(dag=self.dag, prev=condition) 
            for _ in range(n_branches)
        ]

    def finalize(self) -> DAG:
        
        return self.dag


def join(junction: Task, branches: list[DAGBuilder]) -> DAGBuilder:
    if len(branches) < 2:
        raise ValueError("Must have more than 1 branch to combine")

    if not all([branches[0].dag is b.dag for b in branches]):
        raise ValueError("One or more branches correspond to different DAGs")

    if junction.placed:
        raise ValueError(
            "This junction has already been placed somewhere else in the same DAG."
        )
        
    for b in branches:
        if b.prev is None:
            raise ValueError(f"Branches need a root node in order to join")

        b.dag._add_upstream(b.prev, junction)
        junction.deps = [*junction.deps, *b.prev.deps, b.prev.id]


    junction.place()

    return DAGBuilder(dag=branches[0].dag, prev=junction)
    
