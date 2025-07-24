import pytest
from thomas.dag import DAG
from thomas.node import Task, Branch
from thomas.builder import DAGBuilder, join

def get_tasks(num: int) -> tuple[Task, ...]:
    def noop():
        return {} 
    return tuple([
        Task(name=f"Task{i+1}", on_execute=noop)
        for i in range(num)
    ]) 

def get_branches(num: int) -> tuple[Branch, ...]:
    def condition():
        return ""

    return tuple([
        Branch(name=f"Branch{i+1}", on_execute=condition) for i in range(num)
    ])


def test_basic_dag(builder: DAGBuilder) -> None:
    t1, t2, t3 = get_tasks(3)

    builder.add_root(
        t1
    ).add_task(
        t2
    ).add_task(
        t3
    )

    dag = builder.finalize()

    assert [i for i in dag._adj[t1.id]] == [t2.id]
    assert [i for i in dag._adj[t2.id]] == [t3.id]
    assert len(dag._tasks) == 3

def test_branched_dag(builder: DAGBuilder) -> None:
    t1, t2, t3, t4, t5, t6 = get_tasks(6)
    branch = get_branches(1)[0]
    
    b1, b2, b3 = builder.add_root(
        t1
    ).add_task(
        t2
    ).add_task(
        t3
    ).branch(
        condition=branch, n_branches=3
    )

    b1.add_task(t4)
    b2.add_task(t5)
    b3.add_task(t6)

    dag = builder.finalize()

    assert [i for i in dag._adj[t1.id]] == [t2.id]
    assert [i for i in dag._adj[t2.id]] == [t3.id]
    assert [i for i in dag._adj[t3.id]] == [branch.id]
    assert [i for i in dag._adj[branch.id]] == [t4.id, t5.id, t6.id]
    assert len(dag._tasks) == 7


def test_join_then_branch(builder: DAGBuilder) -> None:
    t1, t2, t3, t4, t5 = get_tasks(5)
    branch = get_branches(1)[0]

    r1 = builder.add_root(t1)
    r2 = builder.add_root(t2)

    j = join(t3, [r1, r2])

    b1, b2 = j.branch(condition=branch, n_branches=2)
    b1.add_task(t4)
    b2.add_task(t5)

    dag = j.finalize()

    assert dag._adj[t1.id] == [t3.id] 
    assert dag._adj[t2.id] == [t3.id]
    assert dag._adj[t3.id] == [branch.id]
    assert dag._adj[branch.id] == [t4.id, t5.id]
    
    
def test_joined_dag(builder: DAGBuilder) -> None:
    t1, t2, t3, t4, t5, t6, t7 = get_tasks(7)
    branch = get_branches(1)[0]
    
    b1, b2, b3 = builder.add_root(
        t1
    ).add_task(
        t2
    ).add_task(
        t3
    ).branch(
        condition=branch, n_branches=3
    )

    b1 = b1.add_task(t4)
    b2 = b2.add_task(t5)
    b3 = b3.add_task(t6)
    
    join(junction=t7, branches=[b1, b2, b3])
    
    dag = builder.finalize()

    assert [i for i in dag._adj[t1.id]] == [t2.id]
    assert [i for i in dag._adj[t2.id]] == [t3.id]
    assert [i for i in dag._adj[t3.id]] == [branch.id]
    assert [i for i in dag._adj[branch.id]] == [t4.id, t5.id, t6.id]
    assert [i for i in dag._adj[t4.id]] == [t7.id]
    assert [i for i in dag._adj[t5.id]] == [t7.id]
    assert [i for i in dag._adj[t6.id]] == [t7.id]
    assert len(dag._tasks) == 8


def test_large_dag(builder: DAGBuilder) -> None:
    t1, t2, t3, t4, t5, t6, t7, t8, t9, t10 = get_tasks(10)
    branch1, branch2 = get_branches(2)
    
    b1, b2, b3 = builder.add_root(
        t1
    ).add_task(
        t2
    ).add_task(
        t3
    ).branch(
        condition=branch1, n_branches=3
    )

    b1 = b1.add_task(t4)
    b2 = b2.add_task(t5)
    b3 = b3.add_task(t6)
    
    j1 = join(junction=t7, branches=[b1, b2, b3])
    
    b4, b5 = j1.branch(branch2, n_branches=2)

    b4 = b4.add_task(t8)
    b5 = b5.add_task(t9)

    join(junction=t10, branches=[b4, b5])
    
    dag = builder.finalize()

    assert [i for i in dag._adj[t1.id]] == [t2.id]
    assert [i for i in dag._adj[t2.id]] == [t3.id]
    assert [i for i in dag._adj[t3.id]] == [branch1.id]
    assert [i for i in dag._adj[branch1.id]] == [t4.id, t5.id, t6.id]
    assert [i for i in dag._adj[t4.id]] == [t7.id]
    assert [i for i in dag._adj[t5.id]] == [t7.id]
    assert [i for i in dag._adj[t6.id]] == [t7.id]
    print([dag._tasks[i].name for i in dag._adj[t7.id]])
    assert [i for i in dag._adj[t7.id]] == [branch2.id]
    assert [i for i in dag._adj[branch2.id]] == [t8.id, t9.id]
    assert [i for i in dag._adj[t8.id]] == [t10.id]
    assert [i for i in dag._adj[t9.id]] == [t10.id]
    assert len(dag._tasks) == 12


