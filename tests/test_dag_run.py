from sdag.builder import DAGBuilder
from sdag.dag import DAG
from sdag.node import Task, Branch
from sdag.executors import TestExecutor
from sdag.result import TaskResult

def t1():
    print("Executing t1")
    return {
        "value": 1
    }

def t2(value: int):
    print(value)
    return {
        "value": 2
    }
    
def t3(value: int):
    print(value)
    return {
        "value": 2
    }

def b1(value: int):
    if value == 2:
        return "Task4"
    else:
        return "Task5"

def t4(value: int):
    print(f"Value is {value}")
    return {}
    
def t5(value: int):
    print(f"Value is {value}")
    return {}

def test_simple_dag():
    executor = TestExecutor()
    builder = DAGBuilder(dag=DAG(executor=executor))
    task1 = Task(on_execute=t1, name="t1")
    task2 = Task(on_execute=t2, name="t2")
    task3 = Task(on_execute=t3, name="t3")

    builder.add_root(
        task1
    ).add_task(
        task2
    ).add_task(
        task3
    )

    dag = builder.finalize()

    dag.run()

    assert [task1.id, task2.id, task3.id] == executor.executed


def test_dag_values():
    executor = TestExecutor()
    builder = DAGBuilder(dag=DAG(executor=executor))
    task1 = Task(on_execute=t1, name="t1")
    task2 = Task(on_execute=t2, name="t2")
    task3 = Task(on_execute=t3, name="t3")
    
    builder.add_root(
        task1
    ).add_task(
        task2
    ).add_task(
        task3
    )

    dag = builder.finalize()

    dag.run()

    assert task1._input_value == {}
    assert task2._input_value == {"value": 1}
    assert task3._input_value == {"value": 2}


def test_branch_dag():
    executor = TestExecutor()
    builder = DAGBuilder(dag=DAG(executor=executor))
    task1 = Task(on_execute=t1, name="Task1")
    task2 = Task(on_execute=t2, name="Task2")
    task3 = Task(on_execute=t3, name="Task3")
    branch1 = Branch(on_execute=b1, name="Branch1")
    task4 = Task(on_execute=t4, name="Task4")
    task5 = Task(on_execute=t5, name="Task5")

    left, right = builder.add_root(
        task1
    ).add_task(
        task2        
    ).add_task(
        task3
    ).branch(
        branch1,
        n_branches=2       
    )
    
    left.add_task(task4)
    right.add_task(task5)

    dag = builder.finalize()

    dag.run()

    print(dag._adj[branch1.id])
    print([
        dag._tasks[i].name
        for i in executor.executed
    ])

