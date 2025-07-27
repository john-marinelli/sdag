from sdag.builder import DAGBuilder
from sdag.dag import DAG
from sdag.node import Task
from sdag.executors import TestExecutor

def t1():
    print("Executing t1")
    return {
        "value": "From t1"
    }

def t2(value: str):
    print(value)
    return {
        "value": "From t2"
    }
    
def t3(value: str):
    print(value)
    return {}

# def test_simple_dag():
#     executor = TestExecutor()
#     builder = DAGBuilder(dag=DAG(executor=executor))
#     task1 = Task(on_execute=t1, name="t1")
#     task2 = Task(on_execute=t2, name="t2")
#     task3 = Task(on_execute=t3, name="t3")
#
#     builder.add_root(
#         task1
#     ).add_task(
#         task2
#     ).add_task(
#         task3
#     )
#
#     dag = builder.finalize()
#
#     dag.run()
#
#     assert [task1.id, task2.id, task3.id] == executor.executed
#
#
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
