from thomas.builder import DAGBuilder
from thomas.dag import DAG
from thomas.node import Task
from thomas.executors import TestExecutor

def t1():
    print("t1")
    return {}

def t2():
    print("t2")
    return {}
    
def t3():
    print("t3")
    return {}

def test_simple_dag():
    builder = DAGBuilder(dag=DAG(executor=TestExecutor()))
    builder.add_root(
        Task(on_execute=t1, name="t1")
    ).add_task(
        Task(on_execute=t2, name="t2")
    ).add_task(
        Task(on_execute=t3, name="t3")
    )

    dag = builder.finalize()

    dag.run()

if __name__ == "__main__":
    test_simple_dag()
