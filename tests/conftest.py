import pytest
from thomas.dag import DAG
from thomas.builder import DAGBuilder
from thomas.executors import TestExecutor


@pytest.fixture
def builder() -> DAGBuilder:
    dag = DAG(executor=TestExecutor())
    builder = DAGBuilder(dag=dag)

    return builder
