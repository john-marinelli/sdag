import pytest
from sdag.dag import DAG
from sdag.builder import DAGBuilder
from sdag.executors import TestExecutor


@pytest.fixture
def builder() -> DAGBuilder:
    dag = DAG(executor=TestExecutor())
    builder = DAGBuilder(dag=dag)

    return builder
