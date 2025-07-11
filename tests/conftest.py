import pytest
from thomas.dag import DAGBuilder, DAG


@pytest.fixture
def builder() -> DAGBuilder:
    dag = DAG()
    builder = DAGBuilder(dag=dag)

    return builder
