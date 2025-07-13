from __future__ import annotations
from thomas.state import TaskState, POLICIES, RunPolicy
import threading
from typing import Callable, Any, TypeVar, Protocol, Generic
from uuid import uuid4, UUID
from abc import abstractmethod
from numba import njit, types
from numba.typed import Dict, List
import logging
import inspect

class CallableProtocol(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

JITInputValue = types.UnionType([types.float64, types.int64, types.unicode_type])

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(threadName)s - %(message)s'
)

T = TypeVar("T", bound=Callable[..., Any])
U = TypeVar("U")


class _Node(Generic[T, U]):
    name: str
    id: UUID
    _exe: T
    _jit_exe: Callable[..., Any] | None
    _suc: Callable[..., None] | None
    _err: Callable[..., None] | None
    _sig: list[str]
    _pol: Callable[[list[TaskState]], bool]
    _state: TaskState
    _deps: list[_Node]
    _placed: bool
    _processed: bool
    _input_history: list[dict[str, Any]] | List[Dict[str, Any]]
    _store_input_history: bool
    _jit: bool
    
    def __init__(
        self,
        name: str,
        on_execute: T,
        on_success: Callable[..., None] | None = None,
        on_error: Callable[..., None] | None = None,
        jit: bool = False,
        store_input_history: bool = False
    ) -> None:
        if jit:
            self._jit_exe = njit()(on_execute)
            def jitted(**kwargs) -> dict[str, Any]:
                if self._jit_exe is None:
                    raise Exception(
                        "No jitted function available to call"
                    )
                res = self._jit_exe(**kwargs)
                return {k: v for k, v in res.items()}
            self._exe = jitted  # type: ignore
        else:
            self._exe = on_execute
        self._suc = on_success
        self._err = on_error
        self.name = name
        self.id = uuid4()
        self._state = TaskState.BUILDING
        self._policy = POLICIES[RunPolicy.NEVER]
        self._placed = False
        self._deps = []
        self._input_history = []
        self._store_input_history = store_input_history
        
        sig = inspect.signature(self._exe)
        self._sig = list(sig.parameters.keys())
        
    @abstractmethod
    def run(self, **kwargs) -> U: ...

    @abstractmethod
    def on_success(self) -> None: ...

    @abstractmethod
    def on_error(self) -> None: ...
        
    @property
    def state(self) -> TaskState:
        return self._state
        
    @state.setter
    def state(self, state: TaskState) -> None:
        if state == TaskState.BUILDING:
            raise ValueError("Cannot set state with TaskState.BUILDING.")
        self._state = state

    @property
    def deps(self) -> list[_Node]:
        return self._deps

    @deps.setter
    def deps(self, deps: list[_Node]) -> None:
        self._deps = deps

    @property
    def placed(self) -> bool:
        return self._placed 
    
    def place(self) -> None:
        self._placed = True

    def _register_input(self, input_dict: dict[str, Any]) -> dict | Dict:
        if self._store_input_history:
            if self._jit:
                d = Dict.empty(key_type=types.unicode_type, value_type=JITInputValue) 
                for k, v in input_dict.items():
                    d[k] = v
            else:
                d = input_dict
            self._input_history.append(d)
            d["input_history"] = self._input_history
        else:
            d = input_dict

        return d
        
    
    def __repr__(self):
        return self.name

class Task(_Node[Callable[..., dict[str, Any]], dict[str, Any]]):
    
    def run(self, **kwargs) -> dict[str, Any]:
        d = self._register_input(kwargs)
        params = {
            k: v for k, v in d.items() if k in self._sig
        }
        return self._exe(**params)
    
class Branch(_Node[Callable[..., str], tuple[str, dict[str, Any]]]): 

    def __init__(
        self,
        name: str,
        on_execute: Callable[..., str],
        on_success: Callable[..., None] | None = None,
        on_error: Callable[..., None] | None = None,
        store_input_history: bool = False
    ) -> None:
        super().__init__(
            name=name, 
            on_execute=on_execute, 
            on_success=on_success,
            on_error=on_error, 
            store_input_history=store_input_history
        )
    
    def run(self, **kwargs) -> tuple[str, dict[str, Any]]:
        d = self._register_input(kwargs)
        params = {
            k: v for k, v in d.items() if k in self._sig
        }
        return self._exe(**params), kwargs

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
        
        task.deps = [*self.prev.deps, self.prev]
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

        condition.deps = [*self.prev.deps, self.prev]
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
        junction.deps = [*junction.deps, *b.prev.deps, b.prev]


    junction.place()

    return DAGBuilder(dag=branches[0].dag, prev=junction)
    

class DAG:
    _adj: dict[UUID, list[_Node]]
    _tasks: dict[UUID, _Node]
    _states: dict[UUID, TaskState]
    _roots: list[_Node]
    _lock: threading.RLock
    
    def __init__(self) -> None:
        self._adj = {}
        self._roots = []
        self._lock = threading.RLock()
        self._tasks = {}

    def _add_root(self, task: Task) -> None:
        if task.id in self._adj:
            raise ValueError(
                f"Task {task.name} is already present in DAG"
            )
        self._adj[task.id] = []
        self._roots.append(task)
        self._tasks[task.id] = task
    
    def _add_upstream(self, upstream: _Node, downstream: _Node) -> None:
        if upstream.id not in self._adj:
            self._adj[upstream.id] = []
        self._adj[upstream.id].append(downstream)
        self._tasks[downstream.id] = downstream


    def get_next(self) -> None:
        pass

    def initialize_tasks(self) -> None:
        for t in self._tasks:
            self._tasks[t].state = TaskState.AWAITING_UPSTREAM
            
        for r in self._roots:
            r.state = TaskState.READY
        
    def validate_dag(self) -> None:
        pass
        

