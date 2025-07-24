from __future__ import annotations
from typing import Generic, TypeVar, Callable, Any, Self
from uuid import UUID, uuid4
from numba.typed import List, Dict
from numba import njit
from numba import types
from thomas.state import TaskState, POLICIES, RunPolicy
from abc import abstractmethod
import inspect
import logging

logging.basicConfig(filename="t.log", level=logging.INFO)


T = TypeVar("T", bound=Callable[..., Any])
U = TypeVar("U")
V = TypeVar("V")
JITInputValue = types.UnionType([types.float64, types.int64, types.unicode_type])
EMPTY_DICT = {}


class _Node(Generic[T, V]):
    name: str
    id: UUID
    _exe: T
    _jit_exe: Callable[..., Any] | None = None
    _suc: Callable[..., None] | None
    _err: Callable[..., None] | None
    _sig: list[str]
    _pol: Callable[[list[TaskState]], bool]
    _state: TaskState
    _deps: list[UUID] = []
    _policy: Callable[[list[TaskState]], bool]
    _placed: bool = False
    _processed: bool = False
    _exception: Exception | None = None
    _input_history: list[dict[str, Any]] | List[Dict[str, Any]] = []
    _output: V
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
        self._store_input_history = store_input_history
        self._policy = POLICIES[RunPolicy.ALL_SUCCESS]
        
        sig = inspect.signature(self._exe)
        self._sig = list(sig.parameters.keys())
        
    @abstractmethod
    def run(self) -> Self: ...

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
    def deps(self) -> list[UUID]:
        return self._deps

    @deps.setter
    def deps(self, deps: list[UUID]) -> None:
        self._deps = deps

    def policy(self, states: list[TaskState]) -> bool:
        return self._policy(states)

    @property
    def placed(self) -> bool:
        return self._placed 
    
    def place(self) -> None:
        self._placed = True

    def register_input(self, input_dict: dict[str, Any]) -> None:
        params = {
            k: v for k, v in input_dict.items() if k in self._sig
        }
        if self._store_input_history:
            if self._jit:
                d = Dict.empty(key_type=types.unicode_type, value_type=JITInputValue) 
                for k, v in params.items():
                    d[k] = v
            else:
                d = params
            self._input_history.append(d)
            d["input_history"] = self._input_history
        else:
            self._input_history = [params]
    
    def __repr__(self):
        return self.name

class Task(_Node[Callable[..., dict[str, Any]], dict[str, Any]]):
    
    def run(self) -> Task:
        if self._input_history:
            kwargs = self._input_history[-1]
        else:
            kwargs = {}
        try:
            self._output = self._exe(**kwargs)
        except Exception as e:
            self._exception = e
            self.state = TaskState.FAILED
        self.state = TaskState.SUCCESS
        return self
    
class Branch(_Node[Callable[..., str], str]): 

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
   
    def run(self) -> Branch:
        if len(self._input_history) == 0:
            raise Exception(
                f"No input history available for task {self.name}"
            )
        
        try:
            self._output = self._exe(**self._input_history[-1])
        except Exception as e:
            self._exception = e
            self._state = TaskState.FAILED

        return self

