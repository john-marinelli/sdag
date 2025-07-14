from thomas.node import Task, Branch
from pathos.pools import ProcessPool
from typing import TypeVar, Protocol, Callable, Any
from abc import ABC, abstractmethod

T = TypeVar("T", covariant=True)
type TaskResult = tuple[
    dict[str, Any],
    Task
]
type BranchResult = tuple[
    str,
    dict[str, Any],
    Branch
]

class _PathosFuture(Protocol[T]):
    """Protocol for pathos Futures"""

    def get(self) -> T: ...

    def ready(self) -> bool: ...


class Executor(ABC):

    @abstractmethod
    def submit(
        self, 
        func: Callable, 
        kwargs: dict[str, Any]
    ) -> None: ...
        
    @abstractmethod
    def poll(self) -> list[TaskResult | BranchResult]: ...


class SequentialExecutor(Executor):
    _results: list[TaskResult | BranchResult]

    def __init__(self) -> None:
        self._results = []

    def submit(
        self,
        func: Callable[..., TaskResult] | Callable[..., BranchResult],
        kwargs: dict[str, Any]
    ) -> None:
        self._results.append(func(**kwargs))

class PathosExecutor(Executor):
    _pool: ProcessPool
    _futures: list[
        _PathosFuture[TaskResult] |
        _PathosFuture[BranchResult]
    ]

    def __init__(self, workers=4) -> None:
        self._pool = ProcessPool(workers=workers)
    
    def submit(
        self, 
        func: Callable[..., dict[str, Any]] | Callable[..., str],
        kwargs: dict[str, Any]
    ) -> None:

        future = self._pool.apipe(
            lambda: func(**kwargs)
        )
        self._futures.append(future)

    def poll(self) -> list[TaskResult | BranchResult]:
        return [
            i.get() for i in self._futures if i.ready()
        ]
