from sdag.node import _Node
from sdag.result import Result
from pathos.pools import ProcessPool
from typing import TypeVar, Protocol, Callable, Any
from abc import ABC, abstractmethod
from uuid import UUID

T = TypeVar("T", covariant=True)

class _PathosFuture(Protocol[T]):
    """Protocol for pathos Futures"""

    def get(self) -> T: ...

    def ready(self) -> bool: ...


class Executor(ABC):

    @abstractmethod
    def submit(
        self, 
        func: Callable, 
    ) -> None: ...
        
    @abstractmethod
    def poll(self) -> list[Result]: ...

class TestExecutor(Executor):
    __test__: bool = False
    _results: list[Result]
    executed: list[UUID]

    def __init__(self) -> None:
        self._results = []
        self.executed = []

    def submit(
        self,
        func: Callable[..., Result],
    ) -> None:
        res = func()
        if res is not None:
            self.executed.append(res.id)
        self._results.append(res)

    def poll(self) -> list[Result]:
        return [
            self._results.pop(i) 
            for i in range(len(self._results))
        ]

class SequentialExecutor(Executor):
    _results: list[Result]

    def __init__(self) -> None:
        self._results = []

    def submit(
        self,
        func: Callable[..., Result],
    ) -> None:
        self._results.append(func())

    def poll(self) -> list[Result]:
        return [
            self._results.pop(i) 
            for i in range(len(self._results))
        ]

class PathosExecutor(Executor):
    _pool: ProcessPool
    _futures: list[_PathosFuture[Result]]

    def __init__(self, workers=4) -> None:
        self._pool = ProcessPool(workers=workers)
        self._futures = []
    
    def submit(
        self, 
        func: Callable[..., Result],
    ) -> None:
        self._futures.append(self._pool.apipe(func))

    def poll(self) -> list[Result]:
        finished = []
        for idx, res in enumerate(self._futures):
            if res.ready():
                finished.append(self._futures.pop(idx))
        return finished
    
    def empty(self) -> bool:
        return len(self._futures) == 0

