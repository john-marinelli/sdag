from thomas.node import _Node
from pathos.pools import ProcessPool
from typing import TypeVar, Protocol, Callable, Any
from abc import ABC, abstractmethod

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
    def poll(self) -> list[_Node]: ...

class TestExecutor(Executor):
    _results: list[_Node] = []
    executed: list[_Node] = []

    def submit(
        self,
        func: Callable[..., _Node],
    ) -> None:
        res = func()
        self.executed.append(res)
        self._results.append(res)

    def poll(self) -> list[_Node]:
        if self._results:
            return [
                self._results.pop(i) 
                for i in range(len(self._results))
            ]
        return []

class SequentialExecutor(Executor):
    _results: list[_Node]

    def __init__(self) -> None:
        self._results = []

    def submit(
        self,
        func: Callable[..., _Node],
    ) -> None:
        self._results.append(func())

    def poll(self) -> list[_Node]:
        if self._results:
            return [
                self._results.pop(i) 
                for i in range(len(self._results))
            ]
        return []

class PathosExecutor(Executor):
    _pool: ProcessPool
    _futures: list[_PathosFuture[_Node]]

    def __init__(self, workers=4) -> None:
        self._pool = ProcessPool(workers=workers)
    
    def submit(
        self, 
        func: Callable[..., dict[str, Any]] | Callable[..., str],
    ) -> None:
        self._futures.append(self._pool.apipe(func))

    def poll(self) -> list[_Node]:
        finished = []
        for idx, res in enumerate(self._futures):
            if res.ready():
                finished.append(self._futures.pop(idx))
        return finished
    
    def empty(self) -> bool:
        return len(self._futures) == 0
