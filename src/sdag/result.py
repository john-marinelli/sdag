from dataclasses import dataclass
from typing import Any
from uuid import UUID

@dataclass
class Result:
    id: UUID
    error: Exception | None = None

@dataclass
class TaskResult(Result):
    values: dict[str, Any] = {}


@dataclass
class BranchResult(Result):
    next_task: str | None = None
   
