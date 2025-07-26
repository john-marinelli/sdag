from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

@dataclass
class Result:
    id: UUID
    error: Exception | None = None

@dataclass
class TaskResult(Result):
    values: dict[str, Any] = field(default_factory=dict)


@dataclass
class BranchResult(Result):
    next_task: str | None = None
   
