from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

@dataclass
class Result:
    id: UUID
    error: Exception | None = None

    def __repr__(self) -> str:
        return (
            f"Task ID: {self.id}"
        )

@dataclass
class TaskResult(Result):
    value: dict[str, Any] = field(default_factory=dict)
    
    def __repr__(self) -> str:
        return (
            f"Task ID: {self.id}\n"
            f"Values: {self.value}"
        )

@dataclass
class BranchResult(Result):
    value: str | None = None
   
    def __repr__(self) -> str:
        return (
            f"Task ID: {self.id}\n"
            f"Next task: {self.value}"
        )

