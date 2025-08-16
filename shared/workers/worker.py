from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)
V = TypeVar("V", bound=BaseModel)

class Worker(ABC, Generic[T, V]):
    @abstractmethod
    def process(self, message: T) -> V:
        """Process a message of type T"""
        pass