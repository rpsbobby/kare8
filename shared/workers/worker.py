from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")

class Worker(ABC, Generic[T]):
    @abstractmethod
    def process(self, message: T) -> bool:
        """Process a message of type T"""
        pass