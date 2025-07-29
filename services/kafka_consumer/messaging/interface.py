from abc import ABC, abstractmethod
from typing import Callable, Dict

class MessageBroker(ABC):
    @abstractmethod
    def publish(self, topic: str, message: Dict):
        pass

    @abstractmethod
    def subscribe(self, topic: str, on_message: Callable[[Dict], None]):
        pass
