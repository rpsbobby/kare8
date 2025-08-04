from abc import ABC, abstractmethod
from typing import Callable, Dict


class RabbitMqSubscriberInterface(ABC):
    @abstractmethod
    def subscribe(self, queue: str, on_message: Callable[[Dict], None]):
        pass
