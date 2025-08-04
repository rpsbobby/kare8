from abc import ABC, abstractmethod
from typing import Callable, Dict


class RabbitMqPublisherInterface(ABC):
    @abstractmethod
    def publish(self, topic: str, on_message: Callable[[Dict], None]):
        pass
