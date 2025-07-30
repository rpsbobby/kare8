from abc import ABC, abstractmethod
from typing import Callable, Dict


class KafkaConsumerInterface(ABC):
    @abstractmethod
    def subscribe(self, topic: str, on_message: Callable[[Dict], None]):
        pass
