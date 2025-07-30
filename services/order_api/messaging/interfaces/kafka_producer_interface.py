from abc import ABC, abstractmethod
from typing import Dict


class KafkaProducerInterface(ABC):
    @abstractmethod
    def publish(self, topic: str, message: Dict):
        pass