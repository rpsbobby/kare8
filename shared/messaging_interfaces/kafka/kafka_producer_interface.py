from abc import ABC, abstractmethod
from typing import Dict


class KafkaProducerInterface(ABC):
    @abstractmethod
    def produce(self, topic: str, message: Dict, key: bytes | None = None, headers: Dict[str, str] | None = None):
        pass