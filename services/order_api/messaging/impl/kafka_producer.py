import json
import time
from typing import Dict

from confluent_kafka import Producer

from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface
from utils.logger import get_logger

logger = get_logger("order_api_KafkaProducer")

class KafkaProducer(KafkaProducerInterface):
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer = None

    def _init_producer(self):
        if self._producer is None:
            for i in range(10):
                try:
                    logger.info(f"Trying to connect to Kafka at {self.bootstrap_servers} (attempt {i + 1}/10)")
                    self._producer = Producer({'bootstrap.servers': self.bootstrap_servers})
                    logger.info("✅ Kafka producer ready")
                    break
                except Exception as e:
                    logger.info(f"❌ Kafka not ready yet: {e}")
                    time.sleep(3)
            else:
                raise ConnectionError("Kafka failed to connect after 10 retries")

    def produce(self, topic: str, message: Dict):
        self._init_producer()
        try:
            logger.info(f'📤 Publishing to topic "{topic}": {message}')
            self._producer.produce(topic, value=json.dumps(message))
            self._producer.flush()
        except Exception as e:
            logger.info(f"❌ Failed to publish message: {e}")
