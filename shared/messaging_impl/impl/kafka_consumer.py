import time
from logging import Logger
from typing import Optional, Callable

from confluent_kafka import Consumer

from messaging_impl.impl.helpers.listener import KafkaListener
from messaging_interfaces.kafka.kafka_consumer_interface import KafkaConsumerInterface


class KafkaConsumer(KafkaConsumerInterface):
    def __init__(self, bootstrap_servers: str, group_id: str = "kare8-consumer", logger: Logger = None):
        self.bootstrap_servers=bootstrap_servers
        self.group_id=group_id
        self._consumer: Optional[Consumer]=None
        self._listener: Optional[KafkaListener]=None
        self._logger=logger

    def _init_consumer(self):
        if self._consumer is None:
            for i in range(10):
                try:
                    self._logger.info(f"Connecting to Kafka consumer (attempt {i + 1}/10)…")
                    self._consumer=Consumer({
                        "bootstrap.servers": self.bootstrap_servers,
                        "group.id": self.group_id,
                        "auto.offset.reset": "earliest",
                        "enable.auto.commit": False,
                        "enable.auto.offset.store": False,
                        })
                    self._logger.info("✅ Kafka consumer connected")
                    break
                except Exception as e:
                    self._logger.warning(f"❌ Kafka consumer not ready: {e}")
                    time.sleep(3)
            else:
                raise ConnectionError("Failed to connect to Kafka after 10 retries")

    def consume(self, topic: str, on_message: Callable):
        self._init_consumer()
        self._listener=KafkaListener(self._consumer, topic, on_message, self._logger)
        self._listener.start()

    def stop(self):
        if self._listener:
            self._listener.stop()
            self._listener.join(timeout=5)
        if self._consumer:
            self._consumer.close()
        self._logger.info("🧼 Kafka consumer stopped cleanly")
