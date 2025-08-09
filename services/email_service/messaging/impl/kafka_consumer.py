import json
import threading
import time
from typing import Callable

from confluent_kafka import Consumer
from messaging_interfaces.kafka.kafka_consumer_interface import KafkaConsumerInterface
from utils.logger import get_logger

logger = get_logger("email_service")


class KafkaConsumer(KafkaConsumerInterface):
    def __init__(self, bootstrap_servers: str, group_id: str = "email-consumer"):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._consumer = None
        self._running = False
        self._thread = None

    def _init_consumer(self):
        if self._consumer is None:
            for i in range(10):
                try:
                    logger.info(f"Connecting to Kafka consumer (attempt {i + 1}/10)...")
                    self._consumer = Consumer({
                        'bootstrap.servers': self.bootstrap_servers,
                        'group.id': self.group_id,
                        'auto.offset.reset': 'earliest'
                    })
                    logger.info("✅ Kafka consumer connected")
                    break
                except Exception as e:
                    logger.warning(f"❌ Kafka consumer not ready: {e}")
                    time.sleep(3)
            else:
                raise ConnectionError("Failed to connect to Kafka after 10 retries")

    def consume(self, topic: str, on_message: Callable):
        self._init_consumer()
        self._running = True

        def _listen():
            self._consumer.subscribe([topic])
            logger.info(f"🔄 Subscribed to topic: {topic}")

            while self._running:
                try:
                    msg = self._consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.warning(f"❌ Kafka consumer error: {msg.error()}")
                        continue

                    data = json.loads(msg.value().decode('utf-8'))

                    try:
                        on_message(data)
                    except Exception as e:
                        logger.error(f"💥 Error in message handler: {e} — data: {data}")
                        # Optional: forward to DLQ here
                except Exception as e:
                    logger.error(f"⚠️ Unexpected error in consumer loop: {e}")
                    time.sleep(2)

            logger.info("🛑 Kafka consumer loop exited")

        self._thread = threading.Thread(target=_listen, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        if self._consumer:
            self._consumer.close()
            logger.info("✅ Kafka consumer connection closed")
        logger.info("🧼 Kafka consumer stopped cleanly")
