import json
import threading
import time
from typing import Callable

from confluent_kafka import Consumer

from messaging_interfaces.kafka.kafka_consumer_interface import KafkaConsumerInterface


class KafkaConsumer(KafkaConsumerInterface):
    def __init__(self, bootstrap_servers: str, group_id: str = "kare8-consumer"):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._consumer = None

    def _init_consumer(self):
        if self._consumer is None:
            for i in range(10):
                try:
                    print(f"Connecting to Kafka consumer (attempt {i + 1}/10)...")
                    self._consumer = Consumer({
                        'bootstrap.servers': self.bootstrap_servers,
                        'group.id': self.group_id,
                        'auto.offset.reset': 'earliest'
                    })
                    print("✅ Kafka consumer connected")
                    break
                except Exception as e:
                    print(f"❌ Kafka consumer not ready: {e}")
                    time.sleep(3)
            else:
                raise ConnectionError("Failed to connect to Kafka after 10 retries")

    def subscribe(self, topic: str, on_message: Callable):
        self._init_consumer()

        def _listen():
            self._consumer.subscribe([topic])
            while True:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"❌ Kafka consumer error: {msg.error()}")
                    continue
                data = json.loads(msg.value().decode('utf-8'))
                on_message(data)

        thread = threading.Thread(target=_listen, daemon=True)
        thread.start()
