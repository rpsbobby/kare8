from typing import Dict
from confluent_kafka import Producer, Consumer
from .interface import MessageBroker
import json
import threading

class KafkaBroker(MessageBroker):
    def __init__(self, bootstrap_servers: str, group_id: str = "kare8-consumer"):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })

        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })

    def publish(self, topic: str, message: Dict):
        self.producer.produce(topic, value=json.dumps(message))
        self.producer.flush()

    def subscribe(self, topic: str, on_message):
        def _listen():
            self.consumer.subscribe([topic])
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                data = json.loads(msg.value().decode('utf-8'))
                on_message(data)

        thread = threading.Thread(target=_listen, daemon=True)
        thread.start()
