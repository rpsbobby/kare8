from typing import Dict
from confluent_kafka import Producer, Consumer
from .interface import MessageBroker
import json
import threading

class KafkaBroker(MessageBroker):
    def __init__(self, bootstrap_servers: str, group_id: str = "kare8-consumer"):
        for i in range(10):
            try:
                print(f"Trying to connect to Kafka at {bootstrap_servers} (attempt {i + 1}/10)")
                self.producer = Producer({'bootstrap.servers': bootstrap_servers})
                # Try producing a dummy message to test connection
                self.producer.produce("startup-check", value="init")
                self.producer.flush()

                self.consumer = Consumer({
                    'bootstrap.servers': bootstrap_servers,
                    'group.id': group_id,
                    'auto.offset.reset': 'earliest'
                })
                print("✅ Kafka connected")
                break
            except Exception as e:
                print(f"❌ Kafka not ready yet: {e}")
                time.sleep(3)
        else:
            raise ConnectionError("Kafka failed to connect after 10 retries")
    def publish(self, topic: str, message: Dict):
        print(f'Publishing message to topic {topic}: {message}')
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
