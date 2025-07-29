import time

import pika
import json
from typing import Dict
from .interface import MessageBroker

class RabbitMQBroker(MessageBroker):
    def __init__(self, host='rabbitmq', port=5672):
        for i in range(10):
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=host, port=port)
                )
                self.channel = self.connection.channel()
                print("RabbitMQ connected")
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"RabbitMQ not ready, retrying ({i + 1}/10)...")
                time.sleep(3)
        else:
            raise ConnectionError("Failed to connect to RabbitMQ after 10 retries")

    def publish(self, queue: str, message: Dict):
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2  # Make message persistent
            )
        )

    def subscribe(self, topic: str, on_message):
        raise NotImplementedError("RabbitMQ consumer will be implemented in worker services.")
