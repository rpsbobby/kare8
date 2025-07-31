import json
import time
from typing import Dict

import pika
from ..interfaces.rabbitmq_publisher_interface import RabbitMqPublisherInterface


class RabbitMQPublisher(RabbitMqPublisherInterface):
    def __init__(self, host='rabbitmq', port=5672):
        self.host = host
        self.port = port
        self._connection = None
        self._channel = None

    def _init_connection(self):
        if self._connection is None or self._channel is None:
            for i in range(10):
                try:
                    print(f"Connecting to RabbitMQ (attempt {i + 1}/10)...")
                    self._connection = pika.BlockingConnection(
                        pika.ConnectionParameters(host=self.host, port=self.port)
                    )
                    self._channel = self._connection.channel()
                    print("✅ RabbitMQ connected")
                    break
                except pika.exceptions.AMQPConnectionError as e:
                    print(f"❌ RabbitMQ not ready: {e}")
                    time.sleep(3)
            else:
                raise ConnectionError("Failed to connect to RabbitMQ after 10 retries")

    def publish(self, queue: str, message: Dict):
        self._init_connection()
        self._channel.queue_declare(queue=queue, durable=True)
        self._channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
