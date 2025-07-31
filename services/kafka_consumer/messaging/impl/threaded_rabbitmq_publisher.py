import pika
import json
import threading
import time
from typing import Dict
from queue import Queue, Empty

from ..interfaces.rabbitmq_publisher_interface import RabbitMqPublisherInterface


class ThreadedRabbitMQPublisher(RabbitMqPublisherInterface):
    def __init__(self, host="rabbitmq", port=5672):
        self._host = host
        self._port = port
        self._queue = Queue()
        self._running = True
        self._connection = None
        self._channel = None
        self._thread = threading.Thread(target=self._publisher_loop, daemon=True)

        self._init_connection_with_retry()
        self._thread.start()
        print("🚀 ThreadedRabbitMQPublisher initialized.")

    def _init_connection_with_retry(self):
        for i in range(10):
            try:
                print(f"🔌 Connecting to RabbitMQ ({i + 1}/10)...")
                self._connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self._host, port=self._port)
                )
                self._channel = self._connection.channel()
                print("✅ RabbitMQ connection established.")
                return
            except pika.exceptions.AMQPConnectionError:
                print("❌ Connection failed, retrying...")
                time.sleep(3)
        raise ConnectionError("❌ Failed to connect to RabbitMQ after 10 retries")

    def _publisher_loop(self):
        while self._running:
            try:
                queue_name, message = self._queue.get(timeout=1)
                self._channel.queue_declare(queue=queue_name, durable=True)
                self._channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=json.dumps(message),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                print(f"📬 Published to '{queue_name}': {message}")
            except Empty:
                continue
            except Exception as e:
                print(f"❌ Error in publisher thread: {e}")
                time.sleep(1)

    def publish(self, queue: str, message: Dict):
        self._queue.put((queue, message))

    def stop(self):
        self._running = False
        self._thread.join()
        if self._connection:
            self._connection.close()
        print("🛑 ThreadedRabbitMQPublisher stopped.")
