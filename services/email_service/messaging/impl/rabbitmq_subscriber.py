import json
import threading
import time
from typing import Callable, Dict

import pika

from messaging_interfaces.rabbitmq.rabbitmq_subcriber_interface import RabbitMqSubscriberInterface
from utils.logger import get_logger

logger = get_logger("email_service_RabbitMQSubscriber")


class RabbitMQSubscriber(RabbitMqSubscriberInterface):
    def __init__(self, host='rabbitmq', port=5672):
        self.host = host
        self.port = port
        self._connection = None
        self._channel = None

    def _init_connection(self):
        if self._connection is None or self._channel is None:
            for i in range(10):
                try:
                    logger.info(f"📡 Connecting to RabbitMQ ({self.host}:{self.port}) [attempt {i + 1}/10]...")
                    self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, port=self.port))
                    self._channel = self._connection.channel()
                    logger.info("✅ RabbitMQ consumer connected.")
                    break
                except pika.exceptions.AMQPConnectionError as e:
                    logger.info(f"❌ Connection failed: {e}")
                    time.sleep(3)
            else:
                raise ConnectionError("Failed to connect to RabbitMQ after 10 attempts.")

    def subscribe(self, queue: str, on_message: Callable[[Dict], None]):
        self._init_connection()

        def _consume():
            self._channel.queue_declare(queue=queue, durable=True)

            def callback(ch, method, properties, body):
                data = json.loads(body)
                logger.info(f"📥 Received message from '{queue}': {data}")
                on_message(data)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            self._channel.basic_qos(prefetch_count=1)
            self._channel.basic_consume(queue=queue, on_message_callback=callback)

            logger.info(f"🎧 Listening on RabbitMQ queue: {queue}")
            self._channel.start_consuming()

        thread = threading.Thread(target=_consume, daemon=True)
        thread.start()
