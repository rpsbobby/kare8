import aio_pika
import json
from typing import Dict
from utils.logger import get_logger

from messaging_interfaces.rabbitmq.rabbitmq_publisher_interface import RabbitMqPublisherInterface
logger = get_logger("async-rabbitmq-publisher")

class AsyncRabbitMQPublisher(RabbitMqPublisherInterface):
    def __init__(self, host="rabbitmq", port=5672):
        self._host = host
        self._port = port
        self._connection = None
        self._channel = None

    async def _init(self):
        if not self._connection:
            logger.info("🔌 Connecting to RabbitMQ (async)...")
            self._connection = await aio_pika.connect_robust(
                host=self._host, port=self._port
            )
            self._channel = await self._connection.channel()
            logger.info("✅ Connected to RabbitMQ (async)")

    async def publish(self, queue: str, message: Dict):
        await self._init()
        await self._channel.declare_queue(queue, durable=True)
        body = json.dumps(message).encode()

        await self._channel.default_exchange.publish(
            aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=queue
        )
        logger.info(f"📤 Published to '{queue}': {message}")
