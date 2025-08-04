import os

from messaging_interfaces.rabbitmq.rabbitmq_publisher_interface import RabbitMqPublisherInterface
from ..impl.async_rabbitmq_publisher import AsyncRabbitMQPublisher
from ..impl.threaded_rabbitmq_publisher import ThreadedRabbitMQPublisher


def get_rabbitmq_publisher() -> RabbitMqPublisherInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return  AsyncRabbitMQPublisher(rabbitmq_host, 5672)
