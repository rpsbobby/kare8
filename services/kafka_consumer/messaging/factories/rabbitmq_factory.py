import os

from ..impl.rabbitmq_publisher import RabbitMQPublisher
from ..interfaces.rabbitmq_publisher_interface import RabbitMqPublisherInterface


def get_rabbitmq_publisher() -> RabbitMqPublisherInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return  RabbitMQPublisher(rabbitmq_host, 5672)
