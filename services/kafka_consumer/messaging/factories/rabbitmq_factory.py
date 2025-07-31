import os

from ..impl.threaded_rabbitmq_publisher import ThreadedRabbitMQPublisher
from ..interfaces.rabbitmq_publisher_interface import RabbitMqPublisherInterface


def get_rabbitmq_publisher() -> RabbitMqPublisherInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return  ThreadedRabbitMQPublisher(rabbitmq_host, 5672)
