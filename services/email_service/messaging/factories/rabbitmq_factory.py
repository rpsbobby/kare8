import os

from messaging_interfaces.rabbitmq.rabbitmq_subcriber_interface import RabbitMqSubscriberInterface
from ..impl.rabbitmq_subscriber import RabbitMQSubscriber


def get_rabbitmq_subscriber() -> RabbitMqSubscriberInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return  RabbitMQSubscriber(rabbitmq_host, 5672)
