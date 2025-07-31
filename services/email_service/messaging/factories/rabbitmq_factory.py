import os

from ..impl.rabbitmq_subscriber import RabbitMQSubscriber
from ..interfaces.rabbitmq_subcriber_interface import RabbitMqSubscriberInterface


def get_rabbitmq_subscriber() -> RabbitMqSubscriberInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return  RabbitMQSubscriber(rabbitmq_host, 5672)
