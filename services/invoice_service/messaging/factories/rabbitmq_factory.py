import os

from messaging_interfaces.rabbitmq.rabbitmq_publisher_interface import RabbitMqPublisherInterface
from messaging_interfaces.rabbitmq.rabbitmq_subcriber_interface import RabbitMqSubscriberInterface
from ..impl.rabbitmq_subscriber import RabbitMQSubscriber
from ..impl.async_rabbitmq_publisher import AsyncRabbitMQPublisher


def get_rabbitmq_subscriber() -> RabbitMqSubscriberInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return  RabbitMQSubscriber(rabbitmq_host, 5672)

def get_rabbitmq_publisher() -> RabbitMqPublisherInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return AsyncRabbitMQPublisher(rabbitmq_host, 5672)