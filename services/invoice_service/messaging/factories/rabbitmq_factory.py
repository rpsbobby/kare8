import os

from ..impl.rabbitmq_subscriber import RabbitMQSubscriber
from ..impl.threaded_rabbitmq_publisher import ThreadedRabbitMQPublisher
from ..interfaces.rabbitmq_publisher_interface import RabbitMqPublisherInterface
from ..interfaces.rabbitmq_subcriber_interface import RabbitMqSubscriberInterface


def get_rabbitmq_subscriber() -> RabbitMqSubscriberInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return  RabbitMQSubscriber(rabbitmq_host, 5672)

def get_rabbitmq_publisher() -> RabbitMqPublisherInterface:
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    return ThreadedRabbitMQPublisher(rabbitmq_host, 5672)