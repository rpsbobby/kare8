import os

from messaging_interfaces.kafka.kafka_consumer_interface import KafkaConsumerInterface
from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface
from ..impl.kafka_consumer import KafkaConsumer


def get_kafka_consumer() -> KafkaConsumerInterface:
    kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return KafkaConsumer(kafka_host, group_id="email-consumer")
