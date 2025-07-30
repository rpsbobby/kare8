import os

from ..impl.kafka_producer import KafkaProducer
from ..interfaces.kafka_producer_interface import KafkaProducerInterface


def get_kafka_producer() -> KafkaProducerInterface:
    # bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return KafkaProducer("kafka:9092")
