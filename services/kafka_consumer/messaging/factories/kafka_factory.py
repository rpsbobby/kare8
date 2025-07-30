import os

from ..impl.kafka_consumer import KafkaConsumer
from ..interfaces.kafka_consumer_interface import KafkaConsumerInterface


def get_kafka_consumer() -> KafkaConsumerInterface:
    kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return KafkaConsumer(kafka_host, group_id="kare8-consumer")
