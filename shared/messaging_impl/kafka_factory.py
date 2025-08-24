from logging import Logger

from messaging_impl.impl.kafka_consumer import KafkaConsumer
from messaging_impl.impl.kafka_producer import KafkaProducer
from messaging_interfaces.kafka.kafka_consumer_interface import KafkaConsumerInterface
from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface


def get_kafka_consumer(bootstrap_servers: str, group_id: str, logger: Logger) -> KafkaConsumerInterface:
    return KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=group_id, logger=logger)


def get_kafka_producer(bootstrap_servers: str, logger: Logger) -> KafkaProducerInterface:
    return KafkaProducer(bootstrap_servers=bootstrap_servers, logger=logger)
