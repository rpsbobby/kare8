from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface
from ..impl.kafka_producer import KafkaProducer


def get_kafka_producer() -> KafkaProducerInterface:
    # bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return KafkaProducer("kafka:9092")
