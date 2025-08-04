from messaging.factories.kafka_factory import get_kafka_consumer
from messaging.factories.rabbitmq_factory import get_rabbitmq_publisher
import time

from utils.logger import get_logger

logger = get_logger("kafka_consumer")


def handle_order(order):
    logger.info(f"Received order from Kafka: {order}")
    rabbitmq_publisher.publish("generate-invoice", order)


if __name__ == "__main__":
    logger.iformat = "Starting Kafka Consumer Service... Trying to connect to Kafka and RabbitMQ..."
    kafka_consumer = get_kafka_consumer()
    rabbitmq_publisher = get_rabbitmq_publisher()
    logger.info("Connected to Kafka and RabbitMQ. Subscribing to 'orders' topic...")
    kafka_consumer.consume("orders", handle_order)
    logger.info("Subscribed to 'orders' topic. Waiting for messages...")

    while True:
        time.sleep(1)
