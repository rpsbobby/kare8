from entities.order import Order
from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer
import time

from utils.logger import get_logger
from topics.topics import ORDERS, GENERATE_INVOICE

logger = get_logger("kafka_consumer")


def handle_order(order:Order):
    logger.info(f"[INFO] Received order from Kafka: {order}")
    logger.info(f"[INFO] Forwarding order to Invoice Topic: {order}")
    kafka_producer.produce(GENERATE_INVOICE, order.model_dump())


if __name__ == "__main__":
    logger.iformat = "Starting Kafka Consumer Service... Trying to connect to Kafka..."
    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()
    logger.info("[INFO] Connected to Kafka. Subscribing to 'orders' topic...")

    def wrap_handle_order(message: dict):
        order = Order.model_validate(message)
        handle_order(order)

    kafka_consumer.consume(ORDERS, wrap_handle_order)

    logger.info("[INFO] Subscribed to 'orders' topic. Waiting for messages...")

    while True:
        time.sleep(1)
