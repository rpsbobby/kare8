import os
import time

from entities.dlq_message import DLQMessage
from utils.logger import get_logger
from topics.topics import GENERATE_INVOICE, ORDERS_PARK
from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer
from messaging.wrappers.message_wrappers import dlq_order_wrapper, main_order_wrapper
from entities.order import Order

logger = get_logger("kafka_consumer")
TOPIC = os.getenv("TOPIC", "orders")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

def handle_order(order: Order):
    logger.info(f"[INFO] Processing order: {order.order_id}")
    kafka_producer.produce(GENERATE_INVOICE, order.model_dump())

def park_order(dlq_message: DLQMessage):
    payload = dlq_message.payload
    logger.info(f"[INFO] Parking order: {payload.order_id} (trace_id={dlq_message.trace_id})")
    kafka_producer.produce(ORDERS_PARK, dlq_message.model_dump())

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer Service...")
    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()
    logger.info(f"Connected to Kafka. Subscribing to '{TOPIC}'...")

    def wrap_handle_order(message: dict):
        if TOPIC.endswith(".dlq"):
            dlq_message, order = dlq_order_wrapper(message)
            if dlq_message.attempts > MAX_ATTEMPTS:
                logger.error(f"[ERROR] Max attempts exceeded for DLQ message: {dlq_message.trace_id}")
                park_order(dlq_message)
            else:
                logger.info(f"[INFO] Retrying DLQ order: {order.order_id} (attempt {dlq_message.attempts})")
                handle_order(order)  # retry
        else:
            order = main_order_wrapper(message)
            handle_order(order)

    kafka_consumer.consume(TOPIC, wrap_handle_order)

    logger.info(f"Subscribed to '{TOPIC}', waiting for messages...")
    while True:
        time.sleep(1)
