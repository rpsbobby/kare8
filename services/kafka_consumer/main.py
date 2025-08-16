import os
import time

from entities.order import Order
from handlers.message_handler import MessageHandler
from messaging.workers.order_worker import OrderWorker
from utils.logger import get_logger
from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer

TOPIC_IN = os.getenv("TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", f"{TOPIC_IN}.dlq")
PARK_TOPIC = os.getenv("PARK_TOPIC", f"{TOPIC_IN}.park")
TOPIC_OUT = os.getenv("TOPIC_OUT", "invoice")

MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

logger = get_logger("kafka_consumer")

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer Service...")

    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()
    worker = OrderWorker(logger=logger)
    message_handler = MessageHandler(kafka_producer=kafka_producer, worker=worker, model_cls=Order, topic_in=TOPIC_IN, topic_out=TOPIC_OUT,
                                     dlq_topic=DLQ_TOPIC, park_topic=PARK_TOPIC, logger=logger, max_attempts=MAX_ATTEMPTS)

    logger.info(f"Connected to Kafka. Subscribing to '{TOPIC_IN}'...")

    kafka_consumer.consume(TOPIC_IN, message_handler.handle_message)

    logger.info(f"Subscribed to '{TOPIC_IN}', waiting for messages...")
    while True:
        time.sleep(1)
