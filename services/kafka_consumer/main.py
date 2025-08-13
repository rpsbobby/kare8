import os
import time

from utils.logger import get_logger
from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer
from messaging.handlers.message_handler import MessageHandler

TOPIC = os.getenv("TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", f"{TOPIC}.dlq")
PARK_TOPIC = os.getenv("PARK_TOPIC", f"{TOPIC}.park")
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

logger = get_logger("kafka_consumer")

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer Service...")

    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()
    message_handler = MessageHandler(kafka_consumer=kafka_consumer, topic=TOPIC, dlq_topic=DLQ_TOPIC, park_topic=PARK_TOPIC, logger=logger, max_attempts=MAX_ATTEMPTS)

    logger.info(f"Connected to Kafka. Subscribing to '{TOPIC}'...")

    kafka_consumer.consume(TOPIC, message_handler.handle_message)

    logger.info(f"Subscribed to '{TOPIC}', waiting for messages...")
    while True:
        time.sleep(1)
