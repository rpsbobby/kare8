import os
import time
from multiprocessing.pool import worker

from pydantic import ValidationError

from entities.order import Order
from handlers.message_handler import MessageHandler
from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer
from messaging.workers.invoice_worker import InvoiceWorker
from utils.logger import get_logger
from topics.topics import GENERATE_INVOICE, GENERATE_INVOICE_DLQ, GENERATE_INVOICE_PARK, SEND_EMAIL

TOPIC_IN = os.getenv("TOPIC", GENERATE_INVOICE)
DLQ_TOPIC = os.getenv("DLQ_TOPIC", GENERATE_INVOICE_DLQ)
PARK_TOPIC = os.getenv("PARK_TOPIC", GENERATE_INVOICE_PARK)
TOPIC_OUT = os.getenv("TOPIC_OUT", SEND_EMAIL)

MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

logger = get_logger("invoice_service")

if __name__ == "__main__":
    logger.info("🧾 Starting Invoice Service... Trying to connect to Kafka...")
    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()
    worker = InvoiceWorker(logger=logger)
    message_handler = MessageHandler(kafka_producer=kafka_producer, worker=worker, model_in=Order, topic_in=TOPIC_IN, topic_out=TOPIC_OUT,
                                     dlq_topic=DLQ_TOPIC, park_topic=PARK_TOPIC, logger=logger, max_attempts=MAX_ATTEMPTS)

    logger.info(f"Connected to Kafka. Subscribing to '{TOPIC_IN}'...")

    kafka_consumer.consume(TOPIC_IN, message_handler.handle_message)

    kafka_consumer.consume(GENERATE_INVOICE, )
    logger.info(f"Subscribed to '{GENERATE_INVOICE}' topic. Waiting for messages...")

    while True:
        time.sleep(1)
