import os
import time

from entities.order import Order
from handlers.message_handler import MessageHandler
from messaging.workers.invoice_worker import InvoiceWorker
from messaging_impl.kafka_factory import get_kafka_consumer, get_kafka_producer
from o11y.metrics import start_metrics_server
from topics.topics import GENERATE_INVOICE, GENERATE_INVOICE_DLQ, GENERATE_INVOICE_PARK, SEND_EMAIL
from utils.logger import get_logger

PROMETHEUS_SERVER=int(os.getenv("PROMETHEUS_SERVER", "9000"))
TOPIC_IN=os.getenv("TOPIC", GENERATE_INVOICE)
DLQ_TOPIC=os.getenv("DLQ_TOPIC", GENERATE_INVOICE_DLQ)
PARK_TOPIC=os.getenv("PARK_TOPIC", GENERATE_INVOICE_PARK)
TOPIC_OUT=os.getenv("TOPIC_OUT", SEND_EMAIL)
MAX_ATTEMPTS=int(os.getenv("MAX_ATTEMPTS", "3"))
kafka_host=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
group_id=os.getenv("GROUP_ID", "kare8-consumer")
logger=get_logger("invoice_service")

if __name__ == "__main__":
    logger.info("🧾 Starting Invoice Service... Trying to connect to Kafka...")
    start_metrics_server(PROMETHEUS_SERVER)  # separate port for Prometheus scraping
    logger.info(f"[INFO]✅ Metrics server started on :{PROMETHEUS_SERVER}")

    kafka_consumer=get_kafka_consumer(bootstrap_servers=kafka_host, group_id=group_id, logger=logger)
    kafka_producer=get_kafka_producer(bootstrap_servers=kafka_host, logger=logger)
    worker=InvoiceWorker(logger=logger)
    message_handler=MessageHandler(kafka_producer=kafka_producer,
                                   worker=worker,
                                   model_in=Order,
                                   topic_in=TOPIC_IN,
                                   topic_out=TOPIC_OUT,
                                   dlq_topic=DLQ_TOPIC,
                                   park_topic=PARK_TOPIC,
                                   logger=logger,
                                   max_attempts=MAX_ATTEMPTS)

    logger.info(f"Connected to Kafka. Subscribing to '{TOPIC_IN}'...")

    kafka_consumer.consume(TOPIC_IN, message_handler.handle_message)
    logger.info(f"Subscribed to '{TOPIC_IN}' topic. Waiting for messages...")

    while True:
        time.sleep(1)
