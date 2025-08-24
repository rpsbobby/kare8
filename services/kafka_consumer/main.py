import os
import time

from entities.order import Order
from handlers.message_handler import MessageHandler
from messaging.workers.order_worker import OrderWorker
from messaging_impl.kafka_factory import get_kafka_consumer, get_kafka_producer
from o11y.metrics import start_metrics_server
from topics.topics import ORDERS, GENERATE_INVOICE, ORDERS_DLQ, ORDERS_PARK
from utils.logger import get_logger

PROMETHEUS_SERVER=int(os.getenv("PROMETHEUS_SERVER", "9000"))
TOPIC_IN=os.getenv("TOPIC", ORDERS)
DLQ_TOPIC=os.getenv("DLQ_TOPIC", ORDERS_DLQ)
PARK_TOPIC=os.getenv("PARK_TOPIC", ORDERS_PARK)
TOPIC_OUT=os.getenv("TOPIC_OUT", GENERATE_INVOICE)
MAX_ATTEMPTS=int(os.getenv("MAX_ATTEMPTS", "3"))
group_id=os.getenv("GROUP_ID", "kare8-consumer")
kafka_host=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
logger=get_logger("kafka_consumer")

if __name__ == "__main__":
    logger.info("Starting Kafka Consumer Service...")
    start_metrics_server(PROMETHEUS_SERVER)  # separate port for Prometheus scraping
    logger.info(f"[INFO]✅ Metrics server started on :{PROMETHEUS_SERVER}")

    kafka_consumer=get_kafka_consumer(bootstrap_servers=kafka_host, group_id=group_id, logger=logger)
    kafka_producer=get_kafka_producer(bootstrap_servers=kafka_host, logger=logger)
    worker=OrderWorker(logger=logger)
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

    logger.info(f"Subscribed to '{TOPIC_IN}', waiting for messages...")
    while True:
        time.sleep(1)
