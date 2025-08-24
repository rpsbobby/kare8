import os
import time

from entities.invoice import Invoice
from handlers.message_handler import MessageHandler
from messaging.workers.email_worker import EmailWorker
from messaging_impl.kafka_factory import get_kafka_consumer, get_kafka_producer
from o11y.metrics import start_metrics_server
from topics.topics import SEND_EMAIL, SEND_EMAIL_DLQ, SEND_EMAIL_PARK
from utils.logger import get_logger

PROMETHEUS_SERVER=int(os.getenv("PROMETHEUS_SERVER", "9000"))
TOPIC_IN=os.getenv("TOPIC", SEND_EMAIL)
DLQ_TOPIC=os.getenv("DLQ_TOPIC", SEND_EMAIL_DLQ)
PARK_TOPIC=os.getenv("PARK_TOPIC", SEND_EMAIL_PARK)
TOPIC_OUT=os.getenv("TOPIC_OUT", None)
MAX_ATTEMPTS=int(os.getenv("MAX_ATTEMPTS", "3"))
kafka_host=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
group_id=os.getenv("GROUP_ID", "kare8-consumer")
logger=get_logger("email_service")

if __name__ == "__main__":
    logger.info("📨 Starting Email Service... Trying to connect to Kafka...")
    start_metrics_server(PROMETHEUS_SERVER)  # separate port for Prometheus scraping
    logger.info(f"[INFO]✅ Metrics server started on :{PROMETHEUS_SERVER}")

    kafka_consumer=get_kafka_consumer(bootstrap_servers=kafka_host, group_id=group_id, logger=logger)
    kafka_producer=get_kafka_producer(bootstrap_servers=kafka_host, logger=logger)
    worker=EmailWorker(logger=logger)
    message_handler=MessageHandler(kafka_producer=kafka_producer,
                                   worker=worker,
                                   model_in=Invoice,
                                   topic_in=TOPIC_IN,
                                   topic_out=TOPIC_OUT,
                                   dlq_topic=DLQ_TOPIC,
                                   park_topic=PARK_TOPIC,
                                   logger=logger,
                                   max_attempts=MAX_ATTEMPTS)
    kafka_consumer.consume(SEND_EMAIL, message_handler.handle_message)
    logger.info("✅ Connected to Kafka. Subscribing to 'send-email' topic...")

    while True:
        time.sleep(1)
