import time

from messaging.factories.kafka_factory import get_kafka_consumer
from utils.logger import get_logger
from topics.topics import SEND_EMAIL

logger = get_logger("email_service")

def handle_email_request(message: dict):
    user = message.get("user_id", "unknown")
    order_id = message.get("order_id", "N/A")
    logger.info(f"📧 Sending confirmation email to user '{user}' for order '{order_id}'...")


if __name__ == "__main__":
    logger.info("📨 Starting Email Service... Trying to connect to Kafka...")
    kafka_consumer = get_kafka_consumer()
    kafka_consumer.consume(SEND_EMAIL, handle_email_request)
    logger.info("✅ Connected to Kafka. Subscribing to 'send-email' topic...")

    while True:
        time.sleep(1)