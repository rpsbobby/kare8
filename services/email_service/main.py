import time

from messaging.factories.rabbitmq_factory import get_rabbitmq_subscriber
from utils.logger import get_logger

logger = get_logger("email_service")

def handle_email_request(message: dict):
    user = message.get("user_id", "unknown")
    order_id = message.get("order_id", "N/A")
    logger.info(f"📧 Sending confirmation email to user '{user}' for order '{order_id}'...")


if __name__ == "__main__":
    logger.info("📨 Starting Email Service... Trying to connect to RabbitMQ...")
    rabbitmq = get_rabbitmq_subscriber()
    rabbitmq.subscribe("send-email", handle_email_request)
    logger.info("✅ Connected to RabbitMQ. Subscribing to 'send-email' queue...")

    while True:
        time.sleep(1)