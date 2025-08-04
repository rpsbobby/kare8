import time

from messaging.factories.rabbitmq_factory import get_rabbitmq_subscriber, get_rabbitmq_publisher
from utils.logger import get_logger

logger = get_logger("invoice_service")


def handle_generate_invoice(order: dict):
    logger.info(f"🧾 Generating invoice for order {order['order_id']}...")

    invoice = {
        "invoice_id": f"inv-{order['order_id']}",
        "user_id": order["user_id"],
        "total": order["total"]
    }

    rabbitmq_publisher.publish("send-email", invoice)


if __name__ == "__main__":
    logger.info("🧾 Starting Invoice Service... Trying to connect to RabbitMQ...")
    rabbitmq_subscriber = get_rabbitmq_subscriber()
    rabbitmq_publisher = get_rabbitmq_publisher()

    rabbitmq_subscriber.subscribe("generate-invoice", handle_generate_invoice)

    while True:
        time.sleep(1)