import time
from datetime import datetime, timezone
from pydantic import ValidationError

from entities.order import Order
from entities.invoice import Invoice
from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer
from utils.logger import get_logger
from topics.topics import GENERATE_INVOICE, SEND_EMAIL

logger = get_logger("invoice_service")


def handle_generate_invoice(order: Order):
    logger.info(f"🧾 Generating invoice for order {order.order_id}...")

    invoice = Invoice(
        invoice_id=f"inv-{order.order_id}",
        order_id=order.order_id,
        user_id=order.user_id,
        items=order.items,
        total=order.total,
        issued_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )

    kafka_producer.produce(SEND_EMAIL, invoice.model_dump())  # use .dict() if on Pydantic v1
    logger.info(f"🧾 Invoice generated and sent to '{SEND_EMAIL}': {invoice}")


if __name__ == "__main__":
    logger.info("🧾 Starting Invoice Service... Trying to connect to Kafka...")
    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()

    logger.info(f"Connected to Kafka. Subscribing to '{GENERATE_INVOICE}' topic...")

    def wrapped_handler(message: dict):
        logger.info(f"[INFO] Received message from '{GENERATE_INVOICE}' topic: {message}, parsing to handler...")
        try:
            order = Order.model_validate(message)  # for Pydantic v1: Order(**message)
        except ValidationError as e:
            logger.error(f"[ERROR] Validation error: {e}")
            return
        handle_generate_invoice(order)

    kafka_consumer.consume(GENERATE_INVOICE, wrapped_handler)
    logger.info(f"Subscribed to '{GENERATE_INVOICE}' topic. Waiting for messages...")

    while True:
        time.sleep(1)
