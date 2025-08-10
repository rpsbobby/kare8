import time

from entities.order import Order
from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer
from utils.logger import get_logger
from topics.topics import GENERATE_INVOICE, SEND_EMAIL
from entities.invoice import Invoice
logger = get_logger("invoice_service")


def handle_generate_invoice(order: Order):
    logger.info(f"🧾 Generating invoice for order {order['order_id']}...")

    invoice = Invoice(
        invoice_id=f"inv-{order.order_id}",
        order_id=order.order_id,
        user_id=order.user_id,
        items=order.items,
        total=order.total,
        issued_at=time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    )
    kafka_producer.produce(SEND_EMAIL, invoice.model_dump())
    logger.info(f"🧾 Invoice generated and sent to email topic: {invoice}")


if __name__ == "__main__":
    logger.info("🧾 Starting Invoice Service... Trying to connect to Kafka...")
    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()

    logger.info("Connected to Kafka. Subscribing to 'generate-invoice' topic...")
    def wrapped_handler(message: dict):
        order = Order.model_validate(message)
        handle_generate_invoice(order)

    kafka_consumer.consume(GENERATE_INVOICE, wrapped_handler)
    logger.info("Subscribed to 'invoice' topic. Waiting for messages...")

    while True:
        time.sleep(1)
