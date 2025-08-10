import time

from messaging.factories.kafka_factory import get_kafka_consumer, get_kafka_producer
from utils.logger import get_logger
from topics.topics import GENERATE_INVOICE, SEND_EMAIL
logger = get_logger("invoice_service")


def handle_generate_invoice(order: dict):
    logger.info(f"🧾 Generating invoice for order {order['order_id']}...")

    invoice = {"invoice_id": f"inv-{order['order_id']}", "user_id": order["user_id"], "total": order["total"]}
    kafka_producer.produce(SEND_EMAIL, invoice)
    logger.info(f"🧾 Invoice generated and sent to email topic: {invoice}")


if __name__ == "__main__":
    logger.info("🧾 Starting Invoice Service... Trying to connect to Kafka...")
    kafka_consumer = get_kafka_consumer()
    kafka_producer = get_kafka_producer()

    logger.info("Connected to Kafka. Subscribing to 'generate-invoice' topic...")
    kafka_consumer.consume(GENERATE_INVOICE, handle_generate_invoice)
    logger.info("Subscribed to 'invoice' topic. Waiting for messages...")

    while True:
        time.sleep(1)
