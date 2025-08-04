import asyncio
import threading

from messaging.factories.kafka_factory import get_kafka_consumer
from messaging.factories.rabbitmq_factory import get_rabbitmq_publisher
import time

from utils.logger import get_logger

logger = get_logger("kafka_consumer")
event_loop = asyncio.get_event_loop()
asyncio.set_event_loop(event_loop)
loop_thread = threading.Thread(target=event_loop.run_forever, daemon=True)
loop_thread.start()


def handle_order(order):
    print(f"[INFO] Received order from Kafka: {order}")

    async def safe_publish():
        try:
            await rabbitmq_publisher.publish("generate-invoice", order)
        except Exception as e:
            print(f"[ERROR] Failed to publish message: {e}")

    # Schedule the coroutine to run in the event loop
    asyncio.run_coroutine_threadsafe(safe_publish(), event_loop)


if __name__ == "__main__":
    logger.iformat = "Starting Kafka Consumer Service... Trying to connect to Kafka and RabbitMQ..."
    kafka_consumer = get_kafka_consumer()
    rabbitmq_publisher = get_rabbitmq_publisher()
    logger.info("Connected to Kafka and RabbitMQ. Subscribing to 'orders' topic...")
    kafka_consumer.consume("orders", handle_order)
    logger.info("Subscribed to 'orders' topic. Waiting for messages...")

    while True:
        time.sleep(1)
