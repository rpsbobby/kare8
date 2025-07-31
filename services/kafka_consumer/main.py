from messaging.factories.kafka_factory import get_kafka_consumer
from messaging.factories.rabbitmq_factory import get_rabbitmq_publisher
import time


def handle_order(order):
    print(f"Received order from Kafka: {order}")
    rabbitmq_publisher.publish("generate-invoice", order)


if __name__ == "__main__":
    kafka_consumer = get_kafka_consumer()
    rabbitmq_publisher = get_rabbitmq_publisher()

    kafka_consumer.subscribe("orders", handle_order)

    print("Kafka Consumer is running...")

    while True:
        time.sleep(1)
