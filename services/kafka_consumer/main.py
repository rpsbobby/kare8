import os
from messaging.kafka_backend import KafkaBroker
from messaging.rabbitmq_backend import RabbitMQBroker

def handle_order(order):
    print(f"Received order from Kafka: {order}")
    rabbitmq.publish("send-email", order)
    rabbitmq.publish("generate-invoice", order)

if __name__ == "__main__":
    kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    kafka = KafkaBroker(kafka_host, group_id="kare8-consumer")
    rabbitmq = RabbitMQBroker(rabbitmq_host, 5672)

    kafka.subscribe("orders", handle_order)

    print("Kafka Consumer is running...")
    import time
    while True:
        time.sleep(1)
