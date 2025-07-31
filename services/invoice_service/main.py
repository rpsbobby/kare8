from messaging.factories.rabbitmq_factory import get_rabbitmq_subscriber, get_rabbitmq_publisher

def handle_generate_invoice(order: dict):
    print(f"🧾 Generating invoice for order {order['order_id']}...")

    invoice = {
        "invoice_id": f"inv-{order['order_id']}",
        "user_id": order["user_id"],
        "total": order["total"]
    }

    rabbitmq_publisher.publish("send-email", invoice)

if __name__ == "__main__":
    print("🧾 Starting Invoice Service... Trying to connect to RabbitMQ...")
    rabbitmq_subscriber = get_rabbitmq_subscriber()
    rabbitmq_publisher = get_rabbitmq_publisher()

    print("✅ Connected to RabbitMQ. Subscribing to 'generate-invoice' queue...")
    rabbitmq_subscriber.subscribe("generate-invoice", handle_generate_invoice)

    print("🧾 Invoice service listening on 'generate-invoice' queue...")
    import time
    while True:
        time.sleep(1)
