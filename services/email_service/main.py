from messaging.factories.rabbitmq_factory import get_rabbitmq_subscriber

def handle_email_request(message: dict):
    user = message.get("user_id", "unknown")
    order_id = message.get("order_id", "N/A")
    print(f"📧 Sending confirmation email to user '{user}' for order '{order_id}'...")

if __name__ == "__main__":
    print("📨 Starting Email Service... Trying to connect to RabbitMQ...")
    rabbitmq = get_rabbitmq_subscriber()
    rabbitmq.subscribe("send-email", handle_email_request)
    print("✅ Connected to RabbitMQ. Subscribing to 'send-email' queue...")

    print("📨 Email service is listening on queue: send-email")

    import time
    while True:
        time.sleep(1)
