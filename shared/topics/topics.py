# All Kafka topics in one place

ORDERS = "orders"
GENERATE_INVOICE = "generate-invoice"
SEND_EMAIL = "send-email"

# DLQ topics
ORDERS_DLQ = f"{ORDERS}.dlq"
GENERATE_INVOICE_DLQ = f"{GENERATE_INVOICE}.dlq"
SEND_EMAIL_DLQ = f"{SEND_EMAIL}.dlq"

ALL_TOPICS = [
    ORDERS,
    GENERATE_INVOICE,
    SEND_EMAIL,
    ORDERS_DLQ,
    GENERATE_INVOICE_DLQ,
    SEND_EMAIL_DLQ
]
