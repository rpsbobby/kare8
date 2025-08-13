from logging import Logger

from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface


#
# def main_order_wrapper(message: dict):
#     """Validate a main order payload and parse it into an Order model."""
#     order = Order.model_validate(message)
#     return order
#
# def dlq_order_wrapper(message: dict) -> tuple[DLQMessage, Order]:
#     """Validate a DLQ order payload and increment attempts safely."""
#     dlq_order = DLQMessage.model_validate(message)
#     dlq_order.attempts += 1  # fixed increment
#
#     # Parse payload into Order model (ensure it's the right shape)
#     if isinstance(dlq_order.payload, dict):
#         order = Order.model_validate(dlq_order.payload)
#     elif isinstance(dlq_order.payload, Order):
#         order = dlq_order.payload
#     else:
#         raise ValueError(f"Unexpected payload type in DLQ message: {type(dlq_order.payload)}")
#
#     return dlq_order, order


class MessageHandler:
    """Handler class for processing Kafka messages to Order topic"""
    def __init__(self, kafka_producer: KafkaProducerInterface, topic: str, dlq_topic: str, park_topic: str, logger: Logger, max_attempts: int = 3):
        self.kafka_producer = kafka_producer
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.park_topic = park_topic
        self.logger = logger
        self.max_attempts = max_attempts
        self.is_dlq = topic.endswith("dlq")


    def handle_message(self, message: dict):

        pass


    def _handle_dlq_message(self, message: dict):
        pass

    def _handle_main_order_message(self, message: dict):
        pass

