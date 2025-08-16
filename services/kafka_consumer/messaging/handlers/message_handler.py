import uuid
from dataclasses import Field
from logging import Logger

from entities.dlq_message import DLQMessage
from entities.order import Order
from main import kafka_producer
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
        self.is_dlq = topic.endswith("dlq")
        self.logger = logger
        self.max_attempts = max_attempts


    def handle_message(self, message: dict):
        self.logger.info(f"[INFO] handling message: {message}")
        try:
            if self.is_dlq:
                self._handle_dlq_message(message)
            else:
                self._handle_main_order_message(message)
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to handle message: {message}. Error: {e}")




    def _handle_dlq_message(self, message: dict):
        dlq_order = DLQMessage.model_validate(message)
        order = Order.model_validate(dlq_order.payload)
        try:
            if dlq_order.attempts >= self.max_attempts:
                self.logger.warning(f"[WARNING] Message {dlq_order.trace_id} has exceeded max attempts ({self.max_attempts}). Sending to park topic.")
                self.kafka_producer.produce(self.park_topic, dlq_order.model_dump())
            else:
                self.logger.info(f"[INFO] Retrying message {dlq_order.trace_id}, attempt {dlq_order.attempts + 1}.")
                dlq_order.attempts += 1
                dlq_order.last_attempt_ts = dlq_order.first_seen_ts  # Update last attempt timestamp
                self.kafka_producer.produce(self.topic, order.model_dump())
        except Exception as e:
            kafka_producer.produce(self.dlq_topic, dlq_order.model_dump())


        pass

    def _handle_main_order_message(self, message: dict):
        try:
            order = Order.model_validate(message)
            self.kafka_producer.produce(self.topic, order.model_dump())
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to handle main order message: {message}. Error: {e}")
            # Optionally, you can send to DLQ or park topic here
            dlq_message = DLQMessage(
                payload=message,
                version=1,
                origin_topic=self.topic,
                current_topic=self.dlq_topic,
                trace_id=str(uuid.uuid4()),
                attempts=0,
                first_seen_ts=Field.default_factory,
                last_attempt_ts=Field.default_factory,
                last_error=str(e),
                error_code="HANDLER_ERROR"
            )
            self.kafka_producer.produce(self.dlq_topic, dlq_message.model_dump())
        pass

