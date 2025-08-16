import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Type, Generic, TypeVar, Optional
from logging import Logger
from pydantic import BaseModel

from entities.dlq_message import DLQMessage
from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface
from workers.worker import Worker

T = TypeVar("T", bound=BaseModel)
V = TypeVar("V", bound=BaseModel)

class MessageHandler(Generic[T,V]):
    def __init__(
        self,
        kafka_producer: KafkaProducerInterface,
        worker: Worker[T,V],
        model_in: Type[T],
        topic_in: str,
        topic_out: str,
        dlq_topic: str,
        park_topic: str,
        logger: Logger,
        max_attempts: int = 3,
    ):
        self.producer = kafka_producer
        self.worker = worker
        self.model_in = model_in
        self.topic_in = topic_in
        self.topic_out = topic_out
        self.dlq_topic = dlq_topic
        self.park_topic = park_topic
        self.is_dlq = topic_in.endswith(".dlq")
        self.logger = logger
        self.max_attempts = max_attempts

    def handle_message(self, message: Dict[str, Any]) -> None:
        self.logger.info(f"[INFO] handling message from {self.topic_in}: {message}")
        if self.is_dlq:
            self._handle_dlq_message(message)
        else:
            self._handle_main_message(message)

    # --- Main path ---
    def _handle_main_message(self, message: Dict[str, Any]) -> None:
        obj = self.model_in.model_validate(message)
        res: V = Optional[V]
        try:
            res  = self._process(obj)
        except Exception as e:
            now = datetime.now(timezone.utc)
            dlq = DLQMessage(
                payload=obj.model_dump(),
                origin_topic=self.topic_in,
                current_topic=self.dlq_topic,
                attempts=1,
                trace_id=str(uuid.uuid4()),
                first_seen_ts=now,
                last_attempt_ts=now,
                last_error=str(e),
                error_code=type(e).__name__,
                intended_next_topic=self.topic_out,
            )
            self._produce(self.dlq_topic, dlq.model_dump())
            return

        # success → next hop
        self._produce(self.topic_out, res.model_dump())

    # --- DLQ replay path ---
    def _handle_dlq_message(self, message: Dict[str, Any]) -> None:
        dlq = DLQMessage.model_validate(message)
        next_hop = getattr(dlq, "intended_next_topic", None) or self.topic_out
        # If payload no longer matches schema T → park
        try:
            obj = self.model_in.model_validate(dlq.payload)
        except Exception as e:
            dlq.last_error = str(e)
            dlq.error_code = type(e).__name__
            dlq.last_attempt_ts = datetime.now(timezone.utc)
            dlq.current_topic = self.park_topic
            self._produce(self.park_topic, dlq.model_dump())
            return

        res: V = Optional[V]
        try:
            res = self._process(obj)
        except Exception as e:
            dlq.attempts += 1
            dlq.last_attempt_ts = datetime.now(timezone.utc)
            dlq.current_topic = self.dlq_topic
            dlq.intended_next_topic = next_hop
            dlq.last_error = str(e)
            dlq.error_code = type(e).__name__

            if dlq.attempts >= self.max_attempts:
                dlq.current_topic = self.park_topic
                self._produce(self.park_topic, dlq.model_dump())
            else:
                self._produce(self.dlq_topic, dlq.model_dump())
            return

        # replay success → intended next hop
        self._produce(next_hop, res.model_dump())

    # --- Delegation ---
    def _process(self, obj: T) -> V:
        self.logger.info("[INFO] Passing message to worker…")
        return self.worker.process(obj)

    def _produce(self, topic: str, payload: Dict[str, Any]) -> None:
        self.producer.produce(topic, payload)
