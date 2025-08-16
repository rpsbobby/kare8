import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Type, Generic, TypeVar, Optional
from logging import Logger
from pydantic import BaseModel

from entities.dlq_message import DLQMessage
from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface
from workers.worker import Worker

T=TypeVar("T", bound=BaseModel)
V=TypeVar("V", bound=BaseModel)


class MessageHandler(Generic[T, V]):
    def __init__(
            self,
            kafka_producer: KafkaProducerInterface,
            worker: Worker[T, V],
            model_in: Type[T],
            topic_in: str,
            topic_out: str,
            dlq_topic: str,
            park_topic: str,
            logger: Logger,
            max_attempts: int = 3,
            ):
        self.producer=kafka_producer
        self.worker=worker
        self.model_in=model_in
        self.topic_in=topic_in
        self.topic_out=topic_out
        self.dlq_topic=dlq_topic
        self.park_topic=park_topic
        self.is_dlq=topic_in.endswith(".dlq")
        self.logger=logger
        self.max_attempts=max_attempts

    # --- Entry point ---
    def handle_message(
            self,
            message: Dict[str, Any],
            key: Optional[bytes] = None,
            headers: Optional[dict[str, str]] = None,
            ) -> None:
        self.logger.info(f"[INFO] Handling message from {self.topic_in}: {message}")
        if self.is_dlq:
            self._handle_dlq_message(message, key, headers)
        else:
            self._handle_main_message(message, key, headers)

    # --- Main path ---
    def _handle_main_message(
            self,
            message: Dict[str, Any],
            key: Optional[bytes],
            headers: Optional[dict[str, str]],
            ) -> None:
        obj=self.model_in.model_validate(message)
        try:
            res=self._process(obj)
        except Exception as e:
            now=datetime.now(timezone.utc)
            trace_id=(headers or {}).get("x-trace-id") or str(uuid.uuid4())
            dlq=DLQMessage(
                payload=obj.model_dump(),
                origin_topic=self.topic_in,
                current_topic=self.dlq_topic,
                attempts=1,
                trace_id=trace_id,
                first_seen_ts=now,
                last_attempt_ts=now,
                last_error=str(e),
                error_code=type(e).__name__,
                intended_next_topic=self.topic_out,
                )
            dlq_headers=self._merge_headers(
                headers,
                {
                    "x-trace-id": trace_id,
                    "x-origin-topic": self.topic_in,
                    "x-attempts": "1",
                    "x-error-code": dlq.error_code or "PROCESSING_ERROR",
                    "x-produced-by": "kare8-service",
                    },
                )
            self._produce(self.dlq_topic, dlq.model_dump(), key=key, headers=dlq_headers)
            return

        # success → next hop
        out_headers=self._merge_headers(
            headers,
            {
                "x-trace-id": (headers or {}).get("x-trace-id") or str(uuid.uuid4()),
                "x-origin-topic": self.topic_in,
                "x-produced-by": "kare8-service",
                },
            )
        self._produce(self.topic_out, res.model_dump(), key=key, headers=out_headers)

    # --- DLQ replay path ---
    def _handle_dlq_message(
            self,
            message: Dict[str, Any],
            key: Optional[bytes],
            headers: Optional[dict[str, str]],
            ) -> None:
        dlq=DLQMessage.model_validate(message)
        next_hop=dlq.intended_next_topic or self.topic_out

        # If payload is no longer valid → park
        try:
            obj=self.model_in.model_validate(dlq.payload)
        except Exception as e:
            dlq.last_error=str(e)
            dlq.error_code=type(e).__name__
            dlq.last_attempt_ts=datetime.now(timezone.utc)
            dlq.current_topic=self.park_topic
            park_headers=self._merge_headers(
                headers,
                {
                    "x-trace-id": dlq.trace_id,
                    "x-attempts": str(dlq.attempts),
                    "x-error-code": dlq.error_code,
                    },
                )
            self._produce(self.park_topic, dlq.model_dump(), key=key, headers=park_headers)
            return

        try:
            res=self._process(obj)
        except Exception as e:
            dlq.attempts+=1
            dlq.last_attempt_ts=datetime.now(timezone.utc)
            dlq.current_topic=self.dlq_topic
            dlq.intended_next_topic=next_hop
            dlq.last_error=str(e)
            dlq.error_code=type(e).__name__

            fail_headers=self._merge_headers(
                headers,
                {
                    "x-trace-id": dlq.trace_id,
                    "x-attempts": str(dlq.attempts),
                    "x-error-code": dlq.error_code,
                    },
                )

            if dlq.attempts >= self.max_attempts:
                dlq.current_topic=self.park_topic
                self._produce(self.park_topic, dlq.model_dump(), key=key, headers=fail_headers)
            else:
                self._produce(self.dlq_topic, dlq.model_dump(), key=key, headers=fail_headers)
            return

        # replay success → intended next hop
        success_headers=self._merge_headers(
            headers,
            {
                "x-trace-id": dlq.trace_id,
                "x-attempts": str(dlq.attempts),
                },
            )
        self._produce(next_hop, res.model_dump(), key=key, headers=success_headers)

    # --- Delegation ---
    def _process(self, obj: T) -> V:
        self.logger.info("[INFO] Passing message to worker…")
        return self.worker.process(obj)

    def _produce(
            self,
            topic: str,
            payload: Dict[str, Any],
            key: Optional[bytes] = None,
            headers: Optional[dict[str, str]] = None,
            ) -> None:
        self.producer.produce(topic, payload, key=key, headers=headers)

    def _merge_headers(
            self, base: Optional[dict[str, str]], extra: dict[str, str]
            ) -> dict[str, str]:
        h=dict(base or {})
        h.update({k: str(v) for k, v in extra.items()})
        return h
