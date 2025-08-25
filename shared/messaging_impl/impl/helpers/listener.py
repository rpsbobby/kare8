import json
import threading
import time
from logging import Logger
from typing import Callable, Dict, Any, Optional

from confluent_kafka import Consumer


class KafkaListener(threading.Thread):
    """Background thread that polls Kafka and dispatches messages."""

    def __init__(self, consumer: Consumer, topic: str, on_message: Callable, logger: Logger):
        super().__init__(daemon=True)
        self._consumer=consumer
        self._topic=topic
        self._on_message=on_message
        self._logger=logger
        self._running=False

    def run(self):
        self._consumer.subscribe([self._topic])
        self._logger.info(f"🔄 Subscribed to topic: {self._topic}")
        self._running=True

        while self._running:
            try:
                msg=self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    self._logger.warning(f"❌ Kafka consumer error: {msg.error()}")
                    continue

                key: Optional[bytes]=msg.key()
                raw_headers=msg.headers() or []
                headers: Dict[str, str]={k: (v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else (v or "")) for k, v in raw_headers}

                try:
                    data: Dict[str, Any]=json.loads(msg.value().decode("utf-8"))
                except Exception as e:
                    self._logger.error(f"🧨 JSON decode failed at {msg.topic()}[{msg.partition()}]@{msg.offset()}: {e}")
                    self._consumer.store_offsets(msg)
                    self._consumer.commit(msg, asynchronous=False)
                    continue

                try:
                    self._on_message(data, key, headers)
                    self._consumer.store_offsets(msg)
                    self._consumer.commit(msg, asynchronous=False)
                except Exception as e:
                    self._logger.error(f"💥 Error in message handler: {e} — data: {data}")

            except Exception as e:
                self._logger.error(f"⚠️ Unexpected error in consumer loop: {e}")
                time.sleep(2)

        self._logger.info("🛑 Kafka listener loop exited")

    def stop(self):
        self._running=False
        self._consumer.wakeup()  # interrupt poll()
