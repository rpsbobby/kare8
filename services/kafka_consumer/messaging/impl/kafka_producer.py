import json
import threading
import time
from queue import Queue, Empty
from typing import Dict, Tuple, Optional

from confluent_kafka import Producer
from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface
from utils.logger import get_logger
from utils.retry import retry_with_backoff

logger = get_logger("kafka_consumer")


class KafkaProducer(KafkaProducerInterface):
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._queue:  Queue[Tuple[str, Dict, Optional[bytes], Optional[Dict[str, str]]]] = Queue(maxsize=10000)
        self._producer: Producer = None
        self._running = True
        self._worker_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._worker_thread.start()
        logger.info("🧵 Kafka producer background thread started")

    def produce(self, topic: str, message: Dict, key: bytes | None = None, headers: Dict[str, str] | None = None):
        if self._producer is None:
            self._init_producer()
        logger.debug(f"📥 Enqueued message to topic '{topic}': {message}")
        self._queue.put((topic, message, key, headers))

    def _init_producer(self):
        cfg = {"bootstrap.servers": self.bootstrap_servers, "enable.idempotence": True, "acks": "all", "compression.type": "lz4",
            # or zstd if available
            "linger.ms": 5, "batch.num.messages": 10000, "delivery.timeout.ms": 120000,  # total send time budget
            "retries": 2147483647,  # librdkafka bounds by delivery.timeout.ms
            "max.in.flight.requests.per.connection": 5,  # keep <=5 when idempotent
        }
        for i in range(10):
            try:
                logger.info(f"Trying to connect to Kafka at {self.bootstrap_servers} (attempt {i + 1}/10)")
                self._producer = Producer(cfg)
                logger.info("✅ Kafka producer ready")
                break
            except Exception as e:
                logger.warning(f"❌ Kafka not ready yet: {e}")
                time.sleep(3)
        else:
            raise ConnectionError("Kafka failed to connect after 10 retries")

    def _run_loop(self):
        while self._running:
            try:
                topic, message, key, headers = self._queue.get(timeout=1)
                self._safe_produce(topic, message, key, headers)
            except Empty:
                continue

    @retry_with_backoff(logger=logger)
    def _safe_produce(self, topic: str, message: Dict):
        logger.info(f"📤 Publishing to topic '{topic}': {message} from _self_produce")
        self._producer.produce(topic, value=json.dumps(message))
        self._producer.flush()  # You might want to batch or debounce this in future

    def _delivery(self, err, msg):
        if err:
            logger.warning(f"❌ delivery failed: {err} (topic={msg.topic()}, key={msg.key()})")
        else:
            logger.debug(f"✅ delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    @retry_with_backoff(logger=logger)
    def _safe_produce(self, topic: str, message: Dict, key: bytes | None, headers: Dict[str, str] | None):
        logger.info(f"📤 Publishing to topic '{topic}': {message} from _self_produce")
        payload = json.dumps(message, separators=(",", ":"), default=str)
        h = [(k, v.encode()) for k, v in (headers or {}).items()]
        self._producer.produce(topic, key=key, value=payload, headers=h, callback=self._delivery)
        self._producer.poll(0)  # serve callbacks (no per-message flush!)

    def stop(self):
        self._running = False
        self._worker_thread.join()
        if self._producer:
            # drain remaining events
            remaining = self._queue.qsize()
            if remaining:
                logger.info(f"⏳ draining {remaining} queued messages...")
                while not self._queue.empty():
                    topic, message = self._queue.get_nowait()
                    self._safe_produce(topic, message)
            self._producer.flush(10)  # seconds
        logger.info("🛑 Kafka producer stopped cleanly")
