import json
import threading
import time
from queue import Queue, Empty
from time import sleep
from typing import Dict, Tuple

from confluent_kafka import Producer
from messaging_interfaces.kafka.kafka_producer_interface import KafkaProducerInterface
from utils.logger import get_logger
from utils.retry import retry_with_backoff

logger = get_logger("order_api_KafkaProducer")


class KafkaProducer(KafkaProducerInterface):
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._queue: Queue[Tuple[str, Dict]] = Queue()
        self._producer: Producer = None
        self._running = True
        self._worker_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._worker_thread.start()
        logger.info("🧵 Kafka producer background thread started")

    def produce(self, topic: str, message: Dict):
        if self._producer is None:
            self._init_producer()
        logger.debug(f"📥 Enqueued message to topic '{topic}': {message}")
        self._queue.put((topic, message))

    def _init_producer(self):
        for i in range(10):
            try:
                logger.info(f"Trying to connect to Kafka at {self.bootstrap_servers} (attempt {i + 1}/10)")
                self._producer = Producer({'bootstrap.servers': self.bootstrap_servers})
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
                topic, message = self._queue.get(timeout=1)
                logger.info(f"📬 Actually sending to Kafka: {topic}, {message}")
                self._safe_produce(topic, message)
            except Empty:
                continue

    @retry_with_backoff(logger=logger)
    def _safe_produce(self, topic: str, message: Dict):
        logger.info(f"📤 Publishing to topic '{topic}': {message} from _self_produce")
        self._producer.produce(topic, value=json.dumps(message))
        self._producer.flush()  # You might want to batch or debounce this in future

    def stop(self):
        self._running = False
        self._worker_thread.join()
        if self._producer:
            self._producer.flush()
        logger.info("🛑 Kafka producer stopped cleanly")
