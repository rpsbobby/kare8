import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from entities.order import Order
from handlers.message_handler import MessageHandler
from messaging.factories.kafka_factory import get_kafka_producer
from messaging.workers.order_worker import OrderWorker
from utils.logger import get_logger
from topics.topics import ORDERS, ORDERS_DLQ, ORDERS_PARK, GENERATE_INVOICE
from o11y.metrics import start_metrics_server


logger=get_logger("order_api")
PROMETHEUS_SERVER= int(os.getenv("PROMETHEUS_SERVER", "9000"))  # Port for Prometheus metrics
TOPIC_IN = os.getenv("TOPIC", "NONE")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", ORDERS_DLQ)
PARK_TOPIC = os.getenv("PARK_TOPIC", ORDERS_PARK)
TOPIC_OUT = os.getenv("TOPIC_OUT", ORDERS)
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))

kafka_producer=get_kafka_producer()
worker=OrderWorker(logger=logger)
message_handler=MessageHandler(kafka_producer=kafka_producer, worker=worker, model_in=Order, topic_in=TOPIC_IN, topic_out=TOPIC_OUT,
                               dlq_topic=DLQ_TOPIC, park_topic=PARK_TOPIC, logger=logger, max_attempts=MAX_ATTEMPTS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    start_metrics_server(PROMETHEUS_SERVER)  # separate port for Prometheus scraping
    logger.info(f"✅ Metrics server started on :{PROMETHEUS_SERVER}")

    yield  # 👈 FastAPI will run the app while we are in this context

    # Shutdown (if needed)
    logger.info("🛑 Shutting down order_api service")

app=FastAPI(lifespan=lifespan)
#
@app.post("/order")
def create_order(order: Order):
    logger.info(f"Received order request {order.order_id}")
    kafka_producer.produce(ORDERS, order.model_dump())
    return {"status": "received", "order": order}
