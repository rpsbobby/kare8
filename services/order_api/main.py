from fastapi import FastAPI
from pydantic import BaseModel

from entities.order import Order
from messaging.factories.kafka_factory import get_kafka_producer
from utils.logger import get_logger
from topics.topics import ORDERS

app = FastAPI()

logger = get_logger("order_api")

kafka_producer = get_kafka_producer();


@app.post("/order")
def create_order(order: Order):
    logger.info(f"Received order request {order.order_id}")
    kafka_producer.produce(ORDERS, order.model_dump())
    return {"status": "received", "order": order}
