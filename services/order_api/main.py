from fastapi import FastAPI
from pydantic import BaseModel

from messaging.factories.kafka_factory import get_kafka_producer
from utils.logger import get_logger
app = FastAPI()


logger = get_logger("order_api")

kafka_producer = get_kafka_producer();


class Order(BaseModel):
    order_id: str
    user_id: str
    items: list
    total: float


@app.post("/order")
def create_order(order: Order):
    logger.info(f"Received order request {order.order_id}")
    kafka_producer.produce("orders", order.model_dump())
    return {"status": "received", "order": order}
