from fastapi import FastAPI
from pydantic import BaseModel

from messaging.factories.kafka_factory import get_kafka_producer

app = FastAPI()

kafka_producer = get_kafka_producer();


class Order(BaseModel):
    order_id: str
    user_id: str
    items: list
    total: float


@app.post("/order")
def create_order(order: Order):
    kafka_producer.publish("orders", order.model_dump())
    return {"status": "received", "order": order}
