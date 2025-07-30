from fastapi import FastAPI
from pydantic import BaseModel

from messaging.factories.kafka_factory import get_kafka_producer
from messaging.interfaces.kafka_producer_interface import KafkaProducerInterface

app = FastAPI()

kafka_producer: KafkaProducerInterface = get_kafka_producer();


class Order(BaseModel):
    order_id: str
    user_id: str
    items: list
    total: float


@app.post("/order")
def create_order(order: Order):
    # Publish the order to the Kafka topic
    kafka_producer.publish("orders", order.model_dump())
    return {"status": "received", "order": order}
