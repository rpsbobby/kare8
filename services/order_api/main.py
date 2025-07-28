from fastapi import FastAPI
from pydantic import BaseModel
from messaging.kafka_backend import KafkaBroker

app = FastAPI()

broker = KafkaBroker("localhost:9092")

class Order(BaseModel):
    order_id: str
    user_id: str
    items: list
    total: float

@app.post("/order")
def create_order(order: Order):
    return {"status": "received", "order": order}
