from pydantic import BaseModel
from typing import List

class Order(BaseModel):
    order_id: str
    user_id: str
    user_id: str
    items: List[str]
    total: float