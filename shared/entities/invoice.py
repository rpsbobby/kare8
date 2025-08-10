from pydantic import BaseModel
from typing import List

class Invoice(BaseModel):
    invoice_id: str
    order_id: str
    user_id: str
    items: List[str]
    total: float
    issued_at: str  # ISO datetime string