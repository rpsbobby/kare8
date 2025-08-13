from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import uuid
from entities.invoice import Invoice
from entities.order import Order

class DLQMessage(BaseModel):
    payload: Invoice | Order | dict
    version: int = 1
    origin_topic: str
    current_topic: str
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    attempts: int = 0
    first_seen_ts: datetime = Field(default_factory=datetime.utcnow)
    last_attempt_ts: datetime = Field(default_factory=datetime.utcnow)
    last_error: Optional[str] = None
    error_code: Optional[str] = None
