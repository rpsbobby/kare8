from datetime import datetime, timezone
from entities.order import Order
from workers.worker import Worker
from logging import Logger

class ApiPreWorker(Worker[Order, Order]):
    def __init__(self, logger: Logger):
        self.logger = logger

    def process(self, obj: Order) -> Order:
        # Lightweight validation
        if not obj.order_id:
            raise ValueError("Missing order_id")
        if not obj.user_id:
            raise ValueError("Missing user_id")
        if not obj.items or not isinstance(obj.items, list):
            raise ValueError("Items list cannot be empty")
        if obj.total <= 0:
            raise ValueError("Total must be > 0")

        # Add a received timestamp for traceability
        self.logger.info(f"✔ Order {obj.order_id} prevalidated and enriched")
        obj_dict = obj.model_dump()
        obj_dict["received_ts"] = datetime.now(timezone.utc).isoformat()

        return Order.model_validate(obj_dict)
