from datetime import datetime, timezone
from logging import Logger

from entities.invoice import Invoice
from entities.order import Order
from workers.worker import Worker


class InvoiceWorker(Worker[Order, Invoice]):
    def __init__(self, logger: Logger):
        self.logger = logger

    def process(self, order: Order) -> Invoice:
        self.logger.info(f"🧾 Generating invoice for order {order.order_id}...")
        invoice = Invoice(invoice_id=f"inv-{order.order_id}", order_id=order.order_id, user_id=order.user_id, items=order.items,
                          total=order.total, issued_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        return invoice
