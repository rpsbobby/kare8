from logging import Logger

from entities.invoice import Invoice
from workers.worker import Worker


class EmailWorker(Worker[Invoice, Invoice]):
    def __init__(self, logger: Logger):
        self.logger=logger

    def process(self, invoice: Invoice) -> Invoice:
        self.logger.info(f"🧾 Generating invoice for invoice {invoice.order_id}...")

        return invoice
