from logging import Logger

from entities.order import Order
from workers.worker import Worker


class ApiWorker(Worker[Order, Order]):
    def __init__(self, logger: Logger):
        self.logger = logger

    def process(self, message: Order) -> Order:
        self.logger.info(f"[INFO] API Worker parsing order: {message}")

        return message