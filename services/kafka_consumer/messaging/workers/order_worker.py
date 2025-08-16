from logging import Logger

from entities.order import Order
from workers.worker import Worker


class OrderWorker(Worker[Order]):
    def __init__(self, logger: Logger):
        self.logger = logger

    def process(self, message: Order) -> bool:
        self.logger.info(f"[INFO] Processing order: {message}")
        # do real work here
        return True