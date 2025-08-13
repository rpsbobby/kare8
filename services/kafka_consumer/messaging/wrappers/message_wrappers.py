from entities.dlq_message import DLQMessage
from entities.order import Order


def main_order_wrapper(message: dict):
    """Validate a main order payload and parse it into an Order model."""
    order = Order.model_validate(message)
    return order

def dlq_order_wrapper(message: dict) -> tuple[DLQMessage, Order]:
    """Validate a DLQ order payload and increment attempts safely."""
    dlq_order = DLQMessage.model_validate(message)
    dlq_order.attempts += 1  # fixed increment

    # Parse payload into Order model (ensure it's the right shape)
    if isinstance(dlq_order.payload, dict):
        order = Order.model_validate(dlq_order.payload)
    elif isinstance(dlq_order.payload, Order):
        order = dlq_order.payload
    else:
        raise ValueError(f"Unexpected payload type in DLQ message: {type(dlq_order.payload)}")

    return dlq_order, order
