import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

from entities.order import Order
from handlers.message_handler import MessageHandler
from messaging.factories.kafka_factory import get_kafka_producer
from messaging.workers.api_pre_worker import ApiPreWorker
from messaging.workers.api_worker import ApiWorker
from o11y.metrics import start_metrics_server
from topics.topics import ORDERS, ORDERS_DLQ, ORDERS_PARK
from utils.logger import get_logger

logger=get_logger("order_api")
PROMETHEUS_SERVER=int(os.getenv("PROMETHEUS_SERVER", "9000"))
TOPIC_IN=os.getenv("TOPIC", "NONE")
DLQ_TOPIC=os.getenv("DLQ_TOPIC", ORDERS_DLQ)
PARK_TOPIC=os.getenv("PARK_TOPIC", ORDERS_PARK)
TOPIC_OUT=os.getenv("TOPIC_OUT", ORDERS)
MAX_ATTEMPTS=int(os.getenv("MAX_ATTEMPTS", "3"))

kafka_producer=get_kafka_producer()
worker=ApiWorker(logger=logger)
pre_worker=ApiPreWorker(logger=logger)
message_handler=MessageHandler(kafka_producer=kafka_producer,
                               worker=worker,
                               model_in=Order,
                               topic_in=TOPIC_IN,
                               topic_out=TOPIC_OUT,
                               dlq_topic=DLQ_TOPIC,
                               park_topic=PARK_TOPIC,
                               logger=logger,
                               max_attempts=MAX_ATTEMPTS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    start_metrics_server(PROMETHEUS_SERVER)  # separate port for Prometheus scraping
    logger.info(f"[INFO]✅ Metrics server started on :{PROMETHEUS_SERVER}")
    yield  # 👈 FastAPI will run the app while we are in this context
    # Shutdown (if needed)
    logger.info("🛑 Shutting down order_api service")


app=FastAPI(lifespan=lifespan)


@app.post("/order")
def create_order(order: Order):
    logger.info(f"Received order request {order.order_id}")

    # Pre-validation (reject bad input before pipeline)
    try:
        pre_worker.process(order)
    except Exception as e:
        logger.warning(f"❌ Rejected order {order.order_id}: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid order: {str(e)}")

    try:
        # Hand off to pipeline (fire-and-forget)
        message_handler.handle_message(order.model_dump())
    except Exception as e:
        logger.error(f"🔥 Pipeline failure for order {order.order_id}: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Order could not be enqueued for processing")

    # Return 202 Accepted to indicate async processing
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content={
        "status": "received", "trace_id": "TODO: extract from handler headers", "order": order.model_dump()
        })
