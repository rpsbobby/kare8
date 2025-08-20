# metrics.py
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Counter: goes up only
messages_processed_total = Counter(
    "messages_processed_total",
    "Total number of processed messages",
    ["topic", "status"]  # labels
)

# Counter: goes up only
messages_accepted_total = Counter(
    "messages_accepted_total",
    "Total number of accepted messages into the producer queue",
    ["topic"]
)

# Histogram: latency buckets
processing_latency_seconds = Histogram(
    "processing_latency_seconds",
    "Message processing latency in seconds",
    ["topic"]
)

# Gauge: point-in-time values
queue_depth = Gauge(
    "queue_depth",
    "Current producer queue size",
    ["topic"]
)

def start_metrics_server(port: int = 8000):
    start_http_server(port)

