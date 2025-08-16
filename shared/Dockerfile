FROM python:3.10-slim

WORKDIR /shared

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Install shared modules into /opt/pythonlibs
RUN mkdir -p /opt/pythonlibs
COPY messaging_interfaces /opt/pythonlibs/messaging_interfaces
COPY utils /opt/pythonlibs/utils
COPY topics /opt/pythonlibs/topics
COPY entities /opt/pythonlibs/entities
COPY workers /opt/pythonlibs/workers
COPY handlers /opt/pythonlibs/handlers
RUN chmod -R a+r /opt/pythonlibs

ENV PYTHONPATH="/opt/pythonlibs"
