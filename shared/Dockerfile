FROM python:3.10-slim

WORKDIR /shared

COPY . .

# Install shared modules into /opt/pythonlibs
RUN mkdir -p /opt/pythonlibs && \
    cp -r messaging_interfaces /opt/pythonlibs/ && \
    cp -r utils /opt/pythonlibs/ && \
    chmod -R a+r /opt/pythonlibs

ENV PYTHONPATH="/opt/pythonlibs"
