FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy service source code
COPY . .

# Install dependencies
RUN pip install --no-cache-dir pika

# Set optional Python path if needed
# ENV PYTHONPATH=/app

# Run the service
CMD ["python", "main.py"]
