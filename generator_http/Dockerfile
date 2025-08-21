# services/generator_http/Dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator_http.py .

CMD ["python", "generator_http.py"]
