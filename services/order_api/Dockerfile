ARG BASE_IMAGE_TAG=latest
FROM ghcr.io/rpsbobby/kare8-shared:${BASE_IMAGE_TAG}

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/app:/opt/pythonlibs"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

