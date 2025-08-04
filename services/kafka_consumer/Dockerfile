FROM kare8-shared:latest

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/app:/opt/pythonlibs"

CMD ["python", "main.py"]
