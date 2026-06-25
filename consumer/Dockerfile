FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.3.0 clickhouse-driver==0.2.7

COPY consumer.py .

CMD ["python", "consumer.py"]
