FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka==2.3.0 Faker==24.0.0

COPY producer.py .

CMD ["python", "producer.py"]
