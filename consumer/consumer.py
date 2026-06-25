from confluent_kafka import Consumer
from clickhouse_driver import Client
import json
from datetime import datetime
import logging
import os


def get_env(name):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_int_env(name):
    return int(get_env(name))


kafka_bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS")
kafka_topic = get_env("KAFKA_TOPIC")
consumer_group_id = get_env("CONSUMER_GROUP_ID")
consumer_batch_size = get_int_env("CONSUMER_BATCH_SIZE")

clickhouse_host = get_env("CLICKHOUSE_HOST")
clickhouse_port = get_int_env("CLICKHOUSE_PORT")
clickhouse_user = get_env("CLICKHOUSE_USER")
clickhouse_password = get_env("CLICKHOUSE_PASSWORD")
clickhouse_database = get_env("CLICKHOUSE_DATABASE")

consumer = Consumer({'bootstrap.servers': kafka_bootstrap_servers,
                    'group.id': consumer_group_id,
                    'auto.offset.reset' : 'earliest'})



client = Client(host=clickhouse_host,
                port=clickhouse_port,
                user=clickhouse_user,
                database=clickhouse_database,
                password=clickhouse_password)

consumer.subscribe([kafka_topic])

def parse_message(msg):
    data = json.loads(msg.value().decode('utf-8'))
    data['timestamp'] = datetime.fromisoformat(data['timestamp'])
    data['anomaly'] = bool(data['anomaly'])
    return data

def insert_batch(rows):
    client.execute(
        'INSERT INTO raw_data VALUES',
        rows
    )

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
batch = []


try:
    while True:
        msg = consumer.poll(timeout=1.0)
        logger.debug(msg)
        if msg is None:
                continue
        if msg.error():
            logger.error(f"Kafka error: {msg.error()}")
            continue

        row = parse_message(msg)
        batch.append([
                row['engine_id'], row['timestamp'], row['cycle'],
                row['altitude'], row['mach_number'], row['throttle'],
                row['T2'], row['T50'], row['P2'], row['P15'],
                row['Nf'], row['Nc'], row['Ps30'], row['phi'],
                row['NRf'], row['NRc'], row['BPR'],
                row['anomaly'], row['RUL']
            ])


        if len(batch) >= consumer_batch_size:
            insert_batch(batch)
            logger.info(f"Sent {len(batch)}")
            batch.clear()


except KeyboardInterrupt:
    pass
finally:
    insert_batch(batch)
    batch.clear()
    client.close()
    consumer.close()
