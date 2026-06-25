from confluent_kafka import Consumer, TopicPartition
from clickhouse_driver import Client
import json
from datetime import datetime
import logging
import os
import time


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

consumer = Consumer({
    'bootstrap.servers': kafka_bootstrap_servers,
    'group.id': consumer_group_id,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})

client = Client(host=clickhouse_host,
                port=clickhouse_port,
                user=clickhouse_user,
                database=clickhouse_database,
                password=clickhouse_password)

consumer.subscribe([kafka_topic])


def message_location(msg):
    return f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"


def parse_message(msg):
    data = json.loads(msg.value().decode('utf-8'))
    data['timestamp'] = datetime.fromisoformat(data['timestamp'])
    data['anomaly'] = bool(data['anomaly'])
    return data


def to_clickhouse_row(row):
    return [
        row['engine_id'], row['timestamp'], row['cycle'],
        row['altitude'], row['mach_number'], row['throttle'],
        row['T2'], row['T50'], row['P2'], row['P15'],
        row['Nf'], row['Nc'], row['Ps30'], row['phi'],
        row['NRf'], row['NRc'], row['BPR'],
        row['anomaly'], row['RUL']
    ]


def insert_batch(rows):
    client.execute(
        'INSERT INTO raw_data VALUES',
        rows
    )


def offsets_for_messages(messages):
    offsets_by_partition = {}
    for msg in messages:
        key = (msg.topic(), msg.partition())
        offsets_by_partition[key] = max(offsets_by_partition.get(key, -1), msg.offset())

    return [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in offsets_by_partition.items()
    ]


def commit_offsets(messages):
    offsets = offsets_for_messages(messages)
    if not offsets:
        return

    consumer.commit(offsets=offsets, asynchronous=False)
    logger.info(
        "Committed Kafka offsets for %s messages across %s partitions",
        len(messages),
        len(offsets),
    )


def close_clickhouse_client():
    disconnect = getattr(client, 'disconnect', None)
    close = getattr(client, 'close', None)

    if callable(disconnect):
        disconnect()
    elif callable(close):
        close()


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

batch = []
batch_messages = []
consumed_count = 0


def flush_current_batch():
    if not batch:
        return True

    try:
        insert_batch(batch)
    except Exception:
        logger.exception(
            "ClickHouse insert failed for batch_size=%s; Kafka offsets were not committed",
            len(batch),
        )
        return False

    logger.info("Inserted %s rows into ClickHouse raw_data", len(batch))

    try:
        # Offsets are committed after ClickHouse insert, so delivery is at-least-once.
        commit_offsets(batch_messages)
    except Exception:
        logger.exception(
            "Kafka offset commit failed after ClickHouse insert; records may be reprocessed"
        )

    batch.clear()
    batch_messages.clear()
    return True


def skip_malformed_message(msg, error):
    logger.error(
        "Malformed Kafka message skipped at %s: %s",
        message_location(msg),
        error,
    )
    # Malformed JSON cannot be inserted into raw_data. We skip it explicitly
    # after logging so one poison message does not block the pipeline.
    try:
        commit_offsets([msg])
    except Exception:
        logger.exception(
            "Failed to commit offset for skipped malformed message at %s",
            message_location(msg),
        )


try:
    while True:
        if len(batch) >= consumer_batch_size:
            if not flush_current_batch():
                time.sleep(1)
            continue

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            logger.error("Kafka message error at %s: %s", message_location(msg), msg.error())
            continue

        consumed_count += 1

        try:
            row = parse_message(msg)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, KeyError, TypeError) as error:
            if batch and not flush_current_batch():
                logger.error(
                    "Malformed message at %s was not committed because the current batch insert failed",
                    message_location(msg),
                )
                time.sleep(1)
                continue

            skip_malformed_message(msg, error)
            continue

        batch.append(to_clickhouse_row(row))
        batch_messages.append(msg)

        if consumed_count % consumer_batch_size == 0:
            logger.info("Consumed %s messages; current batch size=%s", consumed_count, len(batch))

except KeyboardInterrupt:
    logger.info("Consumer shutdown requested")
finally:
    if batch:
        if flush_current_batch():
            logger.info("Final batch flush completed")
        else:
            logger.error("Final batch insert failed; offsets were not committed")

    consumer.close()
    close_clickhouse_client()
    logger.info("Consumer stopped cleanly")
