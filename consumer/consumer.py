from confluent_kafka import Consumer
from clickhouse_driver import Client
import json
from datetime import datetime

consumer = Consumer({'bootstrap.servers': 'localhost:29092,localhost:29093,localhost:29094',
                    'group.id': 'turbofan-pipeline-2',
                    'auto.offset.reset' : 'earliest'})



client = Client(host='localhost',
                port=9000,
                user='user',
                database='turbofan',
                password='passwd')

consumer.subscribe(['raw_data'])

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

BATCH_SIZE = 10
batch = []

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        print(msg)
        if msg is None:
                continue
        if msg.error():
            print(f"Error: {msg.error()}")
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


        if len(batch) >= BATCH_SIZE:
            insert_batch(batch)
            batch.clear()


except KeyboardInterrupt:
    pass
finally:
    insert_batch(batch)
    batch.clear()
    client.close()
    consumer.close()
