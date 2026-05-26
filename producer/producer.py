from confluent_kafka import Producer
from faker import Faker
import json
from datetime import datetime
import time
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

producer = Producer({'bootstrap.servers': 'localhost:29092,localhost:29093,localhost:29094'})
fake = Faker()

cycle = 0
def generate_event():
    global cycle
    cycle += 1
    timestamp_ = datetime.now()
    return {
        "engine_id" : fake.random_element(["engine_001", "engine_002", "engine_003"]),
        "timestamp" : str(timestamp_),
        "cycle" : fake.random_int(),
        "altitude"    : fake.pyfloat(min_value=0, max_value=42000),     # футы
        "mach_number" : fake.pyfloat(min_value=0.0, max_value=0.9),     # число Маха
        "throttle"    : fake.pyfloat(min_value=20, max_value=100),       # %
        "T2"          : fake.pyfloat(min_value=445, max_value=550),      # °R температура входа
        "T50"         : fake.pyfloat(min_value=1000, max_value=1600),    # °R температура выхода турбины
        "P2"          : fake.pyfloat(min_value=2, max_value=15),         # psia давление входа
        "P15"         : fake.pyfloat(min_value=5, max_value=25),         # psia
        "Nf"          : fake.pyfloat(min_value=2000, max_value=2600),    # rpm вентилятор
        "Nc"          : fake.pyfloat(min_value=7000, max_value=10000),   # rpm компрессор
        "Ps30"        : fake.pyfloat(min_value=30, max_value=60),        # psia
        "phi"         : fake.pyfloat(min_value=300, max_value=600),      # соотношение топливо/воздух
        "NRf"         : fake.pyfloat(min_value=2000, max_value=2600),    # скорректированный rpm
        "NRc"         : fake.pyfloat(min_value=7000, max_value=10000),   # скорректированный rpm
        "BPR"         : fake.pyfloat(min_value=5, max_value=12),         # bypass ratio
        "anomaly" : fake.boolean(chance_of_getting_true=0.001) ,
        "RUL" : fake.random_int()
    }

while True:
    event = generate_event()
    producer.produce("raw_data", json.dumps(event).encode('utf-8'))
    logger.info(f"Sent: {event['engine_id']} cycle={event['cycle']}")
    producer.flush()
    time.sleep(1)