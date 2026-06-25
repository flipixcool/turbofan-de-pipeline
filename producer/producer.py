from confluent_kafka import Producer
import json
from datetime import datetime
import os
import random
import time
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def get_env(name):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


kafka_bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS")
kafka_topic = get_env("KAFKA_TOPIC")
producer_interval_seconds = float(get_env("PRODUCER_INTERVAL_SECONDS"))

producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

ENGINE_IDS = ("engine_001", "engine_002", "engine_003")
LOG_EVERY_MESSAGES = 100


def new_engine_state():
    initial_rul = random.randint(180, 260)
    return {
        "cycle": 0,
        "initial_rul": initial_rul,
        "RUL": initial_rul,
    }


engine_states = {engine_id: new_engine_state() for engine_id in ENGINE_IDS}


def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def rounded(value):
    return round(value, 3)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Kafka delivery failed: {err}")
        return

    logger.debug(
        "Delivered event to topic=%s partition=%s offset=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def generate_event():
    engine_id = random.choice(ENGINE_IDS)
    state = engine_states[engine_id]

    if state["RUL"] <= 0:
        logger.info(f"Resetting {engine_id} after end-of-life simulation")
        state.update(new_engine_state())

    state["cycle"] += 1
    state["RUL"] = max(0, state["RUL"] - 1)

    cycle = state["cycle"]
    rul = state["RUL"]
    degradation = clamp(1 - (rul / state["initial_rul"]), 0.0, 1.0)

    altitude = clamp(random.gauss(32000, 5000), 0, 42000)
    mach_number = clamp(random.gauss(0.72, 0.08), 0.0, 0.9)
    throttle = clamp(random.gauss(76, 10), 20, 100)

    p2 = clamp(14.7 - (altitude / 42000) * 12 + random.gauss(0, 0.35), 2, 15)
    p15 = clamp(p2 * (2.2 + throttle / 100) + random.gauss(0, 0.6), 5, 25)
    nf = clamp(1980 + throttle * 5.2 + mach_number * 130 - degradation * 85 + random.gauss(0, 22), 2000, 2600)
    nc = clamp(6800 + throttle * 27 + mach_number * 420 + degradation * 330 + random.gauss(0, 70), 7000, 10000)
    t2 = clamp(445 + (altitude / 42000) * 48 + mach_number * 24 + random.gauss(0, 4), 445, 550)
    t50 = clamp(1030 + throttle * 4.2 + degradation * 260 + random.gauss(0, 18), 1000, 1600)
    ps30 = clamp(29 + throttle * 0.25 + degradation * 8 + random.gauss(0, 1.2), 30, 60)
    phi = clamp(305 + throttle * 2.1 + degradation * 75 + random.gauss(0, 12), 300, 600)
    nrf = clamp(nf * (1 + degradation * 0.015) + random.gauss(0, 8), 2000, 2600)
    nrc = clamp(nc * (1 + degradation * 0.015) + random.gauss(0, 25), 7000, 10000)
    bpr = clamp(11.2 - degradation * 2.5 - throttle * 0.01 + random.gauss(0, 0.25), 5, 12)

    anomaly = random.random() < (0.002 + degradation * 0.025)
    if anomaly:
        t50 = clamp(t50 + random.uniform(60, 120), 1000, 1600)
        ps30 = clamp(ps30 - random.uniform(3, 7), 30, 60)
        phi = clamp(phi + random.uniform(20, 40), 300, 600)
        bpr = clamp(bpr - random.uniform(0.5, 1.2), 5, 12)

    timestamp = datetime.now().isoformat(timespec="microseconds")

    return {
        "event_id": f"{engine_id}-{cycle}-{timestamp}",
        "engine_id": engine_id,
        "timestamp": timestamp,
        "cycle": cycle,
        "altitude": rounded(altitude),
        "mach_number": rounded(mach_number),
        "throttle": rounded(throttle),
        "T2": rounded(t2),
        "T50": rounded(t50),
        "P2": rounded(p2),
        "P15": rounded(p15),
        "Nf": rounded(nf),
        "Nc": rounded(nc),
        "Ps30": rounded(ps30),
        "phi": rounded(phi),
        "NRf": rounded(nrf),
        "NRc": rounded(nrc),
        "BPR": rounded(bpr),
        "anomaly": anomaly,
        "RUL": rul,
    }


produced_messages = 0

try:
    while True:
        event = generate_event()
        producer.produce(
            kafka_topic,
            key=event["engine_id"],
            value=json.dumps(event).encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)

        produced_messages += 1
        if produced_messages % LOG_EVERY_MESSAGES == 0:
            logger.info(
                "Produced %s messages; last=%s cycle=%s RUL=%s",
                produced_messages,
                event["engine_id"],
                event["cycle"],
                event["RUL"],
            )

        time.sleep(producer_interval_seconds)
except KeyboardInterrupt:
    logger.info("Producer shutdown requested")
finally:
    producer.flush()
    logger.info("Producer stopped cleanly")
