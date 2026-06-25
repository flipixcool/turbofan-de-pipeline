from airflow.decorators import dag, task
from clickhouse_driver import Client
from datetime import datetime
import os


def get_env(name):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_int_env(name):
    return int(get_env(name))


@dag(schedule_interval = '*/5 * * * *', start_date = datetime(2026, 5, 22), catchup = False)
def aggregation():
    @task
    def insert_batch():
        client = Client(host=get_env("CLICKHOUSE_HOST"),
            port=get_int_env("CLICKHOUSE_PORT"),
            user=get_env("CLICKHOUSE_USER"),
            database=get_env("CLICKHOUSE_DATABASE"),
            password=get_env("CLICKHOUSE_PASSWORD"))
        client.execute('INSERT INTO main_stats SELECT engine_id, now() as timestamp, avg(T2), avg(T50), avg(Nf), avg(Nc), avg(P2), avg(Ps30), countIf(anomaly = true), min(RUL) FROM raw_data WHERE timestamp >= now() - INTERVAL 5 MINUTE GROUP BY engine_id')

    insert_batch()

aggregation()
