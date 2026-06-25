from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from clickhouse_driver import Client
from datetime import datetime, timedelta
import logging
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def get_env(name):
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_int_env(name):
    return int(get_env(name))


def to_clickhouse_datetime(value):
    if getattr(value, "tzinfo", None) is not None:
        value = value.replace(tzinfo=None)
    return value.replace(microsecond=0)


def close_clickhouse_client(client):
    disconnect = getattr(client, 'disconnect', None)
    close = getattr(client, 'close', None)

    if callable(disconnect):
        disconnect()
    elif callable(close):
        close()


@dag(schedule_interval='*/5 * * * *', start_date=datetime(2026, 5, 22), catchup=False)
def aggregation():
    @task
    def insert_batch():
        context = get_current_context()
        window_end = context.get('data_interval_end', context['logical_date'])
        window_start = context.get('data_interval_start', window_end - timedelta(minutes=5))

        window_start = to_clickhouse_datetime(window_start)
        window_end = to_clickhouse_datetime(window_end)
        params = {
            'window_start': window_start,
            'window_end': window_end,
        }

        client = Client(host=get_env("CLICKHOUSE_HOST"),
            port=get_int_env("CLICKHOUSE_PORT"),
            user=get_env("CLICKHOUSE_USER"),
            database=get_env("CLICKHOUSE_DATABASE"),
            password=get_env("CLICKHOUSE_PASSWORD"))

        try:
            logger.info("Aggregating window [%s, %s)", window_start, window_end)

            # Airflow retries rerun the same data interval. Deleting the exact
            # window before inserting fresh rows prevents duplicate aggregates.
            client.execute(
                """
                ALTER TABLE main_stats DELETE
                WHERE window_start = toDateTime(%(window_start)s)
                  AND window_end = toDateTime(%(window_end)s)
                """,
                params,
                settings={'mutations_sync': 1},
            )

            client.execute(
                """
                INSERT INTO main_stats (
                    window_start, window_end, engine_id,
                    avg_T2, avg_T50, avg_Nf, avg_Nc,
                    avg_P2, avg_Ps30, anomaly_count, min_RUL
                )
                SELECT
                    toDateTime(%(window_start)s) AS window_start,
                    toDateTime(%(window_end)s) AS window_end,
                    engine_id,
                    avg(T2),
                    avg(T50),
                    avg(Nf),
                    avg(Nc),
                    avg(P2),
                    avg(Ps30),
                    countIf(anomaly = true),
                    min(RUL)
                FROM raw_data
                WHERE timestamp >= toDateTime(%(window_start)s)
                  AND timestamp < toDateTime(%(window_end)s)
                GROUP BY engine_id
                """,
                params,
            )

            logger.info("Finished aggregating window [%s, %s)", window_start, window_end)
        finally:
            close_clickhouse_client(client)

    insert_batch()

aggregation()
