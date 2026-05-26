from airflow.decorators import dag, task
from clickhouse_driver import Client
from datetime import datetime


@dag(schedule_interval = '*/5 * * * *', start_date = datetime(2026, 5, 22), catchup = False)
def aggregation():
    @task
    def insert_batch():
        client = Client(host='clickhouse',
            port=9000,
            user='user',
            database='turbofan',
            password='passwd')
        client.execute('INSERT INTO main_stats SELECT engine_id, now() as timestamp, avg(T2), avg(T50), avg(Nf), avg(Nc), avg(P2), avg(Ps30), countIf(anomaly = true), min(RUL) FROM raw_data WHERE timestamp >= now() - INTERVAL 5 MINUTE GROUP BY engine_id')

    insert_batch()

aggregation()