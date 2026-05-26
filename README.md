# turbofan-de-pipeline

Real-time IoT data pipeline for turbofan engine telemetry simulation based on NASA CMAPSS dataset. Built as a DE infrastructure project for ML engineers who need a data stream to train RUL (Remaining Useful Life) prediction models.

## Architecture

```
Faker Producer (Python)
  — generates events every second
  — 3 engines: engine_001, engine_002, engine_003
  — anomalies with 0.001 probability
        ↓
      Kafka
   (topic: raw_data, 3 brokers, replication factor 3)
        ↓
  Python Consumer
        ↓
    ClickHouse
   (raw_data + main_stats)
        ↓
  Airflow DAG
   (aggregations every 5 minutes → main_stats)
        ↓
    Grafana dashboard
        ↑
  Prometheus + kafka-exporter
   (Kafka metrics)
```

## Stack

| Tool | Version | Role |
|---|---|---|
| Apache Kafka | 7.5.0 (Confluent) | Message broker |
| ClickHouse | 23.8 | Analytical storage |
| Apache Airflow | 2.8.0 | Orchestration |
| Grafana | 10.2.0 | Visualization |
| Prometheus | 2.45.0 | Metrics |
| PostgreSQL | 15 | Airflow metadata |
| Python | 3.11 | Producer & Consumer |
| Docker Compose | — | Infrastructure |

## Services & Ports

| Service | Port |
|---|---|
| Airflow UI | 8080 |
| Kafka UI | 8090 |
| ClickHouse HTTP | 8123 |
| ClickHouse native | 9000 |
| Kafka broker 1 | 29092 |
| Kafka broker 2 | 29093 |
| Kafka broker 3 | 29094 |
| Grafana | 3000 |
| Prometheus | 9090 |

## Prerequisites

- Docker + Docker Compose
- Python 3.11
- Add to `/etc/hosts`:
```
127.0.0.1 kafka1 kafka2 kafka3
```

## Quick Start

```bash
# 1. Clone
git clone https://github.com/flipixcool/turbofan-de-pipeline
cd turbofan-de-pipeline

# 2. Build and start infrastructure
docker compose build
docker compose up -d

# 3. Install Python deps
python3.11 -m venv venv
source venv/bin/activate
pip install confluent-kafka clickhouse-driver faker

# 4. Start producer
python producer/producer.py

# 5. Start consumer
python consumer/consumer.py
```

Airflow: `localhost:8080` — login `airflow` / `airflow`  
Grafana: `localhost:3000` — login `admin` / `admin`  
Kafka UI: `localhost:8090`

## Project Structure

```
turbofan-de-pipeline/
├── Dockerfile              # Custom Airflow image with clickhouse-driver
├── docker-compose.yml      # Full infrastructure
├── prometheus.yaml         # Prometheus scrape config
├── airflow/
│   └── dags/
│       └── aggregation.py  # DAG: INSERT INTO main_stats every 5 min
├── consumer/
│   └── consumer.py         # Kafka → ClickHouse consumer
├── producer/
│   └── producer.py         # Fake telemetry producer
└── sql/
    └── init.sql            # ClickHouse schema (raw_data, main_stats)
```

## Data Schema

**raw_data** — raw telemetry events from engines (MergeTree, ordered by engine_id, timestamp)

**main_stats** — 5-minute aggregates per engine: avg temperatures, pressures, fan speeds, anomaly count, min RUL
