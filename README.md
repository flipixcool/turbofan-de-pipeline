# turbofan-de-pipeline

Real-time IoT data pipeline for turbofan engine telemetry simulation based on the NASA CMAPSS dataset. Built as DE infrastructure for ML engineers who need a continuous data stream to train RUL (Remaining Useful Life) prediction models.

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
| Prometheus | 2.45.0 | Metrics collection |
| PostgreSQL | 15 | Airflow metadata DB |
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

# 2. Configure environment
cp .env.example .env
# edit .env with your credentials

# 3. Build and start infrastructure
docker compose build
docker compose up -d

# 4. Install Python deps
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 5. Start producer
python producer/producer.py

# 6. Start consumer
python consumer/consumer.py
```

Airflow: `localhost:8080` — login from `.env` (`AIRFLOW_USER` / `AIRFLOW_PASSWORD`)  
Grafana: `localhost:3000` — login `admin` / value from `.env` (`GF_SECURITY_ADMIN_PASSWORD`)  
Kafka UI: `localhost:8090`

## Data Schema

**raw_data** — raw telemetry events written by consumer in real time

| Column | Type | Description |
|---|---|---|
| engine_id | String | Engine identifier |
| timestamp | DateTime | Event time |
| cycle | UInt16 | Flight cycle number |
| altitude | Float32 | Altitude (ft) |
| mach_number | Float32 | Mach number |
| throttle | Float32 | Throttle (%) |
| T2 | Float32 | Fan inlet temperature (°R) |
| T50 | Float32 | LPT outlet temperature (°R) |
| P2 | Float32 | Fan inlet pressure (psia) |
| Nf | Float32 | Fan speed (rpm) |
| Nc | Float32 | Core speed (rpm) |
| anomaly | Bool | Anomaly flag |
| RUL | UInt16 | Remaining useful life (cycles) |

**main_stats** — 5-minute aggregates per engine computed by Airflow DAG

| Column | Type | Description |
|---|---|---|
| engine_id | String | Engine identifier |
| timestamp | DateTime | Aggregation time |
| avg_T2 | Float32 | Avg fan inlet temperature |
| avg_T50 | Float32 | Avg turbine outlet temperature |
| avg_Nf | Float32 | Avg fan speed |
| avg_Nc | Float32 | Avg core speed |
| avg_P2 | Float32 | Avg inlet pressure |
| avg_Ps30 | Float32 | Avg static pressure |
| anomaly_count | UInt16 | Anomaly count in window |
| min_RUL | UInt16 | Minimum RUL in window |

## Project Structure

```
turbofan-de-pipeline/
├── Dockerfile              # Custom Airflow image with clickhouse-driver
├── docker-compose.yml      # Full infrastructure with healthchecks
├── prometheus.yaml         # Prometheus scrape config
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variables template
├── airflow/
│   └── dags/
│       └── aggregation.py  # DAG: INSERT INTO main_stats every 5 min
├── consumer/
│   └── consumer.py         # Kafka → ClickHouse consumer (batch=10)
├── producer/
│   └── producer.py         # Fake telemetry producer (1 event/sec)
└── sql/
    └── init.sql            # ClickHouse schema (raw_data, main_stats)
```
