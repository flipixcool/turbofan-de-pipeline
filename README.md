# turbofan-de-pipeline

[Русская версия](README.ru.md)

Real-time IoT data pipeline for CMAPSS-inspired turbofan engine telemetry simulation. Built as DE infrastructure for ML engineers who need a continuous data stream to train RUL (Remaining Useful Life) prediction models.

## Architecture

```
CMAPSS-inspired Stateful Producer (Python)
  — generates events every second
  — 3 engines: engine_001, engine_002, engine_003
  — anomalies become more likely as engines degrade
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

# 3. Build and start the pipeline
docker compose up -d --build

# 4. Check producer and consumer logs
docker compose logs -f producer
docker compose logs -f consumer
```

Airflow: `localhost:8080` — login from `.env` (`AIRFLOW_USER` / `AIRFLOW_PASSWORD`)  
Grafana: `localhost:3000` — login `admin` / value from `.env` (`GF_SECURITY_ADMIN_PASSWORD`)  
Kafka UI: `localhost:8090`

Schema note: if schema changes are not applied to an existing local ClickHouse volume, run `docker compose down -v` and then `docker compose up -d --build`.

## Data Schema

**raw_data** — raw telemetry events written by consumer in real time

Consumer reliability: manual Kafka offset commit happens only after successful ClickHouse batch insert, providing at-least-once delivery semantics. Raw events include `event_id`, so duplicate raw records are detectable after retries.

| Column | Type | Description |
|---|---|---|
| event_id | String | Deterministic event identifier for duplicate detection |
| engine_id | String | Engine identifier |
| timestamp | DateTime | Event time |
| cycle | UInt16 | Flight cycle number |
| altitude | Float32 | Altitude (ft) |
| mach_number | Float32 | Mach number |
| throttle | Float32 | Throttle (%) |
| T2 | Float32 | Fan inlet temperature (°R) |
| T50 | Float32 | LPT outlet temperature (°R) |
| P2 | Float32 | Fan inlet pressure (psia) |
| P15 | Float32 | Bypass duct pressure (psia) |
| Nf | Float32 | Fan speed (rpm) |
| Nc | Float32 | Core speed (rpm) |
| Ps30 | Float32 | Static pressure at HPC outlet |
| phi | Float32 | Fuel-air ratio signal |
| NRf | Float32 | Corrected fan speed |
| NRc | Float32 | Corrected core speed |
| BPR | Float32 | Bypass ratio |
| anomaly | Bool | Anomaly flag |
| RUL | UInt16 | Remaining useful life (cycles) |

**main_stats** — 5-minute aggregates per engine computed by Airflow DAG

Airflow aggregates fixed 5-minute windows and deletes/reinserts the exact window before inserting, so retries do not create duplicate aggregate rows.

| Column | Type | Description |
|---|---|---|
| window_start | DateTime | Inclusive aggregation window start |
| window_end | DateTime | Exclusive aggregation window end |
| engine_id | String | Engine identifier |
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
│   └── producer.py         # CMAPSS-inspired stateful telemetry producer
└── sql/
    └── init.sql            # ClickHouse schema (raw_data, main_stats)
```
