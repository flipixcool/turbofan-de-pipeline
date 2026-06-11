# turbofan-de-pipeline

IoT-пайплайн данных в реальном времени для симуляции телеметрии турбовентиляторного двигателя на основе датасета NASA CMAPSS. Построен как DE-инфраструктура для ML-инженеров, которым нужен непрерывный поток данных для обучения моделей прогнозирования RUL (Remaining Useful Life, остаточный ресурс).

## Архитектура

```
Faker Producer (Python)
  — генерирует события каждую секунду
  — 3 двигателя: engine_001, engine_002, engine_003
  — аномалии с вероятностью 0.001
        ↓
      Kafka
   (topic: raw_data, 3 брокера, replication factor 3)
        ↓
  Python Consumer
        ↓
    ClickHouse
   (raw_data + main_stats)
        ↓
  Airflow DAG
   (агрегации каждые 5 минут → main_stats)
        ↓
    Grafana dashboard
        ↑
  Prometheus + kafka-exporter
   (метрики Kafka)
```

## Стек

| Инструмент | Версия | Роль |
|---|---|---|
| Apache Kafka | 7.5.0 (Confluent) | Брокер сообщений |
| ClickHouse | 23.8 | Аналитическое хранилище |
| Apache Airflow | 2.8.0 | Оркестрация |
| Grafana | 10.2.0 | Визуализация |
| Prometheus | 2.45.0 | Сбор метрик |
| PostgreSQL | 15 | База метаданных Airflow |
| Python | 3.11 | Producer и Consumer |
| Docker Compose | — | Инфраструктура |

## Сервисы и порты

| Сервис | Порт |
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

## Требования

- Docker + Docker Compose
- Python 3.11
- Добавить в `/etc/hosts`:

```
127.0.0.1 kafka1 kafka2 kafka3
```

## Быстрый старт

```bash
# 1. Клонировать репозиторий
git clone https://github.com/flipixcool/turbofan-de-pipeline
cd turbofan-de-pipeline

# 2. Настроить окружение
cp .env.example .env
# отредактируйте .env, указав свои учетные данные

# 3. Собрать и запустить инфраструктуру
docker compose build
docker compose up -d

# 4. Установить Python-зависимости
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 5. Запустить producer
python producer/producer.py

# 6. Запустить consumer
python consumer/consumer.py
```

Airflow: `localhost:8080` — логин из `.env` (`AIRFLOW_USER` / `AIRFLOW_PASSWORD`)  
Grafana: `localhost:3000` — логин `admin` / значение из `.env` (`GF_SECURITY_ADMIN_PASSWORD`)  
Kafka UI: `localhost:8090`

## Схема данных

**raw_data** — сырые события телеметрии, которые consumer записывает в реальном времени

| Колонка | Тип | Описание |
|---|---|---|
| engine_id | String | Идентификатор двигателя |
| timestamp | DateTime | Время события |
| cycle | UInt16 | Номер полетного цикла |
| altitude | Float32 | Высота (ft) |
| mach_number | Float32 | Число Маха |
| throttle | Float32 | Положение дросселя (%) |
| T2 | Float32 | Температура на входе вентилятора (°R) |
| T50 | Float32 | Температура на выходе LPT (°R) |
| P2 | Float32 | Давление на входе вентилятора (psia) |
| Nf | Float32 | Скорость вентилятора (rpm) |
| Nc | Float32 | Скорость ядра (rpm) |
| anomaly | Bool | Флаг аномалии |
| RUL | UInt16 | Остаточный ресурс (циклы) |

**main_stats** — 5-минутные агрегаты по каждому двигателю, рассчитанные Airflow DAG

| Колонка | Тип | Описание |
|---|---|---|
| engine_id | String | Идентификатор двигателя |
| timestamp | DateTime | Время агрегации |
| avg_T2 | Float32 | Средняя температура на входе вентилятора |
| avg_T50 | Float32 | Средняя температура на выходе турбины |
| avg_Nf | Float32 | Средняя скорость вентилятора |
| avg_Nc | Float32 | Средняя скорость ядра |
| avg_P2 | Float32 | Среднее давление на входе |
| avg_Ps30 | Float32 | Среднее статическое давление |
| anomaly_count | UInt16 | Количество аномалий в окне |
| min_RUL | UInt16 | Минимальный RUL в окне |

## Структура проекта

```
turbofan-de-pipeline/
├── Dockerfile              # Кастомный образ Airflow с clickhouse-driver
├── docker-compose.yml      # Полная инфраструктура с healthcheck'ами
├── prometheus.yaml         # Конфигурация scrape для Prometheus
├── requirements.txt        # Python-зависимости
├── .env.example            # Шаблон переменных окружения
├── airflow/
│   └── dags/
│       └── aggregation.py  # DAG: INSERT INTO main_stats каждые 5 мин
├── consumer/
│   └── consumer.py         # Kafka → ClickHouse consumer (batch=10)
├── producer/
│   └── producer.py         # Producer фейковой телеметрии (1 событие/сек)
└── sql/
    └── init.sql            # Схема ClickHouse (raw_data, main_stats)
```
