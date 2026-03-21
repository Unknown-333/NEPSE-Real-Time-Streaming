# NEPSE Real-Time Stock Market Streaming Pipeline

A production-grade, end-to-end streaming data pipeline that replays historical Nepal Stock Exchange (NEPSE) floorsheet data through a modern streaming architecture — from raw trades to live Grafana dashboards.

## Architecture

```
Historical CSV → Python Simulator (100x) → Kafka (KRaft) → PySpark Streaming → Kafka Aggregated → Python Sink → TimescaleDB → Grafana
```

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Source** | Synthetic Floorsheet Generator | Realistic trade data (50K+ trades/day, GBM prices) |
| **Producer** | Python + confluent-kafka | Replays CSV into Kafka at configurable speed |
| **Broker** | Apache Kafka (KRaft mode) | Distributed message broker (no Zookeeper) |
| **Processor** | PySpark Structured Streaming | 1-min tumbling window OHLCV aggregations |
| **Sink** | Python + psycopg2 | Kafka → TimescaleDB batch upserts (idempotent) |
| **Storage** | TimescaleDB (PostgreSQL) | Hypertable with continuous aggregates |
| **Dashboard** | Grafana | Auto-provisioned real-time visualization |

## Project Structure

```
NEPSE Real-Time Streaming/
├── data/raw/                        # Generated floorsheet CSVs
├── src/
│   ├── config.py                    # Centralized configuration
│   ├── data_generator/
│   │   ├── nepse_symbols.py         # 65 stocks, 9 sectors, 58 brokers
│   │   └── generate_floorsheet.py   # GBM price sim + U-shaped volume
│   ├── producer/
│   │   └── kafka_producer.py        # Kafka replay simulator
│   ├── consumer/
│   │   └── spark_consumer.py        # PySpark OHLCV aggregator
│   ├── sink/
│   │   └── timescaledb_sink.py      # Kafka → TimescaleDB writer
│   └── utils/
│       └── logger.py                # Structured logging factory
├── sql/
│   └── init.sql                     # TimescaleDB hypertable schema
├── grafana/
│   ├── dashboards/
│   │   └── nepse_realtime.json      # Auto-provisioned dashboard (6 panels)
│   └── provisioning/
│       ├── datasources/
│       │   └── timescaledb.yml      # Auto-connect Grafana → TimescaleDB
│       └── dashboards/
│           └── dashboard.yml        # Dashboard file provider config
├── docker-compose.yml               # Full infrastructure stack
├── requirements.txt                 # Pinned Python dependencies
└── .env.example                     # Environment variable template
```

## Quick Start

### Prerequisites
- Python 3.10+
- Docker and Docker Compose
- Java 17+ (for PySpark)

### 1. Setup
```bash
# Clone and enter project
git clone https://github.com/Unknown-333/NEPSE-Real-Time-Streaming.git
cd "NEPSE Real-Time Streaming"

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env
```

### 2. Generate Data
```bash
# Generate 1 year of floorsheet data (~12.5M trades, ~1GB)
python -m src.data_generator.generate_floorsheet

# Or customize: 6 months, 30K trades/day (smaller for testing)
python -m src.data_generator.generate_floorsheet --days 125 --trades 30000
```

### 3. Start Infrastructure
```bash
docker-compose up -d

# Verify all services are healthy
docker-compose ps

# Check TimescaleDB schema
docker logs nepse-kafka-init
docker exec -it nepse-timescaledb psql -U nepse_user -d nepse_streaming -c "\dt"
```

### 4. Run Pipeline
```bash
# Terminal 1: Start producer (replays CSV into Kafka at 100x speed)
python -m src.producer.kafka_producer --speed 100

# Terminal 2: Start PySpark consumer (1-min OHLCV aggregations)
# Windows: ensure HADOOP_HOME is set
set HADOOP_HOME=C:\hadoop
python -m src.consumer.spark_consumer

# Terminal 3: Start TimescaleDB sink (Kafka → database)
python -m src.sink.timescaledb_sink
```

### 5. Monitor
| Service | URL | Credentials |
|---------|-----|------------|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Kafka UI** | http://localhost:8080 | — |
| **Spark UI** | http://localhost:4040 | — (while consumer runs) |

## Services

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `nepse-kafka` | confluentinc/cp-kafka:7.7.1 | 9092 | Kafka broker (KRaft) |
| `nepse-kafka-init` | confluentinc/cp-kafka:7.7.1 | — | Topic creation (one-shot) |
| `nepse-kafka-ui` | provectuslabs/kafka-ui:latest | 8080 | Visual topic monitor |
| `nepse-timescaledb` | timescale/timescaledb:latest-pg16 | 5432 | Time-series database |
| `nepse-grafana` | grafana/grafana:11.4.0 | 3000 | Live dashboard |

## Grafana Dashboard Panels

| Panel | Type | Description |
|-------|------|-------------|
| OHLC Candlestick | Candlestick | Price chart for selected symbol (variable dropdown) |
| Volume Bars | Bar chart | Per-minute volume aligned with candlestick |
| Top 10 by Turnover | Table | Most actively traded stocks (ranked) |
| VWAP by Sector | Donut | Average VWAP breakdown across sectors |
| Live Trade Ticker | Table | Latest 50 OHLCV windows streaming in |
| Pipeline Health | Stat | Total records, active symbols, last update |

## OHLCV Output Schema

The PySpark consumer computes 1-minute tumbling window aggregations per stock symbol:

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | string | Stock ticker (e.g., NABIL, NICA) |
| `window_start` | timestamp | Start of 1-min window |
| `window_end` | timestamp | End of 1-min window |
| `open` | float | First trade price in the window |
| `high` | float | Highest trade price in the window |
| `low` | float | Lowest trade price in the window |
| `close` | float | Last trade price in the window |
| `volume` | int | Total shares traded |
| `turnover` | float | Total trade value (sum of amount) |
| `trade_count` | int | Number of transactions |
| `vwap` | float | Volume-weighted average price |
| `sector` | string | Sector classification |

## TimescaleDB Schema

### Hypertable: `ohlcv_1min`
- Auto-partitioned by `window_start` (7-day chunks)
- Composite unique index: `(symbol, window_start)` for idempotent upserts
- Sector + time index for sector-level queries

### Continuous Aggregate: `ohlcv_5min`
- Pre-computed 5-minute rollup from 1-minute data
- Auto-refreshed every 5 minutes
- OHLCV correctly rolled up (first open, last close, max high, min low)

## Tech Stack Versions

| Package | Version | Why Pinned |
|---------|---------|-----------|
| PySpark | 3.5.4 | Must match Kafka connector JAR |
| Kafka Connector | spark-sql-kafka-0-10_2.12:3.5.4 | Scala 2.12 + Spark 3.5.4 |
| confluent-kafka | 2.6.1 | Stable Python Kafka client |
| cp-kafka (Docker) | 7.7.1 | KRaft-ready, Kafka 3.7 |
| TimescaleDB | latest-pg16 | PostgreSQL 16 + time-series extensions |
| Grafana | 11.4.0 | Candlestick panel support, provisioning API |
| psycopg2-binary | 2.9.10 | PostgreSQL driver for Python sink |
| Kafka UI | provectuslabs/kafka-ui:latest | Visual topic and partition monitor |
| pandas | 2.2.3 | Data generation |
| OpenJDK | 17 | PySpark JVM requirement |

## Key Design Decisions

- **KRaft mode (no Zookeeper)**: Modern Kafka architecture, simpler ops, fewer containers
- **Symbol as Kafka key**: Guarantees all trades for a stock land on the same partition, preserving chronological order for correct OHLC windows
- **3 partitions = 3 Spark cores**: 1:1 mapping between Kafka partitions and PySpark executor threads for zero contention
- **Decoupled sink**: Standalone Python consumer (not Spark JDBC) avoids classpath issues and keeps memory under 30MB
- **Idempotent upserts**: `ON CONFLICT DO UPDATE` handles re-processing safely — exactly-once semantics at the storage layer
- **TimescaleDB hypertable**: Auto-partitioned by time for fast range queries; continuous aggregates pre-compute 5-min candles
- **Grafana auto-provisioning**: Datasource + dashboard mounted as files — zero manual setup on `docker-compose up`
- **Synthetic data generator**: Geometric Brownian Motion for realistic price walks, U-shaped intraday volume profile, 65 real NEPSE stocks across 9 sectors
- **Chunked CSV reading**: 10K rows per chunk keeps memory under 50MB for the 1GB+ dataset

## License

MIT
