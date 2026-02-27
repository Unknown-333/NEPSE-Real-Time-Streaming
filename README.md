# NEPSE Real-Time Stock Market Streaming Pipeline

A production-grade, end-to-end streaming data pipeline that replays historical Nepal Stock Exchange (NEPSE) floorsheet data through a modern streaming architecture.

## Architecture

```
Historical NEPSE CSV --> Python Simulator (100x) --> Apache Kafka (KRaft) --> PySpark Structured Streaming --> TimescaleDB --> Grafana
```

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Source** | Synthetic Floorsheet Generator | Realistic transaction-level data (50K+ trades/day) |
| **Producer** | Python + confluent-kafka | Replays CSV into Kafka at configurable speed |
| **Broker** | Apache Kafka (KRaft mode) | Distributed message broker (no Zookeeper) |
| **Processor** | PySpark Structured Streaming | 1-min tumbling window OHLCV aggregations |
| **Storage** | TimescaleDB (PostgreSQL) | Time-series optimized hypertables |
| **Dashboard** | Grafana | Sub-second live visualization |

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
│   └── utils/
│       └── logger.py                # Structured logging factory
├── grafana/dashboards/              # Grafana dashboard JSON
├── sql/                             # TimescaleDB init scripts
├── docker-compose.yml               # Kafka KRaft + Kafka UI
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

# Verify topics were created
docker logs nepse-kafka-init

# Open Kafka UI at http://localhost:8080
```

### 4. Run Pipeline
```bash
# Terminal 1: Start producer (replays CSV into Kafka at 100x speed)
python -m src.producer.kafka_producer --speed 100

# Terminal 2: Start consumer (PySpark OHLCV aggregations)
# Windows: ensure HADOOP_HOME is set
set HADOOP_HOME=C:\hadoop
python -m src.consumer.spark_consumer
```

### 5. Monitor
- Kafka UI: http://localhost:8080
- Spark UI: http://localhost:4040 (while consumer is running)
- Grafana: http://localhost:3000 (Phase 6)

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

## Floorsheet Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `contract_no` | string | Unique transaction ID (YYYYMMDD-NNNNNN) |
| `symbol` | string | Stock ticker (e.g., NABIL, NICA) |
| `buyer` | int | Buyer broker number (1-58) |
| `seller` | int | Seller broker number (1-58) |
| `quantity` | int | Shares traded |
| `rate` | float | Price per share (NPR) |
| `amount` | float | Total value (quantity x rate) |
| `trade_time` | timestamp | Trade execution time |
| `sector` | string | Sector classification |

## Tech Stack Versions

| Package | Version | Why Pinned |
|---------|---------|-----------|
| PySpark | 3.5.4 | Must match Kafka connector JAR |
| Kafka Connector | spark-sql-kafka-0-10_2.12:3.5.4 | Scala 2.12 + Spark 3.5.4 |
| confluent-kafka | 2.6.1 | Stable Python Kafka client |
| cp-kafka (Docker) | 7.7.1 | KRaft-ready, Kafka 3.7 |
| Kafka UI | provectuslabs/kafka-ui:latest | Visual topic and partition monitor |
| pandas | 2.2.3 | Data generation |
| OpenJDK | 17 | PySpark JVM requirement |

## Key Design Decisions

- **KRaft mode (no Zookeeper)**: Modern Kafka architecture, simpler ops, fewer containers
- **Symbol as Kafka key**: Guarantees all trades for a stock land on the same partition, preserving chronological order for correct OHLC windows
- **3 partitions = 3 Spark cores**: 1:1 mapping between Kafka partitions and PySpark executor threads for zero contention
- **Synthetic data generator**: Geometric Brownian Motion for realistic price walks, U-shaped intraday volume profile, 65 real NEPSE stocks across 9 sectors
- **Chunked CSV reading**: 10K rows per chunk keeps memory under 50MB for the 1GB+ dataset
- **Idempotent producer**: enable.idempotence=True prevents duplicate messages on retry

## License

MIT
