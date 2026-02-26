# 🇳🇵 NEPSE Real-Time Stock Market Streaming Pipeline

A production-grade, end-to-end streaming data pipeline that replays historical Nepal Stock Exchange (NEPSE) floorsheet data through a modern streaming architecture.

## 🏗️ Architecture

```
Historical NEPSE CSV → Python Simulator (100x) → Apache Kafka (KRaft) → PySpark Structured Streaming → TimescaleDB → Grafana
```

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Source** | Synthetic Floorsheet Generator | Realistic transaction-level data (50K+ trades/day) |
| **Producer** | Python + confluent-kafka | Replays CSV into Kafka at configurable speed |
| **Broker** | Apache Kafka (KRaft mode) | Distributed message broker (no Zookeeper) |
| **Processor** | PySpark Structured Streaming | 1-min tumbling window aggregations |
| **Storage** | TimescaleDB (PostgreSQL) | Time-series optimized hypertables |
| **Dashboard** | Grafana | Sub-second live visualization |

## 📁 Project Structure

```
NEPSE Real-Time Streaming/
├── data/raw/                    # Generated floorsheet CSVs
├── src/
│   ├── config.py                # Centralized configuration
│   ├── data_generator/          # Synthetic data generation
│   │   ├── nepse_symbols.py     # 65 stocks, 9 sectors, 58 brokers
│   │   └── generate_floorsheet.py
│   ├── producer/                # Kafka producer (simulator)
│   ├── consumer/                # PySpark streaming consumer
│   └── utils/
│       └── logger.py            # Structured logging
├── grafana/dashboards/          # Grafana dashboard JSON
├── sql/                         # TimescaleDB init scripts
├── docker-compose.yml           # Full infrastructure
├── requirements.txt             # Pinned Python dependencies
└── .env.example                 # Environment variable template
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Java 11+ (for PySpark)

### 1. Setup
```bash
# Clone and enter project
git clone <repo-url>
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
# Generate 1 year of floorsheet data (~12.5M trades)
python -m src.data_generator.generate_floorsheet

# Or customize: 6 months, 30K trades/day (smaller for testing)
python -m src.data_generator.generate_floorsheet --days 125 --trades 30000
```

### 3. Start Infrastructure
```bash
docker-compose up -d
```

### 4. Run Pipeline
```bash
# Start producer (simulate streaming)
python -m src.producer.kafka_producer

# Start consumer (PySpark aggregations)
python -m src.consumer.spark_consumer
```

### 5. View Dashboard
Open Grafana at `http://localhost:3000`

## 📊 Floorsheet Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `contract_no` | string | Unique transaction ID (YYYYMMDD-NNNNNN) |
| `symbol` | string | Stock ticker (e.g., NABIL, NICA) |
| `buyer` | int | Buyer broker number (1-58) |
| `seller` | int | Seller broker number (1-58) |
| `quantity` | int | Shares traded |
| `rate` | float | Price per share (NPR) |
| `amount` | float | Total value (quantity × rate) |
| `trade_time` | timestamp | Trade execution time |
| `sector` | string | Sector classification |

## 🛠️ Tech Stack Versions

| Package | Version | Why Pinned |
|---------|---------|-----------|
| PySpark | 3.5.4 | Must match Kafka connector JAR |
| Kafka Connector | spark-sql-kafka-0-10_2.12:3.5.4 | Scala 2.12 + Spark 3.5.4 |
| confluent-kafka | 2.6.1 | Stable Python Kafka client |
| pandas | 2.2.3 | Data generation |

## 📝 License

MIT
