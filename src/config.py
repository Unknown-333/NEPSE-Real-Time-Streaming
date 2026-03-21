"""
Centralized Configuration Module
=================================
WHY: Hardcoding values across scripts is a maintenance nightmare.
A single config module acts as the "single source of truth" for every
parameter in the pipeline — from Kafka broker addresses to replay speed.

We use python-dotenv to load .env files so you can override settings
per-environment (dev, staging, prod) without changing code.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Resolve project root (two levels up from src/config.py) ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"

# ── Load .env file if it exists ──
load_dotenv(PROJECT_ROOT / ".env")


# ═══════════════════════════════════════════════════════════════
# Kafka Settings
# ═══════════════════════════════════════════════════════════════
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "nepse-raw-ticks")
KAFKA_TOPIC_AGGREGATED = os.getenv("KAFKA_TOPIC_AGGREGATED", "nepse-aggregated")


# ═══════════════════════════════════════════════════════════════
# TimescaleDB / PostgreSQL Settings
# ═══════════════════════════════════════════════════════════════
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "nepse_streaming")
POSTGRES_USER = os.getenv("POSTGRES_USER", "nepse_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "changeme_in_production")

# JDBC URL for PySpark
POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# TimescaleDB sink settings (Phase 5)
TIMESCALEDB_TABLE = os.getenv("TIMESCALEDB_TABLE", "ohlcv_1min")
SINK_BATCH_SIZE = int(os.getenv("SINK_BATCH_SIZE", "100"))


# ═══════════════════════════════════════════════════════════════
# Data Generator / Simulator Settings
# ═══════════════════════════════════════════════════════════════
REPLAY_SPEED = int(os.getenv("REPLAY_SPEED", "100"))
TRADING_DAYS = int(os.getenv("TRADING_DAYS", "250"))
PRODUCER_BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "500"))

# NEPSE trading hours: 11:00 AM to 3:00 PM NST (4 hours)
MARKET_OPEN_HOUR = 11
MARKET_OPEN_MINUTE = 0
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 0


# ═══════════════════════════════════════════════════════════════
# PySpark Settings (Phase 4)
# ═══════════════════════════════════════════════════════════════
# WHY we pin these versions: PySpark's Kafka connector JAR must
# exactly match the Spark and Scala versions. Mismatched JARs
# cause ClassNotFoundException at runtime — the #1 pain point
# for first-time Spark users.
#
# pyspark 3.5.4 uses Scala 2.12, so we need:
#   spark-sql-kafka-0-10_2.12:3.5.4
SPARK_VERSION = "3.5.4"
SCALA_VERSION = "2.12"
SPARK_KAFKA_JAR = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
)


# ═══════════════════════════════════════════════════════════════
# Grafana Settings (Phase 6)
# ═══════════════════════════════════════════════════════════════
GRAFANA_PORT = int(os.getenv("GRAFANA_PORT", "3000"))
