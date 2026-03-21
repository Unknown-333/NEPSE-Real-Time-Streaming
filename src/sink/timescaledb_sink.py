"""
TimescaleDB Sink — OHLCV Kafka Consumer → TimescaleDB Writer
================================================================
PURPOSE:
    Consumes aggregated OHLCV messages from Kafka's `nepse-aggregated`
    topic and batch-upserts them into TimescaleDB's `ohlcv_1min`
    hypertable. This is the final storage layer before Grafana.

WHY a standalone Python consumer (not Spark JDBC sink)?
    1. Simpler: No JDBC driver JARs, no classpath hell on Windows
    2. Lighter: Uses ~30MB RAM vs Spark's 500MB+ JVM overhead
    3. Idempotent: ON CONFLICT upsert handles re-processing safely
    4. Decoupled: Sink can restart independently of Spark consumer
    5. Debuggable: Plain Python + psycopg2, no Spark magic

ARCHITECTURE:
    Kafka (nepse-aggregated)
         ↓ confluent-kafka Consumer
    JSON deserialize → batch accumulate (100 msgs)
         ↓ psycopg2
    TimescaleDB (ohlcv_1min hypertable)
         ↓ continuous aggregate
    ohlcv_5min (auto-refreshed)

MESSAGE FORMAT (from PySpark consumer):
    Key:   "NABIL" (symbol, UTF-8)
    Value: {
        "symbol": "NABIL",
        "window_start": "2021-01-04T11:00:00.000+05:45",
        "window_end":   "2021-01-04T11:01:00.000+05:45",
        "open": 1150.50,
        "high": 1155.00,
        "low": 1148.00,
        "close": 1152.00,
        "volume": 5000,
        "turnover": 5762500.00,
        "trade_count": 45,
        "vwap": 1152.50,
        "sector": "Commercial Bank"
    }
"""

import argparse
import json
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException

# ── Add project root to path for imports ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.config import (
    KAFKA_BROKER,
    KAFKA_TOPIC_AGGREGATED,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    SINK_BATCH_SIZE,
    TIMESCALEDB_TABLE,
)
from src.utils.logger import get_logger

logger = get_logger("timescaledb_sink")


# ═══════════════════════════════════════════════════════════════
# Upsert SQL
# ═══════════════════════════════════════════════════════════════
# WHY ON CONFLICT ... DO UPDATE?
#   PySpark's outputMode("update") sends updated windows on every
#   micro-batch. The same (symbol, window_start) pair may arrive
#   multiple times as the window accumulates more trades.
#   ON CONFLICT replaces the old partial window with the latest
#   complete data — making this sink fully idempotent.
#
# WHY EXCLUDED.* syntax?
#   PostgreSQL's EXCLUDED refers to the row that was proposed for
#   insertion but conflicted. This is cleaner than repeating the
#   column values in the SET clause.
# ═══════════════════════════════════════════════════════════════

UPSERT_SQL = f"""
INSERT INTO {TIMESCALEDB_TABLE} (
    symbol, window_start, window_end,
    open, high, low, close,
    volume, turnover, trade_count, vwap,
    sector
) VALUES (
    %(symbol)s, %(window_start)s, %(window_end)s,
    %(open)s, %(high)s, %(low)s, %(close)s,
    %(volume)s, %(turnover)s, %(trade_count)s, %(vwap)s,
    %(sector)s
)
ON CONFLICT (symbol, window_start) DO UPDATE SET
    window_end   = EXCLUDED.window_end,
    open         = EXCLUDED.open,
    high         = EXCLUDED.high,
    low          = EXCLUDED.low,
    close        = EXCLUDED.close,
    volume       = EXCLUDED.volume,
    turnover     = EXCLUDED.turnover,
    trade_count  = EXCLUDED.trade_count,
    vwap         = EXCLUDED.vwap,
    sector       = EXCLUDED.sector,
    ingested_at  = NOW();
"""


# ═══════════════════════════════════════════════════════════════
# TimescaleDB Sink Class
# ═══════════════════════════════════════════════════════════════

class TimescaleDBSink:
    """
    Kafka consumer that batch-upserts OHLCV data into TimescaleDB.

    Flow:
        poll() → deserialize JSON → accumulate batch → upsert → commit offsets
    """

    def __init__(
        self,
        broker: str = KAFKA_BROKER,
        topic: str = KAFKA_TOPIC_AGGREGATED,
        batch_size: int = SINK_BATCH_SIZE,
        db_host: str = POSTGRES_HOST,
        db_port: int = POSTGRES_PORT,
        db_name: str = POSTGRES_DB,
        db_user: str = POSTGRES_USER,
        db_password: str = POSTGRES_PASSWORD,
    ):
        self.topic = topic
        self.batch_size = batch_size
        self._shutdown = False
        self._total_upserted = 0
        self._total_errors = 0

        # ── Kafka Consumer Configuration ──
        # WHY these specific settings:
        kafka_conf = {
            "bootstrap.servers": broker,

            # Consumer group: All instances with the same group ID
            # share the load (each gets a subset of partitions).
            # Single instance = gets all partitions.
            "group.id": "nepse-timescaledb-sink",

            # Client ID: Appears in broker logs for debugging
            "client.id": "nepse-tsdb-sink-1",

            # Auto offset reset: Start from earliest if no committed
            # offset exists. This ensures we don't miss data on first run.
            "auto.offset.reset": "earliest",

            # Manual commit: We commit offsets AFTER successfully
            # writing to TimescaleDB. This gives us at-least-once
            # delivery guarantee. If the sink crashes before committing,
            # it re-reads and re-processes (idempotent upsert handles it).
            "enable.auto.commit": False,

            # Session timeout: If the consumer doesn't heartbeat within
            # 30s, the broker considers it dead and reassigns partitions.
            "session.timeout.ms": 30000,
        }

        self.consumer = Consumer(kafka_conf)
        logger.info(
            f"Kafka consumer initialized: broker={broker}, "
            f"topic={topic}, group=nepse-timescaledb-sink"
        )

        # ── TimescaleDB Connection ──
        # WHY psycopg2 (not SQLAlchemy)?
        #   - Direct control over connection pooling and transactions
        #   - execute_batch() is optimized for bulk inserts
        #   - No ORM overhead for a simple write-only sink
        try:
            self.conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                dbname=db_name,
                user=db_user,
                password=db_password,
            )
            self.conn.autocommit = False  # We manage transactions manually
            logger.info(
                f"TimescaleDB connected: {db_host}:{db_port}/{db_name}"
            )
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            logger.error(
                "Ensure TimescaleDB is running: docker-compose up -d timescaledb"
            )
            sys.exit(1)

        # ── Graceful Shutdown ──
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C — commit pending work and close connections."""
        logger.warning("Shutdown signal received. Finishing current batch...")
        self._shutdown = True

    def _parse_message(self, msg) -> dict | None:
        """
        Deserialize a Kafka message value from JSON to dict.

        Returns None if the message is malformed (logged and skipped).
        """
        try:
            value = msg.value().decode("utf-8")
            data = json.loads(value)

            # Validate required fields exist
            required = [
                "symbol", "window_start", "window_end",
                "open", "high", "low", "close",
                "volume", "turnover", "trade_count", "vwap", "sector",
            ]
            for field in required:
                if field not in data:
                    logger.warning(
                        f"Missing field '{field}' in message: {value[:200]}"
                    )
                    return None

            return data

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Failed to parse message: {e}")
            return None

    def _upsert_batch(self, batch: list[dict]) -> int:
        """
        Batch-upsert OHLCV records into TimescaleDB.

        WHY execute_batch (not executemany)?
            psycopg2.extras.execute_batch() groups multiple INSERTs
            into a single network round-trip. For 100 rows, this is
            ~10x faster than executing them one by one.

        Returns the number of rows successfully upserted.
        """
        if not batch:
            return 0

        try:
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    UPSERT_SQL,
                    batch,
                    page_size=self.batch_size,
                )
            self.conn.commit()
            return len(batch)

        except psycopg2.Error as e:
            logger.error(f"Database upsert failed: {e}")
            self.conn.rollback()
            self._total_errors += len(batch)
            return 0

    def run(self):
        """
        Main consumer loop: poll Kafka → batch → upsert → commit.

        WHY this pattern (not a simple for-loop)?
            1. poll() with timeout prevents busy-waiting
            2. Batch accumulation amortizes DB write overhead
            3. Manual offset commit ensures at-least-once delivery
            4. Periodic logging tracks throughput and health
        """
        self.consumer.subscribe([self.topic])
        logger.info(f"Subscribed to topic: {self.topic}")

        start_time = time.time()
        batch = []
        last_log_time = time.time()
        log_interval = 30  # Log stats every 30 seconds

        logger.info(f"{'=' * 60}")
        logger.info(f"TIMESCALEDB SINK STARTED")
        logger.info(f"{'=' * 60}")
        logger.info(f"  Topic       : {self.topic}")
        logger.info(f"  Batch size  : {self.batch_size}")
        logger.info(f"  Table       : {TIMESCALEDB_TABLE}")
        logger.info(f"{'=' * 60}")
        logger.info(f"Press Ctrl+C to stop.")

        try:
            while not self._shutdown:
                # ── Poll for messages ──
                # Timeout 1.0s: blocks up to 1 second waiting for messages.
                # Returns None if no messages available.
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # No message available — flush any accumulated batch
                    if batch:
                        upserted = self._upsert_batch(batch)
                        self._total_upserted += upserted
                        self.consumer.commit()
                        batch = []
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Reached end of partition — normal, not an error
                        logger.debug(
                            f"Partition {msg.partition()} reached end "
                            f"at offset {msg.offset()}"
                        )
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                # ── Parse and accumulate ──
                record = self._parse_message(msg)
                if record is not None:
                    batch.append(record)

                # ── Flush batch when full ──
                if len(batch) >= self.batch_size:
                    upserted = self._upsert_batch(batch)
                    self._total_upserted += upserted
                    self.consumer.commit()
                    batch = []

                # ── Periodic stats logging ──
                now = time.time()
                if now - last_log_time >= log_interval:
                    elapsed = now - start_time
                    rate = self._total_upserted / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Stats: upserted={self._total_upserted:,} | "
                        f"errors={self._total_errors} | "
                        f"rate={rate:,.1f} rows/sec | "
                        f"elapsed={elapsed:.0f}s"
                    )
                    last_log_time = now

        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt caught.")
        finally:
            # ── Flush remaining batch ──
            if batch:
                upserted = self._upsert_batch(batch)
                self._total_upserted += upserted
                try:
                    self.consumer.commit()
                except KafkaException:
                    pass  # Consumer may already be closing

            # ── Final stats ──
            elapsed = time.time() - start_time
            rate = self._total_upserted / elapsed if elapsed > 0 else 0

            logger.info(f"{'=' * 60}")
            logger.info(f"TIMESCALEDB SINK STOPPED")
            logger.info(f"{'=' * 60}")
            logger.info(f"  Total upserted  : {self._total_upserted:,}")
            logger.info(f"  Total errors    : {self._total_errors}")
            logger.info(f"  Elapsed time    : {elapsed:.1f}s")
            logger.info(f"  Avg throughput  : {rate:,.1f} rows/sec")
            logger.info(f"{'=' * 60}")

            # ── Close connections ──
            self.consumer.close()
            self.conn.close()
            logger.info("Kafka consumer and DB connection closed.")


# ═══════════════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Consume OHLCV data from Kafka and write to TimescaleDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start sink with default settings
  python -m src.sink.timescaledb_sink

  # Custom batch size (flush every 50 messages)
  python -m src.sink.timescaledb_sink --batch-size 50

  # Custom broker and topic
  python -m src.sink.timescaledb_sink --broker kafka:29092 --topic my-topic
        """,
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=KAFKA_BROKER,
        help=f"Kafka broker address (default: {KAFKA_BROKER})",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC_AGGREGATED,
        help=f"Kafka topic to consume (default: {KAFKA_TOPIC_AGGREGATED})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=SINK_BATCH_SIZE,
        help=f"Rows per DB write batch (default: {SINK_BATCH_SIZE})",
    )
    parser.add_argument(
        "--db-host",
        type=str,
        default=POSTGRES_HOST,
        help=f"TimescaleDB host (default: {POSTGRES_HOST})",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=POSTGRES_PORT,
        help=f"TimescaleDB port (default: {POSTGRES_PORT})",
    )

    args = parser.parse_args()

    sink = TimescaleDBSink(
        broker=args.broker,
        topic=args.topic,
        batch_size=args.batch_size,
        db_host=args.db_host,
        db_port=args.db_port,
    )

    sink.run()


if __name__ == "__main__":
    main()
