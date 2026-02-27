"""
NEPSE Kafka Producer — Floorsheet Replay Simulator
====================================================
PURPOSE:
    Reads the generated floorsheet CSV and replays every transaction into
    Kafka's `nepse-raw-ticks` topic at configurable speed (default: 100x).
    This simulates a real-time market data feed for downstream consumers.

WHY THIS DESIGN:
    1. Chunked CSV reading: The CSV is >1GB. Loading it all into memory is
       wasteful. We read in chunks of 10,000 rows — keeping memory flat.

    2. Message key = stock symbol: Kafka guarantees ordering WITHIN a
       partition. By keying on symbol, all trades for NABIL go to the
       same partition, preserving chronological order. This is critical
       for correct OHLC window calculations in PySpark.

    3. Delivery callbacks: Kafka producers are asynchronous by default.
       We register a callback to catch failed deliveries immediately,
       not 10 minutes later when we realize data is missing.

    4. Replay speed via timestamps: We compare consecutive trade_time
       values and sleep for (real_gap / speed_multiplier). At 100x,
       a 1-second gap becomes 10ms. This produces ~83K events/min.

    5. Graceful shutdown: Ctrl+C triggers flush() to ensure all buffered
       messages are delivered before the process exits. Without this,
       you lose the last batch of messages.

KAFKA MESSAGE FORMAT (JSON):
    Key:   "NABIL" (stock symbol, UTF-8 encoded)
    Value: {
        "contract_no": "20210104-000001",
        "symbol": "NABIL",
        "buyer": 12,
        "seller": 35,
        "quantity": 100,
        "rate": 1150.50,
        "amount": 115050.00,
        "trade_time": "2021-01-04 11:00:00.004",
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

import pandas as pd
from confluent_kafka import Producer, KafkaError

# ── Add project root to path for imports ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.config import (
    DATA_DIR,
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    PRODUCER_BATCH_SIZE,
    REPLAY_SPEED,
)
from src.utils.logger import get_logger

logger = get_logger("kafka_producer")


# ═══════════════════════════════════════════════════════════════
# Delivery Callback
# ═══════════════════════════════════════════════════════════════

class DeliveryTracker:
    """
    Tracks Kafka message delivery success/failure.

    WHY a class instead of a simple function?
    We need to accumulate counters across millions of callbacks.
    A closure or global variables would work but are harder to test.
    """

    def __init__(self):
        self.success_count = 0
        self.error_count = 0

    def on_delivery(self, err, msg):
        """
        Called once per message after Kafka acknowledges (or rejects) it.

        WHY we need this:
        - producer.produce() is ASYNC — it just queues the message
        - Without this callback, you'd never know if delivery failed
        - Common failures: broker down, topic doesn't exist, serialization error
        """
        if err is not None:
            self.error_count += 1
            if self.error_count <= 10:  # Don't spam logs
                logger.error(
                    f"Delivery failed for {msg.topic()} "
                    f"[{msg.partition()}]: {err}"
                )
        else:
            self.success_count += 1


# ═══════════════════════════════════════════════════════════════
# Producer Core
# ═══════════════════════════════════════════════════════════════

class FloorsheetProducer:
    """
    Replays NEPSE floorsheet CSV into Kafka at configurable speed.

    Architecture:
        CSV (chunked read) → JSON serialize → Kafka produce (async)
                                                    ↓
                                            delivery callback
    """

    def __init__(
        self,
        broker: str = KAFKA_BROKER,
        topic: str = KAFKA_TOPIC_RAW,
        speed: int = REPLAY_SPEED,
        batch_size: int = PRODUCER_BATCH_SIZE,
    ):
        self.topic = topic
        self.speed = speed
        self.batch_size = batch_size
        self.tracker = DeliveryTracker()
        self._shutdown = False

        # ── Kafka Producer Configuration ──
        # WHY these specific settings:
        conf = {
            "bootstrap.servers": broker,

            # Client ID: Shows up in Kafka broker logs — helps debugging
            # when you have multiple producers hitting the same cluster
            "client.id": "nepse-floorsheet-producer",

            # Linger: Wait up to 10ms to batch messages together before
            # sending. This DRAMATICALLY improves throughput (fewer network
            # round-trips) at the cost of tiny latency increase.
            # Without this, each produce() triggers a separate network call.
            "linger.ms": 10,

            # Batch size: Max bytes per batch to the broker. 64KB is a
            # good default for our JSON messages (~200 bytes each).
            # Larger batches = fewer requests = higher throughput.
            "batch.size": 65536,

            # Buffer: Total memory the producer can use for buffering.
            # 32MB handles our replay speed without backpressure.
            # NOTE: confluent-kafka uses 'queue.buffering.max.kbytes',
            # NOT 'buffer.memory' (which is the Java client equivalent).
            "queue.buffering.max.kbytes": 32768,

            # Compression: Snappy gives ~70% compression with minimal CPU.
            # Our JSON messages compress extremely well (repeated field names).
            # This reduces network bandwidth and broker disk usage.
            "compression.type": "snappy",

            # Acks: "all" means the broker AND all in-sync replicas must
            # confirm. We only have 1 replica, so this = "1" in practice.
            # In production with 3 replicas, "all" prevents data loss.
            "acks": "all",

            # Retries: Automatically retry transient broker errors
            # (leader election, network blip). 3 retries is conservative.
            "retries": 3,

            # Idempotence: Prevents duplicate messages on retry.
            # Combined with acks=all, this gives exactly-once semantics
            # on the producer side.
            "enable.idempotence": True,
        }

        self.producer = Producer(conf)
        logger.info(
            f"Producer initialized: broker={broker}, topic={topic}, "
            f"speed={speed}x, batch_size={batch_size}"
        )

        # ── Graceful Shutdown ──
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully — flush remaining messages."""
        logger.warning("Shutdown signal received. Flushing remaining messages...")
        self._shutdown = True

    def _serialize_row(self, row: pd.Series) -> str:
        """
        Convert a DataFrame row to a JSON string.

        WHY JSON over Avro/Protobuf?
        - Simpler for learning (human-readable in Kafka UI)
        - No schema registry needed
        - In production, you'd switch to Avro for type safety + smaller size
        """
        return json.dumps({
            "contract_no": str(row["contract_no"]),
            "symbol": str(row["symbol"]),
            "buyer": int(row["buyer"]),
            "seller": int(row["seller"]),
            "quantity": int(row["quantity"]),
            "rate": float(row["rate"]),
            "amount": float(row["amount"]),
            "trade_time": str(row["trade_time"]),
            "sector": str(row["sector"]),
        })

    def _calculate_sleep(self, prev_time: str, curr_time: str) -> float:
        """
        Calculate how long to sleep between messages based on replay speed.

        Real gap between trades / speed multiplier = simulated gap.
        Example: 500ms real gap at 100x speed = 5ms simulated gap.
        """
        try:
            fmt = "%Y-%m-%d %H:%M:%S.%f"
            t1 = datetime.strptime(prev_time, fmt)
            t2 = datetime.strptime(curr_time, fmt)
            real_gap = (t2 - t1).total_seconds()

            if real_gap <= 0:
                return 0.0

            return real_gap / self.speed
        except (ValueError, TypeError):
            return 0.0

    def replay(self, csv_path: str | Path) -> None:
        """
        Read CSV in chunks and replay into Kafka.

        WHY chunked reading?
        - The CSV is 1+ GB. pd.read_csv() on the full file would eat ~4GB RAM.
        - Chunked reading (10K rows/chunk) keeps memory usage under ~50MB.
        - Each chunk is processed and discarded before the next is loaded.
        """
        csv_path = Path(csv_path)
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            logger.error(
                "Run: python -m src.data_generator.generate_floorsheet"
            )
            sys.exit(1)

        # Count total rows for progress tracking
        logger.info(f"Scanning {csv_path.name} for row count...")
        total_rows = sum(1 for _ in open(csv_path, encoding="utf-8")) - 1
        logger.info(f"Total rows to replay: {total_rows:,}")

        # ── Start Replay ──
        start_time = time.time()
        rows_sent = 0
        prev_trade_time = None
        log_interval = 50000  # Log progress every 50K messages

        logger.info(f"{'=' * 60}")
        logger.info(f"STARTING REPLAY @ {self.speed}x SPEED")
        logger.info(f"{'=' * 60}")

        chunk_reader = pd.read_csv(
            csv_path,
            chunksize=10000,
            dtype={
                "contract_no": str,
                "symbol": str,
                "buyer": int,
                "seller": int,
                "quantity": int,
                "rate": float,
                "amount": float,
                "trade_time": str,
                "sector": str,
            },
        )

        try:
            for chunk in chunk_reader:
                if self._shutdown:
                    break

                for _, row in chunk.iterrows():
                    if self._shutdown:
                        break

                    # ── Calculate replay delay ──
                    curr_trade_time = row["trade_time"]
                    if prev_trade_time is not None:
                        sleep_time = self._calculate_sleep(
                            prev_trade_time, curr_trade_time
                        )
                        if sleep_time > 0:
                            time.sleep(sleep_time)
                    prev_trade_time = curr_trade_time

                    # ── Serialize and produce ──
                    key = row["symbol"]  # Partition key!
                    value = self._serialize_row(row)

                    self.producer.produce(
                        topic=self.topic,
                        key=key.encode("utf-8"),
                        value=value.encode("utf-8"),
                        callback=self.tracker.on_delivery,
                    )

                    rows_sent += 1

                    # ── Periodic flush + progress log ──
                    # WHY periodic flush? The producer buffers messages
                    # internally. poll(0) triggers delivery callbacks
                    # without blocking. This prevents the buffer from
                    # growing unbounded.
                    if rows_sent % self.batch_size == 0:
                        self.producer.poll(0)

                    if rows_sent % log_interval == 0:
                        elapsed = time.time() - start_time
                        rate = rows_sent / elapsed if elapsed > 0 else 0
                        pct = (rows_sent / total_rows) * 100
                        logger.info(
                            f"Progress: {rows_sent:>10,}/{total_rows:,} "
                            f"({pct:5.1f}%) | "
                            f"Rate: {rate:,.0f} msg/sec | "
                            f"Delivered: {self.tracker.success_count:,} | "
                            f"Errors: {self.tracker.error_count}"
                        )

        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt caught during replay.")
        finally:
            # ── Final Flush ──
            # WHY flush() is critical:
            # producer.produce() is async — messages sit in an internal buffer.
            # flush() blocks until ALL buffered messages are delivered to Kafka.
            # Without this, you lose the last batch when the process exits.
            logger.info("Flushing remaining messages to Kafka...")
            remaining = self.producer.flush(timeout=30)

            elapsed = time.time() - start_time
            rate = rows_sent / elapsed if elapsed > 0 else 0

            logger.info(f"{'=' * 60}")
            logger.info(f"REPLAY COMPLETE")
            logger.info(f"{'=' * 60}")
            logger.info(f"  Messages sent      : {rows_sent:,}")
            logger.info(f"  Delivered OK       : {self.tracker.success_count:,}")
            logger.info(f"  Delivery errors    : {self.tracker.error_count}")
            logger.info(f"  Unflushed messages : {remaining}")
            logger.info(f"  Elapsed time       : {elapsed:.1f}s")
            logger.info(f"  Avg throughput     : {rate:,.0f} msg/sec")
            logger.info(f"{'=' * 60}")


# ═══════════════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Replay NEPSE floorsheet data into Kafka.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Replay at 100x speed (default)
  python -m src.producer.kafka_producer

  # Replay at 500x speed (faster, ~400K events/min)
  python -m src.producer.kafka_producer --speed 500

  # Replay at 1x (real-time — very slow, for demo purposes)
  python -m src.producer.kafka_producer --speed 1

  # Replay a custom CSV file
  python -m src.producer.kafka_producer --csv data/raw/my_data.csv
        """,
    )
    parser.add_argument(
        "--speed",
        type=int,
        default=REPLAY_SPEED,
        help=f"Replay speed multiplier (default: {REPLAY_SPEED})",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Path to floorsheet CSV (default: data/raw/nepse_floorsheet.csv)",
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
        default=KAFKA_TOPIC_RAW,
        help=f"Kafka topic name (default: {KAFKA_TOPIC_RAW})",
    )

    args = parser.parse_args()

    csv_path = args.csv if args.csv else DATA_DIR / "nepse_floorsheet.csv"

    producer = FloorsheetProducer(
        broker=args.broker,
        topic=args.topic,
        speed=args.speed,
    )

    producer.replay(csv_path)


if __name__ == "__main__":
    main()
