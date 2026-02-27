"""
PySpark Structured Streaming Consumer — NEPSE OHLCV Aggregator
================================================================
PURPOSE:
    Consumes raw floorsheet transactions from Kafka's `nepse-raw-ticks`
    topic and computes 1-minute tumbling window OHLCV aggregations per
    stock symbol. Results are output to console (for debugging) and to
    Kafka's `nepse-aggregated` topic (for downstream TimescaleDB sink).

WHY PySpark Structured Streaming (not legacy DStreams)?
    1. DataFrame API: SQL-like operations (groupBy, agg) — cleaner code
    2. Event-time processing: Windows based on TRADE time, not wall clock
    3. Exactly-once guarantees: Built-in checkpointing + Kafka offsets
    4. Watermarking: Handles late-arriving data gracefully

ARCHITECTURE:
    Kafka (nepse-raw-ticks)
         ↓ readStream
    Raw DataFrame (JSON → typed columns)
         ↓ withWatermark + window
    1-min OHLCV Aggregation per symbol
         ↓ writeStream
    Console Sink (debug) + Kafka Sink (nepse-aggregated)

OHLCV OUTPUT SCHEMA:
    ┌──────────────┬──────────────────────────────────────────────┐
    │ Column       │ Description                                  │
    ├──────────────┼──────────────────────────────────────────────┤
    │ symbol       │ Stock ticker (e.g., NABIL, NICA)             │
    │ window_start │ Start of 1-min window                        │
    │ window_end   │ End of 1-min window                          │
    │ open         │ First trade price in the window              │
    │ high         │ Highest trade price in the window             │
    │ low          │ Lowest trade price in the window              │
    │ close        │ Last trade price in the window               │
    │ volume       │ Total shares traded in the window            │
    │ turnover     │ Total trade value (sum of amount)            │
    │ trade_count  │ Number of transactions in the window         │
    │ vwap         │ Volume-weighted average price                │
    │ sector       │ Sector classification                        │
    └──────────────┴──────────────────────────────────────────────┘
"""

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Add project root to path for imports ──
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.config import (
    KAFKA_BROKER,
    KAFKA_TOPIC_AGGREGATED,
    KAFKA_TOPIC_RAW,
    SPARK_KAFKA_JAR,
)
from src.utils.logger import get_logger

logger = get_logger("spark_consumer")


# ═══════════════════════════════════════════════════════════════
# Floorsheet JSON Schema
# ═══════════════════════════════════════════════════════════════
# WHY explicit schema instead of inferSchema?
#   1. Performance: Spark doesn't have to scan data to guess types
#   2. Safety: Malformed messages get null values instead of crashing
#   3. Predictability: You know exactly what you're working with
# ═══════════════════════════════════════════════════════════════

FLOORSHEET_SCHEMA = StructType([
    StructField("contract_no", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("buyer", IntegerType(), True),
    StructField("seller", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("rate", DoubleType(), True),
    StructField("amount", DoubleType(), True),
    StructField("trade_time", StringType(), True),
    StructField("sector", StringType(), True),
])


# ═══════════════════════════════════════════════════════════════
# Spark Consumer Class
# ═══════════════════════════════════════════════════════════════

class FloorsheetConsumer:
    """
    PySpark Structured Streaming consumer for NEPSE floorsheet data.

    Reads from Kafka, computes 1-minute OHLCV windows, and writes
    results to console and Kafka aggregated topic.
    """


    def __init__(
        self,
        broker: str = KAFKA_BROKER,
        input_topic: str = KAFKA_TOPIC_RAW,
        output_topic: str = KAFKA_TOPIC_AGGREGATED,
        cores: int = 3,
    ):
        self.broker = broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.cores = cores

        # ── Windows Compatibility Fix ──
        # WHY: PySpark on Windows bundles Hadoop JARs that call
        # NativeIO.access0() — a Windows DLL. The JVM loads hadoop.dll
        # via JNI from the system PATH. We must ensure HADOOP_HOME/bin
        # is in PATH so the JVM can find both winutils.exe and hadoop.dll.
        import os
        import platform
        self._is_windows = platform.system() == "Windows"
        if self._is_windows:
            hadoop_home = os.environ.get("HADOOP_HOME", r"C:\hadoop")
            os.environ["HADOOP_HOME"] = hadoop_home
            hadoop_bin = os.path.join(hadoop_home, "bin")
            # Ensure hadoop/bin is in PATH for JNI to find hadoop.dll
            if hadoop_bin not in os.environ.get("PATH", ""):
                os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
            logger.info(f"Windows detected. HADOOP_HOME={hadoop_home}, added {hadoop_bin} to PATH")

        # ── Build SparkSession ──
        # WHY local[3]?
        #   We have 3 Kafka partitions. Spark assigns 1 task per partition.
        #   local[3] = 3 executor threads = perfect 1:1 mapping.
        #   Using local[*] would over-provision threads for 3 partitions.
        #   Using local[1] would serialize consumption — defeating the
        #   purpose of having 3 partitions.
        self.spark = (
            SparkSession.builder
            .appName("NEPSE-Floorsheet-Streaming")
            .master(f"local[{cores}]")

            # ── Kafka Connector JAR ──
            # WHY we pin the exact JAR version:
            #   pyspark 3.5.4 uses Scala 2.12 internally.
            #   The Kafka connector must match BOTH:
            #     - Spark version (3.5.4)
            #     - Scala version (2.12)
            #   Mismatched JARs cause ClassNotFoundException at runtime.
            #   This is the #1 pain point for first-time Spark users.
            .config(
                "spark.jars.packages",
                SPARK_KAFKA_JAR,
            )

            # ── Shuffle partitions ──
            # Default is 200, which is absurd for local development.
            # We have 3 input partitions → 3 shuffle partitions is plenty.
            # Too many partitions = overhead of scheduling tiny tasks.
            .config("spark.sql.shuffle.partitions", "3")

            # ── Streaming UI ──
            # Keep streaming query progress in Spark UI (http://localhost:4040)
            .config("spark.sql.streaming.metricsEnabled", "true")

            # ── Windows NativeIO bypass ──
            # WHY: Hadoop's NativeIO calls Windows DLLs that often
            # fail with version mismatches. This JVM flag forces
            # Hadoop to use pure-Java file I/O instead.
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.logLevel=WARN -Dio.nativeio.disabled=true",
            )

            # ── Hadoop local filesystem ──
            # Force local filesystem implementation (not HDFS)
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")

            .getOrCreate()
        )

        # Reduce Spark's verbose logging
        self.spark.sparkContext.setLogLevel("WARN")

        logger.info(
            f"SparkSession created: app=NEPSE-Floorsheet-Streaming, "
            f"master=local[{cores}], jar={SPARK_KAFKA_JAR}"
        )

    def _read_from_kafka(self):
        """
        Create a streaming DataFrame from Kafka.

        WHY these specific Kafka options:
        - startingOffsets=earliest: Read from the beginning of the topic.
          This ensures we process ALL messages, including those that were
          produced before this consumer started.
        - failOnDataLoss=false: Don't crash if Kafka has compacted old
          segments. In development, topic data may expire (24h retention).
        - maxOffsetsPerTrigger=10000: Process at most 10K messages per
          micro-batch. This prevents Spark from trying to consume millions
          of messages in a single batch and running out of memory.
        """
        raw_stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.broker)
            .option("subscribe", self.input_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 10000)
            .load()
        )

        logger.info(
            f"Kafka stream connected: topic={self.input_topic}, "
            f"broker={self.broker}"
        )
        return raw_stream

    def _parse_json(self, raw_stream):
        """
        Parse raw Kafka messages (key/value bytes) into typed DataFrame.

        Kafka messages arrive as:
            key: bytes (stock symbol)
            value: bytes (JSON string)
            topic, partition, offset, timestamp: metadata

        We:
        1. Cast value from bytes → string
        2. Parse JSON string → struct using our schema
        3. Extract individual columns
        4. Cast trade_time string → proper Timestamp type

        WHY from_json + explicit schema?
        - Structured Streaming CANNOT infer schema from a stream
        - We must declare it upfront (unlike batch mode)
        """
        parsed = (
            raw_stream
            # Cast binary key/value to strings
            .withColumn("key_str", F.col("key").cast("string"))
            .withColumn("value_str", F.col("value").cast("string"))

            # Parse JSON value into struct columns
            .withColumn("data", F.from_json(F.col("value_str"), FLOORSHEET_SCHEMA))

            # Flatten struct into individual columns
            .select(
                F.col("data.contract_no").alias("contract_no"),
                F.col("data.symbol").alias("symbol"),
                F.col("data.buyer").alias("buyer"),
                F.col("data.seller").alias("seller"),
                F.col("data.quantity").alias("quantity"),
                F.col("data.rate").alias("rate"),
                F.col("data.amount").alias("amount"),
                # Cast trade_time string → Timestamp
                # WHY to_timestamp? Spark's window() function requires
                # a proper TimestampType column, not a string.
                F.to_timestamp(
                    F.col("data.trade_time"),
                    "yyyy-MM-dd HH:mm:ss.SSS"
                ).alias("trade_time"),
                F.col("data.sector").alias("sector"),
            )
            # Drop rows where parsing failed (null trade_time)
            .filter(F.col("trade_time").isNotNull())
        )

        return parsed

    def _compute_ohlcv(self, parsed_stream):
        """
        Compute 1-minute OHLCV aggregations per stock symbol.

        KEY CONCEPTS:
        1. Watermark: "10 minutes" means we wait up to 10 minutes for
           late-arriving data before finalizing a window. This handles
           out-of-order events (common in real market data feeds).

        2. Tumbling window: Non-overlapping 1-minute windows.
           [11:00:00 - 11:01:00), [11:01:00 - 11:02:00), ...
           Each trade belongs to exactly ONE window.

        3. OHLCV = Open, High, Low, Close, Volume
           - Open: first trade price in the window
           - High: max price
           - Low: min price
           - Close: last trade price
           - Volume: total shares traded
           Plus: turnover (total value), trade count, VWAP

        WHY first/last for Open/Close?
        - first() gives the earliest trade's price in the window (Open)
        - last() gives the latest trade's price in the window (Close)
        - This works because our data is ordered within each partition
          (we keyed by symbol, remember?)
        """
        ohlcv = (
            parsed_stream
            # Watermark: tolerate up to 10 min of late data
            .withWatermark("trade_time", "10 minutes")

            # Group by: symbol + 1-minute tumbling window
            .groupBy(
                F.col("symbol"),
                F.window(F.col("trade_time"), "1 minute"),
            )

            .agg(
                # OHLCV metrics
                F.first("rate").alias("open"),
                F.max("rate").alias("high"),
                F.min("rate").alias("low"),
                F.last("rate").alias("close"),
                F.sum("quantity").alias("volume"),
                F.sum("amount").alias("turnover"),
                F.count("*").alias("trade_count"),

                # VWAP = total turnover / total volume
                # Volume-Weighted Average Price — the "true" average price
                # weighted by how many shares traded at each price level.
                # More accurate than simple average for financial analysis.
                (F.sum("amount") / F.sum("quantity")).alias("vwap"),

                # Keep sector (same for all trades of a symbol)
                F.first("sector").alias("sector"),
            )

            # Flatten the window struct into start/end columns
            .select(
                F.col("symbol"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("open"),
                F.col("high"),
                F.col("low"),
                F.col("close"),
                F.col("volume"),
                F.col("turnover"),
                F.col("trade_count"),
                F.round(F.col("vwap"), 2).alias("vwap"),
                F.col("sector"),
            )
        )

        return ohlcv

    def start(self):
        """
        Start the streaming pipeline.

        Two output sinks:
        1. Console: For debugging — see aggregated results in terminal
        2. Kafka: Write to nepse-aggregated topic for TimescaleDB pickup

        WHY processingTime trigger?
        - "10 seconds" means Spark processes a micro-batch every 10 seconds
        - Lower = more frequent output but more overhead
        - Higher = larger batches but more latency
        - 10s is a good balance for near-real-time dashboards

        WHY outputMode="update"?
        - "complete": Re-outputs ALL windows every micro-batch (expensive)
        - "append": Only outputs after watermark closes window (high latency)
        - "update": Outputs only changed/new windows (best for streaming)
        """
        logger.info("Building streaming pipeline...")

        # Step 1: Read from Kafka
        raw = self._read_from_kafka()

        # Step 2: Parse JSON into typed DataFrame
        parsed = self._parse_json(raw)

        # Step 3: Compute OHLCV aggregations
        ohlcv = self._compute_ohlcv(parsed)

        # ── Checkpoint base path ──
        project_root = Path(__file__).resolve().parent.parent.parent
        checkpoint_base = project_root / "data" / "checkpoints"
        checkpoint_base.mkdir(parents=True, exist_ok=True)

        # ── Console Sink (for debugging) ──
        console_query = (
            ohlcv
            .writeStream
            .queryName("nepse-ohlcv-console")
            .outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .option(
                "checkpointLocation",
                str(checkpoint_base / "ohlcv-console"),
            )
            .trigger(processingTime="10 seconds")
            .start()
        )

        # ── Kafka Sink (for TimescaleDB downstream) ──
        # WHY we serialize to JSON for the Kafka sink:
        # Kafka stores key-value bytes. We serialize the OHLCV
        # DataFrame row as a JSON string in the 'value' column,
        # and use the symbol as the key (for partitioning).
        kafka_query = (
            ohlcv
            .select(
                F.col("symbol").cast("string").alias("key"),
                F.to_json(F.struct("*")).alias("value"),
            )
            .writeStream
            .queryName("nepse-ohlcv-kafka")
            .outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", self.broker)
            .option("topic", self.output_topic)
            .option(
                "checkpointLocation",
                str(checkpoint_base / "ohlcv-kafka"),
            )
            .trigger(processingTime="10 seconds")
            .start()
        )

        logger.info(f"{'=' * 60}")
        logger.info(f"STREAMING PIPELINE STARTED")
        logger.info(f"{'=' * 60}")
        logger.info(f"  Input topic  : {self.input_topic}")
        logger.info(f"  Output topic : {self.output_topic}")
        logger.info(f"  Window       : 1 minute (tumbling)")
        logger.info(f"  Watermark    : 10 minutes")
        logger.info(f"  Trigger      : every 10 seconds")
        logger.info(f"  Cores        : {self.cores}")
        logger.info(f"  Spark UI     : http://localhost:4040")
        logger.info(f"{'=' * 60}")
        logger.info(f"Press Ctrl+C to stop the streaming pipeline.")

        try:
            # Wait for both queries to terminate (blocks forever)
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.warning("Shutdown signal received.")
        finally:
            logger.info("Stopping streaming queries...")
            console_query.stop()
            kafka_query.stop()
            self.spark.stop()
            logger.info("Spark session stopped cleanly.")


# ═══════════════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="PySpark Structured Streaming consumer for NEPSE data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start consumer with default settings (3 cores, localhost:9092)
  python -m src.consumer.spark_consumer

  # Customize cores and broker
  python -m src.consumer.spark_consumer --cores 4 --broker kafka:29092
        """,
    )
    parser.add_argument(
        "--cores",
        type=int,
        default=3,
        help="Number of Spark executor cores (default: 3, matches partitions)",
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=KAFKA_BROKER,
        help=f"Kafka broker address (default: {KAFKA_BROKER})",
    )

    args = parser.parse_args()

    consumer = FloorsheetConsumer(
        broker=args.broker,
        cores=args.cores,
    )

    consumer.start()


if __name__ == "__main__":
    main()
