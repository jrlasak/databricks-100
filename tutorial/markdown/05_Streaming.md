%md
# Structured Streaming in Databricks — Concepts #41–#50

**Overview**: This notebook covers the second half of the Structured Streaming curriculum:
structured streaming foundations, triggers, output modes, joins, windowing, foreachBatch,
Lakeflow streaming tables, checkpointing, watermarks, and stream-stream joins.

**Environment**: Designed for Databricks Community Edition (single node, free tier).
Structured Streaming *is* available on Community Edition, but with limitations:
- Single-node execution (no distributed recovery)
- Checkpoint recovery may not survive full cluster restarts
- Lakeflow / DLT pipelines are NOT available (covered conceptually)
- Memory-based sinks are best for demonstration

**Datasets Used**:
- `rate` source for synthetic streaming data
- File-based streaming (CSV files written to DBFS / local)
- Simulated IoT, clickstream, and financial event data

```python
```
%md
### Setup — Create Directories and Clean Checkpoints

```python
import os
import shutil
import time
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, DateType
)
from pyspark.sql.functions import (
    col, lit, expr, window, current_timestamp, count,
    sum as _sum, avg, max as _max, min as _min,
    to_timestamp, from_unixtime, unix_timestamp,
    struct, to_json, from_json, when, concat,
    date_format, year, month, dayofmonth, hour,
    minute, second, split, explode, array, rand,
    round as _round
)
from pyspark.sql.streaming import DataStreamWriter

print("✅ Spark session ready")

# ── Working directories ──
BASE_DIR = "/tmp/streaming_demo"
CHECKPOINT_DIR = f"{BASE_DIR}/checkpoints"
STREAM_SRC_DIR  = f"{BASE_DIR}/source_csv"
DELTA_SINK_DIR  = f"{BASE_DIR}/delta_sink"
STATIC_DIR      = f"{BASE_DIR}/static"
DUAL_SINK_1     = f"{BASE_DIR}/dual_sink/events"
DUAL_SINK_2     = f"{BASE_DIR}/dual_sink/summary"

dirs = [BASE_DIR, CHECKPOINT_DIR, STREAM_SRC_DIR,
        DELTA_SINK_DIR, STATIC_DIR, DUAL_SINK_1, DUAL_SINK_2]

for d in dirs:
    os.makedirs(d, exist_ok=True)
    print(f"  📁 {d}")

# Kill any lingering streams
for q in spark.streams.active:
    q.stop()
    print(f"  ⏹️  Stopped: {q.name}")

print("✅ Environment ready")

```
```python
```
%md
---
## Concept 41: Structured Streaming Fundamentals

**Core Idea**: Structured Streaming treats a *stream* as an **unbounded, continuously-
growing table**. New data arriving in the stream is appended as new rows.

**How it differs from batch**:
|   | Batch | Structured Streaming |
|---|---|---|
| Input | Finite, bounded | Unbounded, continuous |
| Query | Runs once on static data | Runs continuously on arriving data |
| Result | Single final result | Continuously updating result (or a new Delta table) |
| Engine | Reads entire table | Reads micro-batches (or continuous processing) |

**The two key APIs**:
- `spark.readStream` — define the streaming DataFrame (source)
- `df.writeStream`  — define the output (sink), trigger, and output mode

Let's start with the simplest possible streaming query: `rate` → `console`.

```python
print("=" * 60)
print("CONCEPT 41 — Structured Streaming Fundamentals")
print("=" * 60)

# ── Simplest streaming query: rate source → console ──
streaming_df = (
    spark.readStream
    .format("rate")              # built-in source: generates increasing (timestamp, value) pairs
    .option("rowsPerSecond", 5)  # 5 rows per second
    .load()
)

print(f"rate source DataFrame type : {type(streaming_df).__name__}")
print(f"isStreaming               : {streaming_df.isStreaming}")
print(f"Schema:\n{streaming_df.printSchema()}")

```
```python
# ═══ Write the stream to a memory sink so we can query it ═══
memory_query = (
    streaming_df
    .writeStream
    .format("memory")               # memory sink → queryable via spark.sql()
    .queryName("rate_stream_q41")   # SQL table name
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print(f"✅ Streaming query started: {memory_query.name}")
print(f"   Status : {memory_query.status['message']}")
print()

# Let it accumulate for ~15 seconds
print("⏳ Accumulating data for 15 seconds...")
time.sleep(15)

# Query the in-memory table
df_mem = spark.sql("SELECT * FROM rate_stream_q41 ORDER BY timestamp DESC LIMIT 10")
print("\n📊 Latest 10 rows from memory sink:")
df_mem.show(truncate=False)

total_rows = spark.sql("SELECT COUNT(*) AS cnt FROM rate_stream_q41").collect()[0]['cnt']
print(f"\n📈 Total rows streamed: {total_rows}")

memory_query.stop()
print("⏹️  Streaming query stopped.")

```
```python
```
%md
### Concept 41 — Delta as Both Source AND Sink (Stream → Delta → Re-read)

Delta Lake is *both* a streaming sink **and** a streaming source. This enables:
- **Sink**: Stream writes continuously to a Delta table
- **Source**: Another stream reads new Delta files as they are committed

```python
# ── Write rate data to a Delta table ──
delta_sink_path = f"{DELTA_SINK_DIR}/rate_sink_q41"

dbutils.fs.rm(delta_sink_path, True)

delta_query = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/q41_delta")
    .trigger(processingTime="10 seconds")
    .start(delta_sink_path)
)

print("📤 Streaming into Delta table...")
time.sleep(20)

# Read the Delta table as a static batch
df_batch = spark.read.format("delta").load(delta_sink_path)
print(f"📊 Delta table row count: {df_batch.count()}")
df_batch.orderBy("timestamp", ascending=False).show(5, truncate=False)

# Now treat the same Delta table as a *streaming source*!
df_streaming_source = (
    spark.readStream
    .format("delta")
    .load(delta_sink_path)
)
print(f"\n🔁 Delta as streaming source — isStreaming: {df_streaming_source.isStreaming}")

delta_query.stop()
print("⏹️  Delta sink query stopped.")

```
```python
```
%md
---
## Concept 42: Triggers

**Triggers** control *how often* the streaming engine processes data.

| Trigger Type | Behavior | Best For |
|---|---|---|
| `processingTime="10 seconds"` | Micro-batch every N seconds | Low-latency, continuous |
| `availableNow=True` | Process all available data, then stop | One-time backfills, testing |
| `once=True` (deprecated) | Process one micro-batch and stop | Legacy; prefer `availableNow` |
| `continuous="10 seconds"` | True continuous mode (not in Community) | Sub-ms latency (not in CE) |

**Choosing the right trigger**:
- Low latency needed → `processingTime` with short interval
- Cost-sensitive / batch-like → longer `processingTime` or `availableNow`
- Backfill historical data → `availableNow`

```python
print("=" * 60)
print("CONCEPT 42 — Triggers")
print("=" * 60)

# ── Trigger 1: processingTime ──
print("\n📌 TRIGGER TYPE 1: processingTime='5 seconds'")
print("   Micro-batch every 5 seconds — data accumulates, then processes\n")

mem_q_processing = (
    spark.readStream.format("rate").option("rowsPerSecond", 3).load()
    .writeStream
    .format("memory")
    .queryName("trigger_processing_q42")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

time.sleep(12)
cnt1 = spark.sql("SELECT COUNT(*) AS c FROM trigger_processing_q42").collect()[0]['c']
print(f"   Rows accumulated (processingTime): {cnt1}")
mem_q_processing.stop()

# ── Trigger 2: availableNow ──
print("\n📌 TRIGGER TYPE 2: availableNow=True")
print("   Process ALL available data in one shot, then stop.\n")

# Write a batch of CSV files to simulate "available now" data
available_dir = f"{STREAM_SRC_DIR}/available_now_q42"
dbutils.fs.rm(available_dir, True)
os.makedirs(available_dir, exist_ok=True)

# Generate CSV files
schema_csv = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType()),
    StructField("ts", TimestampType())
])

for batch in range(3):
    rows = [(i, f"batch_{batch}_val_{i}",
             f"2026-05-07 {(batch * 10 + i) // 60:02d}:{(batch * 10 + i) % 60:02d}:00")
            for i in range(20)]
    df_csv = spark.createDataFrame(rows, schema_csv)
    df_csv.coalesce(1).write.mode("append").csv(available_dir)

# Read CSV as stream with availableNow trigger
schema_infer = StructType([
    StructField("id", IntegerType()),
    StructField("value", StringType()),
    StructField("ts", TimestampType())
])

df_avail = (
    spark.readStream
    .schema(schema_infer)
    .option("maxFilesPerTrigger", 1)
    .csv(available_dir)
)

avail_query = (
    df_avail
    .writeStream
    .format("memory")
    .queryName("trigger_avail_now_q42")
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

# availableNow stops automatically after processing
avail_query.awaitTermination(60)
time.sleep(2)

cnt2 = spark.sql("SELECT COUNT(*) AS c FROM trigger_avail_now_q42").collect()[0]['c']
print(f"   Rows processed (availableNow): {cnt2}  (should be ~60)")

print("\n✅ Both trigger types demonstrated.")

```
```python
```
%md
---
## Concept 43: Output Modes

**Output modes** determine *what* data is written to the sink on each trigger:

| Mode | Behavior | Allowed With |
|---|---|---|
| **Append** | Only new rows appended since last trigger | Any query without aggregation (or with watermark) |
| **Complete** | Entire result table is written every trigger | Aggregation queries only |
| **Update** | Only rows that changed since last trigger | Aggregations + watermarked operations |

**Key Rule**: Aggregations (groupBy) produce "updating" results — you CANNOT use
Append mode without a watermark. Use Complete for small state, Update for efficiency.

```python
print("=" * 60)
print("CONCEPT 43 — Output Modes")
print("=" * 60)

# ── Preview the rate data structure ──
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Add a random "category" to demonstrate grouping
import random
from pyspark.sql.types import StructType, StructField, LongType

def generate_categorized():
    """Simulate IoT sensor categories: A=temp, B=humidity, C=pressure"""
    df = spark.range(100)
    return df.select(
        col("id"),
        (col("id") % 3).cast("int").alias("category"),
        (_round(rand() * 100, 2)).alias("value"),
        current_timestamp().alias("ts")
    )

# We'll use rate source with random categories for streaming
rate_with_cat = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("category", (col("value") % 3).cast("int"))
    .withColumn("measurement", _round(rand() * 100, 2))
)

# ── MODE 1: APPEND (no aggregation) ──
print("\n📌 OUTPUT MODE 1: append")
print("   Only new rows since last trigger are output.\n")

q_append = (
    rate_with_cat
    .writeStream
    .format("memory")
    .queryName("mode_append_q43")
    .outputMode("append")
    .trigger(processingTime="6 seconds")
    .start()
)

time.sleep(12)
cnt_append = spark.sql("SELECT COUNT(*) AS c FROM mode_append_q43").collect()[0]['c']
print(f"   Rows in append sink: {cnt_append}")
spark.sql("SELECT * FROM mode_append_q43 LIMIT 5").show()
q_append.stop()

# ── MODE 2: COMPLETE (with aggregation) ──
print("\n📌 OUTPUT MODE 2: complete")
print("   ENTIRE result table is recomputed and output every trigger.\n")

agg_df = rate_with_cat.groupBy("category").agg(
    _sum("measurement").alias("total_measurement"),
    count("*").alias("row_count")
)
q_complete = (
    agg_df
    .writeStream
    .format("memory")
    .queryName("mode_complete_q43")
    .outputMode("complete")
    .trigger(processingTime="6 seconds")
    .start()
)

time.sleep(12)
print("   Complete mode result (entire table):")
spark.sql("SELECT * FROM mode_complete_q43 ORDER BY category").show()
q_complete.stop()

# ── MODE 3: UPDATE (only changed rows) ──
print("\n📌 OUTPUT MODE 3: update")
print("   Only rows that CHANGED since last trigger.\n")

q_update = (
    agg_df
    .writeStream
    .format("memory")
    .queryName("mode_update_q43")
    .outputMode("update")
    .trigger(processingTime="6 seconds")
    .start()
)

time.sleep(12)
print("   Update mode result (changed rows only):")
spark.sql("SELECT * FROM mode_update_q43 ORDER BY category").show()
q_update.stop()

print("\n✅ All three output modes demonstrated.")

```
```python
```
%md
---
## Concept 44: Stream-Static Joins

**Pattern**: Join a streaming DataFrame with a *static* (batch) DataFrame to enrich
events with reference data. The classic use case: enrich raw IoT sensor readings
with device metadata.

**Important**: The static side is re-read for **every micro-batch**. For large static
tables, broadcast the static DataFrame to avoid repeated shuffles.

**Limitations**:
- ✅ Inner join, left-outer, right-outer, cross join: Supported
- ⚠️ Full outer join: NOT supported on streaming DataFrames
- ✅ Static side can be any size (broadcast if large)

```python
print("=" * 60)
print("CONCEPT 44 — Stream-Static Joins")
print("=" * 60)

# ── Create a static "device catalog" dimension table ──
device_catalog = spark.createDataFrame([
    (0, "Temperature Sensor",   "Warehouse A",       "temp"),
    (1, "Humidity Sensor",      "Warehouse A",       "humidity"),
    (2, "Pressure Sensor",      "Warehouse B",       "pressure"),
    (3, "Vibration Sensor",     "Production Floor",   "vibration"),
    (4, "Light Sensor",         "Office",            "lux"),
    (5, "CO2 Sensor",           "Office",            "co2"),
    (6, "Flow Meter",           "Pipeline C",        "flow"),
    (7, "pH Sensor",            "Tank D",            "ph"),
    (8, "Conductivity Sensor",  "Tank D",            "conductivity"),
    (9, "Thermocouple",         "Furnace",           "temp"),
], ["sensor_id", "sensor_name", "location", "type"])

device_catalog.cache()
print("📋 Static Device Catalog:")
device_catalog.show(truncate=False)

# ── Streaming source: simulated IoT events ──
stream_events = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 8)
    .load()
    .withColumn("sensor_id", (col("value") % 10).cast("int"))
    .withColumn("reading", _round(rand() * 100, 2))
    .withColumn("event_time", current_timestamp())
)

# Perform stream-static join (inner)
enriched_stream = (
    stream_events
    .join(device_catalog, on="sensor_id", how="inner")
    .select(
        "event_time",
        "sensor_id",
        "sensor_name",
        "location",
        "type",
        "reading"
    )
)

print("\n🔗 Streaming events INNER JOIN static catalog:")
enriched_stream.printSchema()

q_enrich_inner = (
    enriched_stream
    .writeStream
    .format("memory")
    .queryName("enriched_events_q44")
    .outputMode("append")
    .trigger(processingTime="6 seconds")
    .start()
)

time.sleep(12)
print("\n📊 Enriched Events (stream + static join):")
spark.sql("SELECT * FROM enriched_events_q44 ORDER BY event_time DESC LIMIT 10").show(truncate=False)
q_enrich_inner.stop()

# ── LEFT OUTER join: keep ALL events, even with no matching device ──
print("\n🔗 LEFT OUTER join (all events, nulls for unmatched sensors):")
enriched_left = (
    stream_events
    .join(device_catalog, on="sensor_id", how="left_outer")
    .select("event_time", "sensor_id", "sensor_name", "location", "reading")
)

q_enrich_left = (
    enriched_left
    .writeStream
    .format("memory")
    .queryName("enriched_left_q44")
    .outputMode("append")
    .trigger(processingTime="6 seconds")
    .start()
)

time.sleep(12)
spark.sql("""
    SELECT sensor_id, sensor_name, COUNT(*) AS cnt
    FROM enriched_left_q44
    GROUP BY sensor_id, sensor_name
    ORDER BY sensor_id
""").show(12, truncate=False)
q_enrich_left.stop()

print("✅ Stream-static joins demonstrated (inner + left-outer).")

```
```python
```
%md
---
## Concept 45: Windowed Aggregations

**Window types**:

- **Tumbling window**: `window(event_time, "10 minutes")` — fixed, non-overlapping
  (slide = window duration → no overlap)

- **Sliding window**: `window(event_time, "10 minutes", "5 minutes")` — overlapping windows
  (slide < window duration → overlap)

**Without a watermark**, Spark keeps ALL state forever (unbounded memory).
For production, always pair windows with a watermark (Concept #49).

```python
print("=" * 60)
print("CONCEPT 45 — Windowed Aggregations")
print("=" * 60)

# ── Source: simulated stock trade events ──
# We'll use rate source + transformations to simulate trades
trade_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("symbol", concat(lit("STK-"), (col("value") % 5 + 1).cast("string")))
    .withColumn("price", (_round(rand() * 1000 + 50, 2)).cast(DoubleType()))
    .withColumn("quantity", ((col("value") % 10 + 1)).cast(IntegerType()))
    .withColumn("trade_time", current_timestamp())
)

# ── TUMBLING WINDOW (10-second windows, no overlap) ──
print("\n📌 TUMBLING WINDOW (window = 10 sec, slide = N/A → no overlap):")

tumbling_agg = (
    trade_stream
    .groupBy(
        col("symbol"),
        window(col("trade_time"), "10 seconds")  # tumbling: slide == duration
    )
    .agg(
        count("*").alias("trade_count"),
        _round(_sum("price"), 2).alias("total_value"),
        _round(avg("price"), 2).alias("avg_price")
    )
)

q_tumble = (
    tumbling_agg
    .writeStream
    .format("memory")
    .queryName("window_tumble_q45")
    .outputMode("complete")  # complete for un-watermarked aggregation
    .trigger(processingTime="10 seconds")
    .start()
)

time.sleep(22)
print("   Tumbling window aggregation (10-second windows):")
spark.sql("""
    SELECT symbol, window, trade_count, total_value, avg_price
    FROM window_tumble_q45
    ORDER BY window.start, symbol
    LIMIT 15
""").show(15, truncate=False)
q_tumble.stop()

# ── SLIDING WINDOW (10-second window, 5-second slide = overlap) ──
print("\n📌 SLIDING WINDOW (window = 10 sec, slide = 5 sec → overlapping):")

sliding_agg = (
    trade_stream
    .groupBy(
        col("symbol"),
        window(col("trade_time"), "10 seconds", "5 seconds")
    )
    .agg(
        count("*").alias("trade_count"),
        _round(avg("price"), 2).alias("avg_price")
    )
)

q_slide = (
    sliding_agg
    .writeStream
    .format("memory")
    .queryName("window_slide_q45")
    .outputMode("complete")
    .trigger(processingTime="10 seconds")
    .start()
)

time.sleep(22)
print("   Sliding window aggregation (10s windows, 5s slide):")
spark.sql("""
    SELECT symbol, window, trade_count, avg_price
    FROM window_slide_q45
    ORDER BY window.start, window.end, symbol
    LIMIT 20
""").show(20, truncate=False)
q_slide.stop()

print("\n✅ Tumbling and sliding windows demonstrated.")

```
```python
```
%md
---
## Concept 46: foreachBatch Pattern

`foreachBatch` lets you apply **arbitrary batch logic** to each micro-batch of a
streaming query. This is the most flexible sink — you can:

- Write to **multiple sinks** from a single streaming query
- Perform **MERGE (upsert)** operations in Delta
- Apply custom transformations
- Call external services (APIs) per micro-batch

⚠️ **Exactly-once warning**: If you write to external systems from foreachBatch,
you must ensure your logic is **idempotent** (Spark may replay micro-batches).

```python
print("=" * 60)
print("CONCEPT 46 — foreachBatch Pattern")
print("=" * 60)

# Prepare Delta sink for MERGE
delta_merge_path = f"{DELTA_SINK_DIR}/foreachBatch_merge_q46"
dbutils.fs.rm(delta_merge_path, True)

# Create an empty Delta table with schema
merge_schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("latest_reading", DoubleType()),
    StructField("reading_count", LongType()),
    StructField("last_updated", TimestampType())
])
empty_df = spark.createDataFrame([], merge_schema)
empty_df.write.format("delta").mode("overwrite").save(delta_merge_path)
print(f"📁 Empty Delta table created at: {delta_merge_path}")

# ── foreachBatch: write to BOTH a memory sink AND a Delta table with MERGE ──
source_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("sensor_id", (col("value") % 5).cast("int"))
    .withColumn("reading", _round(rand() * 100, 2))
)

def process_micro_batch(df_batch, epoch_id):
    """
    Called for each micro-batch.
    1. Aggregate the micro-batch
    2. Merge into Delta table (upsert by sensor_id)
    3. Print a summary
    """
    print(f"\n🔹 foreachBatch — epoch_id={epoch_id}, rows in batch={df_batch.count()}")

    # Aggregate this micro-batch
    agg_batch = (
        df_batch
        .groupBy("sensor_id")
        .agg(
            _round(avg("reading"), 2).alias("avg_reading_batch"),
            count("*").alias("batch_count")
        )
        .withColumn("microbatch_epoch", lit(epoch_id))
        .withColumn("processed_time", current_timestamp())
    )

    # Merge into Delta: upsert per sensor_id
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, delta_merge_path)

    (delta_table.alias("target")
     .merge(agg_batch.alias("source"),
            "target.sensor_id = source.sensor_id")
     .whenMatchedUpdate(set={
         "latest_reading": "source.avg_reading_batch",
         "reading_count": "target.reading_count + source.batch_count",
         "last_updated": "source.processed_time"
     })
     .whenNotMatchedInsert(values={
         "sensor_id": "source.sensor_id",
         "latest_reading": "source.avg_reading_batch",
         "reading_count": "source.batch_count",
         "last_updated": "source.processed_time"
     })
     .execute())

    print(f"   ✅ Merged {agg_batch.count()} sensor aggregates into Delta")

q_foreach = (
    source_stream
    .writeStream
    .foreachBatch(process_micro_batch)
    .outputMode("append")
    .trigger(processingTime="8 seconds")
    .start()
)

time.sleep(30)
q_foreach.stop()

# ── Show the merged Delta table ──
print("\n📊 Final Delta table (upserted by sensor_id):")
spark.read.format("delta").load(delta_merge_path).show(truncate=False)

print("\n✅ foreachBatch pattern with Delta MERGE demonstrated.")

```
```python
```
%md

### foreachBatch — Multiple Sinks from a Single Stream

One of the most powerful patterns: split a single streaming query into multiple
outputs (e.g., raw events + aggregated metrics).

```python
print("\n─── foreachBatch: Multiple Sinks ───")

# Clean up dual-sink directories
for dp in [DUAL_SINK_1, DUAL_SINK_2]:
    dbutils.fs.rm(dp, True)

events_schema = StructType([
    StructField("event_id", LongType()),
    StructField("user_id", IntegerType()),
    StructField("action", StringType()),
    StructField("page", StringType()),
    StructField("event_time", TimestampType()),
])

clickstream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 8)
    .load()
    .withColumn("user_id", (col("value") % 20).cast("int"))
    .withColumn("action",
                when((col("value") % 4) == 0, "click")
                .when((col("value") % 4) == 1, "view")
                .when((col("value") % 4) == 2, "add_to_cart")
                .otherwise("purchase"))
    .withColumn("page", concat(lit("/page/"), (col("value") % 7 + 1).cast("string")))
    .withColumn("event_time", current_timestamp())
    .select("value", "user_id", "action", "page", "event_time")
    .withColumnRenamed("value", "event_id")
)

def write_to_multiple_sinks(df_batch, epoch_id):
    print(f"\n🔹 Multi-sink epoch={epoch_id}, rows={df_batch.count()}")

    # Sink 1: raw events to Delta
    df_batch.write.format("delta").mode("append").save(DUAL_SINK_1)

    # Sink 2: per-user summary to Delta
    summary = (
        df_batch
        .groupBy("user_id")
        .agg(
            count("*").alias("event_count"),
            count(when(col("action") == "purchase", 1)).alias("purchases"),
            collect_list("action").alias("action_list")
        )
        .withColumn("epoch_id", lit(epoch_id))
    )
    summary.write.format("delta").mode("append").save(DUAL_SINK_2)

    print(f"   ✅ Wrote to both sinks")

q_multi = (
    clickstream
    .writeStream
    .foreachBatch(write_to_multiple_sinks)
    .outputMode("append")
    .trigger(processingTime="6 seconds")
    .start()
)

time.sleep(15)
q_multi.stop()

print("\n📊 Sink 1 — Raw events:")
spark.read.format("delta").load(DUAL_SINK_1).show(10, truncate=False)

print("\n📊 Sink 2 — Per-user summaries:")
spark.read.format("delta").load(DUAL_SINK_2).show(10, truncate=False)

print("\n✅ Multiple sinks from a single stream demo complete.")

```
```python
```
%md
---
## Concept 47: Streaming Tables in Lakeflow Pipelines (DLT)

⚠️ **NOTE**: Delta Live Tables (DLT / Lakeflow Pipelines) are **NOT available** in
Community Edition. This section explains the concepts and shows manual equivalents.

**What are Streaming Tables?**
- **Managed streaming ingestion** in DLT — you declare a table "as streaming" using
  the `STREAMING` keyword (or `readStream` in Python DLT decorators)
- DLT auto-manages **checkpoints**, **schema evolution**, and **error handling**
- Streaming tables are always "append-only" — they append rows from the source

**DLT Streaming Table (Python — NOT runnable in CE)**:
```python
import dlt
@dlt.table(name="bronze_events")
def bronze():
    return spark.readStream.format("cloudFiles").load(source_path)
```

**vs. Manual Structured Streaming**:
| Feature | DLT Streaming Tables | Manual Structured Streaming |
|---|---|---|
| Checkpoint mgmt | Auto | Manual (must specify path) |
| Schema enforcement | Built-in expectations | Manual validation |
| Error handling | Dead letter queue | Manual try/except or skip |
| Monitoring | DLT event log | `spark.streams.active`, metrics |
| Restartability | Full auto-recovery | Checkpoint-dependent |

Below is the **manual equivalent** of a streaming table:

```python
print("=" * 60)
print("CONCEPT 47 — Streaming Tables (Manual Equivalent)")
print("=" * 60)

delta_streaming_table_path = f"{DELTA_SINK_DIR}/manual_streaming_table_q47"
dbutils.fs.rm(delta_streaming_table_path, True)

# ── Manual "streaming table": readStream → writeStream to Delta ──
streaming_source = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("processed_at", current_timestamp())
    .withColumn("batch_id", col("timestamp"))
)

manual_streaming_table = (
    streaming_source
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/manual_streaming_q47")
    .trigger(processingTime="5 seconds")
    .start(delta_streaming_table_path)
)

print("📤 Manual streaming table writing to Delta...")
time.sleep(15)

# Read the "streaming table" as a batch
df_streaming_table = spark.read.format("delta").load(delta_streaming_table_path)
print(f"\n📊 Manual streaming table row count: {df_streaming_table.count()}")
df_streaming_table.orderBy(col("timestamp").desc()).show(5, truncate=False)

manual_streaming_table.stop()

# ── What DLT handles for you in production ──
print("""
╔═══════════════════════════════════════════════════════════════════╗
║  WHAT DLT / LAKEFLOW PROVIDES (not in Community Edition):         ║
║  • Auto-managed checkpoints (no need to specify paths)            ║
║  • Schema enforcement & evolution                                 ║
║  • Data quality constraints (expectations)                        ║
║  • Error isolation (bad records → dead letter queue)              ║
║  • Auto-scaling compute                                           ║
║  • Lineage tracking in Unity Catalog                             ║
║  • Incremental materialized views (change-data capture)           ║
╚═══════════════════════════════════════════════════════════════════╝
""")
print("✅ Concept 47 — DLT streaming tables explained.")

```
```python
```
%md
---
## Concept 48: Checkpointing & Exactly-Once Guarantees

**Checkpointing** is the mechanism that enables fault-tolerance and exactly-once
semantics in Structured Streaming.

**What's in a checkpoint directory?**
- `offsets/` — the position in the source stream (which data has been consumed)
- `commits/` — which micro-batches completed successfully
- `metadata` — streaming query identity
- `state/` — aggregation state (for stateful operations with watermark)

**Exactly-once boundaries**:
- ✅ **Source**: replayable sources (Kafka, Delta, Kinesis) → exactly-once
- ✅ **Sink**: idempotent sinks (Delta, file with `_spark_metadata`) → exactly-once
- ⚠️ **External systems** (foreachBatch → REST APIs) → at-least-once (must be idempotent)

**Golden rule**: Each streaming query must have its own **unique, stable checkpoint location**.

```python
print("=" * 60)
print("CONCEPT 48 — Checkpointing & Exactly-Once")
print("=" * 60)

checkpoint_q48 = f"{CHECKPOINT_DIR}/checkpoint_demo_q48"
dbutils.fs.rm(checkpoint_q48, True)

# ── Run a stream with a checkpoint ──
q_with_checkpoint = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("processed", current_timestamp())
    .writeStream
    .format("memory")
    .queryName("checkpointed_stream_q48")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_q48)
    .trigger(processingTime="5 seconds")
    .start()
)

print("📤 Streaming with checkpoint...")
time.sleep(15)
rows_before_restart = spark.sql("SELECT COUNT(*) AS c FROM checkpointed_stream_q48").collect()[0]['c']
print(f"   Rows before simulated restart: {rows_before_restart}")

# Stop the query (simulates failure / restart)
q_with_checkpoint.stop()
print("⏹️  Stream stopped (simulating failure).")

# ── Inspect checkpoint directory ──
print(f"\n📁 Checkpoint directory contents ({checkpoint_q48}):")
for root, dirs, files in os.walk(checkpoint_q48):
    level = root.replace(checkpoint_q48, "").count(os.sep)
    indent = "  " * level
    print(f"  {indent}{os.path.basename(root) or '.'}/")
    for f in files[:5]:
        print(f"  {indent}  📄 {f}")

# ── Restart the stream with the SAME checkpoint ──
# Note: memory sink query name must be unique, so we use a new name but same checkpoint
q_restarted = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("processed", current_timestamp())
    .writeStream
    .format("memory")
    .queryName("checkpointed_stream_q48_restarted")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_q48)
    .trigger(processingTime="5 seconds")
    .start()
)

print("\n🔄 Stream restarted with SAME checkpoint...")
time.sleep(15)
rows_after = spark.sql("SELECT COUNT(*) AS c FROM checkpointed_stream_q48_restarted").collect()[0]['c']
print(f"   Rows in restarted stream: {rows_after}")
print(f"   ⚠️  NOTE: With a NEW memory sink name, we start fresh.")
print(f"   In production (Kafka/Delta), checkpoint ensures NO data loss/duplication across restarts.")
q_restarted.stop()

# ── What happens if you DELETE the checkpoint? ──
dbutils.fs.rm(checkpoint_q48, True)
print(f"\n🗑️  Checkpoint deleted: {checkpoint_q48}")
print("   If you restart without the checkpoint:")
print("   • All offsets are lost → stream re-reads from start (DUPLICATE DATA)")
print("   • All state is lost → aggregations start from scratch")
print("   • NEVER delete a production checkpoint unless you understand the consequences!")

print("\n✅ Checkpointing & exactly-once semantics demonstrated.")

```
```python
```
%md
---
## Concept 49: Watermarks & Late Data

**Problem**: With streaming aggregations, intermediate state grows unboundedly.
Spark must remember ALL seen keys FOREVER — memory leak!

**Solution**: `withWatermark()` — tells Spark "wait up to N seconds for late data,
then drop old state."

**Tradeoff**:
- **Shorter watermark** = less state, lower memory, BUT more data labeled "too late"
- **Longer watermark** = more completeness, BUT more memory and latency

**How it works**:
- Spark tracks the maximum event-time seen so far
- Watermark = max_event_time - watermark_duration
- Any window with end < watermark is DROPPED (state cleaned up)
- New data with timestamp < watermark is considered "too late" and DROPPED

```python
print("=" * 60)
print("CONCEPT 49 — Watermarks & Late Data")
print("=" * 60)

# ── Create a stream with explicit timestamps for watermark demo ──
# Simulate events with timestamps that are explicitly set (not current_timestamp)
# This lets us demonstrate late-arriving data clearly

from pyspark.sql.functions import expr as spark_expr

watermark_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .withColumn("event_id", col("value"))
    .withColumn("sensor", concat(lit("sensor_"), (col("value") % 3).cast("string")))
    .withColumn("reading", _round(rand() * 100 + 20, 2))
    # Simulate event time: 50% of events are "on time", 50% are delayed by 0-90 sec
    .withColumn("delay_seconds", (col("value") % 90).cast("int"))
    .withColumn("event_time",
                spark_expr("timestamp - INTERVAL delay_seconds SECONDS"))
)

# ── Aggregation WITH watermark ──
print("\n📌 WATERMARK = 30 seconds")
print("   Events more than 30 seconds late are dropped.")
print("   State for windows > 30 sec old is cleared.\n")

windowed_with_watermark = (
    watermark_stream
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        col("sensor"),
        window(col("event_time"), "30 seconds", "10 seconds")
    )
    .agg(
        count("*").alias("event_count"),
        _round(avg("reading"), 2).alias("avg_reading")
    )
)

q_watermark = (
    windowed_with_watermark
    .writeStream
    .format("memory")
    .queryName("watermarked_q49")
    .outputMode("update")  # MUST be "update" or "append" with watermark
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/watermark_q49")
    .trigger(processingTime="10 seconds")
    .start()
)

time.sleep(30)
print("   Results with watermark (update mode):")
spark.sql("""
    SELECT sensor, window, event_count, avg_reading
    FROM watermarked_q49
    ORDER BY window.start DESC, sensor
    LIMIT 15
""").show(15, truncate=False)
q_watermark.stop()

# ── CONTRAST: Aggregation WITHOUT watermark ──
print("\n📌 WITHOUT WATERMARK (unbounded state — memory grows forever):")
print("   (Using complete mode since no watermark → append not allowed)")

windowed_no_watermark = (
    watermark_stream
    .groupBy(
        col("sensor"),
        window(col("event_time"), "30 seconds", "10 seconds")
    )
    .agg(
        count("*").alias("event_count"),
        _round(avg("reading"), 2).alias("avg_reading")
    )
)

q_no_watermark = (
    windowed_no_watermark
    .writeStream
    .format("memory")
    .queryName("no_watermark_q49")
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/no_watermark_q49")
    .trigger(processingTime="10 seconds")
    .start()
)

time.sleep(20)
print("   Results WITHOUT watermark (complete mode — state NOT cleaned up):")
spark.sql("""
    SELECT sensor, window, event_count, avg_reading
    FROM no_watermark_q49
    ORDER BY window.start DESC, sensor
    LIMIT 15
""").show(15, truncate=False)
q_no_watermark.stop()

print("""
╔═══════════════════════════════════════════════════════════════════╗
║  WATERMARK SUMMARY:                                               ║
║  • With watermark=30s → state cleaned after ~30s from max event   ║
║  • Without watermark  → state grows FOREVER (memory leak)         ║
║  • Update mode requires watermark for aggregation                 ║
║  • Append mode requires watermark for aggregation                 ║
║  • Complete mode doesn't need watermark (but recomputes EVERYTHING)║
╚═══════════════════════════════════════════════════════════════════╝
""")
print("✅ Watermarks demonstrated — state management vs completeness tradeoff.")

```
```python
```
%md
---
## Concept 50: Stream-Stream Joins

**The hardest streaming problem**: Joining *two* unbounded streams.

**Requirements for stream-stream joins**:
1. **Watermarks on BOTH streams** — Spark must know when to stop waiting for late rows
2. **Time constraint in join condition** — "join within N seconds of each other"
3. **State management** — Spark keeps rows in buffer for watermark duration on both sides

**Join types supported**: Inner, left-outer, right-outer (all require watermarks)

**How it works**: Spark buffers rows from each stream. For each new row, it looks
up the *other* stream's buffer. When watermark advances past a row's timestamp,
the row is evicted from state (prevents infinite memory growth).

```python
print("=" * 60)
print("CONCEPT 50 — Stream-Stream Joins")
print("=" * 60)

# ── Two simulated streams: "impressions" and "clicks" ──
# Stream 1: Ad impressions
impressions = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 15)
    .load()
    .withColumn("ad_id", (col("value") % 5).cast("string"))
    .withColumn("user_id", (col("value") % 10).cast("int"))
    .withColumn("impression_time", current_timestamp())
    .selectExpr(
        "ad_id",
        "user_id",
        "impression_time",
        "CAST(CONCAT('imp_', CAST(value AS STRING)) AS STRING) AS impression_id"
    )
)

# Stream 2: Ad clicks (slightly different rate to create realistic data)
clicks = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("ad_id", (col("value") % 5).cast("string"))
    .withColumn("user_id", (col("value") % 12).cast("int"))
    .withColumn("click_time", current_timestamp())
    .selectExpr(
        "ad_id",
        "user_id",
        "click_time",
        "CAST(CONCAT('click_', CAST(value AS STRING)) AS STRING) AS click_id"
    )
)

# ── Stream-stream INNER JOIN with watermarks on both sides ──
impressions_watermarked = impressions.withWatermark("impression_time", "2 minutes")
clicks_watermarked = clicks.withWatermark("click_time", "2 minutes")

# Join condition: same ad_id AND same user_id AND click within 1 minute of impression
stream_join_result = (
    impressions_watermarked.alias("imp")
    .join(
        clicks_watermarked.alias("clk"),
        expr("""
            imp.ad_id = clk.ad_id
            AND imp.user_id = clk.user_id
            AND clk.click_time >= imp.impression_time
            AND clk.click_time <= imp.impression_time + INTERVAL 1 MINUTE
        """),
        "inner"
    )
    .select(
        col("imp.ad_id"),
        col("imp.user_id"),
        col("imp.impression_time"),
        col("clk.click_time"),
        col("imp.impression_id"),
        col("clk.click_id")
    )
)

print("📌 Stream-Stream INNER JOIN with watermarks on both streams")
print("   Condition: same ad_id + same user_id + click within 60s of impression\n")

q_ss_join = (
    stream_join_result
    .writeStream
    .format("memory")
    .queryName("stream_stream_join_q50")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/stream_join_q50")
    .trigger(processingTime="10 seconds")
    .start()
)

time.sleep(25)
print("   Stream-stream join results:")
try:
    result = spark.sql("""
        SELECT ad_id, user_id, impression_time, click_time, impression_id, click_id
        FROM stream_stream_join_q50
        ORDER BY impression_time DESC
        LIMIT 15
    """)
    if result.count() > 0:
        result.show(15, truncate=False)
    else:
        print("   ⚠️  No joins found in this short window (try running longer).")
        print("   This is expected — join conditions are restrictive with small sample.")
except Exception as e:
    print(f"   ⚠️  No results yet: {str(e)[:100]}")

q_ss_join.stop()

# ── LEFT OUTER stream-stream join (less restrictive, shows unmatched rows) ──
print("\n📌 Stream-Stream LEFT OUTER JOIN (impressions with optional clicks):")

stream_join_left = (
    impressions_watermarked.alias("imp")
    .join(
        clicks_watermarked.alias("clk"),
        expr("""
            imp.ad_id = clk.ad_id
            AND imp.user_id = clk.user_id
            AND clk.click_time >= imp.impression_time
            AND clk.click_time <= imp.impression_time + INTERVAL 1 MINUTE
        """),
        "left_outer"
    )
    .select(
        col("imp.ad_id"),
        col("imp.user_id"),
        col("imp.impression_time"),
        col("imp.impression_id"),
        col("clk.click_id")
    )
)

q_ss_left = (
    stream_join_left
    .writeStream
    .format("memory")
    .queryName("stream_stream_left_q50")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/stream_join_left_q50")
    .trigger(processingTime="10 seconds")
    .start()
)

time.sleep(20)
print("   Left outer join results (impressions with or without clicks):")
try:
    result_left = spark.sql("""
        SELECT ad_id, user_id, impression_time, impression_id, click_id
        FROM stream_stream_left_q50
        ORDER BY impression_time DESC
        LIMIT 15
    """)
    if result_left.count() > 0:
        result_left.show(15, truncate=False)
    else:
        print("   ⚠️  No results in this short window.")
except Exception as e:
    print(f"   ⚠️  {str(e)[:100]}")

q_ss_left.stop()

print("""
╔═══════════════════════════════════════════════════════════════════╗
║  STREAM-STREAM JOIN SUMMARY:                                      ║
║  • Both streams MUST have watermarks                              ║
║  • Join condition MUST include time constraint                    ║
║  • Spark buffers rows up to watermark duration on BOTH sides      ║
║  • Memory = both-side-state ≈ rows_per_sec × watermark_duration   ║
║  • Inner, left-outer, right-outer: supported                      ║
║  • Full outer: NOT supported (unbounded null padding)             ║
║  • Cross join: NOT supported on streams                           ║
╚═══════════════════════════════════════════════════════════════════╝
""")
print("✅ Stream-stream joins demonstrated.")

```
```python
```
%md
---
## 📊 Summary — Concepts #41–#50

| # | Concept | Key Takeaway |
|---|---------|---------------|
| 41 | Structured Streaming Fundamentals | Stream = unbounded table; `readStream` / `writeStream` |
| 42 | Triggers | `processingTime` for latency, `availableNow` for batch-like |
| 43 | Output Modes | Append (new rows), Complete (full result), Update (changed rows) |
| 44 | Stream-Static Joins | Enrich streams with batch reference data (inner/left/right) |
| 45 | Windowed Aggregations | Tumbling (fixed) & sliding (overlapping) windows |
| 46 | foreachBatch | Arbitrary batch logic per micro-batch; MERGE, multi-sink |
| 47 | DLT Streaming Tables | Managed streaming tables (not in CE — explained conceptually) |
| 48 | Checkpointing | Fault tolerance via offsets/commits; NEVER delete checkpoints |
| 49 | Watermarks | Bound state growth: longer = more complete, more memory |
| 50 | Stream-Stream Joins | Both streams need watermarks + time constraint |

**Community Edition Limitations Encountered**:
- ✅ `rate` source works perfectly for synthetic streaming data
- ✅ `memory` sink is ideal for demos and quick inspection
- ✅ Delta as sink & source works on CE
- ✅ Checkpointing works within a single session
- ❌ Cross-session checkpoint recovery may not work (single node, no persistent state)
- ❌ DLT/Lakeflow pipelines are NOT available
- ❌ Kafka source/sink requires external Kafka cluster (not included)

**Next Steps**: Proceed to `06_Advanced_Pipelines.py` for medallion architecture
and advanced Delta operations.

```python
```
%md
---
## 📝 Self-Assessment — Concepts #41–#50

Answer these questions to check your understanding:

1. **How does Structured Streaming differ from batch processing?**

2. **When would you use `availableNow` trigger instead of `processingTime`?**

3. **Why can't you use Append mode with aggregation queries (without watermark)?**

4. **What happens to the static side of a stream-static join on each micro-batch?**

5. **What's the difference between a tumbling window and a sliding window?**

6. **What is foreachBatch used for? Give two practical examples.**

7. **What does DLT provide that raw Structured Streaming does not?**

8. **What's stored in the checkpoint directory, and why must it be unique per query?**

9. **What is the tradeoff of a longer watermark duration?**

10. **What two things are required for stream-stream joins that are not needed for stream-static joins?**

%md
### ✅ CONGRATULATIONS!

You've completed the Structured Streaming section (Concepts #41–#50). You now understand:

- The core streaming API (readStream/writeStream)
- Controlling micro-batch timing with triggers
- Choosing the right output mode for your query
- Enriching streaming data with static reference tables
- Computing windowed aggregations (tumbling & sliding)
- Using `foreachBatch` for advanced custom logic and multi-sink patterns
- The value of DLT streaming tables in managed environments
- Ensuring exactly-once delivery through checkpointing
- Managing state growth with watermarks
- Joining two live streams with time-constrained conditions

Continue to `06_Advanced_Pipelines.py` for the medallion architecture and advanced Delta operations!
