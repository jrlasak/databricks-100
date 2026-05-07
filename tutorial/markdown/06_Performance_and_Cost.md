%md
# 06 — Performance & Cost Optimization

**Concepts Covered:** #51–#60  
**Environment:** Databricks Community Edition (single-node, free)  
**Goal:** Master performance tuning and cost optimization techniques on Databricks.

| # | Concept | Difficulty | Type |
|---|---------|------------|------|
| 51 | Serverless Compute Economics | Easy | Conceptual |
| 52 | Predicate Pushdown & Data Skipping | Medium | Hands-on |
| 53 | Predictive Optimization | Medium | Conceptual + Hands-on |
| 54 | Column Statistics & File Pruning | Medium | Hands-on |
| 55 | Write Optimization | Medium | Hands-on |
| 56 | Cluster Sizing & Autoscaling | Medium | Conceptual |
| 57 | DBU Cost Model & Billing | Medium | Conceptual + Hands-on |
| 58 | Query Profile & Execution Plans | Medium | Hands-on |
| 59 | Join Performance Diagnosis | Hard | Hands-on |
| 60 | MERGE Write Amplification | Hard | Hands-on |

```python

```
%md
## Concept 51 — Serverless Compute Economics

**NOTE:** Serverless compute is **not available** in Databricks Community Edition. This section is conceptual.

### What is Serverless Compute?

Serverless compute removes the need to manage clusters directly. You submit workloads, and Databricks provisions compute resources automatically in your account. You pay only for the resources consumed by your workload.

### DBU-Based Pricing Model

Databricks pricing is based on **DBUs** (Databricks Units) — a normalized unit of processing capability per hour.

| Compute Type | DBU/hr (approx.) | Billing Granularity | Best For |
|---|---|---|---|
| **Serverless SQL** | 0.70–1.10 | Per-second, 1-min minimum | BI dashboards, ad-hoc SQL |
| **Serverless Jobs** | 0.55–0.75 | Per-second, 1-min minimum | Notebook/scheduled jobs |
| **Classic SQL Warehouse** | 0.40–0.55 | Per-second, 10-min minimum | Steady-state SQL workloads |
| **Classic All-Purpose** | 0.40–0.55 | Per-second, 10-min minimum | Development, exploration |
| **Classic Jobs** | 0.30–0.40 | Per-second, 10-min minimum | Scheduled production pipelines |

*Exact pricing varies by tier (Standard/Premium/Enterprise) and cloud provider.*

```python

```
%md
### Serverless vs Classic: When Each Wins

**Serverless is cheaper when:**
- Workloads are **bursty** (intermittent, unpredictable)
- Many short-running queries or jobs
- You want zero cluster management overhead
- Start-up latency is important (serverless starts faster)
- Idle time between jobs is significant

**Classic is cheaper when:**
- Workloads are **sustained** (24/7 processing)
- Jobs run continuously with little idle time
- You can right-size clusters precisely
- Using spot instances for deep discounts (30–50% off)
- Predictable, long-running ETL pipelines

### Elimination of Idle Cluster Waste

With classic clusters:
- Clusters are always on (costing DBU/hr) even when idle
- Autoscaling helps but cannot go to zero
- Manual start/stop introduces latency

With serverless:
- *No idle cost* — you pay only for active processing
- Compute scales down to zero between queries
- No cluster management overhead

```python

```
%md
### Conceptual Pricing Comparison

```python

# Conceptual pricing comparison table
pricing_data = [
    ("Small SQL Query (30s, 50/day)", "Serverless SQL", "0.29", "$0.29"),
    ("Small SQL Query (30s, 50/day)", "Classic SQL", "1.67", "$1.67 (idle time)"),
    ("ETL Job (2hr, nightly)", "Serverless Jobs", "1.20", "$1.20"),
    ("ETL Job (2hr, nightly)", "Classic Jobs", "0.70", "$0.70 (spot)"),
    ("Ad-hoc Dev (4hr, varied)", "Serverless All-Purpose", "2.80", "$2.80"),
    ("Ad-hoc Dev (4hr, varied)", "Classic All-Purpose", "3.20", "$3.20 (idle 2hr)"),
    ("24/7 Streaming", "Classic Jobs", "216.00", "$216/mo (spot)"),
    ("24/7 Streaming", "Serverless", "360.00", "$360/mo"),
]

import pandas as pd
pricing_df = pd.DataFrame(pricing_data, columns=["Scenario", "Compute", "Daily Cost", "Notes"])
print("CONCEPTUAL PRICING COMPARISON\n")
print("(Illustrative — verify at https://databricks.com/product/pricing)\n")
display(pricing_df)

```
```python

```
%md
### Key Takeaways — Serverless

| Factor | Serverless | Classic |
|---|---|---|
| Start-up time | 5–15 seconds | 3–7 minutes |
| Billing minimum | 1 minute | 10 minutes |
| Idle cost | $0 | DBU/hr × idle time |
| Spot instances | Not available | Available (30–50% less) |
| Cluster management | None | Required |
| Max scale | Elastic | Defined by config |
| Photon support | Yes | Yes |


```python

```
%md
## Concept 52 — Predicate Pushdown & Data Skipping

**Available in Community Edition — full hands-on.**

### Theory

**Predicate pushdown** means filters are pushed down to the storage layer, so only relevant data is read from disk.  
**Data skipping** uses Delta Lake per-file statistics (min/max values) to skip entire files that can't match the filter.

**What enables skipping:**
- Partition filters (direct path elimination)
- `ZORDER` clustering on filter columns
- Delta stats (min/max per file for first 32 columns)

**What BREAKS pushdown:**
- Applying functions to filter columns (`YEAR(date_col) = 2023` instead of `date_col BETWEEN`)
- Casting (`CAST(col AS STRING) = 'value'`)
- Complex expressions that can't be evaluated at the file level

```python

```
%md
### Create Test Table with Many Files

```python

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, rand, round as spark_round, lit
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, DoubleType

spark = SparkSession.builder.appName("PerfTest").getOrCreate()

NUM_ROWS = 1_000_000
NUM_FILES = 20

schema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("transaction_date", TimestampType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("category", StringType()),
    StructField("region", StringType()),
    StructField("payment_method", StringType()),
])

print(f"Generating {NUM_ROWS:,} synthetic rows...")
t0 = time.time()

df = spark.range(0, NUM_ROWS).select(
    col("id").alias("transaction_id"),
    (lit("2023-01-01").cast("timestamp") + (col("id") * 347)).cast("timestamp").alias("transaction_date"),
    (col("id") % 50000 + 1).alias("customer_id"),
    (col("id") % 1000 + 1).alias("product_id"),
    (col("id") % 100 + 1).alias("store_id"),
    (rand(42) * 500).alias("amount"),
    ((rand(43) * 10 + 1).cast("int")).alias("quantity"),
)

categories = ['Electronics', 'Clothing', 'Groceries', 'Home', 'Sports']
regions = ['North', 'South', 'East', 'West', 'Central']
payments = ['Credit', 'Debit', 'Cash', 'Digital']

df = df.withColumn("category", col("product_id") % 5)
df = df.withColumn("category",
    when(col("category") == 0, "Electronics")
    .when(col("category") == 1, "Clothing")
    .when(col("category") == 2, "Groceries")
    .when(col("category") == 3, "Home")
    .otherwise("Sports"))
df = df.withColumn("region", col("store_id") % 5)
df = df.withColumn("region",
    when(col("region") == 0, "North")
    .when(col("region") == 1, "South")
    .when(col("region") == 2, "East")
    .when(col("region") == 3, "West")
    .otherwise("Central"))
df = df.withColumn("payment_method", col("customer_id") % 4)
df = df.withColumn("payment_method",
    when(col("payment_method") == 0, "Credit")
    .when(col("payment_method") == 1, "Debit")
    .when(col("payment_method") == 2, "Cash")
    .otherwise("Digital"))

df = df.withColumn("year", year(col("transaction_date")))
df = df.withColumn("month", month(col("transaction_date")))

print(f"Generated {NUM_ROWS:,} rows in {time.time()-t0:.1f}s")
df.printSchema()

```
```python

# Write with many files (partition by region to see partitioning effects)
table_path = "/tmp/delta/perf_demo/transactions"
print(f"Writing {NUM_FILES} files to {table_path}...")
t0 = time.time()

df.repartition(NUM_FILES).write \
    .mode("overwrite") \
    .option("maxRecordsPerFile", 100000) \
    .partitionBy("region") \
    .save(table_path)

print(f"Write completed in {time.time()-t0:.1f}s")

# Verify files
files = dbutils.fs.ls(table_path)
print(f"\nTotal files in Delta table: {len([f for f in files if f.name.endswith('.parquet')])} (not counting _delta_log)")

```
```python

```
%md
### Demonstrate Predicate Pushdown with EXPLAIN

```python

transactions = spark.read.format("delta").load(table_path)

print("=" * 80)
print("QUERY 1: Filter on partition column (region = 'North')")
print("=" * 80)
transactions.filter(col("region") == "North").explain(extended=True)

```
```python

print("=" * 80)
print("QUERY 2: Filter on non-partition column (amount > 400)")
print("=" * 80)
transactions.filter(col("amount") > 400).explain(extended=True)

```
```python

print("=" * 80)
print("QUERY 3: Filter with function that BREAKS pushdown YEAR(txn_date) = 2023")
print("=" * 80)
transactions.filter(year(col("transaction_date")) == 2023).explain(extended=True)

```
```python

print("=" * 80)
print("QUERY 4: Range filter that ENABLES pushdown")
print("=" * 80)
transactions.filter(
    (col("transaction_date") >= "2023-06-01") &
    (col("transaction_date") < "2023-09-01")
).explain(extended=True)

```
```python

```
%md
### Timing Comparison: Good vs Bad Predicates

```python

from pyspark.sql.functions import when, lit

# Force materialization — first run to warm up Delta cache
transactions.count()

print("\n" + "=" * 80)
print("TIMING EXPERIMENT")
print("=" * 80)

# Good predicate — partition filter + range filter
t0 = time.time()
result1 = transactions.filter(col("region") == "North") \
    .filter(col("amount") > 400) \
    .count()
t1 = time.time() - t0

# Bad predicate — function on filter column
t0 = time.time()
result2 = transactions.filter(year("transaction_date") == 2023) \
    .filter(col("amount") > 400) \
    .count()
t2 = time.time() - t0

# Good predicate — partition filter + direct date comparison
t0 = time.time()
result3 = transactions.filter(col("region") == "North") \
    .filter(col("transaction_date") >= "2023-06-01") \
    .filter(col("transaction_date") < "2023-09-01") \
    .count()
t3 = time.time() - t0

print(f"\n{'Query':<55} {'Time':>8} {'Rows':>10}")
print("-" * 75)
print(f"{'Partition filter (region=North) + amount>400':<55} {t1:>7.2f}s {result1:>10,}")
print(f"{'YEAR(date)=2023 (breaks pushdown) + amount>400':<55} {t2:>7.2f}s {result2:>10,}")
print(f"{'Partition + date range + count':<55} {t3:>7.2f}s {result3:>10,}")

```
```python

```
%md
### Show Delta Statistics with File Skipping

```python

# Check the statistics Delta maintains
print("DESCRIBE DETAIL:")
display(spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`"))

```
```python

print("DESCRIBE HISTORY:")
display(spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`"))

```
```python

# Inspect Delta log for statistics
delta_log_path = f"{table_path}/_delta_log"
log_files = dbutils.fs.ls(delta_log_path)
print(f"Delta log files at {delta_log_path}:")
for f in log_files[:10]:
    print(f"  {f.name} ({f.size} bytes)")

# Read the last checkpoint file if it exists
checkpoint_path = f"{table_path}/_delta_log/_last_checkpoint"
try:
    checkpoint_info = spark.read.text(checkpoint_path).collect()
    print(f"\nLast checkpoint: {checkpoint_info}")
except:
    print("\nNo _last_checkpoint file (expected after first write)")

```
```python

```
%md
### Key Takeaways — Predicate Pushdown

| Pattern | Effect | Example |
|---|---|---|
| Partition column filter | Entire partitions skipped | `WHERE region = 'North'` |
| Direct column comparison | File-level min/max stats used | `WHERE amount > 400` |
| Function on column | Stats cannot be used | `WHERE YEAR(date) = 2023` |
| Cast on column | Stats cannot be used | `WHERE CAST(id AS STRING) = '5'` |
| Range on column | Stats used effectively | `WHERE date BETWEEN 'A' AND 'B'` |
| `IN` clause | Converted to range checks | `WHERE region IN ('N','S')` |


```python

```
%md
## Concept 53 — Predictive Optimization

**NOTE:** Predictive Optimization is a **full-platform feature** (not available in Community Edition). The concept is explained here, and manual equivalents are demonstrated for CE.

### What Is Predictive Optimization?

Predictive Optimization is a **managed service** that automatically runs maintenance operations on tables:

| Operation | What It Does | Trigger |
|---|---|---|
| **OPTIMIZE** | Compacts small files, ZORDERs data | After 5+ writes to the table |
| **VACUUM** | Removes old file versions | Retained for 7 days (default) |
| **ANALYZE** | Refreshes column statistics | After significant data change |

**Key characteristics:**
- Enabled by default on Unity Catalog managed tables (full platform)
- Monitored via `system.information_schema.predictive_optimizations_history`
- No user configuration required
- Zero performance impact on user queries
- No additional cost (included)

```python

```
%md
### Manual Maintenance (Community Edition Equivalent)

```python

manual_maintenance_path = "/tmp/delta/perf_demo/manual_maintenance"

# Create a fresh table with small files
print("Creating table with many small files...")
df_maintenance = spark.range(0, 500000).select(
    col("id"),
    (col("id") % 365).alias("day_of_year"),
    (col("id") % 50).alias("category_id"),
    (rand(42) * 1000).alias("value"),
)

# Write many small files
df_maintenance.repartition(100).write \
    .mode("overwrite") \
    .format("delta") \
    .save(manual_maintenance_path)

# Count files before OPTIMIZE
files_before = len([f for f in dbutils.fs.ls(manual_maintenance_path) if f.name.endswith('.parquet')])
print(f"Files before OPTIMIZE: {files_before}")

# Read table, check file count via DESCRIBE DETAIL
detail_before = spark.sql(f"DESCRIBE DETAIL delta.`{manual_maintenance_path}`")
detail_before.select("numFiles", "sizeInBytes").show()

```
```python

# Run OPTIMIZE (manual equivalent of Predictive Optimization)
print("Running manual OPTIMIZE...")
t0 = time.time()

spark.sql(f"OPTIMIZE delta.`{manual_maintenance_path}`")

t_optimize = time.time() - t0
print(f"OPTIMIZE completed in {t_optimize:.1f}s")

# Check file count after
detail_after = spark.sql(f"DESCRIBE DETAIL delta.`{manual_maintenance_path}`")
detail_after.select("numFiles", "sizeInBytes").show()

files_after = detail_after.select("numFiles").collect()[0][0]
print(f"\nFiles reduced: {files_before} → {files_after} ({100*(1-files_after/files_before):.0f}% reduction)")

```
```python

```
%md
### When Manual Maintenance Is Still Needed (Even With Predictive Optimization)

Predictive Optimization helps but doesn't eliminate the need for manual intervention:

| Scenario | Why Manual |
|---|---|
| **Precise ZORDER** | Predictive Optimization chooses columns automatically; you may want specific ZORDER columns |
| **Custom VACUUM retention** | Default is 7 days; you may want longer for recovery |
| **Immediate maintenance** | Predictive Optimization runs on a schedule; you may need immediate compaction after a large write |
| **Tables with deletion vectors** | Manual REORG TABLE may be needed |
| **Non-managed tables** | Predictive Optimization only works on managed tables in Unity Catalog |

```python
# Manual OPTIMIZE with custom ZORDER
spark.sql("OPTIMIZE my_table ZORDER BY (date_col, category)")

# Manual VACUUM with custom retention
spark.sql("VACUUM my_table RETAIN 168 HOURS")

# Manual ANALYZE
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS")
```

```python

```
%md
## Concept 54 — Column Statistics & File Pruning

**Available in Community Edition — hands-on.**

### How Delta Collects Statistics

Delta Lake automatically collects **per-file min/max statistics** for the **first 32 columns** of each Parquet file. These statistics are stored in the Delta transaction log and enable:

- **File-level pruning**: If `WHERE amount > 500` and a file's max amount is 300, the entire file is skipped.
- **No data read needed** to determine if a file matches — just check the metadata.

**Column order matters**: Statistics are only kept for the first 32 columns in the schema. Columns beyond 32 don't have stats available for file skipping.

```python

# Create a wide table to demonstrate the 32-column statistics limit
print("Creating wide table (40 columns) to demonstrate stats limit...")

wide_cols = [col("id").alias("col_00")]
for i in range(1, 40):
    wide_cols.append((col("id") % (100 + i * 10)).alias(f"col_{i:02d}"))

df_wide = spark.range(0, 200000).select(*wide_cols)

wide_table_path = "/tmp/delta/perf_demo/wide_table"
df_wide.repartition(10).write \
    .mode("overwrite") \
    .format("delta") \
    .save(wide_table_path)

print("Wide table created.")
spark.read.format("delta").load(wide_table_path).printSchema()

```
```python

```
%md
### ANALYZE TABLE to Refresh Statistics

```python

# ANALYZE the wide table
print("Running ANALYZE TABLE to collect statistics...")
t0 = time.time()
spark.sql(f"ANALYZE TABLE delta.`{wide_table_path}` COMPUTE STATISTICS FOR ALL COLUMNS")
print(f"ANALYZE completed in {time.time()-t0:.1f}s")

# Show statistics
print("\nColumn Statistics (DESCRIBE EXTENDED):")
spark.sql(f"DESCRIBE EXTENDED delta.`{wide_table_path}`").show(50, truncate=False)

```
```python

```
%md
### Demonstrate File Skipping with Column Statistics

Filter on a column with statistics (col_01, within first 32) vs without (col_35, beyond 32).

```python

wide_df = spark.read.format("delta").load(wide_table_path)

print("=" * 80)
print("FILTER ON col_01 (within first 32 — HAS STATISTICS)")
print("=" * 80)
wide_df.filter(col("col_01") == 50).explain(extended=True)

print("\n" + "=" * 80)
print("FILTER ON col_35 (beyond first 32 — NO STATISTICS)")
print("=" * 80)
wide_df.filter(col("col_35") == 50).explain(extended=True)

```
```python

# Timing comparison: filter on column with stats vs without
print("TIMING COMPARISON: Column position effect on performance\n")

# Filter on column 01 (has stats)
t0 = time.time()
result_with_stats = wide_df.filter(col("col_01") == 50).count()
t_with = time.time() - t0

# Filter on column 35 (no stats)
t0 = time.time()
result_no_stats = wide_df.filter(col("col_35") == 50).count()
t_without = time.time() - t0

print(f"Filter on col_01 (with stats):    {t_with:.3f}s — returned {result_with_stats:,} rows")
print(f"Filter on col_35 (no stats):      {t_without:.3f}s — returned {result_no_stats:,} rows")
print(f"Speedup from stats:               {t_without/t_with:.1f}x faster with statistics")

```
```python

```
%md
### DESCRIBE DETAIL — Check Table Statistics

```python

# Show table details including statistics configuration
print("TABLE DETAILS:")

detail = spark.sql(f"DESCRIBE DETAIL delta.`{wide_table_path}`")
detail.select("name", "numFiles", "sizeInBytes", "properties").show(truncate=False)

print("\nTABLE HISTORY (last 5 operations):")
history = spark.sql(f"DESCRIBE HISTORY delta.`{wide_table_path}`")
history.select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=False)

```
```python

```
%md
### Statistics Property Configuration

```python

# Show how to configure statistics collection
print("Delta table statistics properties:")
print("""
| Property | Default | Description |
|---|---|---|
| delta.dataSkippingNumIndexedCols | 32 | Number of columns to collect stats for |
| delta.statistics.columns | (all) | Specific columns to collect stats for |
| delta.stats.collected | N/A | Columns with stats in this file |

You can set these when creating or altering a table:
```
SET spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols = 40

ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.dataSkippingNumIndexedCols' = '50'
)
```
""")

```
```python

```
%md
## Concept 55 — Write Optimization

**Available in Community Edition — full hands-on.**

### Overview

Write optimization techniques improve **read performance** by controlling output file size and layout:

| Technique | What It Does |
|---|---|
| `optimizeWrite` | Coalesces small files before writing (auto-shuffle) |
| `maxRecordsPerFile` | Caps records per output file |
| `targetFileSize` | Desired file size (default 1GB for Parquet) |
| Repartition before write | Controls number of output files |
| Coalesce before write | Reduces files without shuffle |
| ZORDER optimization | Clusters related data in files |

```python

```
%md
### Create Test Table and Benchmark Different Write Strategies

```python

write_test_path = "/tmp/delta/perf_demo/write_opt"

# Cleanup if exists
dbutils.fs.rm(write_test_path + "_base", True)
dbutils.fs.rm(write_test_path + "_coalesced", True)
dbutils.fs.rm(write_test_path + "_optimized", True)

# Generate test data
print("Generating write test data...")
df_write = spark.range(0, 500000).select(
    col("id"),
    (col("id") * 37 + 100).alias("key"),
    rand(42).alias("value_a"),
    rand(43).alias("value_b"),
    rand(44).alias("value_c"),
    (col("id") % 100).alias("group_id"),
)

# Split into varied-size partitions to simulate real-world data skew
df_write = df_write.repartition(200)

# Strategy 1: No optimization (many small files from skewed partitions)
print("\nStrategy 1: Writing without optimization (many small files)...")
t0 = time.time()
df_write.write.mode("overwrite").format("delta") \
    .save(write_test_path + "_base")
t1 = time.time() - t0
print(f"  Write time: {t1:.1f}s")

# Strategy 2: Coalesce before write (reduce files without shuffle)
print("\nStrategy 2: Writing with coalesce(8) before write...")
t0 = time.time()
df_write.coalesce(8).write.mode("overwrite").format("delta") \
    .save(write_test_path + "_coalesced")
t2 = time.time() - t0
print(f"  Write time: {t2:.1f}s")

# Strategy 3: maxRecordsPerFile
print("\nStrategy 3: Writing with maxRecordsPerFile=50000...")
t0 = time.time()
df_write.coalesce(16).write.mode("overwrite").format("delta") \
    .option("maxRecordsPerFile", 50000) \
    .save(write_test_path + "_optimized")
t3 = time.time() - t0
print(f"  Write time: {t3:.1f}s")

```
```python

```
%md
### Compare File Counts and Read Performance

```python

# Analyze each strategy
strategies = [
    ("No Optimization", write_test_path + "_base"),
    ("Coalesce(8)", write_test_path + "_coalesced"),
    ("maxRecordsPerFile=50000", write_test_path + "_optimized"),
]

print(f"{'Strategy':<30} {'Files':>6} {'Size (MB)':>10} {'Read Time':>10}")
print("-" * 60)

for name, path in strategies:
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
    row = detail.select("numFiles", "sizeInBytes").collect()[0]
    num_files = row[0]
    size_mb = row[1] / (1024 * 1024)
    
    # Time a read
    df_test = spark.read.format("delta").load(path)
    t0 = time.time()
    df_test.filter(col("group_id") == 50).count()
    read_time = time.time() - t0
    
    print(f"{name:<30} {num_files:>6} {size_mb:>9.1f} {read_time:>9.2f}s")

```
```python

```
%md
### optimizeWrite Setting

```python

optimize_write_path = "/tmp/delta/perf_demo/optimize_write"
dbutils.fs.rm(optimize_write_path, True)

# Test with optimizeWrite enabled
print("Writing with optimizeWrite enabled...")
t0 = time.time()

spark.conf.set("spark.databricks.delta.optimizeWrite.numShuffleBlocks", "2000000")
spark.conf.set("spark.databricks.delta.optimizeWrite.binSize", "512")

df_write.write.mode("overwrite").format("delta") \
    .option("optimizeWrite", "true") \
    .save(optimize_write_path)

t_opt_write = time.time() - t0
print(f"Write time (optimizeWrite=true): {t_opt_write:.1f}s")

# Check file count
detail = spark.sql(f"DESCRIBE DETAIL delta.`{optimize_write_path}`")
detail.select("numFiles", "sizeInBytes").show()

# Time a read
t0 = time.time()
spark.read.format("delta").load(optimize_write_path) \
    .filter(col("group_id") == 50).count()
t_read_opt = time.time() - t0
print(f"Read time (optimized): {t_read_opt:.2f}s")

```
```python

```
%md
### Write Optimization Summary

```python

summary_data = [
    ("No optimization", "Good", "Poor (many files)", "Poor (high overhead)"),
    ("coalesce(N)", "Better", "Good (N files)", "Better (fewer files)"),
    ("repartition(N)", "Worse (shuffle)", "Good (N files)", "Better (fewer files)"),
    ("maxRecordsPerFile", "Same", "Controlled", "Better (ideal sizes)"),
    ("optimizeWrite=true", "Slightly higher", "Optimized", "Best (auto)"),
    ("OPTIMIZE after write", "Adds step", "Best", "Best"),
]

write_summary_df = pd.DataFrame(summary_data, 
    columns=["Strategy", "Write Cost", "File Layout", "Read Performance"])
print("WRITE OPTIMIZATION STRATEGIES COMPARISON")
display(write_summary_df)

```
```python

```
%md
## Concept 56 — Cluster Sizing & Autoscaling

**NOTE:** Community Edition is **single-node only** (1 driver, 0 workers). This section is conceptual but includes Spark config guidance applicable to CE.

### Cluster Sizing Principles

| Workload Type | Cores | Memory | Workers | Rationale |
|---|---|---|---|---|
| **SQL Analytics / BI** | 4–8 per worker | 2–4 GB/core | 2–20 | High concurrency, small queries |
| **ETL / Data Engineering** | 8–16 per worker | 2–4 GB/core | 2–50 | CPU-intensive, large shuffles |
| **ML Training** | 8–16 per worker | 4–8 GB/core | 2–20 | GPU optional, memory for models |
| **Streaming** | 4–8 per worker | 4 GB/core | 2–10 | Sustained usage, low latency |
| **Development / Exploration** | 4 per worker | 8 GB/core | 1–2 | Interactive, low cost |

### The `spark.sql.shuffle.partitions` Rule

Default: 200 partitions per shuffle operation.  
**Rule of thumb:** Set to 2–3× total cores in cluster.

| Cluster Size | Total Cores | Recommended shuffle.partitions |
|---|---|---|
| 1 worker × 4 cores | 4 | 8–12 |
| 4 workers × 8 cores | 32 | 64–96 |
| 10 workers × 16 cores | 160 | 320–480 |

```python

```
%md
### Autoscaling Configuration

```python

print("""
AUTOSCALING CONFIGURATION EXAMPLES

1. ETL Pipeline (cost-sensitive, uses spot instances):
   Workers: min=2, max=20
   Worker type: i3.2xlarge (8 cores, 61 GB)
   Spark configs:
     spark.sql.shuffle.partitions = 80
     spark.databricks.delta.autoCompact.maxFileSize = 134217728
     spark.sql.adaptive.enabled = true
   Spot instances: enabled (save 30–50%)

2. BI Dashboard (consistent, predictable):
   Workers: min=5, max=10
   Worker type: i3.xlarge (4 cores, 30.5 GB)
   Spark configs:
     spark.sql.shuffle.partitions = 40
     spark.sql.adaptive.coalescePartitions.enabled = true

3. ML Training (GPU-enabled):
   Workers: min=2, max=8
   Worker type: g4dn.xlarge (4 cores, 16 GB, 1 GPU)
   Driver: g4dn.xlarge (for single-node debugging)

4. Development (Community Edition - single node):
   Workers: min=0, max=0 (not configurable)
   Spark configs:
     spark.sql.shuffle.partitions = 8 (since single-node)
     spark.sql.adaptive.enabled = true
""")

```
```python

```
%md
### Spot Instance Usage

```python

spot_data = [
    ("On-Demand", "Regular price ($0.55–$1.10/DBU)", "None", 100),
    ("Spot (bid 100%)", "30–50% discount", "Low", 80),
    ("Spot + On-Demand fallback", "20–40% net savings", "Zero", 85),
]

spot_df = pd.DataFrame(spot_data, columns=["Strategy", "Cost", "Interruption Risk", "Reliability %"])
print("SPOT INSTANCE COMPARISON")
display(spot_df)

```
```python

```
%md
### Right-Sizing for Community Edition

In Community Edition, you have a single node. Key Spark configurations to tune:

```python

print("Current Spark configuration for Community Edition:")
important_configs = [
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.databricks.delta.autoCompact.enabled",
    "spark.databricks.delta.optimizeWrite.enabled",
    "spark.sql.files.maxPartitionBytes",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
]

for config in important_configs:
    try:
        value = spark.conf.get(config)
        print(f"  {config:<55} = {value}")
    except:
        print(f"  {config:<55} = (not set, using default)")

```
```python

```
%md
## Concept 57 — DBU Cost Model & Billing

**NOTE:** System tables for billing are not available in Community Edition. This section explains the DBU model conceptually with cost calculation examples you can apply.

### What Is a DBU?

**DBU** = Databricks Unit — the unit of measure for Databricks capacity consumption.

**1 DBU = 1 hour of processing on a standardized unit of compute.**

The actual cost per DBU depends on:
- **Tier**: Standard, Premium, or Enterprise
- **SKU**: All-Purpose Compute, Jobs Compute, SQL Compute, Serverless
- **Cloud provider**: AWS, Azure, GCP (prices vary slightly)

```python

```
%md
### Billing SKUs

```python

sku_data = [
    ("All-Purpose Compute", "$0.40–0.55", "10 min", "Interactive notebooks, development"),
    ("Jobs Compute", "$0.30–0.40", "10 min", "Scheduled jobs, automated pipelines"),
    ("Jobs Compute (Light)", "$0.22–0.30", "1 min", "Short jobs, DLT maintenance"),
    ("SQL Warehouse (Classic)", "$0.40–0.55", "10 min", "BI, SQL endpoints"),
    ("SQL Warehouse (Serverless)", "$0.70–1.10", "1 min", "Serverless BI, SQL"),
    ("Serverless (Jobs)", "$0.55–0.75", "1 min", "Serverless notebook jobs"),
    ("DLT (Core)", "$0.55–0.85", "Continuous", "Delta Live Tables pipeline"),
    ("DLT (Advanced)", "$0.75–1.10", "Continuous", "DLT with CDC, SCD Type 2"),
]

sku_df = pd.DataFrame(sku_data, columns=["SKU", "DBU/hr (approx)", "Billing Min", "Use Case"])
print("DATABRICKS BILLING SKUs")
display(sku_df)

```
```python

```
%md
### Cost Calculation Examples

```python

print("""
COST CALCULATION EXAMPLES
=========================

Example 1: Nightly ETL Job (2 hours, Jobs Compute, 4 workers × 8 cores)
  DBU rate: 0.35/DBU (Jobs Compute, Premium)
  DBU/hr: 4 workers × 0.75 DBU/hr per core × 8 cores = 24 DBU/hr
  Daily: 24 DBU/hr × 2 hr = 48 DBU × $0.35 = $16.80
  Monthly: $16.80 × 30 = $504.00
  With spot instances (40% off): $302.40/month

Example 2: BI Dashboard (SQL Warehouse, 8hr/day, Classic)
  DBU rate: 0.48/DBU (SQL Classic, Premium)
  Cluster: Medium (10 DBU/hr)
  Daily: 10 DBU/hr × 8 hr = 80 DBU × $0.48 = $38.40
  Monthly (business days): $38.40 × 22 = $844.80
  Note: Serverless would cost ~$1,450/mo but save idle DBU

Example 3: Development (4 hr/day, All-Purpose, 1 worker × 4 cores)
  DBU rate: 0.45/DBU (All-Purpose, Premium)
  DBU/hr: 1 worker × 0.55 DBU/hr × 4 cores = 2.2 DBU/hr
  Daily: 2.2 DBU/hr × 4 hr = 8.8 DBU × $0.45 = $3.96
  Monthly: $3.96 × 22 = $87.12

Example 4: Serverless Bursty Workload (50 queries/day, 30s avg)
  DBU rate: 0.90/DBU (Serverless SQL)
  Processing time: 50 × 30s = 1,500s = 0.42 hr
  Monthly: 0.42 DBU/hr × 0.90 × 30 days = $11.34
  This same workload on Classic with 10-min minimum would cost MUCH more
""")

```
```python

```
%md
### Tagging Jobs for Cost Allocation

```python

print("""
COST ALLOCATION TAGS
=====================
You can tag clusters, jobs, and pools to track costs by team/project:

# Set tags when creating a cluster
{
  "cluster_name": "etl-pipeline",
  "custom_tags": {
    "team": "data-engineering",
    "project": "customer-360",
    "environment": "production",
    "cost_center": "DE-1234"
  }
}

# These tags appear in billing reports and usage analysis.
# Use system tables (full platform) to query:
# SELECT * FROM system.billing.usage
# WHERE custom_tags.cost_center = 'DE-1234'

# For Community Edition, you can still add tags for local tracking.
""")

```
```python

```
%md
### Budget Policies (Full Platform Feature)

```python

print("""
CLUSTER POLICIES (Full Platform)
================================
Cluster policies enforce cost controls:

1. Restrict instance types: Allow only i3.xlarge and i3.2xlarge
2. Limit autoscaling: max 10 workers
3. Enforce autotermination: max 60 minutes of inactivity
4. Mandatory tags: Require "team" and "cost_center" tags

Usage monitoring (full platform):
  SELECT
    usage_date,
    sku_name,
    SUM(usage_quantity) as total_dbu
  FROM system.billing.usage
  WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
  GROUP BY usage_date, sku_name
  ORDER BY usage_date DESC
""")

```
```python

```
%md
## Concept 58 — Query Profile & Execution Plans

**Available in Community Edition — full hands-on with EXPLAIN.**

### Understanding EXPLAIN Output

`EXPLAIN` shows how Spark will execute a query. There are three plan levels:

| Plan | What It Shows |
|---|---|
| **Parsed Logical Plan** | What you wrote — unresolved references |
| **Analyzed Logical Plan** | Resolved column names and types |
| **Optimized Logical Plan** | After Catalyst optimizer applied rules |
| **Physical Plan** | Actual execution strategy (how Spark runs it) |

```python

# Create a more complex dataset for query plan analysis
plan_test_path = "/tmp/delta/perf_demo/query_plan"
dbutils.fs.rm(plan_test_path, True)

print("Creating test data for query plan analysis...")
df_plan = spark.range(0, 500000).select(
    col("id").alias("order_id"),
    (col("id") % 5000 + 1).alias("customer_id"),
    (col("id") % 100 + 1).alias("store_id"),
    rand(42).alias("order_amount"),
    (col("id") % 365).alias("day_number"),
    (rand(43) * 100).cast("int").alias("item_count"),
)

df_plan.write.mode("overwrite").format("delta").save(plan_test_path)

orders = spark.read.format("delta").load(plan_test_path)
print(f"Orders: {orders.count():,} rows")

```
```python

```
%md
### Simple EXPLAIN — See the Physical Plan

```python

print("=" * 80)
print("EXPLAIN: Simple aggregation")
print("=" * 80)
orders.groupBy("store_id").agg(
    {"order_amount": "avg", "order_amount": "sum", "item_count": "sum"}
).explain()

```
```python

```
%md
### EXPLAIN EXTENDED — Full Plan Details

```python

print("=" * 80)
print("EXPLAIN EXTENDED: Multi-step query")
print("=" * 80)

from pyspark.sql.functions import avg, sum as spark_sum, count as spark_count

query = orders.filter(col("order_amount") > 0.5) \
    .groupBy("store_id", "day_number") \
    .agg(
        spark_sum("order_amount").alias("total_amount"),
        spark_count("*").alias("order_count"),
        avg("item_count").alias("avg_items"),
    ) \
    .filter(col("total_amount") > 1000) \
    .orderBy(col("total_amount").desc())

query.explain(extended=True)

```
```python

```
%md
### Walking Through Plan Nodes

```python

print("""
KEY PLAN NODES — WHAT TO LOOK FOR
==================================

SCAN / FileScan
  Reads data from storage. Check:
  - "number of files read" → are you reading too many?
  - "partition filters" → is predicate pushdown working?
  - "dataFilters" vs "partitionFilters" → is partitioning effective?

PROJECT
  Selects columns. Fine unless projecting after a huge join.

FILTER
  Applies WHERE conditions. Check:
  - Is it pushed before or after JOIN?
  - Good: Filter → Join (filter early)
  - Bad: Join → Filter (filter late)

JOIN (BroadcastHashJoin / SortMergeJoin / ShuffledHashJoin)
  BroadcastHashJoin: best — small table broadcast to all executors
  SortMergeJoin: common — both sides shuffled and sorted
  ShuffledHashJoin: rare — one side shuffled and hashed

AGGREGATE
  Grouping/summarizing. Check:
  - Is it hash-based (partial + final) or sort-based?
  - Number of output rows — large cardinality group-bys are expensive

SORT / ORDER BY
  Global ordering requires a single partition — BOTTLENECK!
  Prefer: sort within partitions or use SORT BY instead
  Use: repartition(range) for sorted output

EXCHANGE / SHUFFLE
  Data moves between executors — most expensive operation!
  "SinglePartition" suffix = all data to one node (slow)
  "hashpartitioning" = distributed (better)
""")

```
```python

```
%md
### Analyze a Complex Query Plan Step by Step

```python

print("=" * 80)
print("COMPLEX QUERY WITH EXPLAIN EXTENDED")
print("=" * 80)

# Create a second table for join analysis
customer_path = "/tmp/delta/perf_demo/customers"
dbutils.fs.rm(customer_path, True)

spark.range(1, 5001).select(
    col("id").alias("customer_id"),
    (col("id") % 5).alias("region_id"),
    rand(44).alias("loyalty_score"),
).write.mode("overwrite").format("delta").save(customer_path)

customers = spark.read.format("delta").load(customer_path)

# Complex query with join, filter, aggregate, order
complex_query = orders.alias("o") \
    .join(customers.alias("c"), col("o.customer_id") == col("c.customer_id"), "inner") \
    .filter(col("c.loyalty_score") > 0.5) \
    .groupBy("o.store_id", "c.region_id") \
    .agg(
        spark_sum("o.order_amount").alias("total_revenue"),
        spark_count("o.order_id").alias("num_orders"),
    ) \
    .filter(col("total_revenue") > 5000) \
    .orderBy(col("total_revenue").desc()) \
    .limit(10)

complex_query.explain(extended=True)

```
```python

```
%md
### Identifying Slow Operators

```python

print("""
IDENTIFYING SLOW OPERATORS IN PLANS
====================================

🚩 RED FLAGS in Physical Plans:

1. "SortMergeJoin" with large tables
   → Both sides shuffled → heavy network/disk I/O
   Fix: Use broadcast hint if one table < 10MB after filtering

2. "ObjectHashAggregate" instead of "HashAggregate"
   → Indicates non-primitive (object) types → slower aggregation
   Fix: Use primitive types (Long/Double) where possible

3. "Sort" with "SinglePartition"
   → Global sort forced to one node → bottleneck
   Fix: Use df.orderBy().limit(N) or repartition before sort

4. Many "Exchange hashpartitioning" nodes
   → Multiple shuffles in sequence → each is expensive
   Fix: Denormalize, pre-aggregate, or use broadcast joins

5. "Scan parquet → Filter → Project → Scan parquet" repeating
   → Subquery executed for each row (correlated)
   Fix: Rewrite as JOIN with broadcast

6. "WholeStageCodegen" missing
   → Collapse virtual functions not happening → overhead
   Fix: Check for UDFs blocking codegen (Python UDFs cannot be codegen'd)
""")

```
```python

```
%md
### Generate and Analyze Your Own Query Profile

```python

# Time a query and correlate with the plan
print("Running query with timing...\n")

query_to_run = orders.filter(col("order_amount") > 0.3) \
    .groupBy("store_id") \
    .agg(spark_sum("order_amount").alias("total")) \
    .filter(col("total") > 5000)

# Show plan
print("QUERY PLAN:")
query_to_run.explain()

# Execute and time
t0 = time.time()
result = query_to_run.collect()
t_query = time.time() - t0

print(f"\nQuery executed in {t_query:.2f}s")
print(f"Result rows: {len(result)}")
print(f"\nFirst 5 rows:")
for row in result[:5]:
    print(f"  store_id={row['store_id']}, total={row['total']:.2f}")

```
```python

```
%md
## Concept 59 — Join Performance Diagnosis

**Available in Community Edition — full hands-on.**

### Join Strategies in Spark

| Strategy | When Used | Cost | Best For |
|---|---|---|---|
| **Broadcast Hash Join (BHJ)** | One side < threshold (default 10MB) | No shuffle | Small lookup tables |
| **Sort Merge Join (SMJ)** | Both sides large, join keys sortable | Shuffle both sides | Large-large joins |
| **Shuffled Hash Join (SHJ)** | One side 3× smaller than other | Shuffle one side | Medium-large joins |
| **Broadcast Nested Loop Join (BNLJ)** | Non-equi join, one side small | Cartesian risk | Cross/custom joins |
| **Cartesian Product Join** | No join condition | Very expensive | Avoid! |

```python

```
%md
### Create Large Tables for Join Benchmarking

```python

join_test_dir = "/tmp/delta/perf_demo/joins"
dbutils.fs.rm(join_test_dir, True)

NUM_FACT = 1000000
NUM_DIM = 1000

print(f"Creating fact table ({NUM_FACT:,} rows) and dimension table ({NUM_DIM:,} rows)...")

# Fact table — large
fact = spark.range(0, NUM_FACT).select(
    col("id").alias("sale_id"),
    (col("id") % NUM_DIM + 1).alias("product_id"),
    (col("id") % 500 + 1).alias("customer_id"),
    rand(42).alias("sale_amount"),
    (rand(43) * 10 + 1).cast("int").alias("quantity"),
    (col("id") % 365).alias("day_of_year"),
)

# Dimension table — small (fits in broadcast)
dim = spark.range(1, NUM_DIM + 1).select(
    col("id").alias("product_id"),
    (col("id") % 50 + 1).alias("category_id"),
    (col("id") % 20 + 1).alias("supplier_id"),
    rand(44).alias("list_price"),
)

# Write both
fact.write.mode("overwrite").format("delta").save(f"{join_test_dir}/fact")
dim.write.mode("overwrite").format("delta").save(f"{join_test_dir}/dim")

print(f"Fact table: {fact.count():,} rows")
print(f"Dim table:  {dim.count():,} rows")

```
```python

```
%md
### Test 1: Default Join (Auto Broadcast)

```python

fact_df = spark.read.format("delta").load(f"{join_test_dir}/fact")
dim_df = spark.read.format("delta").load(f"{join_test_dir}/dim")

print("TEST 1: Default join (auto broadcast)")
print("-" * 50)

t0 = time.time()
result1 = fact_df.alias("f").join(
    dim_df.alias("d"),
    col("f.product_id") == col("d.product_id"),
    "inner"
).select("f.sale_id", "f.sale_amount", "d.category_id", "d.list_price") \
 .filter(col("f.sale_amount") > 0.5)
result1.explain()
print(f"\nExecution time: {time.time()-t0:.2f}s")
result1_count = result1.count()
print(f"Result rows: {result1_count:,}")

```
```python

```
%md
### Test 2: Sort-Merge Join (Disable Broadcast, Force Shuffle)

```python

print("TEST 2: Sort-Merge Join (broadcast disabled)")
print("-" * 50)

# Disable broadcast to force sort-merge
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

t0 = time.time()
result2 = fact_df.alias("f").join(
    dim_df.alias("d"),
    col("f.product_id") == col("d.product_id"),
    "inner"
).select("f.sale_id", "f.sale_amount", "d.category_id", "d.list_price") \
 .filter(col("f.sale_amount") > 0.5)
result2.explain()
print(f"\nExecution time: {time.time()-t0:.2f}s")

# Reset broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
result2_count = result2.count()
print(f"Result rows: {result2_count:,}")

```
```python

```
%md
### Test 3: Broadcast Hint

```python

from pyspark.sql.functions import broadcast

print("TEST 3: Explicit broadcast hint")
print("-" * 50)

t0 = time.time()
result3 = fact_df.alias("f").join(
    broadcast(dim_df.alias("d")),
    col("f.product_id") == col("d.product_id"),
    "inner"
).select("f.sale_id", "f.sale_amount", "d.category_id", "d.list_price") \
 .filter(col("f.sale_amount") > 0.5)
result3.explain()
print(f"\nExecution time: {time.time()-t0:.2f}s")
result3_count = result3.count()
print(f"Result rows: {result3_count:,}")

```
```python

```
%md
### Test 4: Join After Pre-Aggregation (Reduce Shuffle)

```python

print("TEST 4: Pre-aggregate before join (reduce data movement)")
print("-" * 50)

# Pre-aggregate the fact table to reduce rows before join
t0_prep = time.time()

aggregated_fact = fact_df.filter(col("sale_amount") > 0.1) \
    .groupBy("product_id", "day_of_year") \
    .agg(
        F.sum("sale_amount").alias("total_sales"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("transaction_count"),
    )

print(f"Aggregated fact: {aggregated_fact.count():,} rows (from {NUM_FACT:,})")

t0 = time.time()
result4 = aggregated_fact.alias("a").join(
    broadcast(dim_df.alias("d")),
    col("a.product_id") == col("d.product_id"),
    "inner"
).select("a.product_id", "a.total_sales", "d.category_id", "d.list_price") \
 .filter(col("a.total_sales") > 1000)
result4.explain()
t_join = time.time() - t0

print(f"\nPre-aggregation + join: {t_join:.2f}s")
print(f"Result rows: {result4.count():,}")

```
```python

```
%md
### Tuning Join Thresholds

```python

print("JOIN CONFIGURATION PARAMETERS")
print("=" * 60)

join_configs = {
    "spark.sql.autoBroadcastJoinThreshold": "10m (10 MB) — max size for broadcast",
    "spark.sql.join.preferSortMergeJoin": "true — prefer SMJ over SHJ",
    "spark.sql.adaptive.enabled": "true — adaptive query execution",
    "spark.sql.adaptive.coalescePartitions.enabled": "true — coalesce post-shuffle",
    "spark.sql.adaptive.skewJoin.enabled": "true — handle skewed joins",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5 — skew detection factor",
    "spark.sql.broadcastTimeout": "300s — timeout for broadcast",
}

for config, description in join_configs.items():
    try:
        current = spark.conf.get(config) if config.startswith("spark.sql.") else "N/A"
    except:
        current = "(default)"
    print(f"{config:<55} = {description}")
    print(f"{'':55}   Current: {current}")

# Check current broadcast threshold
current_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"\nCurrent broadcast threshold: {current_threshold}")
print(f"If table size < {current_threshold}, Spark uses Broadcast Hash Join.")

```
```python

```
%md
### Join Performance Summary

```python

# Run all join tests with consistent timing
print("\n" + "=" * 60)
print("JOIN PERFORMANCE COMPARISON")
print("=" * 60)

# Reset threshold for fair comparison
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

timings = []

# Test A: Broadcast Hash Join (default)
t0 = time.time()
fact_df.join(dim_df, "product_id", "inner").select("sale_amount").count()
timings.append(("Broadcast Hash Join", time.time() - t0))

# Test B: Sort-Merge Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
t0 = time.time()
fact_df.join(dim_df, "product_id", "inner").select("sale_amount").count()
timings.append(("Sort-Merge Join", time.time() - t0))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

# Test C: Pre-aggregate then broadcast join
t0 = time.time()
agg = fact_df.groupBy("product_id").agg(F.sum("sale_amount").alias("total"))
agg_count = agg.join(dim_df, "product_id", "inner").count()
timings.append(("Pre-agg + Broadcast", time.time() - t0))

print(f"\n{'Strategy':<30} {'Time':>8} {'Speedup':>10}")
print("-" * 50)
baseline = timings[0][1] if timings[0][1] > 0 else 1
for name, t in timings:
    speedup = timings[1][1] / t if name != "Sort-Merge Join" else 1.0
    print(f"{name:<30} {t:>7.2f}s {'baseline' if name == timings[0][0] else f'{speedup:>9.1f}x'} ")

```
```python

```
%md
## Concept 60 — MERGE Write Amplification

**Available in Community Edition — full hands-on.**

### What Is Write Amplification?

When MERGE matches rows in a file, it **rewrites the ENTIRE file**, not just the matched rows. If 1 row in a 100MB file is updated, MERGE rewrites all 100MB. This is the core of write amplification.

**Deletion Vectors** (Delta 3.0+, full platform) solve this by recording which rows are deleted/matched, avoiding full file rewrites. Community Edition uses an older Delta version without this feature.

```python

```
%md
### Create Test Table for MERGE Experiments

```python

merge_test_path = "/tmp/delta/perf_demo/merge_test"
dbutils.fs.rm(merge_test_path, True)

print("Creating base table for MERGE testing...")

# Generate a base table — each product_id maps to multiple rows
merge_base = spark.range(0, 500000).select(
    col("id").alias("row_id"),
    (col("id") % 10000 + 1).alias("product_id"),
    (col("id") % 365).alias("day_num"),
    rand(42).alias("amount"),
    rand(43).alias("quantity"),
    lit("old").alias("status"),
)

merge_base.write.mode("overwrite").format("delta").save(merge_test_path)
print(f"Base table: {merge_base.count():,} rows")

# Show file distribution
detail = spark.sql(f"DESCRIBE DETAIL delta.`{merge_test_path}`")
print(f"Initial files: {detail.select('numFiles').collect()[0][0]}")
print(f"Initial size: {detail.select('sizeInBytes').collect()[0][0] / 1024 / 1024:.1f} MB")

```
```python

```
%md
### MERGE 1: Wide MERGE (Matches Many Files — High Amplification)

```python

# Create updates that span many products (wide update — touches many files)
updates_wide = spark.range(1, 5001).select(
    col("id").alias("product_id"),
    lit("updated").alias("status"),
    (rand(55) * 1000).alias("new_amount"),
)

# Create a temp view for MERGE
updates_wide.createOrReplaceTempView("updates_wide")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW base_table AS
    SELECT * FROM delta.`{merge_test_path}`
""")

print("MERGE 1: Wide MERGE (touching many products across many files)")
print("-" * 60)

merge_sql_1 = f"""
MERGE INTO delta.`{merge_test_path}` AS target
USING updates_wide AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
  target.status = source.status,
  target.amount = source.new_amount
WHEN NOT MATCHED THEN INSERT *
"""

t0 = time.time()
spark.sql(merge_sql_1)
t_merge_wide = time.time() - t0

# Check what happened
history_wide = spark.sql(f"DESCRIBE HISTORY delta.`{merge_test_path}`") \
    .filter(col("operation") == "MERGE") \
    .select("operationMetrics") \
    .collect()

print(f"MERGE Wide completed in {t_merge_wide:.2f}s")
if history_wide:
    print(f"Operation metrics: {history_wide[0]['operationMetrics']}")

detail1 = spark.sql(f"DESCRIBE DETAIL delta.`{merge_test_path}`")
files_after_wide = detail1.select("numFiles").collect()[0][0]
print(f"Files after wide MERGE: {files_after_wide}")

```
```python

```
%md
### Reset Table for Second Test

```python

# Reset the table for a fair second test
dbutils.fs.rm(merge_test_path, True)
merge_base.write.mode("overwrite").format("delta").save(merge_test_path)
print("Table reset to initial state.")

```
```python

```
%md
### MERGE 2: Targeted MERGE (Single Product — Low Amplification)

```python

# Create updates for a SINGLE product (narrow update — touches minimal files)
updates_narrow = spark.range(1, 2).select(
    col("id").alias("product_id"),
    lit("targeted_update").alias("status"),
    (rand(55) * 1000).alias("new_amount"),
)

updates_narrow.createOrReplaceTempView("updates_narrow")

print("MERGE 2: Targeted MERGE (single product)")
print("-" * 60)

merge_sql_2 = f"""
MERGE INTO delta.`{merge_test_path}` AS target
USING updates_narrow AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
  target.status = source.status,
  target.amount = source.new_amount
WHEN NOT MATCHED THEN INSERT *
"""

t0 = time.time()
spark.sql(merge_sql_2)
t_merge_narrow = time.time() - t0

history_narrow = spark.sql(f"DESCRIBE HISTORY delta.`{merge_test_path}`") \
    .filter(col("operation") == "MERGE") \
    .select("operationMetrics") \
    .collect()

print(f"MERGE Targeted completed in {t_merge_narrow:.2f}s")
if history_narrow:
    print(f"Operation metrics: {history_narrow[0]['operationMetrics']}")

detail2 = spark.sql(f"DESCRIBE DETAIL delta.`{merge_test_path}`")
files_after_narrow = detail2.select("numFiles").collect()[0][0]
print(f"Files after targeted MERGE: {files_after_narrow}")

```
```python

```
%md
### MERGE 3: DELETE + INSERT Approach (Alternative to MERGE)

```python

# Reset table
dbutils.fs.rm(merge_test_path, True)
merge_base.write.mode("overwrite").format("delta").save(merge_test_path)
print("Table reset for DELETE + INSERT test.")

merge_base_df = spark.read.format("delta").load(merge_test_path)

print("\nMERGE 3: DELETE + INSERT approach (alternative to MERGE)")
print("-" * 60)

from pyspark.sql.functions import col as spark_col

t0 = time.time()

# Step 1: Find products to update
products_to_update = [p.product_id for p in updates_wide.select("product_id").distinct().collect()]

# Step 2: Delete old rows
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, merge_test_path)

# For the DELETE + INSERT approach, we'll use the DeltaTable API
# First delete matching rows, then insert new ones
delta_table.delete(spark_col("product_id").isin(products_to_update))

# Step 3: Calculate and insert updated rows
updated_rows = merge_base_df.filter(spark_col("product_id").isin(products_to_update)) \
    .select("row_id", "product_id", "day_num") \
    .withColumn("amount", rand(55) * 1000) \
    .withColumn("quantity", col("quantity")) \
    .withColumn("status", lit("delete_insert_updated"))

# Create the insert dataframe with proper schema
insert_df = updated_rows.select(
    col("row_id"), col("product_id"), col("day_num"),
    col("amount"), col("quantity"), col("status")
)

insert_df.write.mode("append").format("delta").save(merge_test_path)

t_delete_insert = time.time() - t0

print(f"DELETE + INSERT completed in {t_delete_insert:.2f}s")

detail3 = spark.sql(f"DESCRIBE DETAIL delta.`{merge_test_path}`")
print(f"Files after DELETE + INSERT: {detail3.select('numFiles').collect()[0][0]}")

```
```python

```
%md
### Analyze MERGE Performance with DESCRIBE HISTORY

```python

print("=" * 80)
print("DESCRIBE HISTORY — COMPARE ALL OPERATIONS")
print("=" * 80)

history = spark.sql(f"DESCRIBE HISTORY delta.`{merge_test_path}`")
history.select("version", "timestamp", "operation", "operationMetrics").show(20, truncate=False)

# Also show DESCRIBE DETAIL for current state
print("\nCURRENT TABLE STATE:")
detail_final = spark.sql(f"DESCRIBE DETAIL delta.`{merge_test_path}`")
detail_final.select("numFiles", "sizeInBytes").show()

```
```python

```
%md
### MERGE 4: MERGE with Matching on Clustered Keys (Optimization)

```python

merge_clustered_path = "/tmp/delta/perf_demo/merge_clustered"
dbutils.fs.rm(merge_clustered_path, True)

print("Creating ZORDERED table for optimized MERGE...")

# Create and write with ZORDER on product_id (the merge key)
merge_base_clustered = spark.range(0, 500000).select(
    col("id").alias("row_id"),
    (col("id") % 10000 + 1).alias("product_id"),
    (col("id") % 365).alias("day_num"),
    rand(42).alias("amount"),
    lit("old").alias("status"),
)

# Write with ZORDER
merge_base_clustered.write.mode("overwrite").format("delta") \
    .save(merge_clustered_path)

# Run OPTIMIZE with ZORDER on merge key
spark.sql(f"OPTIMIZE delta.`{merge_clustered_path}` ZORDER BY (product_id)")
print("ZORDER optimization on product_id completed.")

# Now run MERGE on clustered table
print("\nMERGE on ZORDERED table:")
t0 = time.time()

merge_sql_clustered = f"""
MERGE INTO delta.`{merge_clustered_path}` AS target
USING updates_wide AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
  target.status = source.status,
  target.amount = source.new_amount
"""

spark.sql(merge_sql_clustered)
t_merge_clustered = time.time() - t0

print(f"MERGE on ZORDERED table: {t_merge_clustered:.2f}s")

```
```python

```
%md
### MERGE Amplification Summary

```python

# Build summary from measured timings
print("\n" + "=" * 80)
print("MERGE / WRITE AMPLIFICATION SUMMARY")
print("=" * 80)

merge_summary = [
    ("Wide MERGE (many products)", "High (all files touched)", "Slow", f"N/A (need reset)"),
    ("Targeted MERGE (1 product)", "Low (few files touched)", "Fast", "Much less IO"),
    ("DELETE + INSERT approach", "Medium (delete + write)", "Medium", "More operations, less file rewrite"),
    ("MERGE with ZORDER key", "Low (clustered data)", "Fastest", "Optimal when match on ZORDER key"),
]

print(f"\n{'Strategy':<35} {'Write Amplification':<25} {'Relative Speed':<15}")
print("-" * 80)
for strategy, amp, speed, note in merge_summary:
    print(f"{strategy:<35} {amp:<25} {speed:<15}")

print(f"""
KEY INSIGHTS:
1. MERGE rewrites ENTIRE FILES containing ANY matched row
2. ZORDER on the merge key localizes matches to fewer files
3. Targeted MERGE (single key) is dramatically faster than wide MERGE
4. DELETE + INSERT can be more efficient when you know exact rows
5. Deletion Vectors (Delta 3.0+) solve this entirely — no rewrites!
6. OPTIMIZE after MERGE helps compact the new files created

TO REDUCE MERGE AMPLIFICATION:
- ZORDER on columns used in MERGE ON clause
- Use targeted filters in the source view
- Batch updates by key ranges
- Consider DELETE + INSERT for large updates
- Upgrade to Delta 3.0+ for Deletion Vectors (full platform)
""")

```
```python

```
%md
## Performance & Cost — Summary Table

```python

print("""
╔══════════════════════════════════════════════════════════════════════════════════╗
║               PERFORMANCE & COST OPTIMIZATION — MASTER SUMMARY                   ║
╠════╤═══════════════════════╤══════════╤══════════════════════════════════════════╣
║ #  │ Concept               │ Difficulty│ Key Technique                           ║
╠════╪═══════════════════════╪══════════╪══════════════════════════════════════════╣
║ 51 │ Serverless Economics  │ Easy     │ DBU pricing; choose serverless vs classic║
║ 52 │ Predicate Pushdown    │ Medium   │ Partition filters, avoid functions on cols║
║ 53 │ Predictive Opt        │ Medium   │ Auto OPTIMIZE/VACUUM; manual equivalents ║
║ 54 │ Column Statistics     │ Medium   │ First 32 cols have stats; ANALYZE TABLE  ║
║ 55 │ Write Optimization    │ Medium   │ maxRecordsPerFile, coalesce, optimizeWrite║
║ 56 │ Cluster Sizing        │ Medium   │ Right-size workers, shuffle.partitions    ║
║ 57 │ DBU Cost Model        │ Medium   │ Billing SKUs, tagging, cost calculations  ║
║ 58 │ Query Plans           │ Medium   │ EXPLAIN EXTENDED, read physical plan      ║
║ 59 │ Join Performance      │ Hard     │ Broadcast hint, pre-agg, threshold tuning ║
║ 60 │ MERGE Amplification   │ Hard     │ ZORDER match keys, DELETE+INSERT, DVs    ║
╚════╧═══════════════════════╧══════════╧══════════════════════════════════════════╝
""")

```
```python

```
%md
## Self-Assessment Questions

Test your understanding of concepts #51–#60:

**Concept 51 — Serverless Economics:**
1. When is serverless more expensive than classic compute?
2. What is the minimum billing period for serverless vs classic SQL warehouses?
3. How does serverless eliminate idle cluster waste?

**Concept 52 — Predicate Pushdown:**
4. Why does `WHERE YEAR(date_col) = 2023` prevent predicate pushdown?
5. What must you do to enable file-level statistics for a column?
6. Does repartitioning to a different column break data skipping?

**Concept 53 — Predictive Optimization:**
7. What three operations does Predictive Optimization run automatically?
8. When would you still manually run OPTIMIZE even with Predictive Optimization?

**Concept 54 — Column Statistics:**
9. For how many columns does Delta collect per-file statistics by default?
10. What command refreshes column statistics?

**Concept 55 — Write Optimization:**
11. What's the difference between `coalesce()` and `repartition()` before writing?
12. What does `maxRecordsPerFile` control?

**Concept 56 — Cluster Sizing:**
13. What's the recommended `spark.sql.shuffle.partitions` relative to core count?
14. When would you use spot instances?

**Concept 57 — DBU Cost Model:**
15. Which SKU is cheapest per DBU for scheduled jobs?
16. How do you allocate costs to specific teams?

**Concept 58 — Query Plans:**
17. What's the difference between a logical plan and a physical plan?
18. What does an Exchange node in the physical plan indicate?

**Concept 59 — Join Performance:**
19. What join strategy does Spark use when one table is smaller than 10MB?
20. How can you reduce shuffle during a join?

**Concept 60 — MERGE Amplification:**
21. Why does MERGE rewrite entire files?
22. What technique reduces the number of files touched by MERGE?
23. What Delta 3.0+ feature eliminates merge write amplification entirely?

```python

```
%md
### Quick Knowledge Check — Write Your Answers Below

```python

print("""
SELF-ASSESSMENT ANSWER KEY (check your knowledge)
==================================================

1. Serverless is more expensive for sustained 24/7 workloads
   because the per-DBU rate is higher than classic Jobs Compute.

2. Serverless: 1-minute minimum; Classic: 10-minute minimum.

3. Compute scales to zero between queries; you pay only for active time.

4. YEAR() is a function that transforms the column value; Delta cannot
   apply file-level min/max stats to the result of a function.

5. The column must be within the first 32 columns in the schema (or
   you can increase delta.dataSkippingNumIndexedCols).

6. Not necessarily, but the new repartition column may not be the one
   you query — causing more files to be read.

7. OPTIMIZE (compaction + ZORDER), VACUUM, ANALYZE.

8. When you need immediate compaction after a large write, specific
   ZORDER columns, or non-default VACUUM retention.

9. 32 columns by default.

10. ANALYZE TABLE <name> COMPUTE STATISTICS FOR ALL COLUMNS

11. coalesce() avoids shuffle (faster, reduces partitions only).
    repartition() performs a full shuffle (can both increase and
    decrease partitions, balanced data distribution).

12. Maximum number of records written to a single file; helps control
    file sizes for optimal read performance.

13. 2-3x the total number of cores in the cluster.

14. For fault-tolerant, cost-sensitive batch workloads.

15. Jobs Compute (Light) at ~$0.22/DBU.

16. Apply custom_tags to clusters/jobs (team, project, cost_center)
    and query billing usage tables.

17. Logical plan = what to compute (optimized expression tree).
    Physical plan = how to compute (actual execution strategy).

18. A shuffle operation — data moves between partitions/executors.
    This is the most expensive operation in Spark.

19. Broadcast Hash Join (BHJ).

20. Pre-aggregate/filter before joining; use broadcast for small
    tables; partition both sides on the same key.

21. MERGE operates at file granularity, not row. If even 1 row in a
    file is matched, the whole file is rewritten.

22. ZORDER on the merge key (localizes matching rows to fewer files).

23. Deletion Vectors — they mark rows as deleted without rewriting
    the underlying Parquet files.
""")

```
```python

```
%md
## Performance Optimization Cheat Sheet

```python

print("""
╔══════════════════════════════════════════════════════════════════╗
║                PERFORMANCE OPTIMIZATION CHEAT SHEET               ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  READS (FASTER QUERIES)                                          ║
║  ├─ Partition on filter columns                                  ║
║  ├─ ZORDER on high-cardinality filter columns                    ║
║  ├─ Use statistics (first 32 columns, ANALYZE TABLE)             ║
║  ├─ Avoid functions on filter columns                            ║
║  ├─ Broadcast hint for small join tables (< 10MB)               ║
║  ├─ Pre-aggregate/filter before joins                            ║
║  └─ Liquid clustering (Delta 3.0+, full platform)                ║
║                                                                  ║
║  WRITES (FASTER INGESTION)                                       ║
║  ├─ optimizeWrite = true (auto-coalesce)                         ║
║  ├─ maxRecordsPerFile for ideal file sizes                       ║
║  ├─ coalesce() not repartition() to reduce shuffle               ║
║  ├─ OPTIMIZE after large writes for compaction                   ║
║  └─ ZORDER on MERGE key column                                   ║
║                                                                  ║
║  COST (LOWER BILLS)                                              ║
║  ├─ Use Jobs Compute for scheduled pipelines                     ║
║  ├─ Serverless for bursty workloads (SQL, ad-hoc)                ║
║  ├─ Spot instances for batch (checkpoint often)                  ║
║  ├─ Autotermination: 15-30 minutes                               ║
║  ├─ Tag resources for cost allocation                            ║
║  └─ Monitor with system.billing.usage (full platform)            ║
║                                                                  ║
║  CONFIGURATION                                                   ║
║  ├─ spark.sql.shuffle.partitions = 2-3x cores                    ║
║  ├─ spark.sql.adaptive.enabled = true                            ║
║  ├─ spark.sql.autoBroadcastJoinThreshold = 10m                   ║
║  ├─ spark.sql.adaptive.skewJoin.enabled = true                   ║
║  └─ spark.databricks.delta.retentionDurationCheck.enabled = true ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
""")

```

print("End of 06_Performance_and_Cost notebook. All 10 concepts (#51–#60) covered.")
