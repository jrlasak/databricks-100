# 08 — Lakeflow Declarative Pipelines (Concepts #71–#80)

## Overview

Lakeflow Declarative Pipelines (formerly **Delta Live Tables / DLT**) is Databricks' declarative ETL
framework for building reliable, maintainable, and testable data pipelines. It handles
orchestration, error handling, quality enforcement, and state management automatically.

**IMPORTANT**: Lakeflow / DLT pipelines are **NOT available in Databricks Community Edition**.
This notebook therefore uses a dual approach throughout:

- **"With Lakeflow"** blocks (commented — show the declarative syntax)
- **"Manual Equivalent (Community Edition)"** blocks (executable — replicate the same logic)

### Concepts Covered
| # | Concept | Difficulty |
|---|---------|------------|
| 71 | Python vs. SQL Pipeline Syntax | Easy |
| 72 | Streaming Tables vs. Materialized Views | Medium |
| 73 | Expectations (Data Quality Rules) | Medium |
| 74 | Pipeline Development & Testing | Medium |
| 75 | Pipeline Monitoring & Event Log | Medium |
| 76 | Pipeline Modes: Triggered vs. Continuous | Medium |
| 77 | CDC Processing: AUTO CDC INTO | Hard |
| 78 | Error Handling & Dead Letter Patterns | Hard |
| 79 | Pipeline Parameters & Configuration | Hard |
| 80 | Multi-Pipeline Architecture | Hard |

### Dataset
We generate **synthetic retail sales data** throughout — orders, customers, products, and CDC
change events — stored in Delta Lake tables for realistic pipeline scenarios.

```python

```
## Setup — Environment & Synthetic Data

We create the three core datasets used across all ten concepts:
- **raw_orders** — streaming source of retail orders
- **customers** — customer dimension table
- **products** — product dimension table

```python

import os
import time
import json
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import DataStreamWriter
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Lakeflow_Concept_71_80") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

BASE_PATH = "/tmp/lakeflow_demo"
os.makedirs(BASE_PATH, exist_ok=True)
print(f"Working directory: {BASE_PATH}")
print(f"Spark version: {spark.version}")

```

```python

```
### Generate Synthetic Retail Data

```python

random.seed(42)

PRODUCT_IDS = [f"P{str(i).zfill(4)}" for i in range(1, 21)]
CATEGORIES  = ["Electronics", "Clothing", "Home", "Books", "Sports"]
CUSTOMER_IDS = [f"C{str(i).zfill(5)}" for i in range(1, 101)]
STATUSES    = ["pending", "shipped", "delivered", "cancelled"]
REGIONS     = ["US-West", "US-East", "EU-Central", "APAC-South"]

# --- Products Dimension ---
products_data = [(
    pid,
    f"Product-{pid}",
    random.choice(CATEGORIES),
    round(random.uniform(5.0, 500.0), 2),
    random.choice([True, False])
) for pid in PRODUCT_IDS]

products_df = spark.createDataFrame(products_data, schema=["product_id", "name", "category", "price", "active"])
products_df.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/products")
print(f"[OK] Products: {products_df.count()} rows → {BASE_PATH}/products")

# --- Customers Dimension ---
customers_data = [(
    cid,
    f"customer_{cid}@example.com",
    random.choice(["Basic", "Premium", "Enterprise"]),
    random.choice(REGIONS),
    (datetime.now() - timedelta(days=random.randint(1, 1095))).strftime("%Y-%m-%d")
) for cid in CUSTOMER_IDS]

customers_df = spark.createDataFrame(customers_data, schema=["customer_id", "email", "tier", "region", "signup_date"])
customers_df.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/customers")
print(f"[OK] Customers: {customers_df.count()} rows → {BASE_PATH}/customers")

# --- Raw Orders (append-friendly source) ---
def generate_orders(num_rows=200):
    ts = datetime.now() - timedelta(seconds=random.randint(0, 86400))
    return [(
        f"ORD-{ts.strftime('%Y%m%d%H%M%S')}-{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))}",
        random.choice(CUSTOMER_IDS),
        random.choice(PRODUCT_IDS),
        random.randint(1, 10),
        round(random.uniform(5.0, 500.0), 2),
        random.choice(STATUSES),
        random.choice(REGIONS),
        random.randint(0, 50),
        ts.strftime("%Y-%m-%d %H:%M:%S"),
        None
    ) for _ in range(num_rows)]

order_schema = T.StructType([
    T.StructField("order_id",    T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id",  T.StringType()),
    T.StructField("quantity",    T.IntegerType()),
    T.StructField("amount",      T.DoubleType()),
    T.StructField("status",      T.StringType()),
    T.StructField("region",      T.StringType()),
    T.StructField("discount_pct",T.IntegerType()),
    T.StructField("order_ts",    T.StringType()),
    T.StructField("cancellation_reason", T.StringType()),
])

batch1 = generate_orders(100)
batch2 = generate_orders(100)

orders_df = spark.createDataFrame(batch1 + batch2, schema=order_schema)
orders_df.write.format("delta").mode("append").save(f"{BASE_PATH}/raw_orders")
print(f"[OK] Raw Orders: {orders_df.count()} rows → {BASE_PATH}/raw_orders")

# --- Refresh all tables ---
spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW products  AS SELECT * FROM delta.`{BASE_PATH}/products`")
spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW customers AS SELECT * FROM delta.`{BASE_PATH}/customers`")
spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW raw_orders AS SELECT * FROM delta.`{BASE_PATH}/raw_orders`")
print("\nAll datasets ready.")

```

```python

```
---
## Concept 71 — Python vs. SQL Pipeline Syntax

**Difficulty: Easy**

Lakeflow supports two equivalent syntaxes for defining pipelines. The choice is about **team
preference**, not capability — both produce identical execution plans.

| Aspect | SQL Syntax | Python Syntax |
|--------|------------|----------------|
| Streaming table | `CREATE STREAMING TABLE` | `@table` decorator |
| Materialized view | `CREATE MATERIALIZED VIEW` | `@materialized_view` decorator |
| Temporary view | `CREATE TEMPORARY LIVE VIEW` | `@temporary_view` decorator |
| Expectations | Inline constraints | `expect()` / `expect_or_drop()` / `expect_or_fail()` |
| Legacy import | — | `import dlt` (still works) |

### Rule of Thumb
- **SQL preferred** when: analysts write pipelines, SQL logic is straightforward
- **Python preferred** when: complex transformations, ML feature engineering, testing with pytest
- Both can be **mixed** in the same pipeline notebook

```python

```
### Python Syntax (Lakeflow / DLT) — Commented Reference

```python

# =============================================================================
#      LAKEFLOW / DLT — PYTHON DECORATOR SYNTAX (not executable in CE)
# =============================================================================
# import dlt                          # Legacy import (still works)
# from pyspark.pipelines import table, materialized_view, temporary_view
#
# @table(
#     name="bronze_orders",
#     comment="Raw orders ingested into bronze layer",
#     table_properties={"pipelines.autoOptimize.managed": "true"}
# )
# def bronze_orders():
#     return spark.readStream.table("raw_orders")
#
# @table(name="silver_orders")
# def silver_orders():
#     return dlt.read("bronze_orders") \
#         .withColumn("processed_at", F.current_timestamp())
#
# @materialized_view(name="gold_daily_sales", schedule_cron="0 0 6 * * ?")
# def gold_daily_sales():
#     return dlt.read("silver_orders") \
#         .groupBy(F.window("processed_at", "1 day"), "region") \
#         .agg(F.sum("amount").alias("total_sales"))
#
# @temporary_view(name="tmp_filtered")
# def tmp_filtered():
#     return dlt.read("silver_orders").filter(F.col("amount") > 0)

# =============================================================================
#      MANUAL EQUIVALENT — COMMUNITY EDITION
# =============================================================================

bronze_orders = (
    spark.readStream
         .format("delta")
         .load(f"{BASE_PATH}/raw_orders")
         .withColumn("ingested_at", F.current_timestamp())
)

bronze_query = (
    bronze_orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{BASE_PATH}/_checkpoint/bronze")
    .trigger(processingTime="10 seconds")
    .start(f"{BASE_PATH}/bronze_orders")
)
print("Bronze streaming write started...")

time.sleep(8)

silver_orders = spark.read.format("delta").load(f"{BASE_PATH}/bronze_orders") \
    .withColumn("processed_at", F.current_timestamp())

silver_orders.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/silver_orders")
print(f"Silver orders created: {silver_orders.count()} rows")

gold_daily_sales = (
    spark.read.format("delta").load(f"{BASE_PATH}/silver_orders")
    .withColumn("order_date", F.to_date(F.col("order_ts")))
    .groupBy("order_date", "region")
    .agg(F.sum("amount").alias("total_sales"), F.count("*").alias("order_count"))
)

gold_daily_sales.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/gold_daily_sales")
print(f"Gold daily sales created: {gold_daily_sales.count()} rows")

# Stop streaming to keep things clean
bronze_query.stop()
print("Bronze stream stopped.")

```

```python

```
### SQL Syntax (Lakeflow / DLT) — Commented Reference

```python

# =============================================================================
#      LAKEFLOW / DLT — SQL SYNTAX (not executable in CE)
# =============================================================================
# -- Bronze: streaming ingestion
# CREATE OR REFRESH STREAMING TABLE bronze_orders_sql
# COMMENT "Raw orders ingested via SQL syntax"
# TBLPROPERTIES ("pipelines.autoOptimize.managed" = "true")
# AS SELECT
#     order_id,
#     customer_id,
#     product_id,
#     CAST(order_ts AS TIMESTAMP) AS order_ts,
#     quantity,
#     amount,
#     status,
#     region,
#     discount_pct,
#     current_timestamp() AS ingested_at
# FROM STREAM(LIVE.raw_orders);
#
# -- Silver: cleaned & deduplicated
# CREATE OR REFRESH STREAMING TABLE silver_orders_sql AS
# SELECT DISTINCT *
# FROM STREAM(LIVE.bronze_orders_sql)
# WHERE amount > 0;
#
# -- Gold: materialized aggregation
# CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales_sql AS
# SELECT
#     DATE(order_ts) AS order_date,
#     region,
#     SUM(amount) AS total_sales,
#     COUNT(*)    AS order_count
# FROM LIVE.silver_orders_sql
# GROUP BY DATE(order_ts), region;
#
# -- Temporary view
# CREATE OR REFRESH TEMPORARY LIVE VIEW tmp_high_value AS
# SELECT * FROM LIVE.silver_orders_sql WHERE amount > 200;

# =============================================================================
#      MANUAL EQUIVALENT — COMMUNITY EDITION (SQL-centric)
# =============================================================================

spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW orders_source AS SELECT * FROM delta.`{BASE_PATH}/raw_orders`")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW bronze_orders_sql AS
    SELECT
        order_id, customer_id, product_id,
        CAST(order_ts AS TIMESTAMP) AS order_ts,
        quantity, amount, status, region, discount_pct,
        current_timestamp() AS ingested_at
    FROM orders_source
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW silver_orders_sql AS
    SELECT DISTINCT *
    FROM bronze_orders_sql
    WHERE amount > 0
""")

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW gold_daily_sales_sql AS
    SELECT
        DATE(order_ts) AS order_date,
        region,
        SUM(amount) AS total_sales,
        COUNT(*)    AS order_count
    FROM silver_orders_sql
    GROUP BY DATE(order_ts), region
""")

print("SQL pipeline views created successfully.")
spark.sql("SELECT * FROM gold_daily_sales_sql ORDER BY order_date DESC").show(5)

```

```python

```
### Side-by-Side Comparison

```python

```

```
╔══════════════════════════════════════════════════════════════════════╗
║                      LAKEFLOW / DLT vs. MANUAL                       ║
╠═══════════════════════╦══════════════════════════════════════════════╣
║ With Lakeflow (DLT)   ║ Without Lakeflow (CE - Manual)               ║
╠═══════════════════════╬══════════════════════════════════════════════╣
║ @table decorator      ║ Manual readStream / writeStream orchestration║
║ Automatic lineage     ║ Must track lineage yourself                  ║
║ Built-in quality      ║ Must implement quality checks manually       ║
║ Auto-handles state    ║ Must manage checkpoints, schema evolution    ║
║ Declarative - "what"  ║ Imperative - "how"                           ║
║ Pipeline UI dashboard ║ No built-in pipeline monitoring              ║
║ Incremental refresh   ║ Must implement incremental logic manually    ║
║ SQL + Python unified  ║ Separate code paths for SQL vs PySpark       ║
╚═══════════════════════╩══════════════════════════════════════════════╝
```

```python

```
---
## Concept 72 — Streaming Tables vs. Materialized Views

**Difficulty: Medium**

### Core Difference

| | Streaming Table | Materialized View |
|---|---|---|
| **Write mode** | Append-only | Full recompute (overwrite) |
| **Use case** | Ingestion, CDC, event streams | Aggregations, joins, derived datasets |
| **Exactly-once** | Yes (Delta guarantees) | Yes (idempotent recompute) |
| **Refresh** | Incremental, near real-time | On schedule or pipeline trigger |
| **State management** | Auto-managed checkpoints | Full result recalc per refresh |
| **Schema evolution** | Auto-handled | Handled on each recompute |

In Lakeflow, you never write `writeStream` / `readStream` — the framework decides
whether incremental or full recompute is appropriate based on the declaration.

```python

```
### Manual Streaming Table (Community Edition)

```python

# =============================================================================
#      MANUAL STREAMING TABLE — Append-only ingestion to Delta
# =============================================================================

checkpoint_stream = f"{BASE_PATH}/_checkpoint/manual_streaming_table"

streaming_input = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 2)
    .load(f"{BASE_PATH}/raw_orders")
    .withColumn("streamed_at", F.current_timestamp())
)

stream_writer = (
    streaming_input.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_stream)
    .trigger(processingTime="5 seconds")
    .queryName("manual_streaming_table")
    .start(f"{BASE_PATH}/manual_streaming_table")
)

print("Manual streaming table started — append-only, exactly-once via Delta log")
time.sleep(10)
stream_writer.stop()
print("Streaming table stopped.")

# Show what was ingested
spark.read.format("delta").load(f"{BASE_PATH}/manual_streaming_table") \
    .select("order_id", "amount", "streamed_at") \
    .orderBy(F.desc("streamed_at")).show(5, False)

```

```python

```
### Manual Materialized View (Community Edition)

```python

# =============================================================================
#      MANUAL MATERIALIZED VIEW — Full recomputation of aggregations
# =============================================================================

def refresh_materialized_view():
    """Simulates Lakeflow's materialized view refresh — full recompute."""
    source = spark.read.format("delta").load(f"{BASE_PATH}/raw_orders") \
        .withColumn("order_date", F.to_date("order_ts"))

    mv_customer_metrics = (
        source.groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("amount").alias("lifetime_value"),
            F.avg("amount").alias("avg_order_value"),
            F.max("order_ts").alias("last_order"),
            F.collect_set("region").alias("regions_ordered_from")
        )
    )

    mv_customer_metrics.write.format("delta").mode("overwrite") \
        .save(f"{BASE_PATH}/mv_customer_metrics")
    return mv_customer_metrics

mv1 = refresh_materialized_view()
print(f"Materialized view created: {mv1.count()} rows")
mv1.show(5, False)

# Add more data, then re-refresh (simulating Lakeflow's incremental pipeline run)
more_orders = spark.createDataFrame(generate_orders(30), schema=order_schema)
more_orders.write.format("delta").mode("append").save(f"{BASE_PATH}/raw_orders")

mv2 = refresh_materialized_view()
print(f"\nAfter new data — re-refreshed: {mv2.count()} rows")
mv2.show(5, False)

```

```python

```
### Comparison Summary

```python

```

```
╔══════════════════════════════════════════════════════════════════════════════╗
║ STREAMING TABLE (Lakeflow)                                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ @table(name="bronze_orders")                                                ║
║ def bronze_orders():                                                        ║
║     return spark.readStream.table("raw_orders")                             ║
║                      ↓ DECLARATIVE ↓                                        ║
║ Lakeflow handles: checkpoints, retries, schema evolution, monitoring.       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ VS. MANUAL (Community Edition) — 30+ lines of boilerplate                    ║
║ .readStream → .writeStream → checkpoint dir → trigger config → monitor      ║
╚══════════════════════════════════════════════════════════════════════════════╝
║ MATERIALIZED VIEW (Lakeflow)                                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ @materialized_view(name="gold_daily", schedule_cron="0 0 * * *")            ║
║ def gold_daily():                                                           ║
║     return dlt.read("silver").groupBy(...).agg(...)                         ║
║                      ↓ DECLARATIVE ↓                                        ║
║ Lakeflow handles: recompute scheduling, overwrite, backfill, dependencies.  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ VS. MANUAL (Community Edition)                                              ║
║ Manual scheduled function + .mode("overwrite") + cron scheduler + monitoring║
╚══════════════════════════════════════════════════════════════════════════════╝
```

```python

```
---
## Concept 73 — Expectations (Data Quality Rules)

**Difficulty: Medium**

Lakeflow expectations are **inline data quality constraints** with three violation actions:

| Action | Lakeflow Syntax | Behavior |
|--------|----------------|----------|
| **Warn** | `expect("desc", "condition")` | Log metric, keep row |
| **Drop** | `expect_or_drop("desc", "condition")` | Log metric, discard row |
| **Fail** | `expect_or_fail("desc", "condition")` | Log metric, stop pipeline |

Metrics are tracked in the event log: pass %, fail %, row counts per expectation.

```python

```
### Lakeflow Expectations (Commented Reference)

```python

# =============================================================================
#      LAKEFLOW / DLT EXPECTATIONS (not executable in CE)
# =============================================================================
# import dlt
#
# @table(name="silver_orders_with_quality")
# @dlt.expect("valid_order_id",    "order_id IS NOT NULL AND length(order_id) > 10")
# @dlt.expect("positive_amount",   "amount > 0")
# @dlt.expect_or_drop("valid_status_simple", "status IN ('pending','shipped','delivered','cancelled')")
# @dlt.expect_or_fail("has_customer", "customer_id IS NOT NULL")
# def silver_orders_with_quality():
#     return dlt.read("bronze_orders") \
#         .withColumn("processed_at", F.current_timestamp())
#
# # SQL equivalent:
# # CREATE OR REFRESH STREAMING TABLE silver_sql
# # CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
# # CONSTRAINT has_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
# # AS SELECT * FROM STREAM(LIVE.bronze_orders);

```

```python

```
### Manual Expectation Framework (Community Edition)

```python

class ExpectationFramework:
    """Replicates Lakeflow expectation behavior manually."""

    def __init__(self, name="default"):
        self.name = name
        self.metrics = {}
        self.dropped_rows = {"total": 0}

    def expect(self, df, description, condition, action="warn"):
        """Apply an expectation: warn, drop, or fail."""
        passed = df.filter(condition)
        failed = df.filter(~condition)

        pass_count = passed.count()
        fail_count = failed.count()
        total = pass_count + fail_count
        pass_pct = round(pass_count / total * 100, 2) if total > 0 else 100.0

        metric_key = f"{description} ({action})"
        self.metrics[metric_key] = {
            "passed": pass_count, "failed": fail_count,
            "pass_pct": pass_pct, "action": action
        }

        if action == "fail" and fail_count > 0:
            raise ValueError(
                f"❌ Expectation FAILED: '{description}' — {fail_count} violations. Pipeline stopped."
            )

        if action == "drop":
            self.dropped_rows.setdefault(description, 0)
            self.dropped_rows[description] += fail_count
            self.dropped_rows["total"] += fail_count
            print(f"  ⚠️  [DROP] '{description}': {fail_count}/{total} rows dropped")
            return passed

        if action == "warn":
            print(f"  ⚠️  [WARN] '{description}': {fail_count}/{total} rows ({pass_pct}% passed)")
            return df

        return df

    def expect_not_null(self, df, col, action="warn"):
        return self.expect(df, f"{col} IS NOT NULL", F.col(col).isNotNull(), action)

    def expect_unique(self, df, col, action="warn"):
        """Check if column values are unique."""
        total = df.count()
        distinct = df.select(col).distinct().count()
        violated = total - distinct
        condition = F.lit(True)  # We calculate separately above

        metric_key = f"{col} IS UNIQUE ({action})"
        self.metrics[metric_key] = {
            "passed": distinct, "failed": violated,
            "pass_pct": round(distinct / total * 100, 2) if total > 0 else 100.0,
            "action": action
        }

        if action == "fail" and violated > 0:
            raise ValueError(f"❌ Expectation FAILED: '{col} IS UNIQUE' — {violated} duplicates.")
        if action == "drop":
            print(f"  ⚠️  [DROP] '{col} IS UNIQUE': {violated} duplicate rows dropped")
            return df.dropDuplicates([col])

        print(f"  ⚠️  [WARN] '{col} IS UNIQUE': {violated}/{total} duplicates")
        return df

    def expect_range(self, df, col, min_val, max_val, action="warn"):
        return self.expect(df, f"{col} BETWEEN {min_val} AND {max_val}",
                          (F.col(col) >= min_val) & (F.col(col) <= max_val), action)

    def report(self):
        """Print quality report à la Lakeflow event log."""
        print(f"\n{'='*70}")
        print(f"  QUALITY REPORT — Pipeline: {self.name}")
        print(f"{'='*70}")
        for metric, stats in self.metrics.items():
            icon = "✅" if stats["pass_pct"] == 100.0 else "⚠️"
            print(f"  {icon} {metric}: {stats['passed']}/{stats['passed']+stats['failed']} "
                  f"({stats['pass_pct']}%) — action={stats['action']}")
        print(f"  🗑️  Total dropped rows: {self.dropped_rows['total']}")
        print(f"{'='*70}")


# --- DEMO: Apply expectations ---
raw_df = spark.read.format("delta").load(f"{BASE_PATH}/raw_orders")
print(f"Raw data: {raw_df.count()} rows\n")

ef = ExpectationFramework(name="silver_quality_demo")

clean_df = ef.expect_not_null(raw_df, "order_id", action="fail")
clean_df = ef.expect_not_null(clean_df, "customer_id", action="fail")
clean_df = ef.expect(clean_df, "positive_amount", F.col("amount") > 0, action="drop")
clean_df = ef.expect(clean_df, "valid_status",
                     F.col("status").isin("pending", "shipped", "delivered", "cancelled"), action="drop")
clean_df = ef.expect_range(clean_df, "discount_pct", 0, 100, action="warn")
clean_df = ef.expect(clean_df, "amount_not_outlier",
                     (F.col("amount") > 0) & (F.col("amount") < 1500), action="warn")

clean_df.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/silver_orders_quality")
print(f"\nCleaned data: {clean_df.count()} rows → {BASE_PATH}/silver_orders_quality")

ef.report()

```

```python

```
### Quality Monitoring Table

```python

# Build a persistent quality monitoring table (simulates Lakeflow event log)
quality_records = []
for metric, stats in ef.metrics.items():
    quality_records.append((
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "silver_quality_demo",
        metric,
        stats["passed"],
        stats["failed"],
        stats["pass_pct"],
        stats["action"]
    ))

quality_schema = T.StructType([
    T.StructField("run_ts",        T.StringType()),
    T.StructField("pipeline_name", T.StringType()),
    T.StructField("expectation",   T.StringType()),
    T.StructField("passed",        T.IntegerType()),
    T.StructField("failed",        T.IntegerType()),
    T.StructField("pass_pct",      T.DoubleType()),
    T.StructField("action",        T.StringType()),
])

quality_df = spark.createDataFrame(quality_records, schema=quality_schema)
quality_df.write.format("delta").mode("append").save(f"{BASE_PATH}/pipeline_quality_log")

print("Quality monitoring table:")
spark.read.format("delta").load(f"{BASE_PATH}/pipeline_quality_log").show(truncate=False)

```

```python

```
---
## Concept 74 — Pipeline Development & Testing

**Difficulty: Medium**

Lakeflow supports two operational modes:

| Mode | Behavior | Pipeline Setting |
|------|----------|-----------------|
| **Development** | Uses `dev_` prefix, stops on failure, cheaper clusters | `development: true` |
| **Production** | No prefix, continues on expectation failures | `development: false` (default) |

**Refresh types:**
- **Full refresh**: reprocesses all data from scratch
- **Incremental**: only processes new data since last run

Testing involves creating representative data fixtures and asserting outputs.

```python

```
### Test Data Fixtures (Community Edition)

```python

# =============================================================================
#      TEST FIXTURES — Representative data for pipeline testing
# =============================================================================

def create_test_fixtures():
    """Create known test datasets for deterministic pipeline testing."""

    # Clean test data
    orders_test_clean = spark.createDataFrame([
        ("ORD-TEST-001", "C00001", "P0001", 2,  99.99, "shipped",   "US-West",  10, "2024-01-15 10:30:00"),
        ("ORD-TEST-002", "C00002", "P0002", 1, 249.50, "delivered", "US-East",  0,  "2024-01-15 11:00:00"),
        ("ORD-TEST-003", "C00001", "P0003", 5,  50.00, "pending",   "EU-Central", 5, "2024-01-15 12:00:00"),
    ], schema=order_schema)

    # Dirty test data (for expectation/error handling tests)
    orders_test_dirty = spark.createDataFrame([
        (None, "C00001", "P0001", 2,  99.99, "shipped", "US-West",  10, "2024-01-15 10:30:00"),   # null order_id
        ("ORD-TEST-004", None, "P0002", 1, -50.00, "shipped", "US-East", 0, "2024-01-15 11:00:00"),  # null customer, neg amount
        ("ORD-TEST-005", "C00002", "P0003", 0,   0.00, "unknown", "??", -5, "2024-01-15 12:00:00"),  # bad values
        ("ORD-TEST-006", "C00003", "P0004", 5, 250.00, "shipped", "APAC-South", 5, "2024-01-15 13:00:00"), # clean
    ], schema=order_schema)

    return orders_test_clean, orders_test_dirty


clean_test, dirty_test = create_test_fixtures()
print(f"Clean fixture: {clean_test.count()} rows")
print(f"Dirty fixture: {dirty_test.count()} rows")
clean_test.show(truncate=False)

```

```python

```
### Pipeline Testing Patterns (Community Edition)

```python

# =============================================================================
#      LOCAL PIPELINE VALIDATION — Test before "deployment"
# =============================================================================

def transform_silver(input_df):
    """The actual transformation logic — testable in isolation."""
    return (
        input_df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("amount") > 0)
        .filter(F.col("status").isin("pending", "shipped", "delivered", "cancelled"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("total_amount", F.col("quantity") * F.col("amount"))
    )


def test_pipeline_logic(verbose=True):
    """Run assertions against known fixtures."""
    results = {"passed": 0, "failed": 0, "details": []}

    # Test 1: Clean data passes through unchanged
    output = transform_silver(clean_test)
    assert output.count() == 3, f"Expected 3 clean rows, got {output.count()}"
    results["passed"] += 1
    results["details"].append("✅ Test 1: Clean data — 3 rows pass through")

    # Test 2: null order_id is filtered out
    dirty_output = transform_silver(dirty_test)
    assert dirty_output.count() == 1, f"Expected 1 row from dirty data, got {dirty_output.count()}"
    results["passed"] += 1
    results["details"].append("✅ Test 2: Dirty data — only 1 row survives filtering")

    # Test 3: No negative amounts
    amounts = [r.amount for r in dirty_output.select("amount").collect()]
    assert all(a > 0 for a in amounts), f"Found non-positive amounts: {amounts}"
    results["passed"] += 1
    results["details"].append("✅ Test 3: All amounts positive after filtering")

    # Test 4: All statuses are valid
    valid_set = {"pending", "shipped", "delivered", "cancelled"}
    statuses = set(r.status for r in dirty_output.select("status").collect())
    assert statuses.issubset(valid_set), f"Invalid status found: {statuses - valid_set}"
    results["passed"] += 1
    results["details"].append(f"✅ Test 4: All statuses valid: {statuses}")

    # Test 5: Schema check — processed_at column exists
    assert "processed_at" in output.columns, "processed_at column missing"
    results["passed"] += 1
    results["details"].append("✅ Test 5: Schema — processed_at column present")

    # Test 6: total_amount computed correctly
    row = output.filter(F.col("order_id") == "ORD-TEST-001").first()
    assert row.total_amount == 199.98, f"Expected 199.98, got {row.total_amount}"
    results["passed"] += 1
    results["details"].append("✅ Test 6: total_amount = quantity * amount computed correctly")

    if verbose:
        print("=" * 60)
        print("  PIPELINE TESTS RESULTS")
        print("=" * 60)
        for detail in results["details"]:
            print(f"  {detail}")
        print(f"\n  {results['passed']} passed, {results['failed']} failed")
        print("=" * 60)

    return results


test_results = test_pipeline_logic()

```

```python

```
---
## Concept 75 — Pipeline Monitoring & Event Log

**Difficulty: Medium**

Lakeflow automatically writes an **event log** to a `__events__` table in the storage
# MV location. The event log contains:
- **Flow events**: table creates, updates, drops
- **Progress events**: row counts, processing times
- **Expectation events**: quality metric pass/fail/drop counts
- **Cluster events**: resource allocation

Queries against the event log enable monitoring, alerting, and lineage tracking.

```python

```
### Lakeflow Event Log Access (Commented Reference)

```python

# =============================================================================
#      LAKEFLOW / DLT — EVENT LOG QUERIES (not executable in CE)
# =============================================================================
# # Query the event log directly
# SELECT
#     timestamp,
#     details.flow_definition.output_table_name AS table_name,
#     details.flow_progress.num_output_rows    AS rows_written,
#     details.flow_progress.execution_time_ms  AS execution_ms
# FROM event_log("my_pipeline_storage_mv")
# WHERE event_type = 'flow_progress'
# ORDER BY timestamp DESC;
#
# # Get expectation metrics
# SELECT
#     details.expectation.name       AS expectation,
#     details.expectation.passed_records AS passed,
#     details.expectation.failed_records AS failed,
#     details.expectation.action     AS action
# FROM event_log("my_pipeline_storage_mv")
# WHERE event_type = 'expectation_violation';
#
# # Lineage tracking
# SELECT * FROM event_log("my_pipeline") WHERE event_type = 'flow_definition';

```

```python

```
### Manual Pipeline Monitoring Framework (Community Edition)

```python

import uuid

class PipelineRunTracker:
    """Mimics Lakeflow event log for manual pipeline monitoring."""

    def __init__(self, pipeline_name, storage_location=BASE_PATH):
        self.pipeline_name = pipeline_name
        self.run_id = str(uuid.uuid4())[:8]
        self.log_path = f"{storage_location}/_monitoring/pipeline_runs"
        self.events = []
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = datetime.now()
        self._record("pipeline_start", {
            "run_id": self.run_id, "pipeline": self.pipeline_name,
            "start_ts": self.start_time.isoformat()
        })
        print(f"▶️  Pipeline '{self.pipeline_name}' started — run_id={self.run_id}")
        return self

    def log_step(self, step_name, rows_in, rows_out, duration_ms, error=None):
        event = {
            "run_id": self.run_id,
            "step": step_name,
            "rows_input": rows_in,
            "rows_output": rows_out,
            "duration_ms": duration_ms,
            "error": str(error) if error else None,
            "ts": datetime.now().isoformat()
        }
        self.events.append(event)
        status = "❌" if error else "✅"
        print(f"  {status} {step_name}: {rows_in}→{rows_out} rows ({duration_ms}ms)")
        return self

    def log_quality(self, expectation_name, passed, failed, action):
        event = {
            "run_id": self.run_id,
            "event_type": "quality_metric",
            "expectation": expectation_name,
            "passed": passed,
            "failed": failed,
            "action": action,
            "ts": datetime.now().isoformat()
        }
        self.events.append(event)
        return self

    def finish(self, success=True):
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        self._record("pipeline_finish", {
            "run_id": self.run_id,
            "success": success,
            "duration_sec": duration,
            "steps": len([e for e in self.events if "step" in e.get("event_type", e.get("step", ""))])
        })
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"⏹️  Pipeline '{self.pipeline_name}' {status} — {duration:.1f}s")

        # Persist all events to Delta
        self._persist()
        return self

    def _record(self, event_type, data):
        self.events.append({"event_type": event_type, **data})

    def _persist(self):
        df = spark.createDataFrame(self.events)
        df.write.format("delta").mode("append").save(self.log_path)


# --- DEMO: Track a pipeline run ---
tracker = PipelineRunTracker("daily_retail_etl")
tracker.start()

# Simulate multi-step pipeline
raw_count = spark.read.format("delta").load(f"{BASE_PATH}/raw_orders").count()
tracker.log_step("read_raw", None, raw_count, 120)

t0 = time.time()
silver = transform_silver(raw_df)
silver_count = silver.count()
tracker.log_step("clean_silver", raw_count, silver_count, int((time.time()-t0)*1000))

t0 = time.time()
gold = (
    silver.withColumn("order_date", F.to_date("order_ts"))
    .groupBy("order_date", "region")
    .agg(F.sum("amount").alias("daily_revenue"))
)
gold_count = gold.count()
tracker.log_step("aggregate_gold", silver_count, gold_count, int((time.time()-t0)*1000))

# Log quality metrics
tracker.log_quality("amount > 0", silver_count, raw_count - silver_count, "drop")
tracker.log_quality("status valid", silver_count, 0, "drop")

tracker.finish(success=True)

```

```python

```
### Querying Run History

```python

# =============================================================================
#      QUERY PIPELINE RUN HISTORY
# =============================================================================
monitoring_path = f"{BASE_PATH}/_monitoring/pipeline_runs"

try:
    run_history = spark.read.format("delta").load(monitoring_path)

    print("=== Recent Pipeline Runs ===")
    run_history.filter(F.col("event_type").isin("pipeline_start", "pipeline_finish")) \
        .select("event_type", "pipeline", "run_id", "success", "duration_sec") \
        .orderBy("ts", ascending=False) \
        .show(truncate=False)

    print("\n=== Step-Level Details ===")
    run_history.filter(F.col("step").isNotNull()) \
        .select("run_id", "step", "rows_input", "rows_output", "duration_ms", "error") \
        .show(truncate=False)

except Exception as e:
    print(f"No run history yet: {e}")

```

```python

```
---
## Concept 76 — Pipeline Modes: Triggered vs. Continuous

**Difficulty: Medium**

Lakeflow supports two execution modes that map closely to Structured Streaming triggers:

| Mode | Behavior | Use Case | Trigger |
|------|----------|----------|---------|
| **Triggered** | Processes available data, then stops | Scheduled batch ETL | `availableNow` |
| **Continuous** | Always running, sub-second latency | Real-time dashboards | `processingTime` |

**Cost/Latency Tradeoff**:
- Triggered = cheaper (clusters shut down between runs) but higher latency
- Continuous = lower latency but higher cost (always-on compute)

```python

```
### Lakeflow Mode Configuration (Commented Reference)

```python

# =============================================================================
#      LAKEFLOW / DLT — PIPELINE MODE (configured in pipeline settings, not code)
# =============================================================================
# # In pipeline configuration JSON:
# {
#   "name": "triggered_daily_etl",
#   "edition": "ADVANCED",
#   "continuous": false,         # ← TRIGGERED MODE
#   "development": false
# }
#
# # vs.
# {
#   "name": "continuous_realtime",
#   "edition": "ADVANCED",
#   "continuous": true,          # ← CONTINUOUS MODE
#   "development": false
# }

```

```python

```
### Manual — Triggered Mode (availableNow)

```python

# =============================================================================
#      MANUAL — TRIGGERED PIPELINE (availableNow)
#      Processes all available data, then stops.
# =============================================================================
checkpoint_triggered = f"{BASE_PATH}/_checkpoint/triggered_demo"

triggered_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 5)
    .load(f"{BASE_PATH}/raw_orders")
    .withColumn("trigger_type", F.lit("TRIGGERED"))
    .withColumn("processed_at", F.current_timestamp())
)

triggered_query = (
    triggered_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_triggered)
    .trigger(availableNow=True)           # ← "triggered" mode
    .queryName("triggered_pipeline")
    .start(f"{BASE_PATH}/silver_triggered")
)

triggered_query.awaitTermination()
print(f"Triggered pipeline finished. Status: {triggered_query.status['message']}")
spark.read.format("delta").load(f"{BASE_PATH}/silver_triggered").show(5)

```

```python

```
### Manual — Continuous Mode (processingTime)

```python

# =============================================================================
#      MANUAL — CONTINUOUS PIPELINE (processingTime)
#      Always running — processes micro-batches every N seconds.
# =============================================================================
checkpoint_continuous = f"{BASE_PATH}/_checkpoint/continuous_demo"

continuous_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)
    .load(f"{BASE_PATH}/raw_orders")
    .withColumn("trigger_type", F.lit("CONTINUOUS"))
    .withColumn("processed_at", F.current_timestamp())
)

continuous_query = (
    continuous_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_continuous)
    .trigger(processingTime="5 seconds")   # ← "continuous" mode
    .queryName("continuous_pipeline")
    .start(f"{BASE_PATH}/silver_continuous")
)

print("Continuous pipeline running — will stop after 12 seconds for demo...")
time.sleep(12)
continuous_query.stop()
print("Continuous query stopped.")

spark.read.format("delta").load(f"{BASE_PATH}/silver_continuous") \
    .select("order_id", "trigger_type", "processed_at").show(5, False)

```

```python

```
### Mode Comparison Summary

```python

```

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                         TRIGGERED vs. CONTINUOUS                             ║
╠═══════════════════════╦══════════════════════════════════════════════════════╣
║ Triggered             ║ Continuous                                          ║
╠═══════════════════════╬══════════════════════════════════════════════════════╣
║ .trigger(availableNow)║ .trigger(processingTime="X seconds")                 ║
║ Stops when done       ║ Runs until explicitly stopped                       ║
║ Minutes latency       ║ Sub-second latency                                  ║
║ Lower cost (ephemeral)║ Higher cost (always-on)                             ║
║ Good for: daily ETL   ║ Good for: real-time dashboards, alerting            ║
║ Lakeflow continuous:  ║ Lakeflow triggered:                                 ║
║   "continuous": false  ║   "continuous": true                                ║
╚═══════════════════════╩══════════════════════════════════════════════════════╝
```

```python

```
---
## Concept 77 — CDC Processing: AUTO CDC INTO

**Difficulty: Hard**

CDC (Change Data Capture) processes row-level changes (INSERT, UPDATE, DELETE) from
# MGIC source systems. Lakeflow provides **`AUTO CDC INTO`** (formerly `APPLY CHANGES INTO`)
# MGIC to handle this declaratively:

- **SCD Type 1**: Overwrites old values with new values (no history)
- **SCD Type 2**: Tracks full history with `__START_AT` and `__END_AT` columns

Key concepts:
- **Sequencing column**: determines order of changes (e.g., `sequence_num`, `timestamp`)
- **Primary keys**: identify which rows to update
- **Deduplication**: latest change per sequence wins

```python

```
### Generate Synthetic CDC Events

```python

# =============================================================================
#      SYNTHETIC CDC FEED — Simulates database change log
# =============================================================================

cdc_events = spark.createDataFrame([
    # (op, customer_id, email, tier, region, signup_date, sequence)
    ("INSERT", "C00001", "alice@example.com",    "Basic",    "US-West",    "2024-01-10", 1),
    ("INSERT", "C00002", "bob@example.com",      "Basic",    "US-East",    "2024-01-11", 2),
    ("INSERT", "C00003", "charlie@example.com",  "Premium",  "EU-Central", "2024-01-12", 3),
    ("UPDATE", "C00001", "alice@example.com",    "Premium",  "US-West",    "2024-01-10", 4),   # Alice upgraded
    ("INSERT", "C00004", "diana@example.com",    "Basic",    "APAC-South", "2024-01-13", 5),
    ("UPDATE", "C00002", "bob.new@example.com",  "Enterprise","US-East",   "2024-01-11", 6),   # Bob changed email + tier
    ("DELETE", "C00003", "charlie@example.com",  "Premium",  "EU-Central", "2024-01-12", 7),   # Charlie deleted
    ("INSERT", "C00005", "eve@example.com",      "Basic",    "EU-Central", "2024-01-14", 8),
    ("UPDATE", "C00001", "alice.new@example.com","Enterprise","US-West",   "2024-01-10", 9),   # Alice changed again
], schema=["op", "customer_id", "email", "tier", "region", "signup_date", "sequence"])

cdc_events.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/cdc_feed")
print("CDC events:")
cdc_events.orderBy("sequence").show(truncate=False)

```

```python

```
### Lakeflow AUTO CDC INTO (Commented Reference)

```python

# =============================================================================
#      LAKEFLOW / DLT — AUTO CDC INTO (not executable in CE)
# =============================================================================
# -- SQL SYNTAX (preferred for CDC)
# CREATE OR REFRESH STREAMING TABLE customers_scd2;
#
# APPLY CHANGES INTO LIVE.customers_scd2       -- or AUTO CDC INTO
# FROM STREAM(LIVE.cdc_feed)
# KEYS (customer_id)
# SEQUENCE BY sequence
# COLUMNS * EXCEPT (op)
# STORE AS SCD TYPE 2;
#
# -- Python syntax:
# import dlt
# dlt.create_streaming_table("customers_scd2")
#
# dlt.apply_changes(
#     target="customers_scd2",
#     source="cdc_feed",
#     keys=["customer_id"],
#     sequence_by=F.col("sequence"),
#     apply_as_deletes=F.expr("op = 'DELETE'"),
#     stored_as_scd_type=2
# )
#
# # Result table automatically gets:
# #   __START_AT        — when the row version became active
# #   __END_AT          — when the row version was superseded (null = current)
# #   __is_deleted      — flag for deleted records

```

```python

```
### Manual CDC Processing — SCD Type 2 (Community Edition)

```python

# =============================================================================
#      MANUAL CDC — SCD TYPE 2 IMPLEMENTATION
# =============================================================================

def apply_cdc_scd2(source_df, target_path, keys, sequence_col, op_col="op",
                   delete_op="DELETE", insert_op="INSERT", update_op="UPDATE"):
    """
    Manual implementation of CDC SCD Type 2 (mirrors Lakeflow APPLY CHANGES).
    """

    from delta.tables import DeltaTable

    target_full_path = f"{BASE_PATH}/{target_path}"

    # 1. Get the latest version of each key (deduplication by sequence)
    latest_changes = (
        source_df
        .withColumn("_rn", F.row_number().over(
            F.Window.partitionBy(keys).orderBy(F.desc(sequence_col))
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Separate inserts/deletes/updates
    deletes = latest_changes.filter(F.col(op_col) == delete_op).select(keys)
    inserts = latest_changes.filter(F.col(op_col) == insert_op)
    updates = latest_changes.filter(F.col(op_col) == update_op)

    if not DeltaTable.isDeltaTable(spark, target_full_path):
        # First run: initialize target table
        inserts \
            .withColumn("__START_AT", F.current_timestamp()) \
            .withColumn("__END_AT", F.lit(None).cast("timestamp")) \
            .write.format("delta").mode("overwrite").save(target_full_path)
        print(f"[INIT] Created SCD2 table: {target_full_path} with {inserts.count()} rows")
        return DeltaTable.forPath(spark, target_full_path)

    target = DeltaTable.forPath(spark, target_full_path)
    now = F.current_timestamp()

    # 2. Handle DELETES: close current records
    if deletes.count() > 0:
        join_cond = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        target.alias("target").merge(
            deletes.alias("source"), join_cond
        ).whenMatchedUpdate(
            condition="target.__END_AT IS NULL",
            set={"__END_AT": "current_timestamp()"}
        ).execute()
        print(f"[CDC] Closed {deletes.count()} deleted record(s)")

    # 3. Handle UPDATES: close old version + insert new version
    if updates.count() > 0:
        # Step A: Close current versions
        join_cond = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        target.alias("target").merge(
            updates.alias("source"), join_cond
        ).whenMatchedUpdate(
            condition="target.__END_AT IS NULL",
            set={"__END_AT": "current_timestamp()"}
        ).execute()

        # Step B: Insert new rows (only columns that exist in target)
        existing_cols = [f.name for f in target.toDF().schema.fields
                        if f.name not in ("__START_AT", "__END_AT")]
        cols_to_insert = [c for c in existing_cols if c in updates.columns]

        new_versions = (
            updates.select(*cols_to_insert)
            .withColumn("__START_AT", F.current_timestamp())
            .withColumn("__END_AT", F.lit(None).cast("timestamp"))
        )
        new_versions.write.format("delta").mode("append").save(target_full_path)
        print(f"[CDC] Closed + inserted {updates.count()} updated record(s)")

    # 4. Handle INSERTS
    if inserts.count() > 0:
        inserts \
            .withColumn("__START_AT", F.current_timestamp()) \
            .withColumn("__END_AT", F.lit(None).cast("timestamp")) \
            .write.format("delta").mode("append").save(target_full_path)
        print(f"[CDC] Inserted {inserts.count()} new record(s)")

    return DeltaTable.forPath(spark, target_full_path)


# --- DEMO: Apply CDC to customer dimension ---
cdc_source = spark.read.format("delta").load(f"{BASE_PATH}/cdc_feed")

target = apply_cdc_scd2(
    source_df=cdc_source,
    target_path="customers_scd2",
    keys=["customer_id"],
    sequence_col="sequence",
)

print("\n=== SCD2 Target Table (current records only) ===")
target.toDF() \
    .filter(F.col("__END_AT").isNull()) \
    .select("customer_id", "email", "tier", "region", "__START_AT") \
    .orderBy("customer_id") \
    .show(truncate=False)

print("\n=== Full History (all versions) ===")
target.toDF() \
    .select("customer_id", "email", "tier", "__START_AT", "__END_AT") \
    .orderBy("customer_id", "__START_AT") \
    .show(truncate=False)

print(f"\n✅ Alice started Basic → Premium → Enterprise (3 versions)")
print(f"✅ Bob started Basic → Enterprise (2 versions, email changed)")
print(f"✅ Charlie deleted — __END_AT is set (0 current records)")

```

```python

```
### CDC SCD Type 1 (No History) — Manual Equivalent

```python

# =============================================================================
#      MANUAL CDC — SCD TYPE 1 (Overwrite, no history)
# =============================================================================
def apply_cdc_scd1(source_df, target_path, keys, sequence_col):
    from delta.tables import DeltaTable
    target_full_path = f"{BASE_PATH}/{target_path}"

    latest = (
        source_df
        .withColumn("_rn", F.row_number().over(
            F.Window.partitionBy(keys).orderBy(F.desc(sequence_col))
        ))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if not DeltaTable.isDeltaTable(spark, target_full_path):
        latest.write.format("delta").mode("overwrite").save(target_full_path)
        print(f"[INIT] Created SCD1 table: {target_full_path}")
        return

    join_cond = " AND ".join([f"target.{k} = source.{k}" for k in keys])
    DeltaTable.forPath(spark, target_full_path).alias("target").merge(
        latest.alias("source"), join_cond
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print(f"[CDB] SCD1 upserted {latest.count()} rows")


apply_cdc_scd1(
    source_df=cdc_source.filter(F.col("op") != "DELETE"),
    target_path="customers_scd1",
    keys=["customer_id"],
    sequence_col="sequence",
)

print("\nSCD1 Table (overwritten — no history):")
spark.read.format("delta").load(f"{BASE_PATH}/customers_scd1") \
    .orderBy("customer_id").show(truncate=False)

```

```python

```
---
## Concept 78 — Error Handling & Dead Letter Patterns

**Difficulty: Hard**

In production pipelines, not all data is clean. Lakeflow provides:

- **`expect_or_drop`**: Route bad records out of the main pipeline
- **`expect_or_fail`**: Halt pipeline on critical violations
- **On violation metrics**: Track error rates in event log

The **Dead Letter Queue (DLQ)** pattern captures bad records for later inspection:
- Main table → only valid records
- Dead letter table → invalid records with error metadata

```python

```
### Lakeflow Dead Letter Pattern (Commented Reference)

```python

# =============================================================================
#      LAKEFLOW / DLT — DEAD LETTER PATTERN (not executable in CE)
# =============================================================================
# @table(name="silver_orders_valid")
# @dlt.expect_or_drop("valid_order_id",   "order_id IS NOT NULL")
# @dlt.expect_or_drop("positive_amount",  "amount > 0")
# @dlt.expect_or_drop("valid_status",     "status IN ('pending','shipped','delivered','cancelled')")
# def silver_orders_valid():
#     return dlt.read("bronze_orders")
#
# @table(name="silver_orders_dead_letter")
# def silver_orders_dead_letter():
#     return dlt.read("bronze_orders") \
#         .filter("order_id IS NULL OR amount <= 0 OR status NOT IN ('pending','shipped','delivered','cancelled')") \
#         .withColumn("error_message", F.concat_ws(", ",
#             F.when(F.col("order_id").isNull(), "missing_order_id"),
#             F.when(F.col("amount") <= 0, "invalid_amount"),
#             F.when(~F.col("status").isin("pending","shipped","delivered","cancelled"), "invalid_status")
#         ))

```

```python

```
### Manual Dead Letter Queue (Community Edition)

```python

# =============================================================================
#      MANUAL DEAD LETTER QUEUE — Error categorization and quarantine
# =============================================================================

def build_dead_letter_pipeline(source_df, rules, valid_output_path, dlq_output_path):
    """
    Routes valid rows to the main output and invalid rows to a dead letter queue.

    rules: list of (name, condition, error_message) tuples
    """
    # Build the "is valid" composite condition
    valid_condition = F.lit(True)
    for name, condition, _ in rules:
        valid_condition = valid_condition & condition

    # --- Main output: valid records only ---
    valid_df = source_df.filter(valid_condition) \
        .withColumn("pipeline_status", F.lit("VALID")) \
        .withColumn("processed_at", F.current_timestamp())

    valid_df.write.format("delta").mode("overwrite").save(valid_output_path)
    print(f"[MAIN] Valid records: {valid_df.count()} → {valid_output_path}")

    # --- DLQ output: invalid records with error details ---
    dlq_df = source_df.filter(~valid_condition)
    if dlq_df.count() > 0:
        dlq_df = dlq_df.drop("pipeline_status") if "pipeline_status" in dlq_df.columns else dlq_df
        dlq_df = dlq_df.withColumn("pipeline_status", F.lit("ERROR"))

        # Categorize each error
        for name, condition, err_msg in rules:
            dlq_df = dlq_df.withColumn(
                f"err_{name}",
                F.when(~condition, F.lit(err_msg)).otherwise(F.lit(None))
            )

        # Combine all errors into a single column
        err_cols = [f"err_{name}" for name, _, _ in rules]
        dlq_df = dlq_df.withColumn(
            "error_messages",
            F.concat_ws("; ", *[F.col(c) for c in err_cols])
        ).withColumn("error_timestamp", F.current_timestamp())

        # Keep only relevant metadata
        dlq_output = dlq_df.select(
            "order_id", "customer_id", "product_id", "amount", "status",
            "error_messages", "error_timestamp", "pipeline_status",
            *[c for c in dlq_df.columns if c not in err_cols and c not in [
                "error_messages", "error_timestamp", "pipeline_status", "order_id",
                "customer_id", "product_id", "amount", "status"
            ]]
        )

        dlq_output.write.format("delta").mode("overwrite").save(dlq_output_path)
        print(f"[DLQ]  Dead letter records: {dlq_output.count()} → {dlq_output_path}")
    else:
        print("[DLQ]  No dead letter records — all data valid")

    return valid_df


# --- Define quality rules ---
quality_rules = [
    ("null_order_id",    F.col("order_id").isNotNull(),
     "order_id is null"),
    ("null_customer_id", F.col("customer_id").isNotNull(),
     "customer_id is null"),
    ("positive_amount",  F.col("amount") > 0,
     "amount <= 0"),
    ("valid_status",     F.col("status").isin("pending", "shipped", "delivered", "cancelled"),
     f"invalid status: not in (pending,shipped,delivered,cancelled)"),
    ("non_zero_qty",     F.col("quantity") > 0,
     "quantity <= 0"),
    ("valid_discount",   (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 100),
     "discount_pct out of range [0,100]"),
]

# Test with dirty data
dirty_source = spark.createDataFrame([
    (None, "C00001", "P0001", 2,  99.99, "shipped",   "US-West",  10, "2024-01-15 10:00:00"),  # null order_id
    ("ORD-001", None, "P0001", 2,  99.99, "shipped",   "US-West",  10, "2024-01-15 10:00:00"),  # null customer
    ("ORD-002", "C00002", "P0001", 0, -10.00, "UNKNOWN","??",      -5, "2024-01-15 10:00:00"),  # multiple errors
    ("ORD-003", "C00003", "P0002", 5, 250.00, "shipped", "US-East", 5, "2024-01-15 10:00:00"),   # clean
    ("ORD-004", "C00004", "P0003", 1,   0.00, "cancelled","EU-Central",0,"2024-01-15 10:00:00"), # zero amount
], schema=order_schema)

print("Source data:")
dirty_source.show(truncate=False)

result = build_dead_letter_pipeline(
    source_df=dirty_source,
    rules=quality_rules,
    valid_output_path=f"{BASE_PATH}/silver_valid",
    dlq_output_path=f"{BASE_PATH}/silver_dead_letter",
)

```

```python

```
### Inspect Dead Letter Queue

```python

print("=== MAIN TABLE (valid records) ===")
spark.read.format("delta").load(f"{BASE_PATH}/silver_valid") \
    .select("order_id", "amount", "status", "pipeline_status").show(truncate=False)

print("\n=== DEAD LETTER QUEUE (errors) ===")
spark.read.format("delta").load(f"{BASE_PATH}/silver_dead_letter") \
    .select("order_id", "amount", "status", "error_messages", "error_timestamp") \
    .show(truncate=False)

# --- Error Categorization & Retry Simulation ---
dlq_df = spark.read.format("delta").load(f"{BASE_PATH}/silver_dead_letter")
print("\n=== Error Breakdown ===")
dlq_df.groupBy("error_messages").count().orderBy(F.desc("count")).show(truncate=False)

# Simulate a retry: fix data and re-run
print("\n=== RETRY SIMULATION: Fix null order_id, reprocess ===")
fixed = dlq_df.fillna({"order_id": "ORD-RETRY-FIXED"})
print(f"Retrying {fixed.count()} records after fixes...")
# In real scenario, would re-run through the main pipeline

```

```python

```
---
## Concept 79 — Pipeline Parameters & Configuration

**Difficulty: Hard**

Production pipelines need different behavior per environment. Lakeflow supports:

- **Pipeline settings** JSON/YAML configuration
- **Spark configuration** for cluster-level settings
- **Databricks widgets** for parameter injection at runtime

Common parameterization patterns:
- **Storage paths**: dev → `/tmp/dev`, prod → `/mnt/prod-lake`
- **Refresh cadence**: dev → on-demand, prod → hourly
- **Quality thresholds**: dev → lower, prod → strict

```python

```
### Lakeflow Pipeline Parameters (Commented Reference)

```python

# =============================================================================
#      LAKEFLOW / DLT — PARAMETERIZED PIPELINE (not executable in CE)
# =============================================================================
# # In pipeline settings JSON:
# {
#   "name": "retail_etl_{{environment}}",
#   "configuration": {
#     "source_path": "/mnt/{{environment}}/raw/orders",
#     "target_path": "/mnt/{{environment}}/curated",
#     "quality_mode": "{{quality_mode}}",
#     "max_late_data_hours": "24"
#   }
# }
#
# # In notebook code:
# import dlt
#
# source_path = spark.conf.get("source_path")
# quality_mode = spark.conf.get("quality_mode")
#
# @table(name="bronze_orders")
# def bronze_orders():
#     df = spark.readStream.format("delta").load(source_path)
#     if quality_mode == "strict":
#         dlt.expect_or_fail("has_order_id", "order_id IS NOT NULL")
#     return df

```

```python

```
### Manual Configuration Management (Community Edition)

```python

# =============================================================================
#      MANUAL CONFIGURATION MANAGEMENT
# =============================================================================

class PipelineConfig:
    """Environment-aware pipeline configuration."""

    ENVIRONMENTS = {
        "dev": {
            "source_path": f"{BASE_PATH}/raw_orders",
            "target_path": f"{BASE_PATH}/curated_dev",
            "quality_mode": "warn",        # dev: don't break on bad data
            "batch_size": 100,
            "trigger_interval": "30 seconds",
            "retention_hours": 1,
            "log_level": "DEBUG",
        },
        "staging": {
            "source_path": f"{BASE_PATH}/raw_orders",
            "target_path": f"{BASE_PATH}/curated_staging",
            "quality_mode": "drop",        # staging: drop bad rows, don't fail
            "batch_size": 1000,
            "trigger_interval": "5 minutes",
            "retention_hours": 168,
            "log_level": "INFO",
        },
        "prod": {
            "source_path": f"{BASE_PATH}/raw_orders",
            "target_path": f"{BASE_PATH}/curated_prod",
            "quality_mode": "fail",        # prod: fail on critical violations
            "batch_size": 5000,
            "trigger_interval": "1 minute",
            "retention_hours": 8760,       # 1 year
            "log_level": "WARN",
        },
    }

    def __init__(self, environment="dev"):
        if environment not in self.ENVIRONMENTS:
            raise ValueError(f"Unknown environment: {environment}. "
                             f"Choose from: {list(self.ENVIRONMENTS.keys())}")
        self.env = environment
        self.config = self.ENVIRONMENTS[environment]
        self.runtime = {}

    def get(self, key, default=None):
        return self.config.get(key, default)

    def set_runtime(self, key, value):
        self.runtime[key] = value

    def print_config(self):
        print(f"\n{'='*60}")
        print(f"  PIPELINE CONFIGURATION — Environment: {self.env.upper()}")
        print(f"{'='*60}")
        for k, v in self.config.items():
            print(f"  {k:25s} = {v}")
        print(f"{'='*60}\n")


class ParameterizedPipeline:
    """Pipeline that adapts behavior based on configuration."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.source_path = config.get("source_path")
        self.target_path = config.get("target_path")
        self.quality_mode = config.get("quality_mode")
        self.batch_size = config.get("batch_size")

    def extract(self):
        print(f"[EXTRACT] Reading from {self.source_path} (batch_size={self.batch_size})")
        source_df = spark.read.format("delta").load(self.source_path) \
            .withColumn("_pipeline_env", F.lit(self.config.env)) \
            .withColumn("_processed_at", F.current_timestamp())
        return source_df.limit(self.batch_size)

    def transform(self, df):
        print(f"[TRANSFORM] Quality mode: {self.quality_mode}")
        if self.quality_mode == "fail":
            assert df.filter(F.col("order_id").isNull()).count() == 0, \
                "PROD: null order_ids detected — failing!"
            assert df.filter(F.col("amount") <= 0).count() == 0, \
                "PROD: non-positive amounts detected — failing!"
            result = df
        elif self.quality_mode == "drop":
            result = df.filter(F.col("order_id").isNotNull()) \
                       .filter(F.col("amount") > 0)
        else:  # "warn"
            null_ids = df.filter(F.col("order_id").isNull()).count()
            if null_ids > 0:
                print(f"  [WARN] {null_ids} null order_ids found (dev mode)")
            result = df

        return result.withColumn("total_sale", F.col("quantity") * F.col("amount"))

    def load(self, df):
        output_path = f"{self.target_path}"
        df.write.format("delta").mode("overwrite").save(output_path)
        print(f"[LOAD] Wrote {df.count()} rows to {output_path}")
        return df.count()

    def run(self):
        print(f"\n▶️  Starting pipeline — ENV: {self.config.env.upper()}")
        raw = self.extract()
        transformed = self.transform(raw)
        count = self.load(transformed)
        print(f"⏹️  Pipeline complete — {count} rows processed\n")
        return count


# --- DEMO: Run same pipeline logic across environments ---
for env_name in ["dev", "staging", "prod"]:
    cfg = PipelineConfig(env_name)
    cfg.print_config()
    pipeline = ParameterizedPipeline(cfg)
    pipeline.run()

```

```python

```
### Simulating Databricks Widgets for Runtime Parameters

```python

# =============================================================================
#      DATABRICKS WIDGETS SIMULATION (dbutils.widgets in actual Databricks)
# =============================================================================

# In real Databricks, you'd use:
# dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"])
# dbutils.widgets.text("start_date", "2024-01-01")
# env = dbutils.widgets.get("environment")

# For CE / local, we use a config dictionary or environment variable:
import os as _os

def get_widget_value(name, default=None):
    """Simulates dbutils.widgets.get() for Community Edition."""
    # In real DB: return dbutils.widgets.get(name)
    # In CE: fall back to environment variable or hard-coded default
    return _os.environ.get(name.upper(), default)

simulated_env      = get_widget_value("environment", "dev")
simulated_start_dt = get_widget_value("start_date",  "2024-01-01")
simulated_batch    = int(get_widget_value("batch_size", "500"))

print(f"Widget 'environment'  = {simulated_env}")
print(f"Widget 'start_date'   = {simulated_start_dt}")
print(f"Widget 'batch_size'   = {simulated_batch}")
print(f"\nReal Databricks equivalent:")
print(f"  dbutils.widgets.dropdown('environment', '{simulated_env}', ['dev','staging','prod'])")
print(f"  dbutils.widgets.text('start_date', '{simulated_start_dt}')")
print(f"  dbutils.widgets.text('batch_size', '{simulated_batch}')")

```

```python

```
---
## Concept 80 — Multi-Pipeline Architecture

**Difficulty: Hard**

As pipeline complexity grows, splitting into **focused, smaller pipelines** improves
maintainability, debuggability, and team ownership. Lakeflow supports sharing tables
across pipelines and orchestrating execution order.

### When to Split

| Keep Together | Split Apart |
|---------------|-------------|
| Tightly coupled transformations | Different SLAs (real-time vs. daily) |
| Single team owns all | Multiple teams own different stages |
| Simple linear flow | Complex DAG with dependencies |
| < 10 tables | Many tables / high cardinality |

### Common Architecture Pattern
```
Pipeline 1: Ingestion    (bronze)     → raw → validated
Pipeline 2: Transformation (silver)    → validated → enriched
Pipeline 3: Aggregation   (gold)       → enriched → metrics
Pipeline 4: ML Features   (feature store) → enriched → features
```

```python

```
### Lakeflow Multi-Pipeline (Commented Reference)

```python

# =============================================================================
#      LAKEFLOW / DLT — MULTI-PIPELINE (not executable in CE)
# =============================================================================
# # Pipeline 1: Ingestion (ingestion_pipeline.py)
# @table(name="bronze_orders")
# def bronze_orders():
#     return spark.readStream.format("delta").table("raw_source.orders")
#
# # Pipeline 2: Transformation (transformation_pipeline.py)
# # Can read tables from Pipeline 1 using LIVE.<table_name>
# @table(name="silver_orders_enriched")
# def silver_orders_enriched():
#     orders   = dlt.read("bronze_orders")       # from Pipeline 1
#     products = dlt.read("product_dim")
#     return orders.join(products, "product_id", "left") \
#         .withColumn("category_revenue", F.col("quantity") * F.col("price"))
#
# # Pipeline 3: Aggregation (aggregation_pipeline.py)
# @materialized_view(name="gold_daily_kpis")
# def gold_daily_kpis():
#     return dlt.read("silver_orders_enriched") \
#         .groupBy(F.to_date("order_ts").alias("date"), "category") \
#         .agg(F.sum("category_revenue").alias("daily_revenue"))
#
# # Orchestration via Databricks Workflows or Job API:
# # Ingestion → Transform → Aggregate

```

```python

```
### Manual Multi-Pipeline Orchestration (Community Edition)

```python

from datetime import datetime as dt

# =============================================================================
#      MANUAL MULTI-PIPELINE ORCHESTRATION
# =============================================================================

class PipelineStep:
    """Represents one step in a multi-pipeline architecture."""

    def __init__(self, name, func, dependencies=None):
        self.name = name
        self.func = func
        self.dependencies = dependencies or []
        self.status = "PENDING"    # PENDING | RUNNING | SUCCESS | FAILED
        self.start_time = None
        self.end_time = None
        self.rows_output = 0

    def run(self):
        self.status = "RUNNING"
        self.start_time = dt.now()
        print(f"  ▶️  [{self.name}] Starting...")
        try:
            self.rows_output = self.func()
            self.status = "SUCCESS"
        except Exception as e:
            self.status = "FAILED"
            print(f"  ❌ [{self.name}] FAILED: {e}")
            raise
        finally:
            self.end_time = dt.now()
        print(f"  ✅ [{self.name}] Complete — {self.rows_output} rows "
              f"({(self.end_time - self.start_time).total_seconds():.1f}s)")

    def __repr__(self):
        return f"PipelineStep({self.name}, status={self.status})"


class MultiPipelineOrchestrator:
    """Orchestrates multiple pipeline steps respecting dependencies."""

    def __init__(self, name="multi_pipeline"):
        self.name = name
        self.steps = []
        self.execution_log = []

    def add_step(self, name, func, dependencies=None):
        self.steps.append(PipelineStep(name, func, dependencies))
        return self

    def _ready(self, step, completed):
        return all(dep in completed for dep in step.dependencies)

    def _show_dag(self):
        print(f"\n{'='*60}")
        print(f"  PIPELINE DAG: {self.name}")
        print(f"{'='*60}")
        for s in self.steps:
            deps = f" ← depends on: {s.dependencies}" if s.dependencies else ""
            print(f"  [{s.name}]{deps}")
        print(f"{'='*60}\n")

    def run(self):
        self._show_dag()
        completed = set()
        failed = set()
        remaining = list(self.steps)

        while remaining:
            ready = [s for s in remaining if self._ready(s, completed)]
            if not ready:
                pending = [s.name for s in remaining]
                print(f"❌ Deadlock or failure — pending steps: {pending}")
                break

            for step in ready:
                try:
                    step.run()
                    completed.add(step.name)
                    self.execution_log.append({
                        "step": step.name,
                        "status": step.status,
                        "rows": step.rows_output,
                        "duration_sec": (step.end_time - step.start_time).total_seconds()
                    })
                except Exception:
                    failed.add(step.name)
                    self.execution_log.append({
                        "step": step.name,
                        "status": "FAILED",
                        "rows": 0,
                        "duration_sec": 0
                    })
                remaining.remove(step)

        success_count = len([s for s in self.steps if s.status == "SUCCESS"])
        print(f"\n{'='*60}")
        print(f"  PIPELINE COMPLETE: {success_count}/{len(self.steps)} steps succeeded")
        print(f"{'='*60}")
        return self.execution_log


# ---- DEFINE PIPELINE FUNCTIONS ----

# Pipeline 1: Ingestion
def ingest_raw_orders():
    df = spark.read.format("delta").load(f"{BASE_PATH}/raw_orders") \
        .withColumn("ingested_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/pipeline_bronze")
    return df.count()

def validate_bronze():
    df = spark.read.format("delta").load(f"{BASE_PATH}/pipeline_bronze")
    valid = df.filter(F.col("order_id").isNotNull()) \
              .filter(F.col("amount") > 0) \
              .filter(F.col("status").isin("pending", "shipped", "delivered", "cancelled"))
    valid.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/pipeline_bronze_validated")
    invalid = df.filter(~F.col("order_id").isNotNull() |
                         (F.col("amount") <= 0) |
                         ~F.col("status").isin("pending", "shipped", "delivered", "cancelled"))
    invalid.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/pipeline_bronze_dlq")
    print(f"     Valid: {valid.count()}, Invalid (DLQ): {invalid.count()}")
    return valid.count()

# Pipeline 2: Transformation (depends on ingestion)
def enrich_silver():
    orders = spark.read.format("delta").load(f"{BASE_PATH}/pipeline_bronze_validated")
    products = spark.read.format("delta").load(f"{BASE_PATH}/products")
    customers = spark.read.format("delta").load(f"{BASE_PATH}/customers")

    enriched = orders \
        .join(products.select("product_id", F.col("price").alias("unit_price"), F.col("category")),
              "product_id", "left") \
        .join(customers.select("customer_id", "tier", "signup_date"),
              "customer_id", "left") \
        .withColumn("line_total", F.col("quantity") * F.col("unit_price")) \
        .withColumn("discount_amount",
                    F.round(F.col("line_total") * F.col("discount_pct") / 100.0, 2)) \
        .withColumn("net_revenue", F.col("line_total") - F.col("discount_amount"))

    enriched.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/pipeline_silver")
    return enriched.count()

# Pipeline 3: Aggregation (depends on transformation)
def aggregate_gold():
    silver = spark.read.format("delta").load(f"{BASE_PATH}/pipeline_silver")

    daily_metrics = silver \
        .withColumn("order_date", F.to_date("order_ts")) \
        .groupBy("order_date", "category", "region") \
        .agg(
            F.sum("net_revenue").alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.avg("net_revenue").alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold")
        ) \
        .withColumn("processed_at", F.current_timestamp())

    daily_metrics.write.format("delta").mode("overwrite") \
        .save(f"{BASE_PATH}/pipeline_gold_daily_metrics")

    # Customer-tier KPIs
    tier_metrics = silver \
        .groupBy("tier") \
        .agg(
            F.sum("net_revenue").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("net_revenue").alias("avg_revenue_per_order")
        )

    tier_metrics.write.format("delta").mode("overwrite") \
        .save(f"{BASE_PATH}/pipeline_gold_tier_metrics")

    return daily_metrics.count()

# Pipeline 4: Data Product / Feature Store (depends on transformation)
def build_features():
    silver = spark.read.format("delta").load(f"{BASE_PATH}/pipeline_silver")

    features = silver \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("lifetime_orders"),
            F.sum("net_revenue").alias("lifetime_value"),
            F.avg("net_revenue").alias("avg_order_value"),
            F.datediff(F.current_date(), F.min("signup_date")).alias("days_since_signup"),
            F.size(F.collect_set("category")).alias("categories_purchased"),
            F.max(F.to_date("order_ts")).alias("last_order_date")
        ) \
        .withColumn("churn_risk",
                     F.when(F.datediff(F.current_date(), F.col("last_order_date")) > 90, "HIGH")
                      .when(F.datediff(F.current_date(), F.col("last_order_date")) > 60, "MEDIUM")
                      .otherwise("LOW"))

    features.write.format("delta").mode("overwrite").save(f"{BASE_PATH}/pipeline_customer_features")
    return features.count()


# ---- ORCHESTRATE ----
orchestrator = MultiPipelineOrchestrator(name="Retail_ETL_MultiPipeline")

orchestrator \
    .add_step("1_ingest",      ingest_raw_orders) \
    .add_step("1_validate",    validate_bronze,     dependencies=["1_ingest"]) \
    .add_step("2_enrich",      enrich_silver,       dependencies=["1_validate"]) \
    .add_step("3_daily_kpis",  aggregate_gold,      dependencies=["2_enrich"]) \
    .add_step("4_features",    build_features,      dependencies=["2_enrich"])

execution_log = orchestrator.run()

```

```python

```
### Inspect Multi-Pipeline Outputs

```python

print("\n=== EXECUTION LOG ===")
for entry in execution_log:
    status_icon = "✅" if entry["status"] == "SUCCESS" else "❌"
    print(f"  {status_icon} {entry['step']}: {entry['rows']} rows, {entry['duration_sec']:.1f}s")

print("\n=== GOLD: Daily KPIs (sample) ===")
spark.read.format("delta").load(f"{BASE_PATH}/pipeline_gold_daily_metrics") \
    .orderBy(F.desc("order_date"), F.desc("total_revenue")) \
    .show(10, truncate=False)

print("\n=== GOLD: Tier Metrics ===")
spark.read.format("delta").load(f"{BASE_PATH}/pipeline_gold_tier_metrics") \
    .show(truncate=False)

print("\n=== CUSTOMER FEATURES: Top 10 by Lifetime Value ===")
spark.read.format("delta").load(f"{BASE_PATH}/pipeline_customer_features") \
    .orderBy(F.desc("lifetime_value")) \
    .select("customer_id", "lifetime_orders", "lifetime_value", "churn_risk") \
    .show(10, truncate=False)

```

```python

```
### Dependency Graph Visualization

```python

# =============================================================================
#      DEPENDENCY GRAPH (ASCII)
# =============================================================================
graph = """
  Multi-Pipeline Architecture
  ===========================

        ┌──────────────┐
        │  1_ingest    │  (Streaming Table)
        │  raw→bronze  │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │  1_validate  │  (Table + Expectations)
        │  quality DLQ │
        └──────┬───────┘
               │
               ▼
        ┌──────────────┐
        │   2_enrich   │  (Streaming Table)
        │  joins + calc│
        └──┬────────┬──┘
           │        │
     ┌─────▼──┐ ┌───▼──────────┐
     │3_daily │ │ 4_features   │
     │ KPIs   │ │ churn + ML   │
     │(Mater. │ │(FeatureStore)│
     │ View)  │ │              │
     └────────┘ └──────────────┘

  Pipeline 3 and 4 run IN PARALLEL after Pipeline 2.
"""

print(graph)

```

```python

```
---
## Summary — Lakeflow / DLT vs. Manual Approach

```python

```

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║  CONCEPT              │ WITH LAKEFLOW (DLT)            │ WITHOUT (CE MANUAL)       ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 71. Syntax            │ @table / CREATE STREAMING TABLE│ Manual readStream + write ║
║                       │ Mix SQL + Python seamlessly   │ Separate code paths       ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 72. Streaming vs MV   │ Auto-handled by framework      │ Manual checkpoint mgmt    ║
║                       │ Exactly-once built-in          │ Manual append/overwrite   ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 73. Expectations      │ expect / expect_or_drop / fail │ Custom filter functions    ║
║                       │ Metrics auto-logged to events  │ Manual quality logging     ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 74. Dev & Testing     │ Development mode (dev_ prefix) │ Manual fixture creation    ║
║                       │ Incremental / full refresh     │ Manual testing patterns    ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 75. Monitoring        │ Event log auto-populated       │ Custom PipelineRunTracker  ║
║                       │ Built-in lineage & metrics     │ Manual monitoring table    ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 76. Pipeline Modes    │ continuous: true/false config  │ availableNow / processing  ║
║                       │ UI toggle, no code change      │ Explicit trigger parameter ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 77. CDC Processing    │ AUTO CDC INTO (1 line)         │ Manual MERGE + SCD logic   ║
║                       │ SCD1/SCD2 automatic            │ ~70 lines of MERGE code    ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 78. Error Handling    │ expect_or_drop → auto DLQ-ish  │ Manual validation + DLQ    ║
║                       │ Event log tracks violations    │ Manual error categorization║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 79. Parameters        │ JSON pipeline settings         │ Config dict / env vars     ║
║                       │ Built-in environment support   │ Manual environment switch   ║
╠═══════════════════════╪════════════════════════════════╪═══════════════════════════╣
║ 80. Multi-Pipeline    │ Share tables via LIVE.<name>   │ Manual orchestration code  ║
║                       │ Databricks Workflows native    │ Custom dependency DAG       ║
╚═══════════════════════╩════════════════════════════════╩═══════════════════════════╝
```

```python

```
### Key Takeaways

1. **Lakeflow = Declarative**: You describe *what* you want; the framework handles *how*.
   Manual approaches require explicit orchestration, checkpointing, and error handling.

2. **Community Edition Reality**: Everything Lakeflow does can be replicated manually with
   Structured Streaming + Delta Lake + custom monitoring. The manual approach teaches you
   what Lakeflow automates — valuable for understanding and debugging.

3. **Quality First**: Lakeflow's expectation framework is its killer feature. The manual
   equivalent requires careful implementation of filtering, logging, and alerting.

4. **CDC is the hardest**: Lakeflow's `AUTO CDC INTO` eliminates 70+ lines of MERGE logic
   for SCD Type 2. Understanding the manual approach makes you appreciate the abstraction.

5. **Multi-pipeline thinking**: Even without Lakeflow, the pattern of splitting pipelines
   by medallion layer (bronze → silver → gold) is best practice for maintainability.

```python

```
---
## Self-Assessment Checklist

After completing this notebook, you should be able to:

- [ ] **Concept 71**: Write the same pipeline in both Python decorator syntax AND SQL syntax
  and explain when to prefer each.
- [ ] **Concept 72**: Distinguish streaming tables from materialized views and implement
  manual equivalents for each in Community Edition.
- [ ] **Concept 73**: Implement data quality rules with warn/drop/fail semantics both via
  Lakeflow expectations and via a manual ExpectationFramework class.
- [ ] **Concept 74**: Create test fixtures, write assertion-based pipeline tests, and
  explain the difference between development and production modes.
- [ ] **Concept 75**: Build a PipelineRunTracker that logs execution metadata, query run
  history, and explain how Lakeflow's event log simplifies monitoring.
- [ ] **Concept 76**: Configure both triggered (availableNow) and continuous (processingTime)
  pipelines and articulate the cost/latency tradeoffs.
- [ ] **Concept 77**: Implement manual SCD Type 2 with MERGE, sequence-based deduplication,
  and explain how `AUTO CDC INTO` eliminates this boilerplate.
- [ ] **Concept 78**: Build a dead letter queue that captures bad records with error
  categorization and retry logic — and explain how Lakeflow's `expect_or_drop` simplifies this.
- [ ] **Concept 79**: Create environment-aware pipeline configurations and demonstrate
  the same pipeline logic running across dev/staging/prod with different behaviors.
- [ ] **Concept 80**: Design a multi-pipeline architecture with dependency management,
  implement a simple orchestrator, and explain when to split vs. keep pipelines together.

---

### Resources
- Databricks DLT Documentation: https://docs.databricks.com/en/delta-live-tables/index.html
- Lakeflow Blog (March 2025): https://www.databricks.com/blog/introducing-lakeflow
- Structured Streaming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Delta Lake MERGE: https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

```python

# Cleanup streaming queries if any are still running
for q in spark.streams.active:
    try:
        q.stop()
        print(f"Stopped streaming query: {q.name}")
    except Exception as e:
        print(f"Could not stop query {q.name}: {e}")

print("\n" + "="*60)
print("  Notebook complete — all 10 concepts demonstrated.")
print("="*60)
```