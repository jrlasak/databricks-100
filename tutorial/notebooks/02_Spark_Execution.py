# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02: Spark Execution Engine
# MAGIC
# MAGIC **Concepts #11-#20** — How the Spark engine works under the hood
# MAGIC
# MAGIC | Concept | Topic | Difficulty |
# MAGIC |---------|-------|------------|
# MAGIC | 11 | Lazy Evaluation & Actions | Easy |
# MAGIC | 12 | Photon Engine | Easy |
# MAGIC | 13 | Catalyst Optimizer & Query Plans | Medium |
# MAGIC | 14 | Adaptive Query Execution (AQE) | Medium |
# MAGIC | 15 | Shuffle Operations | Medium |
# MAGIC | 16 | Join Strategies | Medium |
# MAGIC | 17 | Reading the Spark UI | Medium |
# MAGIC | 18 | Partitioning & Parallelism | Medium |
# MAGIC | 19 | Data Skew: Detection & Mitigation | Hard |
# MAGIC | 20 | Memory Management & Spill | Hard |
# MAGIC
# MAGIC **Designed for:** Databricks Community Edition (single node, no Photon, no serverless)
# MAGIC
# MAGIC **Datasets:** Synthetic data generated in-notebook + `/databricks-datasets/` samples

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Generate Synthetic Datasets
# MAGIC
# MAGIC We create all data upfront so every concept has what it needs:
# MAGIC - **sales_df** — 1M rows of sales transactions (fact table)
# MAGIC - **products_df** — 1K unique products (dimension table, small)
# MAGIC - **customers_df** — 500K customers (large dimension)
# MAGIC - **skewed_sales_df** — deliberately skewed for skew demos
# MAGIC - **orders_df** — 500K orders with many columns for pruning demos

# COMMAND ----------

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, randn, expr, broadcast, lit, sum, count, avg, max, min, when, floor, concat, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, LongType
import random

spark = SparkSession.builder.appName("SparkExecutionDeepDive").getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

print(f"Spark version     : {spark.version}")
print(f"Application ID    : {spark.sparkContext.applicationId}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Products (1K — small dimension table)

# COMMAND ----------

categories = ["Electronics", "Clothing", "Home", "Sports", "Books", "Food", "Toys", "Automotive"]
product_ids = list(range(1, 1001))

products_data = []
for pid in product_ids:
    products_data.append({
        "product_id": pid,
        "product_name": f"Product_{pid:04d}",
        "category": random.choice(categories),
        "price": round(random.uniform(5.0, 500.0), 2),
        "cost": round(random.uniform(2.0, 200.0), 2)
    })

products_df = spark.createDataFrame(products_data)
products_df.cache()
products_df.count()
print(f"Products: {products_df.count()} rows")
products_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Sales (1M rows — large fact table)

# COMMAND ----------

num_sales = 1_000_000
sales_schema = StructType([
    StructField("sale_id", LongType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("sale_date", StringType(), False),
    StructField("region", StringType(), False),
    StructField("payment_type", StringType(), False)
])

regions = ["North", "South", "East", "West", "Central"]
payments = ["Credit", "Debit", "Cash", "Digital"]

def generate_sales_row(i):
    return (
        i,
        random.randint(1, 1000),
        random.randint(1, 500000),
        random.randint(1, 50),
        random.randint(1, 10),
        round(random.uniform(5.0, 500.0), 2),
        f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        random.choice(regions),
        random.choice(payments)
    )

sales_rdd = spark.sparkContext.parallelize(range(num_sales), 8).map(lambda i: generate_sales_row(i))
sales_df = spark.createDataFrame(sales_rdd, schema=sales_schema)
sales_df = sales_df.withColumn("revenue", col("quantity") * col("unit_price"))
print(f"Sales: {sales_df.count()} rows")
sales_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customers (500K — large dimension)

# COMMAND ----------

num_customers = 500_000
customers_data = []

for cid in range(1, num_customers + 1, 5000):
    batch = []
    for c in range(cid, min(cid + 5000, num_customers + 1)):
        batch.append({
            "customer_id": c,
            "customer_name": f"Customer_{c:06d}",
            "customer_region": random.choice(regions),
            "loyalty_tier": random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
            "signup_date": f"202{random.randint(0,4)}-{random.randint(1,12):02d}-01"
        })
    for row in batch:
        customers_data.append(row)
    if cid % 50000 == 0:
        print(f"  Generated {cid} customers...")

customers_df = spark.createDataFrame(customers_data)
print(f"Customers: {customers_df.count()} rows")
customers_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Skewed Sales (for skew demonstrations)

# COMMAND ----------

skewed_data = []
num_skewed = 200_000

for i in range(num_skewed):
    if i < num_skewed * 0.8:
        pid = 1
    elif i < num_skewed * 0.9:
        pid = 2
    elif i < num_skewed * 0.95:
        pid = 3
    elif i < num_skewed * 0.98:
        pid = 4
    else:
        pid = random.randint(5, 1000)

    skewed_data.append({
        "sale_id": i,
        "product_id": pid,
        "customer_id": random.randint(1, 100000),
        "store_id": random.randint(1, 20),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10.0, 200.0), 2)
    })

skewed_sales_df = spark.createDataFrame(skewed_data)
print(f"Skewed Sales: {skewed_sales_df.count()} rows")
skewed_sales_df.groupBy("product_id").count().orderBy(col("count").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Wide Orders (many columns for pruning demos)

# COMMAND ----------

num_orders = 500_000
wide_columns = ["order_id", "customer_id", "product_id", "quantity", "price", "status"] + \
               [f"attr_{i}" for i in range(1, 31)]

broad_orders = []
for i in range(num_orders):
    row = {
        "order_id": i,
        "customer_id": random.randint(1, 100000),
        "product_id": random.randint(1, 1000),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10.0, 500.0), 2),
        "status": random.choice(["placed", "shipped", "delivered", "cancelled"])
    }
    for a in range(1, 31):
        row[f"attr_{a}"] = round(random.random() * 100, 2)
    broad_orders.append(row)
    if i % 100000 == 0 and i > 0:
        print(f"  Generated {i} orders...")

orders_df = spark.createDataFrame(broad_orders)
orders_df.cache()
orders_df.count()
print(f"Orders: {orders_df.count()} rows, {len(orders_df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 11: Lazy Evaluation & Actions [Easy]
# MAGIC
# MAGIC **Key idea:** Transformations (`filter`, `select`, `groupBy`) build an execution plan but don't run until an **action** (`count`, `collect`, `show`, `write`) triggers execution.
# MAGIC
# MAGIC This is why Spark can optimize across your entire pipeline — it sees the full DAG before doing any work.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation Chain (No Execution Yet)
# MAGIC
# MAGIC Watch — no Spark jobs fire for any of these lines:

# COMMAND ----------

t0 = time.time()

# These are ALL transformations — nothing executes yet
filtered = sales_df.filter(col("region") == "North")
grouped = filtered.groupBy("product_id").agg(sum("revenue").alias("total_revenue"))
sorted_result = grouped.orderBy(col("total_revenue").desc())
topped = sorted_result.limit(10)

t1 = time.time()
print(f"Transformations defined in: {(t1 - t0)*1000:.1f} ms")
print(f"No Spark jobs have run yet!")
print(f"Execution plan is built lazily — it's just a recipe, not executed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now Trigger an Action — Execution Happens

# COMMAND ----------

print("Triggering action: .show()\n")

t0 = time.time()
topped.show()
t1 = time.time()

print(f"\nAction (.show) completed in: {t1 - t0:.2f} seconds")
print(f"All transformations + the action ran in ONE optimized pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare: Action vs No-Action Timing

# COMMAND ----------

print("--- Timing: Transformations only (no action) ---")
t0 = time.time()
for i in range(5):
    step1 = sales_df.filter(col("region") == "East")
    step2 = step1.groupBy("store_id").agg(avg("revenue").alias("avg_rev"))
    step3 = step2.filter(col("avg_rev") > 50)
    step4 = step3.orderBy(col("avg_rev").desc())
t1 = time.time()
print(f"No-action pipeline defined 5x in: {(t1 - t0)*1000:.1f} ms")

print("\n--- Timing: Transformations + Action ---")
t0 = time.time()
for i in range(5):
    step1 = sales_df.filter(col("region") == "East")
    step2 = step1.groupBy("store_id").agg(avg("revenue").alias("avg_rev"))
    step3 = step2.filter(col("avg_rev") > 50)
    step4 = step3.orderBy(col("avg_rev").desc())
    step4.count()
t1 = time.time()
print(f"With-action pipeline executed 5x in: {t1 - t0:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Transformations vs Actions
# MAGIC
# MAGIC | Transformations (lazy) | Actions (trigger execution) |
# MAGIC |------------------------|-----------------------------|
# MAGIC | `filter()`, `where()` | `count()`, `first()` |
# MAGIC | `select()`, `drop()` | `collect()`, `take(n)` |
# MAGIC | `groupBy()`, `agg()` | `show()`, `head()` |
# MAGIC | `join()`, `union()` | `foreach()`, `foreachPartition()` |
# MAGIC | `orderBy()`, `sort()` | `write`, `saveAsTable` |
# MAGIC | `withColumn()` | `toPandas()` (be careful!) |
# MAGIC | `distinct()`, `dropDuplicates()` | `reduce()`, `fold()` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution Plan Built Lazily
# MAGIC
# MAGIC The plan is built step-by-step during transformations. `explain()` lets you peek at it without executing:

# COMMAND ----------

# Build a moderately complex query lazily
q = (sales_df
     .filter(col("quantity") > 3)
     .join(products_df, "product_id")
     .filter(col("category") == "Electronics")
     .groupBy("region", "store_id")
     .agg(sum("revenue").alias("total_rev"), count("*").alias("num_sales"))
     .filter(col("total_rev") > 1000)
     .orderBy(col("total_rev").desc()))

# No execution yet — just show the plan
print("=== Execution Plan (no action triggered) ===")
q.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Lazy evaluation is Spark's superpower. It lets the Catalyst optimizer rearrange, combine, and optimize your pipeline before a single byte is read. You pay the cost only when you ask for results.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 12: Photon Engine [Easy]
# MAGIC
# MAGIC **Photon** is Databricks' native vectorized query engine written in C++. It replaces parts of Spark's JVM-based execution with a high-performance columnar engine.
# MAGIC
# MAGIC **Community Edition limitation:** Photon is NOT available. We explain the architecture and concepts here.
# MAGIC
# MAGIC ### What Photon Does
# MAGIC - **Vectorized execution:** Processes data in batches (columnar batches) rather than row-by-row
# MAGIC - **Native C++:** Bypasses JVM overhead for CPU-bound operations
# MAGIC - **SIMD instructions:** Uses modern CPU vector instructions for parallel processing within a core
# MAGIC - **Memory-efficient:** Better cache locality, fewer allocations
# MAGIC
# MAGIC ### Which Ops Benefit Most?
# MAGIC | High Photon Impact | Moderate Impact | Low Impact |
# MAGIC |--------------------|-----------------|------------|
# MAGIC | Filter, Project (select) | Aggregations | I/O-bound reads |
# MAGIC | Joins (hash joins) | Windows | Small data ops |
# MAGIC | Sorting | Grouped aggregations | Metadata ops |
# MAGIC | Expressions / UDFs | Union, Intersect | File listing |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking Photon Status
# MAGIC
# MAGIC In a full Databricks platform, you can check if Photon is active:

# COMMAND ----------

# Check if Photon is available — will show "not enabled" on Community Edition
try:
    photon_status = spark.conf.get("spark.databricks.photon.enabled")
    print(f"Photon enabled: {photon_status}")
except Exception:
    print("Photon: NOT AVAILABLE (Community Edition / Standard Runtime)")
    print("This concept is explained for awareness — it works on full Databricks platform.")

# Check Databricks Runtime info
try:
    dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    print(f"Databricks Runtime: {dbr_version}")
except Exception:
    print(f"Databricks Runtime info not available via config")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Photon Changes Execution
# MAGIC
# MAGIC Without Photon, Spark runs this pipeline:
# MAGIC ```
# MAGIC Scan → Filter (JVM) → Project (JVM) → Aggregate (JVM) → Sort (JVM) → Output
# MAGIC ```
# MAGIC
# MAGIC With Photon, the same pipeline becomes:
# MAGIC ```
# MAGIC Scan → [Photon: Filter → Project → Aggregate → Sort] → Output
# MAGIC ```
# MAGIC
# MAGIC Photon replaces the entire shaded region with a single C++ pipeline operating on columnar batches.
# MAGIC
# MAGIC ### Performance Comparison (Conceptual)
# MAGIC
# MAGIC Typical improvements from Photon (from Databricks benchmarks):
# MAGIC - **SQL queries:** 2-8x faster
# MAGIC - **ETL pipelines:** 1.5-3x faster
# MAGIC - **Joins & aggregations:** 3-5x faster
# MAGIC - **Scan-heavy workloads:** 1.5-2x faster
# MAGIC
# MAGIC > In Community Edition, we run without Photon. The concepts of query optimization still apply — Photon just makes them even faster.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulated Comparison: Row-at-a-Time vs Batch
# MAGIC
# MAGIC Python simulation to illustrate why batch/vectorized processing is faster:

# COMMAND ----------

import random as rnd
import math

# Simulate processing 1M rows column-by-column vs row-by-row
n_rows = 1_000_000
col_a = [rnd.random() for _ in range(n_rows)]
col_b = [rnd.random() for _ in range(n_rows)]

# Row-at-a-time (simulates JVM iterator pattern)
t0 = time.time()
result_row = []
for i in range(n_rows):
    if col_a[i] > 0.5:
        result_row.append(col_a[i] * col_b[i] + col_b[i])
t1 = time.time()
print(f"Row-at-a-time (Python loop)        : {t1 - t0:.4f}s")

# Batch / vectorized using list comprehensions (simulates columnar batch)
t0 = time.time()
mask = [a > 0.5 for a in col_a]
result_batch = [a * b + b for a, b, m in zip(col_a, col_b, mask) if m]
t1 = time.time()
print(f"Batch-style (comprehensions)       : {t1 - t0:.4f}s")
print(f"\nThis is a simplified illustration. Photon uses C++ with SIMD — much faster in practice.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Photon is Databricks' C++ vectorized engine. While not available in Community Edition, understanding its role helps you appreciate query optimization and why certain patterns (filter early, use columnar operations) matter regardless of engine.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 13: Catalyst Optimizer & Query Plans [Medium]
# MAGIC
# MAGIC **Catalyst** is Spark's query optimizer. It takes your DataFrame/SQL code and produces an optimized execution plan through multiple phases:
# MAGIC 1. **Parsed Logical Plan** — raw AST from your code
# MAGIC 2. **Analyzed Logical Plan** — resolved table/column references
# MAGIC 3. **Optimized Logical Plan** — rule-based optimizations applied
# MAGIC 4. **Physical Plan** — how it will actually execute (with costs)
# MAGIC
# MAGIC `df.explain(True)` shows all four phases. `df.explain()` shows just the physical plan.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Plan Walkthrough with `explain(True)`

# COMMAND ----------

# Build a query that benefits from optimization
query = (sales_df
         .filter(col("region") == "West")
         .filter(col("quantity") > 2)
         .join(products_df, "product_id")
         .filter(col("category") == "Electronics")
         .select("sale_id", "product_name", "price", "revenue", "region")
         .where(col("revenue") > 100)
         .orderBy(col("revenue").desc())
         .limit(20))

print("=" * 70)
print("FULL CATALYST PLAN WALKTHROUGH — explain(True)")
print("=" * 70)
query.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Four Plan Phases
# MAGIC
# MAGIC **1. Parsed Logical Plan** — Raw tree from your code:
# MAGIC ```
# MAGIC Limit
# MAGIC  └── Sort (revenue DESC)
# MAGIC       └── Filter (revenue > 100)
# MAGIC            └── Project [sale_id, product_name, ...]
# MAGIC                 └── Join (product_id)
# MAGIC                      ├── Filter (category = Electronics)
# MAGIC                      │    └── Relation[products]
# MAGIC                      └── Filter (quantity > 2)
# MAGIC                           └── Filter (region = West)
# MAGIC                                └── Relation[sales]
# MAGIC ```
# MAGIC
# MAGIC **2. Analyzed Logical Plan** — Resolves table/column names using catalog
# MAGIC
# MAGIC **3. Optimized Logical Plan** — Catalyst applies rule-based optimizations
# MAGIC
# MAGIC **4. Physical Plan** — Cost-based choice of join strategy, scan type, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Predicate Pushdown in Action
# MAGIC
# MAGIC Catalyst pushes filters as close to the data source as possible — filtering early means less data to shuffle/sort.

# COMMAND ----------

# Build a query with filters on both sides of a join
pred_push_query = (sales_df
                   .filter(col("region") == "North")   # pushdown: filter sales early
                   .join(products_df.filter(col("category") == "Sports"), "product_id")  # pushdown both sides
                   .filter(col("revenue") > 50)          # pushdown: filter after join
                   .select("sale_id", "product_name", "revenue", "category")
                   .orderBy(col("revenue").desc()))

print("=== Optimized Plan (shows predicate pushdown) ===")
pred_push_query.explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Pruning
# MAGIC
# MAGIC Catalyst only reads the columns you actually need. Wide tables benefit massively:

# COMMAND ----------

# The orders_df has 36 columns — we only need 3
# Catalyst will prune the other 33 columns at scan time
pruned_query = (orders_df
                .filter(col("status") == "delivered")
                .select("order_id", "customer_id", "price")
                .groupBy("customer_id")
                .agg(sum("price").alias("total_spent")))

print("=== Optimized Plan (notice only 3 of 36 columns scanned) ===")
pruned_query.explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Equivalent — Same Optimizations
# MAGIC
# MAGIC DataFrame API and Spark SQL go through the same Catalyst optimizer:

# COMMAND ----------

sales_df.createOrReplaceTempView("sales")
products_df.createOrReplaceTempView("products")
orders_df.createOrReplaceTempView("orders")

sql_query = spark.sql("""
    SELECT p.product_name, p.category, SUM(s.revenue) AS total_rev
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    WHERE s.region = 'South'
      AND s.quantity > 1
      AND p.category IN ('Electronics', 'Sports')
      AND s.revenue > 50
    GROUP BY p.product_name, p.category
    ORDER BY total_rev DESC
    LIMIT 10
""")

print("=== SQL Query Optimized Plan ===")
sql_query.explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark: Optimization in Action
# MAGIC
# MAGIC Timing a query with and without early filtering:

# COMMAND ----------

# Version A: Filter late (bad pattern — but Catalyst may fix it!)
print("--- Version A: Filter applied late in code ---")
t0 = time.time()
result_a = (sales_df
            .join(products_df, "product_id")
            .filter(col("region") == "East")
            .filter(col("category") == "Books")
            .groupBy("product_name")
            .agg(sum("revenue").alias("total"))
            .orderBy(col("total").desc()))
result_a.limit(5).show()
t1 = time.time()
print(f"Time: {t1 - t0:.2f}s")

# Version B: Filter early (good practice — works with Catalyst)
print("\n--- Version B: Filter applied early in code ---")
t0 = time.time()
result_b = (sales_df
            .filter(col("region") == "East")
            .join(products_df.filter(col("category") == "Books"), "product_id")
            .groupBy("product_name")
            .agg(sum("revenue").alias("total"))
            .orderBy(col("total").desc()))
result_b.limit(5).show()
t1 = time.time()
print(f"Time: {t1 - t0:.2f}s")

print("\nCatalyst optimizes both to similar plans, but explicit early filtering is good practice.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Catalyst optimizes your query in 4 phases. Predicate pushdown and column pruning are two of its most impactful optimizations. Always filter early, select only needed columns, and review plans with `explain()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 14: Adaptive Query Execution (AQE) [Medium]
# MAGIC
# MAGIC **AQE** re-optimizes the query plan at runtime based on actual data statistics — not just estimates. Key features:
# MAGIC 1. **Dynamically coalesce shuffle partitions** — reduce partitions when data is small
# MAGIC 2. **Dynamically switch join strategies** — switch to broadcast if data is small enough
# MAGIC 3. **Dynamically optimize skew joins** — split skewed partitions
# MAGIC 4. **Dynamically detect empty partitions** — skip empty partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ### AQE Configuration

# COMMAND ----------

print("=== AQE Configuration ===")
print(f"spark.sql.adaptive.enabled                    = {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"spark.sql.adaptive.coalescePartitions.enabled  = {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")
try:
    print(f"spark.sql.adaptive.advisoryPartitionSizeInBytes = {spark.conf.get('spark.sql.adaptive.advisoryPartitionSizeInBytes')}")
except Exception:
    print("spark.sql.adaptive.advisoryPartitionSizeInBytes = (default 64MB)")

# Toggle AQE for comparison
print("\nYou can control AQE at the session level:")
print('  spark.conf.set("spark.sql.adaptive.enabled", "true")')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrating Coalescing Shuffle Partitions
# MAGIC
# MAGIC AQE can reduce the 200 shuffle partitions to far fewer when data volume is small:

# COMMAND ----------

# Run same query with and without AQE coalescing
from pyspark.sql import functions as F

# Use a small subset to show coalescing
small_data = sales_df.filter(col("sale_id") < 50000)

print("=== With AQE Coalescing Enabled ===")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

t0 = time.time()
result_with_aqe = (small_data
                   .groupBy("region", "store_id")
                   .agg(sum("revenue").alias("total"), count("*").alias("cnt"))
                   .orderBy(col("total").desc()))
result_with_aqe.explain("extended")
row_count = result_with_aqe.count()
t1 = time.time()
print(f"Result rows: {row_count}, Time: {t1 - t0:.2f}s")
print(f"Notice in the plan: AQE may coalesce partitions at shuffle stage.")

print("\n=== Without AQE Coalescing ===")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")

t0 = time.time()
result_without_aqe = (small_data
                      .groupBy("region", "store_id")
                      .agg(sum("revenue").alias("total"), count("*").alias("cnt"))
                      .orderBy(col("total").desc()))
result_without_aqe.explain("extended")
row_count = result_without_aqe.count()
t1 = time.time()
print(f"Result rows: {row_count}, Time: {t1 - t0:.2f}s")

# Restore AQE
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Runtime Plan Changes with AQE
# MAGIC
# MAGIC AQE can change the plan mid-execution. For instance, a sort-merge join might become a broadcast join if the actual data is small enough:

# COMMAND ----------

small_products = products_df.filter(col("product_id") < 30)

print("=== Without AQE — join strategy is fixed at planning ===")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Force hint to see the difference
(spark.range(10000).toDF("id")
 .join(small_products, spark.range(10000).toDF("id").col("id") == small_products.col("product_id"))
 .explain())

spark.conf.set("spark.sql.adaptive.enabled", "true")

print("\n=== With AQE — may switch join strategy at runtime ===")
(spark.range(10000).toDF("id")
 .join(small_products, spark.range(10000).toDF("id").col("id") == small_products.col("product_id"))
 .explain())

# COMMAND ----------

# MAGIC %md
# MAGIC ### AQE and Empty Partitions
# MAGIC
# MAGIC AQE can detect and skip empty partitions, avoiding wasted task scheduling:

# COMMAND ----------

# Create a heavily filtered dataset — many partitions will be empty after filtering
filtered_sales = sales_df.filter(col("region") == "Central").filter(col("quantity") == 10)

t0 = time.time()
filtered_sales.count()
t1 = time.time()
print(f"Filtered to {filtered_sales.count()} rows from 1M in {t1 - t0:.2f}s")

# Show partition count before and after
print(f"\nOriginal partitions      : {sales_df.rdd.getNumPartitions()}")
print(f"Filtered partitions (RDD): {filtered_sales.rdd.getNumPartitions()}")
print("AQE can detect empty partitions and optimize the plan at runtime.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** AQE is a game-changer — it adapts the plan at runtime based on actual data stats. Key features are coalescing shuffle partitions, switching join strategies, handling skew, and skipping empty partitions. It's enabled by default in Databricks Runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 15: Shuffle Operations [Medium]
# MAGIC
# MAGIC **Shuffle** is Spark's mechanism for redistributing data across partitions. Operations that require data from multiple partitions trigger a shuffle:
# MAGIC - `groupBy()`, `agg()` — data must be co-located by key
# MAGIC - `join()` — matching keys must be in the same partition
# MAGIC - `distinct()` — duplicates must be co-located
# MAGIC - `repartition()` — explicit redistribution
# MAGIC - `orderBy()` — total ordering requires shuffle
# MAGIC - Window functions (with `PARTITION BY`) — rows in same partition need co-location

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operations That Trigger Shuffle

# COMMAND ----------

from pyspark.sql import functions as F

print("=== Operations That Trigger Shuffle ===\n")

# 1. groupBy — shuffle by key
print("1. GroupBy — triggers shuffle")
(sales_df.groupBy("region").agg(count("*").alias("cnt")).explain())
print()

# 2. Join — shuffle by join key
print("2. Join — triggers shuffle (unless broadcast)")
(sales_df.join(products_df, "product_id", "inner").select("sale_id", "product_name").explain())
print()

# 3. distinct — shuffle to deduplicate
print("3. Distinct — triggers shuffle")
sales_df.select("product_id").distinct().explain()
print()

# 4. window function with partitionBy
print("4. Window with PARTITION BY — triggers shuffle")
from pyspark.sql.window import Window
w = Window.partitionBy("region").orderBy(col("revenue").desc())
sales_df.select("sale_id", "region", "revenue", F.row_number().over(w).alias("rn")).explain()
print()

# 5. Operations that DON'T shuffle
print("5. Operations WITHOUT shuffle:")
print("   - filter(), select(), withColumn()")
print("   - map(), flatMap() within partition")
print("   - coalesce() (reduces partitions without shuffle)")
print("   - foreachPartition()")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shuffle Read/Write with Different Partition Counts

# COMMAND ----------

import time

def time_groupby_with_partitions(n_partitions, label):
    spark.conf.set("spark.sql.shuffle.partitions", str(n_partitions))
    t0 = time.time()
    result = sales_df.groupBy("region").agg(
        sum("revenue").alias("total_rev"),
        count("*").alias("num_sales")
    ).collect()
    t1 = time.time()
    print(f"  {label}: spark.sql.shuffle.partitions={n_partitions}, time={t1 - t0:.2f}s, result_rows={len(result)}")

print("=== Shuffle Partition Count vs Performance ===\n")

# Too few partitions — under-utilization
time_groupby_with_partitions(2,  "Too few ")

# Good for small result
time_groupby_with_partitions(10, "Low     ")

# Default
time_groupby_with_partitions(200, "Default ")

# Too many for small data — overhead
time_groupby_with_partitions(400, "Too many")

# Restore
spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shuffle Cost: Many Small Partitions vs Few Optimal Partitions
# MAGIC
# MAGIC Each shuffle partition generates a task. Too many small partitions = task scheduling overhead. Too few = under-utilization.

# COMMAND ----------

print("=== Shuffle Cost Analysis ===\n")
print("Task overhead per shuffle partition:")
print("  - Task scheduling   : ~5-50ms per task")
print("  - Task deserialization: ~1-10ms per task")
print("  - Network overhead  : per partition metadata")
print("")
print("Optimal shuffle partition size: 100-200 MB per partition")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")
print(f"Shuffle partitions  : {spark.conf.get('spark.sql.shuffle.partitions')}")

# Show the formula
data_size_mb = sales_df.count() * 100 / (1024 * 1024)  # rough est: ~100 bytes per row
optimal_partitions = max(1, data_size_mb // 128)
print(f"\nFor {sales_df.count():,} rows (~{data_size_mb:.0f} MB):")
print(f"  Optimal partitions: ~{optimal_partitions}")
print(f"  Current setting:    {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  Guidance: sp.sql.shuffle.partitions = cluster_cores * 2 to 3")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitoring Shuffle in Spark UI
# MAGIC
# MAGIC Navigate to the Spark UI (typically port 4040) to see:
# MAGIC - **Stages tab** → Shuffle Read Size / Shuffle Write Size per stage
# MAGIC - **SQL tab** → Each query's DAG with shuffle exchange nodes
# MAGIC - **Executors tab** → Shuffle Read/Write Memory per executor
# MAGIC
# MAGIC Key shuffle metrics to watch:
# MAGIC - **Shuffle Write** — data serialized and written to local disk for other executors to read
# MAGIC - **Shuffle Read** — data fetched from remote executors
# MAGIC - **Shuffle Spill (Memory)** — data that didn't fit in memory during shuffle
# MAGIC - **Shuffle Spill (Disk)** — data spilled to disk during shuffle
# MAGIC
# MAGIC > On Community Edition (single node), shuffle is local but still involves disk I/O.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitoring Shuffle from Within Code

# COMMAND ----------

print("=== Monitoring Shuffle from Code ===\n")

# Get SparkContext status tracker info
sc = spark.sparkContext
print(f"Application ID: {sc.applicationId}")
print(f"Spark UI URL: {sc.uiWebUrl}")
print(f"Default parallelism: {sc.defaultParallelism}")

# Trigger a shuffle-heavy operation
print("\nRunning shuffle-heavy operation (groupBy + agg)...")
t0 = time.time()
heavy_result = (sales_df
                .groupBy("region", "store_id", "product_id")
                .agg(sum("revenue").alias("total_rev"),
                     count("*").alias("num_sales"),
                     avg("quantity").alias("avg_qty"))
                .filter(col("total_rev") > 500)
                .orderBy(col("total_rev").desc()))
heavy_count = heavy_result.count()
t1 = time.time()
print(f"Done. {heavy_count:,} rows in {t1 - t0:.2f}s")

# Show the stages that occurred
print(f"\nCheck Spark UI at: {sc.uiWebUrl}")
print("Look for: Jobs tab → find this job → click to see Stages")
print("Key metrics on each stage:")
print("  - Input Size / Records")
print("  - Shuffle Write")
print("  - Shuffle Read")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Shuffle is the most expensive operation in Spark. It involves serialization, disk I/O, and network transfer. Minimize shuffles by filtering early, choosing the right partition count, using broadcast joins for small tables, and leveraging AQE. Monitor shuffle via the Spark UI's SQL and Stages tabs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 16: Join Strategies [Medium]
# MAGIC
# MAGIC Spark uses multiple join strategies. The optimizer (Catalyst + AQE) picks based on table sizes and hints:
# MAGIC
# MAGIC | Strategy | When Used | Data Movement |
# MAGIC |----------|-----------|---------------|
# MAGIC | **Broadcast Hash Join (BHJ)** | One side < `autoBroadcastJoinThreshold` (10MB default) | Small table sent to all executors — NO shuffle |
# MAGIC | **Sort-Merge Join (SMJ)** | Both sides large | Both sides shuffled by join key, then sorted and merged |
# MAGIC | **Shuffle Hash Join (SHJ)** | One side 3x smaller than other | Both sides shuffled, smaller side hashed |
# MAGIC | **Broadcast Nested Loop Join** | No equi-join condition | Small side broadcast, nested loop |
# MAGIC | **Cartesian Product** | Cross join | Very expensive — avoid |
# MAGIC
# MAGIC In practice, BHJ and SMJ cover 99% of cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 1: Broadcast Hash Join (Best for Small + Large)
# MAGIC
# MAGIC The small table is sent to every executor. The large table never needs to shuffle.

# COMMAND ----------

print("=== Broadcast Hash Join ===\n")

# products_df is small (1K rows) — good broadcast candidate
print(f"Products table: {products_df.count()} rows — SMALL (~few KB)")
print(f"Sales table   : {sales_df.count():,} rows — LARGE")

# BHJ with hint
print("\n--- Broadcast Join with hint ---")
broadcast_join = sales_df.join(broadcast(products_df), "product_id", "inner")
broadcast_join.explain("extended")
print()

# Show it works
t0 = time.time()
bj_result = broadcast_join.select("sale_id", "product_name", "category", "revenue")
bj_result.show(5)
t1 = time.time()
print(f"Broadcast join time: {t1 - t0:.2f}s")

# Without explicit hint — Catalyst may still choose broadcast
print("\n--- Auto-detected Broadcast Join (no hint) ---")
auto_join = sales_df.join(products_df, "product_id", "inner")
auto_join.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Strategy 2: Sort-Merge Join (For Two Large Tables)

# COMMAND ----------

print("=== Sort-Merge Join ===\n")

# Both sides are large — must be shuffled and sorted for merge join
print(f"Sales table  : {sales_df.count():,} rows — LARGE")
print(f"Customers    : {customers_df.count():,} rows — LARGE")

smj = sales_df.join(customers_df, "customer_id", "inner")
print("\n--- Sort-Merge Join Plan ---")
smj.explain()

t0 = time.time()
smj_result = smj.select("sale_id", "customer_name", "loyalty_tier", "revenue")
smj_result.show(5)
t1 = time.time()
print(f"Sort-merge join time: {t1 - t0:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Controlling the Broadcast Threshold

# COMMAND ----------

# Show current threshold
broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"autoBroadcastJoinThreshold: {broadcast_threshold} bytes ({int(broadcast_threshold) / (1024*1024):.1f} MB)")

# Temporarily increase to force broadcast on medium tables
print("\n--- Increasing threshold to 50MB ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
print(f"New threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')} bytes")

# Force smaller threshold
print("\n--- Reducing threshold to 1 byte (disables broadcast) ---")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1")
dis_join = sales_df.join(products_df, "product_id", "inner")
print("With broadcast disabled, small join becomes sort-merge:")
dis_join.explain()

# Restore
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparing Join Strategies Performance

# COMMAND ----------

# Small table broadcast join
small_for_broadcast = products_df.filter(col("product_id") < 50)
print(f"Small table: {small_for_broadcast.count()} products")

# Test 1: Broadcast hash join (hinted)
t0 = time.time()
bhj = sales_df.join(broadcast(small_for_broadcast), "product_id").count()
t1 = time.time()
print(f"\nBroadcast Hash Join (hinted): {t1 - t0:.2f}s")

# Test 2: Let Catalyst choose (will also broadcast since it's small)
t0 = time.time()
auto = sales_df.join(small_for_broadcast, "product_id").count()
t1 = time.time()
print(f"Auto-chosen join           : {t1 - t0:.2f}s")

# Test 3: Sort-merge join on large-large
t0 = time.time()
smj = sales_df.join(customers_df, "customer_id").count()
t1 = time.time()
print(f"Sort-Merge Join (large)    : {t1 - t0:.2f}s")

# Test 4: Disable broadcast, force sort-merge on small join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
t0 = time.time()
forced_smj = sales_df.join(small_for_broadcast, "product_id").count()
t1 = time.time()
print(f"SMJ forced on small join   : {t1 - t0:.2f}s (much worse!)")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Strategy Selection Rules
# MAGIC
# MAGIC ```
# MAGIC 1. Is either side small (< autoBroadcastJoinThreshold)?
# MAGIC    YES → Broadcast Hash Join
# MAGIC    NO  → Continue to step 2
# MAGIC
# MAGIC 2. Can AQE optimize at runtime?
# MAGIC    Small enough → Convert to Broadcast Hash Join
# MAGIC    Skew detected → Split skewed partition
# MAGIC
# MAGIC 3. Default: Sort-Merge Join
# MAGIC    Both sides shuffled, sorted, merged
# MAGIC ```
# MAGIC
# MAGIC **Pro tip:** Always `cache()` small dimension tables used in multiple joins. Explicitly `broadcast()` when you know a table is small but Catalyst might not.

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Broadcast Hash Join is the fastest (no shuffle on large side). Sort-Merge Join handles large-large joins. Use `broadcast()` hint for small tables and tune `autoBroadcastJoinThreshold`. AQE can switch strategies at runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 17: Reading the Spark UI [Medium]
# MAGIC
# MAGIC The Spark UI is your primary observability tool. It has these key tabs:
# MAGIC
# MAGIC | Tab | Purpose | What to Look For |
# MAGIC |-----|---------|------------------|
# MAGIC | **Jobs** | All Spark jobs | Duration, stages per job, status |
# MAGIC | **Stages** | Individual stages | Task duration variance, shuffle size, spill |
# MAGIC | **Tasks** | Task-level detail | Skew (max >> median), GC time, errors |
# MAGIC | **Storage** | Cached RDDs/DataFrames | Cache size, partitions cached, memory used |
# MAGIC | **Environment** | Config & JARs | Verify settings took effect |
# MAGIC | **Executors** | Executor metrics | Memory used, tasks completed, shuffle I/O |
# MAGIC | **SQL** | Query DAG details | Plan visualization, metrics per node |
# MAGIC
# MAGIC > On Community Edition, the Spark UI is available at the URL printed by `spark.sparkContext.uiWebUrl`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show the Spark UI URL

# COMMAND ----------

sc = spark.sparkContext
print(f"=" * 70)
print(f"  SPARK UI: {sc.uiWebUrl}")
print(f"=" * 70)
print(f"\nApplication ID : {sc.applicationId}")
print(f"Application Name : {sc.appName}")
print(f"\nOpen this URL in your browser to follow along.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Queries That Generate Interesting UI Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC **Query 1: Shuffle-heavy aggregation** — Look at the Stages tab for shuffle read/write

# COMMAND ----------

# Query with significant shuffle
print("Running shuffle-heavy aggregation...")
shuffle_query = (sales_df
                 .filter(col("quantity") > 1)
                 .groupBy("region", "product_id", "payment_type")
                 .agg(
                     sum("revenue").alias("total_rev"),
                     count("*").alias("transaction_count"),
                     avg("quantity").alias("avg_qty")
                 )
                 .filter(col("total_rev") > 100)
                 .orderBy(col("total_rev").desc()))

t0 = time.time()
shuffle_query.count()
t1 = time.time()
print(f"Completed in {t1 - t0:.2f}s")

print("""
In Spark UI → Jobs tab:
  - Find this job (most recent)
  - Click to expand → see stages
  - Stage 0: Scan + filter (no shuffle)
  - Stage 1: Shuffle (Exchange hashpartitioning) + aggregation
  - Check: Shuffle Write size, Shuffle Read size
""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Query 2: Join with broadcast hint** — Compare SQL tab DAG to shuffle-heavy query

# COMMAND ----------

print("Running broadcast join...")
small_subset = products_df.filter(col("product_id") < 100)

t0 = time.time()
bj_query = sales_df.join(broadcast(small_subset), "product_id").filter(col("category") == "Electronics")
bj_query.count()
t1 = time.time()
print(f"Completed in {t1 - t0:.2f}s")

print("""
In Spark UI → SQL tab:
  - Click the query ID for this broadcast join
  - Notice: No "Exchange" (shuffle) node — BroadcastExchange instead
  - Compare to a sort-merge join query for contrast
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnosing a Slow Query from UI Metrics
# MAGIC
# MAGIC Here's a decision tree for diagnosing issues using the Spark UI:

# COMMAND ----------

print("""
=== SLOW QUERY DIAGNOSIS FLOWCHART ===

1. JOBS TAB → Find slow job
   Q: Is one stage much slower than others?
   
2. STAGES TAB → Click slow stage
   Q: Is there high task duration variance?
      YES → Look at Tasks tab for skew
             Some tasks take 10x others? → DATA SKEW (Concept 19)
      NO  → All tasks uniformly slow? Continue
   
3. SQL TAB → Click the query
   Q: Is there a Shuffle (Exchange) node?
      YES → Check Shuffle Write size
             Large shuffle? → Tune partitions (Concept 18)
             Spill > 0? → Memory issue (Concept 20)
      NO  → Check Scan node
             Many files? → Small file problem
             
4. EXECUTORS TAB
   Q: GC Time high (>10% of task time)?
      YES → GC pressure → Increase executor memory or tune GC
   
5. STORAGE TAB
   Q: Is data being evicted from cache?
      YES → Not enough storage memory → Increase or don't cache

=== KEY METRICS TO WATCH ===
- Task Duration (Max / Median) → > 2x indicates skew
- Shuffle Spill (Memory / Disk) → > 0 means spill occurred
- Input Size / Records → source data volume
- GC Time → executor overhead
- Scheduler Delay → cluster busy
- Task Deserialization Time → code/closure too large
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating UI Data: Run Several Diverse Queries

# COMMAND ----------

print("Running diverse queries to populate the Spark UI...")

# 1. Simple filter + select (no shuffle)
query1 = sales_df.filter(col("store_id") == 1).select("sale_id", "revenue", "region")
query1.count()

# 2. GroupBy aggregation (shuffle)
query2 = sales_df.groupBy("region", "store_id").agg(sum("revenue"), count("*"))
query2.count()

# 3. Join (shuffle)
query3 = sales_df.join(customers_df, "customer_id").select("sale_id", "customer_name", "loyalty_tier")
query3.count()

# 4. Window function (shuffle)
from pyspark.sql.window import Window
w = Window.partitionBy("region").orderBy(col("revenue").desc())
query4 = sales_df.select("sale_id", "region", "revenue", F.rank().over(w).alias("rank"))
query4.count()

print(f"\nAll queries complete. Open Spark UI at {sc.uiWebUrl}")
print("Compare the SQL tab entries for each query.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** The Spark UI is your diagnostic tool. The Jobs tab tells you what's slow. The Stages tab reveals why (skew, spill, shuffle size). The SQL tab shows the optimized plan visually. The Executors tab reveals memory/GC issues. Learn to navigate all tabs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 18: Partitioning & Parallelism [Medium]
# MAGIC
# MAGIC **Partitions** are the fundamental unit of parallelism in Spark. Each partition = one task = one CPU core working on a subset of data.
# MAGIC
# MAGIC Key settings:
# MAGIC - `spark.sql.shuffle.partitions` (default 200) — partitions after shuffle
# MAGIC - `spark.default.parallelism` — RDD operations default
# MAGIC - `repartition(n)` — full shuffle to N partitions
# MAGIC - `coalesce(n)` — reduce partitions WITHOUT shuffle (combine adjacent)
# MAGIC - `spark.sql.files.maxPartitionBytes` — max bytes per partition when reading files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitions and Parallelism
# MAGIC
# MAGIC In Community Edition (single node), parallelism is limited by CPU cores, but the concepts of partition count still affect memory usage, spill, and performance.

# COMMAND ----------

executor_cores = 4  # Community Edition typical
shuffle_partitions = int(spark.conf.get("spark.sql.shuffle.partitions"))

print(f"=== Partitioning & Parallelism ===\n")
print(f"Shuffle partitions (config)      : {shuffle_partitions}")
print(f"Default parallelism              : {spark.sparkContext.defaultParallelism}")
print(f"Typical Community Edition cores  : ~{executor_cores}")
print(f"\nParallelism limit: At most {executor_cores} tasks run concurrently.")
print(f"Partitions > cores: Tasks queue up — more scheduling overhead, but better memory distribution.")
print(f"Partitions < cores: Cores idle — under-utilization.")
print(f"\nRule of thumb: partitions = cores * 2 to 4 (for pipelining)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `repartition()` vs `coalesce()` — Critical Difference

# COMMAND ----------

# Create a DataFrame with few partitions
few_partitions_df = sales_df.coalesce(2)
print(f"Few partitions: {few_partitions_df.rdd.getNumPartitions()}")

# repartition(): FULL shuffle — redistributes data evenly across N partitions
t0 = time.time()
repartitioned = few_partitions_df.repartition(16)
t1 = time.time()
print(f"\nrepartition(16):")
print(f"  New partition count: {repartitioned.rdd.getNumPartitions()}")
print(f"  Data redistribution: FULL SHUFFLE (expensive)")
print(f"  Use when: you need MORE partitions or even distribution")

# coalesce(): NO shuffle — combines adjacent partitions
t0 = time.time()
coalesced = repartitioned.coalesce(4)
t1 = time.time()
print(f"\ncoalesce(4):")
print(f"  New partition count: {coalesced.rdd.getNumPartitions()}")
print(f"  Data redistribution: NO SHUFFLE (cheap)")
print(f"  Use when: reducing partitions (only!) for downstream efficiency")
print(f"  WARNING: coalesce(n) where n > current is a NO-OP (use repartition)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Over-Partitioning vs Under-Partitioning

# COMMAND ----------

import time

def run_with_partitions(data_df, n_partitions, label):
    spark.conf.set("spark.sql.shuffle.partitions", str(n_partitions))
    t0 = time.time()
    result = (data_df
              .groupBy("product_id", "store_id")
              .agg(sum("revenue").alias("total"), count("*").alias("cnt"))
              .filter(col("total") > 100))
    cnt = result.count()
    t1 = time.time()
    print(f"  {label}: {n_partitions} partitions → {cnt:,} rows in {t1 - t0:.2f}s")
    return t1 - t0

print("=== Effect of Partition Count on Performance ===\n")

# Use a subset to highlight differences
test_data = sales_df.filter(col("sale_id") < 500000)

times = {}
times[2]    = run_with_partitions(test_data, 2,    "Under    ")
times[10]   = run_with_partitions(test_data, 10,   "Low      ")
times[50]   = run_with_partitions(test_data, 50,   "Moderate ")
times[200]  = run_with_partitions(test_data, 200,  "Default  ")
times[400]  = run_with_partitions(test_data, 400,  "High     ")

# Restore
spark.conf.set("spark.sql.shuffle.partitions", "200")

print(f"\n=== Summary ===")
print(f"Optimal appeared around 10-50 partitions for this data size")
print(f"Default of 200 is fine in most cases, but can be tuned down for small data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking Partition Distribution
# MAGIC
# MAGIC Are your partitions balanced?

# COMMAND ----------

# Show partition sizes for a grouped DataFrame
grouped = sales_df.groupBy("product_id").agg(count("*").alias("sales_count"), sum("revenue").alias("total_rev"))

# Check how many partitions and their approximate sizes
print(f"Grouped DataFrame partitions: {grouped.rdd.getNumPartitions()}")
print(f"\nPartition record counts (first 10 partitions):")

# Use glom() to see records per partition (use carefully on large data)
partition_sizes = grouped.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
avg_size = sum(partition_sizes) / len(partition_sizes)
max_size = max(partition_sizes)
min_size = min(partition_sizes)

print(f"  Total partitions : {len(partition_sizes)}")
print(f"  Avg per partition: {avg_size:.0f}")
print(f"  Max partition    : {max_size}")
print(f"  Min partition    : {min_size}")
print(f"  Skew ratio       : {max_size / max(1, avg_size):.1f}x")
print(f"  Min/max ratio    : {max_size / max(1, min_size):.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Effect of Partition Size on Memory
# MAGIC
# MAGIC Too-large partitions = spill. Too-small partitions = scheduling overhead.

# COMMAND ----------

print("=== Partition Size Effects ===\n")
print(f"Files read by Spark are split into partitions of up to ~128MB")
print(f"After shuffle, partitions are defined by spark.sql.shuffle.partitions")
print()
print("Too-large partitions (>200MB):")
print("  - May cause OOM or excessive spill")
print("  - GC pressure increases")
print("  - Solution: increase spark.sql.shuffle.partitions")
print()
print("Too-small partitions (<10MB):")
print("  - Excessive task scheduling overhead")
print("  - Many small shuffle files")  
print("  - I/O inefficiency from many small reads")
print("  - Solution: coalesce or reduce spark.sql.shuffle.partitions")
print()
print("Ideal partition size: 100-200 MB")
print(f"For our ~{sales_df.count():,}-row table, optimal partitions ≈ {sales_df.count() * 100 // (128 * 1024 * 1024) + 1}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Partitions determine parallelism. `repartition()` does a full shuffle (expensive but evenly distributed). `coalesce()` avoids shuffle (cheap but can be uneven). Tune `spark.sql.shuffle.partitions` to match data size: ~100-200MB per partition. For Community Edition, 16-64 partitions is often optimal.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 19: Data Skew — Detection & Mitigation [Hard]
# MAGIC
# MAGIC **Data skew** occurs when some keys have disproportionately more data than others. In a distributed operation (join, groupBy), the few tasks processing the "hot" keys become stragglers — they hold up the entire job.
# MAGIC
# MAGIC **Symptoms in Spark UI:**
# MAGIC - High variance in task duration (max task time >> median task time)
# MAGIC - A few tasks process far more records than others
# MAGIC - Spill occurring only on certain tasks
# MAGIC
# MAGIC **Mitigation strategies:**
# MAGIC 1. **Salting** — Add random prefix to key to spread hot keys
# MAGIC 2. **AQE skew join optimization** — Spark automatically splits skewed partitions
# MAGIC 3. **Broadcast join** — If one side is small, broadcast avoids shuffle entirely
# MAGIC 4. **Separate skewed keys** — Process skewed keys separately, then union

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Intentionally Skewed Data

# COMMAND ----------

# Show the skew in our pre-built skewed dataset
print("=== Skewed Sales Data Distribution ===\n")
skew_stats = skewed_sales_df.groupBy("product_id").count().orderBy(col("count").desc())
skew_stats.show(15)

print(f"\nProduct 1 has ~80% of ALL data!")
product_1_count = skewed_sales_df.filter(col("product_id") == 1).count()
total_count = skewed_sales_df.count()
print(f"Product 1 rows: {product_1_count:,}")
print(f"Total rows    : {total_count:,}")
print(f"Product 1 share: {product_1_count / total_count * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detecting Skew — Task Duration Analysis
# MAGIC
# MAGIC Run a join with the skewed data and observe the task metrics:

# COMMAND ----------

# Join skewed sales with products — product_id=1 dominates
print("=== Join with Skewed Data ===\n")

t0 = time.time()
skewed_join = skewed_sales_df.join(products_df, "product_id").groupBy("product_id", "product_name", "category").agg(
    sum(col("quantity") * col("price")).alias("total_revenue"),
    count("*").alias("num_sales")
).orderBy(col("num_sales").desc())
skewed_join.show(10)
t1 = time.time()
print(f"\nTotal time: {t1 - t0:.2f}s")
print("\nCheck Spark UI → Stages → Tasks:")
print("  - Task for product_id=1 will process ~160K rows")
print("  - Other tasks process ~20 rows each")
print("  - MAX task duration will be MUCH larger than MEDIAN → SKEW!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mitigation Strategy 1: Salting (Manual Skew Handling)

# COMMAND ----------

import random
from pyspark.sql.functions import col, concat, lit, split

SALT_BUCKETS = 20

print(f"=== Salting Technique ({SALT_BUCKETS} salt buckets) ===\n")

# Step 1: Add salt column to the skewed table (0 to SALT_BUCKETS-1)
skewed_with_salt = skewed_sales_df.withColumn(
    "salt", (col("product_id") * 2654435761 % SALT_BUCKETS).cast("int")
)

print("Skewed sales with salt column:")
skewed_with_salt.select("sale_id", "product_id", "salt").show(10)

# Step 2: Explode the products dimension to have salted copies
# For each product, create SALT_BUCKETS copies, one per salt value
products_exploded = products_df.crossJoin(
    spark.range(SALT_BUCKETS).withColumnRenamed("id", "salt")
).withColumn("salted_product_id", col("product_id"))

# Step 3: Join on (product_id, salt)
print("\nPerforming salted join...")
t0 = time.time()
salted_join_result = (skewed_with_salt
    .join(products_exploded, 
          (skewed_with_salt.product_id == products_exploded.product_id) & 
          (skewed_with_salt.salt == products_exploded.salt))
    .groupBy("product_id", "product_name", "category")
    .agg(count("*").alias("num_sales"))
    .orderBy(col("num_sales").desc()))
salted_join_result.show(10)
t1 = time.time()
print(f"Salted join time: {t1 - t0:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mitigation Strategy 2: AQE Skew Join Optimization

# COMMAND ----------

print("=== AQE Skew Join Optimization ===\n")

# Enable AQE skew join handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
try:
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
except Exception as e:
    print(f"Skew config note: {e}")

# Run join with AQE
print("Running join with AQE skew optimization enabled...")
t0 = time.time()
aqe_join_result = (skewed_sales_df
    .join(products_df, "product_id")
    .groupBy("product_id", "product_name", "category")
    .agg(count("*").alias("num_sales"))
    .orderBy(col("num_sales").desc()))
aqe_join_result.show(10)
t1 = time.time()
print(f"AQE-enabled join time: {t1 - t0:.2f}s")

print("""
AQE detects skewed partitions at runtime:
  - If a partition is > skewPartitionFactor × median partition size
  - AND > skewPartitionThresholdInBytes
  → Spark splits that partition into smaller sub-partitions
  → Each sub-partition joins independently
  → Reduces max task time dramatically

Check Spark UI → SQL tab → this query's DAG:
  - Look for "OptimizeSkewedJoin" in the plan
  - Notice the physical plan changes at runtime
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mitigation Strategy 3: Broadcast Join (Eliminates Shuffle)

# COMMAND ----------

# When the dimension table is small enough to broadcast, skew becomes irrelevant
# (because there's NO shuffle on the large side)
print("=== Broadcast Join Avoids Skew ===\n")
print(f"Products table: {products_df.count()} rows")

t0 = time.time()
broadcast_join_result = (skewed_sales_df
    .join(broadcast(products_df), "product_id")
    .groupBy("product_id", "product_name", "category")
    .agg(count("*").alias("num_sales"))
    .orderBy(col("num_sales").desc()))
broadcast_join_result.show(10)
t1 = time.time()
print(f"Broadcast join time: {t1 - t0:.2f}s")
print("\nBroadcast join eliminates the shuffle entirely → skew doesn't matter!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before/After Comparison: Skew Impact

# COMMAND ----------

print("=== Skew Impact Summary ===\n")
print(f"{'Strategy':<30} {'Effect':<50}")
print("-" * 80)
print(f"{'No mitigation (raw join)':<30} {'1 task processes 80% of data → straggler':<50}")
print(f"{'Salting (20 buckets)':<30} {'Hot key spread across 20 partitions':<50}")
print(f"{'AQE skew optimization':<30} {'Automatic partition splitting at runtime':<50}")
print(f"{'Broadcast join':<30} {'No shuffle on large side → skew irrelevant':<50}")
print(f"{'Filter out skewed keys':<30} {'Process skewed + non-skewed separately, then union':<50}")

print("""
=== When to Use Each Strategy ===

SALTING:
  - Both sides are large
  - Specific keys are known to be hot
  - You control the join logic
  
AQE SKEW JOIN:
  - Spark 3.0+ with AQE enabled
  - Automatic detection and handling
  - Best when you don't know which keys are hot

BROADCAST JOIN:
  - One side fits in memory (< ~10MB typical, tunable)
  - Eliminates skew completely for that join
  - Always try this first for small dimension tables

SEPARATE PROCESSING:
  - Very specific skew patterns
  - When neither salting nor AQE is sufficient
  - Manual but gives full control
""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Data skew causes straggler tasks and slow jobs. The Spark UI reveals skew through task duration variance. Mitigate with AQE (automatic), broadcast joins (when possible), salting (manual but effective), or separate processing. Always profile your data distribution before optimizing.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 20: Memory Management & Spill [Hard]
# MAGIC
# MAGIC Spark uses a **unified memory model** where execution memory and storage memory share a pool:
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────┐
# MAGIC │              JVM Heap (executor.memory)       │
# MAGIC ├──────────┬──────────────┬─────────────────────┤
# MAGIC │ Reserved │ Spark Memory │     User Memory      │
# MAGIC │  Memory  │  (unified)   │  (UDFs, data structs)│
# MAGIC │ (300MB)  ├──────┬───────┤  (40% of remaining)  │
# MAGIC │          │Exec  │Storage│                      │
# MAGIC │          │Mem   │ Memory│                      │
# MAGIC │          │(50%) │ (50%) │                      │
# MAGIC └──────────┴──────┴───────┴──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Spill** happens when execution memory is exhausted — data is serialized and written to disk, which is orders of magnitude slower than in-memory processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Unified Memory Model

# COMMAND ----------

print("=== Spark Unified Memory Model ===\n")

try:
    exec_mem = spark.conf.get("spark.executor.memory")
    print(f"Executor memory: {exec_mem}")
except Exception:
    exec_mem = "2g (Community Edition default)"
    print(f"Executor memory: {exec_mem}")

try:
    mem_fraction = spark.conf.get("spark.memory.fraction")
    print(f"spark.memory.fraction: {mem_fraction}")
except Exception:
    mem_fraction = "0.6"
    print(f"spark.memory.fraction: {mem_fraction} (default)")

try:
    storage_fraction = spark.conf.get("spark.memory.storageFraction")
    print(f"spark.memory.storageFraction: {storage_fraction}")
except Exception:
    storage_fraction = "0.5"
    print(f"spark.memory.storageFraction: {storage_fraction} (default)")

print(f"""
Memory breakdown (approximate for {exec_mem}):
  Reserved memory (300MB):                    300 MB
  User memory ({1 - float(mem_fraction):.0%} of {(2048-300)*float(mem_fraction):.0f} MB):      ~{int((2048-300)*(1-float(mem_fraction)))} MB
  Spark unified memory pool:                  ~{(2048-300)*float(mem_fraction):.0f} MB
    - Execution memory (50% default):          ~{(2048-300)*float(mem_fraction)*0.5:.0f} MB
    - Storage memory (50% default):            ~{(2048-300)*float(mem_fraction)*0.5:.0f} MB

Execution memory is used for: shuffles, joins, sorts, aggregations
Storage memory is used for: cached data, broadcast variables
Execution CAN evict storage (but not vice versa)

When execution memory is full → SPILL TO DISK
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Spill Scenario
# MAGIC
# MAGIC We're going to deliberately cause spill by setting very few shuffle partitions — each partition becomes too large to fit in memory:

# COMMAND ----------

print("=== Deliberately Causing Spill ===\n")

# Force very few shuffle partitions → each partition is large → spills
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Heavy aggregation on all data — each partition processes 250K rows
t0 = time.time()
spill_query = (sales_df
               .groupBy("product_id", "store_id")
               .agg(
                   sum("revenue").alias("total_rev"),
                   count("*").alias("cnt"),
                   avg("quantity").alias("avg_qty"),
                   max("unit_price").alias("max_price"),
                   sum("quantity").alias("total_qty")
               )
               .orderBy(col("total_rev").desc()))

spill_result = spill_query.collect()
t1 = time.time()
print(f"Query with 4 shuffle partitions: {t1 - t0:.2f}s")
print(f"Result rows: {len(spill_result)}")

print("""
Check Spark UI → Stages → this job:
  - Look for "Spill (Memory)" and "Spill (Disk)" > 0
  - Each task processes ~250K rows — likely exceeds execution memory
  - Spill means data was written to/read from disk mid-computation
  
In SQL tab, you'll see Spill metrics for each operation.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reducing Spill by Tuning Partitions

# COMMAND ----------

print("=== Reducing Spill with More Partitions ===\n")

for n_partitions in [4, 16, 64, 200]:
    spark.conf.set("spark.sql.shuffle.partitions", str(n_partitions))
    t0 = time.time()
    cnt = (sales_df
           .groupBy("product_id", "store_id")
           .agg(sum("revenue").alias("total_rev"), count("*").alias("cnt"))
           .orderBy(col("total_rev").desc())
           .count())
    t1 = time.time()
    partition_mb = (sales_df.count() * 100) / (1024 * 1024) / n_partitions
    print(f"  {n_partitions:>4} partitions → ~{partition_mb:.0f} MB/task → {t1 - t0:.2f}s")

# Restore
spark.conf.set("spark.sql.shuffle.partitions", "200")
print(f"\nMore partitions = smaller per-partition data = less spill = potentially faster")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detecting Spill from Code

# COMMAND ----------

print("=== Monitoring Memory and Spill ===\n")
print("""
Spill indicators in the Spark UI:

1. STAGES TAB → Click a stage → Summary Metrics:
   - "Spill (Memory)" > 0 → Memory wasn't enough
   - "Spill (Disk)" > 0 → Data was written to disk
   - High spill = significant performance penalty (disk ~100x slower than memory)

2. TASKS TAB → Individual task metrics:
   - "Shuffle Spill (Memory)" — bytes spilled during shuffle
   - "Shuffle Spill (Disk)" — disk bytes written for spill
   - "Peak Execution Memory" — peak memory used by task
   
3. SQL TAB → Query details:
   - Each operator shows spill metrics
   - Sort, Aggregate, and Join are most spill-prone

4. What spill looks like numerically:
   - Spill > 0 on any operation → check partition sizes
   - Spill > input records → severe spill (data didn't fit at all)
   - Spill = 0 → all data fit in memory (ideal)
""")

# Show how to estimate memory per task
total_rows = sales_df.count()
est_bytes_per_row = 150  # rough estimate with overhead
est_mb_per_partition_default = (total_rows * est_bytes_per_row) / (1024 * 1024) / 200
est_mb_per_partition_low = (total_rows * est_bytes_per_row) / (1024 * 1024) / 8

print(f"Estimated memory per task:")
print(f"  With 200 partitions: ~{est_mb_per_partition_default:.0f} MB/task")
print(f"  With 8 partitions  : ~{est_mb_per_partition_low:.0f} MB/task")
print(f"  Available exec mem  : ~520 MB (estimated for Community Edition)")
print(f"  Spill threshold     : when per-task data > available execution memory")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Memory Configuration Effects

# COMMAND ----------

print("=== Memory Configuration Reference ===\n")

print("""
KEY MEMORY CONFIGURATIONS:

spark.executor.memory (e.g., "4g")
  - Total JVM heap per executor
  - Community Edition: typically 2-4 GB
  - Increase for: large shuffles, large cached datasets

spark.memory.fraction (default 0.6)
  - Fraction of heap for Spark unified memory (execution + storage)
  - Remaining 40% is for user data structures + metadata
  - Decrease if: out of memory from user objects / UDFs

spark.memory.storageFraction (default 0.5)
  - Fraction of Spark memory for storage (cache) vs execution
  - Execution can evict storage, but not vice versa
  - Increase if: you cache a lot and want to avoid eviction

spark.sql.shuffle.partitions (default 200)
  - More partitions = smaller tasks = less memory per task = less spill
  - But too many = excessive overhead
  - Tune based on data size

spark.shuffle.file.buffer (default 32k)
  - Buffer size for shuffle write
  - Larger = fewer disk seeks but more memory

spark.reducer.maxSizeInFlight (default 48m)
  - Max data fetched per reduce task at once
  - Larger = faster but more memory

TUNING FOR COMMUNITY EDITION (single node, 2-4 GB):
  1. spark.executor.memory = "3g" (if allowed)
  2. spark.sql.shuffle.partitions = 8-32 (single node doesn't need 200)
  3. spark.memory.fraction = 0.7 (slightly more for Spark)
  4. Use broadcast joins whenever possible (avoids shuffle)
  5. Filter early, select only needed columns
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practical: Compare High-Spill vs Low-Spill Configurations

# COMMAND ----------

print("=== High-Spill Configuration ===\n")
spark.conf.set("spark.sql.shuffle.partitions", "2")

t0 = time.time()
high_spill = (sales_df
              .groupBy("product_id", "region", "store_id")
              .agg(sum("revenue").alias("total"), count("*").alias("cnt"))
              .collect())
t1 = time.time()
high_spill_time = t1 - t0
print(f"2 partitions  → {high_spill_time:.2f}s")
print("Check Spark UI for Spill metrics — expect significant spill")

print("\n=== Low-Spill Configuration ===\n")
spark.conf.set("spark.sql.shuffle.partitions", "200")

t0 = time.time()
low_spill = (sales_df
             .groupBy("product_id", "region", "store_id")
             .agg(sum("revenue").alias("total"), count("*").alias("cnt"))
             .collect())
t1 = time.time()
low_spill_time = t1 - t0
print(f"200 partitions → {low_spill_time:.2f}s")
print("Check Spark UI — expect less or no spill")

print(f"\n=== Comparison ===")
print(f"High-spill (2 partitions)  : {high_spill_time:.2f}s")
print(f"Low-spill  (200 partitions): {low_spill_time:.2f}s")
print(f"Difference                 : {high_spill_time - low_spill_time:.2f}s")
print(f"\nToo few partitions → each task processes too much data → spill → slowdown")

# Restore default
spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key takeaway:** Spill occurs when execution memory is exhausted, writing data to disk (100x slower). Tune `spark.sql.shuffle.partitions` so each partition fits in execution memory (~100-200MB). Use the Spark UI Stages and Tasks tabs to detect spill metrics. In Community Edition, with limited memory, partition tuning is even more critical.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary: Concepts #11-#20
# MAGIC
# MAGIC | # | Concept | Key Takeaway |
# MAGIC |---|---------|--------------|
# MAGIC | 11 | **Lazy Evaluation** | Transformations build a plan; actions execute it. Catalyst optimizes the full DAG before running. |
# MAGIC | 12 | **Photon Engine** | C++ vectorized engine (not in Community Edition). Understand why batch processing is faster. |
# MAGIC | 13 | **Catalyst Optimizer** | 4-phase optimization: parsed → analyzed → optimized → physical. Predicate pushdown & column pruning. |
# MAGIC | 14 | **Adaptive Query Execution** | Runtime re-optimization: coalesce partitions, switch joins, handle skew, skip empty partitions. |
# MAGIC | 15 | **Shuffle Operations** | Most expensive operation. GroupBy, Join, Distinct trigger it. Monitor via Spark UI. |
# MAGIC | 16 | **Join Strategies** | Broadcast Hash Join (small+large, no shuffle) vs Sort-Merge Join (large+large). Use `broadcast()` hint. |
# MAGIC | 17 | **Spark UI** | Jobs → Stages → Tasks → SQL tabs. Diagnose skew, spill, and shuffle bottlenecks. |
# MAGIC | 18 | **Partitioning & Parallelism** | `repartition()` (full shuffle) vs `coalesce()` (no shuffle). Tune `spark.sql.shuffle.partitions`. |
# MAGIC | 19 | **Data Skew** | Some keys dominate → straggler tasks. Mitigate with salting, AQE, broadcast joins. |
# MAGIC | 20 | **Memory & Spill** | Unified memory model. Spill = disk I/O = slow. Tune partitions to fit data in memory. |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self-Assessment: Score Yourself
# MAGIC
# MAGIC Rate yourself 5 for each concept (0 = no understanding, 5 = can explain clearly):
# MAGIC
# MAGIC | Concept | Topic | Your Score (0-5) |
# MAGIC |---------|-------|------------------|
# MAGIC | 11 | Lazy Evaluation & Actions | |
# MAGIC | 12 | Photon Engine | |
# MAGIC | 13 | Catalyst Optimizer | |
# MAGIC | 14 | Adaptive Query Execution | |
# MAGIC | 15 | Shuffle Operations | |
# MAGIC | 16 | Join Strategies | |
# MAGIC | 17 | Reading the Spark UI | |
# MAGIC | 18 | Partitioning & Parallelism | |
# MAGIC | 19 | Data Skew Detection & Mitigation | |
# MAGIC | 20 | Memory Management & Spill | |
# MAGIC
# MAGIC **Total: /50**
# MAGIC
# MAGIC | Score | Level | Next Step |
# MAGIC |-------|-------|-----------|
# MAGIC | 40-50 | Senior | Move to Notebook 03 |
# MAGIC | 25-39 | Mid-level | Revisit concepts scored 3 or below |
# MAGIC | 15-24 | Foundation | Re-run the code cells for weaker concepts |
# MAGIC | 0-14 | Beginner | Review each concept's markdown explanation |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What's Next?
# MAGIC
# MAGIC **Notebook 03: SQL & DataFrames** — Concepts #21-#30 covering daily transformation work with both APIs.
# MAGIC
# MAGIC ### Quick Reference Card
# MAGIC
# MAGIC ```python
# MAGIC # Lazy evaluation — build, don't execute
# MAGIC df.filter(...).select(...).groupBy(...)  # transformations only
# MAGIC df.show()  # action — triggers execution
# MAGIC
# MAGIC # See the plan
# MAGIC df.explain(True)  # full 4-phase plan
# MAGIC df.explain()      # physical plan only
# MAGIC
# MAGIC # Joins — pick the right strategy
# MAGIC large_df.join(broadcast(small_df), "key")  # broadcast hash join
# MAGIC large1.join(large2, "key")                  # sort-merge join
# MAGIC
# MAGIC # Partitions — control parallelism
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "50")
# MAGIC df.repartition(50)   # full shuffle, even distribution
# MAGIC df.coalesce(10)      # no shuffle, reduce partitions
# MAGIC
# MAGIC # AQE — let Spark optimize at runtime
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", "true")
# MAGIC spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# MAGIC
# MAGIC # Debugging
# MAGIC spark.sparkContext.uiWebUrl  # Spark UI URL
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC *End of Notebook 02 — Spark Execution*

# COMMAND ----------

# Unpersist cached data to free memory
try:
    products_df.unpersist()
    orders_df.unpersist()
    print("Cached DataFrames unpersisted.")
except Exception:
    pass

print("\nNotebook 02 complete. Ready for Notebook 03: SQL & DataFrames.")
