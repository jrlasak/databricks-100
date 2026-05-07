# Databricks notebook source

# MAGIC %md
# MAGIC # 10 — Architecture & Advanced Patterns
# MAGIC
# MAGIC **Concepts Covered:** #91–#100  
# MAGIC **Environment:** Databricks Community Edition (single-node, free)  
# MAGIC **Goal:** Master advanced architecture patterns, performance diagnostics, testing frameworks, and capstone integration of all 100 concepts.
# MAGIC
# MAGIC | # | Concept | Difficulty | Type |
# MAGIC |---|---------|------------|------|
# MAGIC | 91 | Shallow vs. Deep Clones | Easy | Hands-on |
# MAGIC | 92 | Managed vs. External Tables: Architecture Tradeoffs | Medium | Hands-on |
# MAGIC | 93 | Legacy: Partitioning & Z-ORDER | Medium | Hands-on |
# MAGIC | 94 | Lakehouse Table Design Patterns | Medium | Hands-on |
# MAGIC | 95 | Delta UniForm & Multi-Engine Interoperability | Medium | Conceptual |
# MAGIC | 96 | Compute Policies & Cluster Governance | Medium | Conceptual |
# MAGIC | 97 | AI Functions for Data Engineers | Hard | Conceptual |
# MAGIC | 98 | Multi-Workspace Architecture | Hard | Conceptual |
# MAGIC | 99 | Performance Troubleshooting Methodology | Hard | Hands-on |
# MAGIC | 100 | Testing Patterns for Data Pipelines | Hard | Hands-on |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### GRAND FINALE
# MAGIC
# MAGIC This is the **capstone notebook** of the 100-Concept Databricks Professional Learning series. It ties together concepts from all previous notebooks (#1–#90) and pushes into production-grade architecture and engineering patterns.
# MAGIC
# MAGIC **References to prior notebooks:**
# MAGIC - Notebook 01: Delta Lake Fundamentals (#1–#10)
# MAGIC - Notebook 02: Spark Execution Model (#11–#20)
# MAGIC - Notebook 03: SQL & DataFrames (#21–#30)
# MAGIC - Notebook 04: Data Ingestion Patterns (#31–#40)
# MAGIC - Notebook 05: Streaming & Incremental Patterns (#41–#50)
# MAGIC - Notebook 06: Performance & Cost Optimization (#51–#60)
# MAGIC - Notebook 07: Security & Governance (#61–#70)
# MAGIC - Notebook 08: Orchestration & Workflows (#71–#80)
# MAGIC - Notebook 09: MLflow & ML Engineering (#81–#90)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Environment & Synthetic Data
# MAGIC
# MAGIC We create a dedicated working directory and generate synthetic sales, customer, and product datasets used throughout all 10 concepts.

# COMMAND ----------

import os
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

base_dir = "/tmp/architecture_advanced_patterns"
dbutils.fs.rm(base_dir, recurse=True)
print(f"Working directory: {base_dir}")

# ── Synthetic Sales Transactions ──────────────────────────────────
sales_data = []
for i in range(1, 5001):
    sales_data.append((
        i,                                           # transaction_id
        f"cust_{i % 200 + 1:04d}",                  # customer_id
        f"prod_{i % 50 + 1:03d}",                   # product_id
        round(abs(hash(f"s{i}") % 5000) / 100, 2),  # amount
        f"store_{(i % 10) + 1}",                     # store_id
        f"region_{(i % 4) + 1}",                     # region
        (2024 + (i % 2)),                             # year
        (i % 12) + 1,                                 # month
        "completed" if i % 10 != 0 else "returned"   # status
    ))

sales_df = spark.createDataFrame(sales_data, [
    "transaction_id", "customer_id", "product_id",
    "amount", "store_id", "region", "year", "month", "status"
])
sales_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
    .save(f"{base_dir}/sales")

# ── Customer Dimension ────────────────────────────────────────────
customer_data = []
segments = ["Enterprise", "SMB", "Startup", "Government", "Education"]
tiers = ["Platinum", "Gold", "Silver", "Bronze"]
for i in range(1, 201):
    customer_data.append((
        f"cust_{i:04d}",
        f"Customer_{i}",
        segments[i % 5],
        tiers[i % 4],
        f"region_{(i % 4) + 1}",
        f"202{i % 4 + 1}-01-01" if i % 4 == 0 else None,
    ))

cust_df = spark.createDataFrame(customer_data, [
    "customer_id", "customer_name", "segment", "tier", "region", "acquisition_date"
])
cust_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
    .save(f"{base_dir}/customers")

# ── Product Dimension ─────────────────────────────────────────────
product_data = []
categories = ["Electronics", "Clothing", "Food", "Hardware", "Software"]
for i in range(1, 51):
    product_data.append((
        f"prod_{i:03d}",
        f"Product_{i}",
        categories[i % 5],
        round(abs(hash(f"p{i}") % 5000) / 100, 2)
    ))

prod_df = spark.createDataFrame(product_data, [
    "product_id", "product_name", "category", "list_price"
])
prod_df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
    .save(f"{base_dir}/products")

print("Setup complete. Created:")
print(f"  sales:     {sales_df.count()} rows x {len(sales_df.columns)} cols")
print(f"  customers: {cust_df.count()} rows x {len(cust_df.columns)} cols")
print(f"  products:  {prod_df.count()} rows x {len(prod_df.columns)} cols")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 91 — Shallow vs. Deep Clones [Easy]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC You need a copy of a Delta table for development, testing, or migration — but copying terabytes of data is slow and expensive. Delta Lake provides two cloning strategies that let you choose between speed and independence.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC | Property | SHALLOW CLONE | DEEP CLONE |
# MAGIC |---|---|---|
# MAGIC | **Data copy?** | No — references source files | Yes — full independent copy |
# MAGIC | **Speed** | O(1) — metadata-only operation | O(data size) |
# MAGIC | **Storage cost** | Nearly zero (metadata only) | Full duplicate of data |
# MAGIC | **Independence** | Depends on source files existing | Fully independent |
# MAGIC | **VACUUM risk** | If source is VACUUMed, clone breaks | No risk |
# MAGIC | **Use case** | Dev/test, short-lived experiments | Migration, snapshots, hand-offs |
# MAGIC
# MAGIC ### Real-World Use Cases
# MAGIC - **Shallow Clone:** Create a QA copy of production data for testing — instant, costs nothing extra.
# MAGIC - **Deep Clone:** Migrate a table to a new storage location or cloud provider; create an immutable snapshot for audit.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the Source Table

# COMMAND ----------

print("=" * 60)
print("CONCEPT 91: SHALLOW vs DEEP CLONES")
print("=" * 60)

source_table = f"{base_dir}/clone_source"
sales_df.write.format("delta").mode("overwrite").save(source_table)

# Register as SQL table
spark.sql(f"CREATE OR REPLACE TABLE clone_source USING DELTA LOCATION '{source_table}'")

print("\nSource table details:")
spark.sql("DESCRIBE DETAIL clone_source").select(
    "name", "numFiles", "sizeInBytes", "location"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a SHALLOW CLONE (Zero-Copy)

# COMMAND ----------

t0 = time.time()
spark.sql(f"CREATE OR REPLACE TABLE clone_shallow SHALLOW CLONE clone_source")
t_shallow = time.time() - t0

print(f"SHALLOW CLONE created in {t_shallow:.3f}s")
print("\nShallow clone details:")
spark.sql("DESCRIBE DETAIL clone_shallow").select(
    "name", "numFiles", "sizeInBytes", "location"
).show(truncate=False)

shallow_loc = spark.sql("DESCRIBE DETAIL clone_shallow").select("location").collect()[0][0]
source_loc = spark.sql("DESCRIBE DETAIL clone_source").select("location").collect()[0][0]
print(f"\nSource location:     {source_loc}")
print(f"Shallow clone loc:   {shallow_loc}")
print(f"\nNOTE: Shallow clone has its OWN _delta_log directory ({shallow_loc})")
print(f"      but the data files (.parquet) still live in the source location.")
print(f"      This is why it's near-instant: only metadata is duplicated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create a DEEP CLONE (Full Independent Copy)

# COMMAND ----------

t0 = time.time()
spark.sql(f"CREATE OR REPLACE TABLE clone_deep DEEP CLONE clone_source")
t_deep = time.time() - t0

print(f"DEEP CLONE created in {t_deep:.3f}s")
print("\nDeep clone details:")
spark.sql("DESCRIBE DETAIL clone_deep").select(
    "name", "numFiles", "sizeInBytes", "location"
).show(truncate=False)

deep_loc = spark.sql("DESCRIBE DETAIL clone_deep").select("location").collect()[0][0]
print(f"\nDeep clone location: {deep_loc}")
print(f"\nSpeed comparison:")
print(f"  Shallow clone: {t_shallow:.3f}s (metadata only)")
print(f"  Deep clone:    {t_deep:.3f}s (full data copy + metadata)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Verify Data Independence

# COMMAND ----------

# Show all three tables have identical data
print("Row counts across all three tables:")
display(spark.sql("""
    SELECT 'source'  AS table_name, COUNT(*) AS row_count FROM clone_source
    UNION ALL
    SELECT 'shallow' AS table_name, COUNT(*) AS row_count FROM clone_shallow
    UNION ALL
    SELECT 'deep'    AS table_name, COUNT(*) AS row_count FROM clone_deep
"""))

# Now INSERT into the shallow clone — does it affect the source?
spark.sql("INSERT INTO clone_shallow SELECT 99999, 'cust_test', 'prod_001', 99.99, 'store_1', 'region_1', 2025, 1, 'pending'")

print("\nAfter inserting into SHALLOW clone:")
print(f"  Source rows:   {spark.sql('SELECT COUNT(*) FROM clone_source').collect()[0][0]}")
print(f"  Shallow rows:  {spark.sql('SELECT COUNT(*) FROM clone_shallow').collect()[0][0]}")
print(f"  Deep rows:     {spark.sql('SELECT COUNT(*) FROM clone_deep').collect()[0][0]}")
print("  → Shows clones are logically independent (their own transaction logs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Decision Tree

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                      CLONE STRATEGY DECISION TREE                        ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  Need a table copy?                                                      ║
║  ├─ Short-lived (dev/test, experiments)?                                 ║
║  │  └─ SHALLOW CLONE — instant, near-zero cost                           ║
║  │                                                                       ║
║  ├─ Long-lived and independent?                                          ║
║  │  └─ Will source ever be VACUUMed or deleted?                          ║
║  │     ├─ YES → DEEP CLONE — data lives independently                    ║
║  │     └─ NO  → SHALLOW CLONE may be fine (but risky)                    ║
║  │                                                                       ║
║  ├─ Migrating to new location/cloud?                                     ║
║  │  └─ DEEP CLONE — with LOCATION clause to specify new path             ║
║  │                                                                       ║
║  ├─ Immutable snapshot for audit?                                        ║
║  │  └─ DEEP CLONE — guarantee independence from source mutations         ║
║  │                                                                       ║
║  └─ PARTITION FILTER needed? (clone only specific partitions)            ║
║     └─ Use CLONE with WHERE clause                                       ║
║                                                                          ║
║  KEY INSIGHT: Shallow clone files are in SOURCE directory;               ║
║              if source is VACUUMed → shallow clone BREAKS.               ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 92 — Managed vs. External Tables: Architecture Tradeoffs [Medium]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC When you create a Delta table, you choose who controls the data's lifecycle — Databricks (managed) or you (external). This decision has deep implications for governance, multi-engine access, disaster recovery, and cost.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC | Property | MANAGED TABLE | EXTERNAL TABLE |
# MAGIC |---|---|---|
# MAGIC | **Data location** | Databricks chooses (Unity Catalog managed storage) | You specify the path |
# MAGIC | **DROP TABLE** | Deletes data AND metadata | Deletes metadata only; data survives |
# MAGIC | **Lifecycle** | Databricks controls retention, VACUUM, cleanup | You manage everything |
# MAGIC | **Multi-engine access** | Limited (only through Databricks) | Any engine that reads Delta (Spark, Trino, Dremio, etc.) |
# MAGIC | **Predictive Optimization** | Supported | Not supported (requires managed tables in Unity Catalog) |
# MAGIC | **Governance** | Full Unity Catalog integration | You manage permissions on cloud storage too |
# MAGIC
# MAGIC **NOTE:** Managed tables require Unity Catalog (full platform). In Community Edition we use `LOCATION` to control storage explicitly.
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC **Choose Managed when:**
# MAGIC - Databricks is the sole compute engine
# MAGIC - You want Predictive Optimization
# MAGIC - Governance and lifecycle are managed centrally
# MAGIC - Simpler operational model
# MAGIC
# MAGIC **Choose External when:**
# MAGIC - Multiple engines read the same data (Spark, Trino, Presto, Dremio, Snowflake, BigQuery)
# MAGIC - You own the cloud storage lifecycle (regulatory/data residency requirements)
# MAGIC - You need fine-grained control over file layout and retention policies
# MAGIC - The data must survive DROP TABLE or workspace deletion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create Managed-Style and External-Style Tables

# COMMAND ----------

print("=" * 60)
print("CONCEPT 92: MANAGED vs EXTERNAL TABLES")
print("=" * 60)

# ── Managed-style table (no explicit LOCATION) ────────────────────
spark.sql("DROP TABLE IF EXISTS sales_managed")
spark.sql("""
    CREATE TABLE sales_managed (
        transaction_id INT,
        customer_id STRING,
        product_id STRING,
        amount DECIMAL(10,2),
        store_id STRING
    ) USING DELTA
""")
spark.sql("INSERT INTO sales_managed SELECT transaction_id, customer_id, product_id, amount, store_id FROM clone_source")
print("Managed-style table created (Databricks controls the data location).")

# ── External-style table (explicit LOCATION) ─────────────────────
spark.sql("DROP TABLE IF EXISTS sales_external")
external_data_loc = f"{base_dir}/external_data/sales"
spark.sql(f"""
    CREATE TABLE sales_external (
        transaction_id INT,
        customer_id STRING,
        product_id STRING,
        amount DECIMAL(10,2),
        store_id STRING
    ) USING DELTA
    LOCATION '{external_data_loc}'
""")
spark.sql("INSERT INTO sales_external SELECT transaction_id, customer_id, product_id, amount, store_id FROM clone_source")
print(f"External-style table created at: {external_data_loc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: DESCRIBE EXTENDED — See the Table Type

# COMMAND ----------

print("Managed table — DESCRIBE EXTENDED:")
desc_managed = spark.sql("DESCRIBE EXTENDED sales_managed").filter(
    col("col_name").isin("Location", "Type", "Provider")
)
desc_managed.show(truncate=False)

print("\nExternal table — DESCRIBE EXTENDED:")
desc_external = spark.sql("DESCRIBE EXTENDED sales_external").filter(
    col("col_name").isin("Location", "Type", "Provider")
)
desc_external.show(truncate=False)

# ── List files for external table location ───────────────────────
print(f"\nFiles at external data location ({external_data_loc}):")
for f in dbutils.fs.ls(external_data_loc):
    print(f"  {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: DROP Behavior — The Critical Difference

# COMMAND ----------

# ── Create two MORE tables specifically for the drop test ─────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS drop_test_managed (
        transaction_id INT, amount DECIMAL(10,2)
    ) USING DELTA
""")
spark.sql("INSERT INTO drop_test_managed VALUES (1, 100.00), (2, 200.00)")

drop_test_external_loc = f"{base_dir}/drop_test_external"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS drop_test_external (
        transaction_id INT, amount DECIMAL(10,2)
    ) USING DELTA
    LOCATION '{drop_test_external_loc}'
""")
spark.sql("INSERT INTO drop_test_external VALUES (1, 100.00), (2, 200.00)")

# Verify data files exist
print("Before DROP:")
print(f"  External data files: {len([f for f in dbutils.fs.ls(drop_test_external_loc) if f.name.endswith('.parquet')])}")

# ── DROP BOTH TABLES ─────────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS drop_test_managed")
spark.sql("DROP TABLE IF EXISTS drop_test_external")

print("\nAfter DROP:")
try:
    files_after = [f for f in dbutils.fs.ls(drop_test_external_loc) if f.name.endswith('.parquet')]
    print(f"  External data files STILL EXIST: {len(files_after)}")
    print("  ✓ External table data SURVIVES DROP")
except Exception as e:
    print(f"  External data files NOT FOUND: {e}")
    print("  (Managed table data would be deleted — cannot verify in Community Edition)")

# Clean up
dbutils.fs.rm(drop_test_external_loc, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decision Matrix

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                MANAGED vs EXTERNAL — DECISION MATRIX                     ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  REQUIREMENT                        MANAGED    EXTERNAL                  ║
║  ──────────────────────────────────────────────────────                  ║
║  Databricks-only access               ✓          ✓                       ║
║  Multi-engine reads (Trino/Snowflake)  ✗          ✓                      ║
║  Predictive Optimization               ✓          ✗                      ║
║  Data survives DROP TABLE              ✗          ✓                      ║
║  Data survives workspace deletion      ✗          ✓                      ║
║  Centralized lifecycle management      ✓          ✗                      ║
║  Regulatory data residency              ✗*         ✓                     ║
║  Unity Catalog governance               ✓         ✓(registered)          ║
║  Liquid Clustering                      ✓          ✓                     ║
║  DELETE/UPDATE/MERGE                    ✓          ✓                     ║
║  Time Travel (DESCRIBE HISTORY)         ✓          ✓                     ║
║                                                                          ║
║  * Managed tables use Databricks-managed storage; you choose             ║
║    the region at workspace creation but have less granular control.      ║
║                                                                          ║
║  BEST PRACTICE: Start with MANAGED for operational simplicity;           ║
║                switch to EXTERNAL when you hit an explicit need.         ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 93 — Legacy: Partitioning & Z-ORDER [Medium]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC Before Liquid Clustering (Delta 3.0+, full platform), the primary ways to organize data for fast queries were **Hive-style partitioning** and **Z-ORDER**. While Liquid Clustering has largely superseded these, they remain relevant for legacy systems, external readers, and specific low-cardinality use cases.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC **Partitioning:**
# MAGIC - Divides data into folder hierarchies (e.g., `year=2024/month=01/`)
# MAGIC - File-level pruning via directory listing — no metadata read needed
# MAGIC - Works with ANY reader (even non-Delta!) — this is why it still matters
# MAGIC - **Downside:** Too many partitions → small file problem; too few → no pruning benefit
# MAGIC - **Rule of thumb:** Only partition on low-cardinality columns (year, month, region, category)
# MAGIC
# MAGIC **Z-ORDER:**
# MAGIC - Co-locates related data within the same set of files
# MAGIC - Speeds up queries with filters on the Z-ORDER columns
# MAGIC - Works WITHIN partitions (not as a replacement)
# MAGIC - **Downside:** Must be reapplied after writes; maintenance overhead
# MAGIC
# MAGIC **Why Liquid Clustering is better:**
# MAGIC - No manual partition key selection
# MAGIC - Adaptive — reclusters incrementally
# MAGIC - No small-file problem from bad partition choices
# MAGIC - BUT: Requires Delta 3.0+ and full Databricks platform

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a Hive-Partitioned Table

# COMMAND ----------

print("=" * 60)
print("CONCEPT 93: LEGACY PARTITIONING & Z-ORDER")
print("=" * 60)

# ── Create a larger dataset for meaningful partitioning ───────────
partitioned_path = f"{base_dir}/legacy_partitioned"

df_part = sales_df.withColumn("year_month", 
    concat(col("year").cast("string"), lit("-"), lpad(col("month").cast("string"), 2, "0"))
)

df_part.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(partitioned_path)

print("Partitioned table created with partitionBy('year', 'month')")
print(f"\nDirectory structure at {partitioned_path}:")
for item in sorted(dbutils.fs.ls(partitioned_path), key=lambda x: x.name):
    if item.name.startswith("year"):
        print(f"  {item.name}/")
        for sub in dbutils.fs.ls(item.path):
            if sub.name.startswith("month"):
                file_count = len([f for f in dbutils.fs.ls(sub.path) if f.name.endswith('.parquet')])
                print(f"    {sub.name}/  ({file_count} parquet files)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Demonstrate Partition Pruning

# COMMAND ----------

print("Query 1: Full table scan (no partition filter)")
t0 = time.time()
result_full = spark.sql(f"SELECT COUNT(*) FROM delta.`{partitioned_path}`").collect()[0][0]
t_full = time.time() - t0

print(f"  Full scan: {result_full} rows in {t_full:.3f}s")

print("\nQuery 2: Single partition filter (year=2024, month=6)")
t0 = time.time()
result_part = spark.sql(
    f"SELECT COUNT(*) FROM delta.`{partitioned_path}` WHERE year = 2024 AND month = 6"
).collect()[0][0]
t_part = time.time() - t0

print(f"  Partition filter: {result_part} rows in {t_part:.3f}s")

# Show the query plan — note the PartitionFilters
print("\nQuery plan WITH partition filter:")
spark.sql(
    f"EXPLAIN EXTENDED SELECT * FROM delta.`{partitioned_path}` WHERE year = 2024 AND month = 6"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Apply Z-ORDER and Compare

# COMMAND ----------

zorder_path = f"{base_dir}/legacy_zordered"

# Create a non-partitioned version for Z-ORDER demo
sales_df.write.format("delta").mode("overwrite").save(zorder_path)

# Test query speed before Z-ORDER
print("Before Z-ORDER:")
spark.sql(f"DESCRIBE DETAIL delta.`{zorder_path}`").select("numFiles", "sizeInBytes").show()

t0 = time.time()
result_before = spark.sql(
    f"SELECT * FROM delta.`{zorder_path}` WHERE customer_id = 'cust_0050'"
).count()
t_before_z = time.time() - t0
print(f"Filter on customer_id (no Z-ORDER): {result_before} matching rows in {t_before_z:.3f}s")

# ── Apply Z-ORDER ────────────────────────────────────────────────
print("\nApplying Z-ORDER BY (customer_id)...")
t0 = time.time()
spark.sql(f"OPTIMIZE delta.`{zorder_path}` ZORDER BY (customer_id)")
t_zorder = time.time() - t0
print(f"Z-ORDER completed in {t_zorder:.2f}s")

print("\nAfter Z-ORDER:")
spark.sql(f"DESCRIBE DETAIL delta.`{zorder_path}`").select("numFiles", "sizeInBytes").show()

# Test query speed after Z-ORDER
t0 = time.time()
result_after = spark.sql(
    f"SELECT * FROM delta.`{zorder_path}` WHERE customer_id = 'cust_0050'"
).count()
t_after_z = time.time() - t0
print(f"Filter on customer_id (WITH Z-ORDER): {result_after} matching rows in {t_after_z:.3f}s")

print(f"\n  Improvement: {t_before_z:.3f}s → {t_after_z:.3f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Migration Strategy — Legacy to Liquid Clustering

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║             LEGACY → LIQUID CLUSTERING MIGRATION STRATEGY                ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  1. AUDIT existing partitioning schemes                                  ║
║     └─ Identify tables with many small files (bad partition key)         ║
║     └─ Identify tables where partitioning adds little value              ║
║                                                                          ║
║  2. PRIORITIZE high-impact tables                                        ║
║     └─ Tables with > 10k partitions → migrate first                      ║
║     └─ Tables where OPTIMIZE runs frequently → migrate                   ║
║                                                                          ║
║  3. ENABLE Liquid Clustering (Delta 3.0+, full platform)                 ║
║     └─ ALTER TABLE t CLUSTER BY (column1, column2)                       ║
║     └─ Liquid Clustering works alongside existing partitions             ║
║                                                                          ║
║  4. GRADUALLY remove partitioning                                        ║
║     └─ Create CLONED table (DEEP CLONE) without partition columns        ║
║     └─ Apply CLUSTER BY on the new table                                 ║
║     └─ Validate query performance, then swap                             ║
║                                                                          ║
║  5. When to KEEP legacy partitioning:                                    ║
║     └─ External readers (Trino/Presto/Dremio) that benefit from          ║
║        directory pruning                                                 ║
║     └─ Very low cardinality (year, continent) — directory pruning        ║
║        is still fast                                                     ║
║     └─ Mixed workload where partition-awareness matters downstream       ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 94 — Lakehouse Table Design Patterns [Medium]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC Should you store data in a wide denormalized table for fast queries, or in a normalized star schema for flexibility? The lakehouse architecture supports both — and the choice has significant performance, maintainability, and cost implications.
# MAGIC
# MAGIC ### Key Patterns
# MAGIC
# MAGIC | Pattern | Structure | Best For | Downside |
# MAGIC |---|---|---|---|
# MAGIC | **Wide Denormalized** | All columns in one table | BI dashboards, ad-hoc analytics | Redundant data, slower writes, complex updates |
# MAGIC | **Star Schema** | Fact + dimension tables | ETL pipelines, data warehouse | JOIN required for every query |
# MAGIC | **One Big Table (OBT)** | Pre-joined, pre-aggregated | Real-time dashboards | Extremely wide, expensive refreshes |
# MAGIC | **Data Vault** | Hub/Link/Satellite tables | Enterprise data integration, audit trails | Complex to query |
# MAGIC
# MAGIC ### Design Principles for Lakehouse
# MAGIC 1. **Medallion Architecture**: Bronze (raw) → Silver (cleansed) → Gold (business-level)
# MAGIC 2. **Column ordering matters**: First 32 columns get statistics — put filter columns first
# MAGIC 3. **Clustering keys**: Align with most common query filters, not just any column
# MAGIC 4. **Right-size data types**: DECIMAL(10,2) not DOUBLE for currency; INT not BIGINT when possible

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Design a Star Schema (Gold Layer)

# COMMAND ----------

print("=" * 60)
print("CONCEPT 94: LAKEHOUSE TABLE DESIGN PATTERNS")
print("=" * 60)

# ── Fact Table: sales_facts (thin, many rows) ────────────────────
fact_sales = spark.sql(f"""
    CREATE OR REPLACE TABLE gold.fact_sales
    USING DELTA
    LOCATION '{base_dir}/gold/fact_sales'
    AS
    SELECT
        transaction_id,
        customer_id,
        product_id,
        store_id,
        amount,
        year,
        month,
        CASE WHEN status = 'returned' THEN 1 ELSE 0 END AS is_returned,
        CAST(amount AS DECIMAL(10,2)) AS net_amount
    FROM delta.`{base_dir}/sales`
""")
print("Fact table created: gold.fact_sales")

# ── Dimension: Customer ───────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TABLE gold.dim_customer
    USING DELTA
    LOCATION '{base_dir}/gold/dim_customer'
    AS SELECT * FROM delta.`{base_dir}/customers`
""")
print("Dimension table created: gold.dim_customer")

# ── Dimension: Product ────────────────────────────────────────────
spark.sql(f"""
    CREATE OR REPLACE TABLE gold.dim_product
    USING DELTA
    LOCATION '{base_dir}/gold/dim_product'
    AS SELECT * FROM delta.`{base_dir}/products`
""")
print("Dimension table created: gold.dim_product")

# ── Dimension: Date (generated) ───────────────────────────────────
date_data = [(2024, m, f"2024-{m:02d}-01") for m in range(1, 13)] + \
            [(2025, m, f"2025-{m:02d}-01") for m in range(1, 13)]
date_df = spark.createDataFrame(date_data, ["year", "month", "month_start"])

date_df.write.format("delta").mode("overwrite").save(f"{base_dir}/gold/dim_date")
spark.sql(f"""
    CREATE OR REPLACE TABLE gold.dim_date
    USING DELTA
    LOCATION '{base_dir}/gold/dim_date'
    AS SELECT * FROM delta.`{base_dir}/gold/dim_date`
""")
print("Dimension table created: gold.dim_date")

print("\nStar Schema Summary:")
display(spark.sql("""
    SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM gold.fact_sales
    UNION ALL
    SELECT 'dim_customer', COUNT(*) FROM gold.dim_customer
    UNION ALL
    SELECT 'dim_product',  COUNT(*) FROM gold.dim_product
    UNION ALL
    SELECT 'dim_date',     COUNT(*) FROM gold.dim_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Query Pattern — Star Schema JOIN

# COMMAND ----------

print("Star Schema Query: Sales by customer segment and product category")
t0 = time.time()

star_query = spark.sql("""
    SELECT
        c.segment,
        p.category,
        SUM(f.net_amount) AS total_revenue,
        COUNT(DISTINCT f.transaction_id) AS transaction_count,
        ROUND(AVG(f.amount), 2) AS avg_transaction_value
    FROM gold.fact_sales f
    JOIN gold.dim_customer c ON f.customer_id = c.customer_id
    JOIN gold.dim_product  p ON f.product_id = p.product_id
    WHERE f.is_returned = 0
    GROUP BY c.segment, p.category
    ORDER BY c.segment, total_revenue DESC
""")

display(star_query)
print(f"Star schema query completed in {time.time() - t0:.3f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Alternative — Wide Denormalized Table for Reporting

# COMMAND ----------

# ── Pre-join everything into one wide table ───────────────────────
wide_reporting_path = f"{base_dir}/gold/wide_sales_reporting"

spark.sql(f"""
    CREATE OR REPLACE TABLE gold.wide_sales_reporting
    USING DELTA
    LOCATION '{wide_reporting_path}'
    AS
    SELECT
        f.transaction_id,
        f.amount,
        f.net_amount,
        f.year,
        f.month,
        f.store_id,
        f.is_returned,
        c.customer_name,
        c.segment AS customer_segment,
        c.tier AS customer_tier,
        c.region AS customer_region,
        p.product_name,
        p.category AS product_category,
        p.list_price
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customer c ON f.customer_id = c.customer_id
    LEFT JOIN gold.dim_product  p ON f.product_id = p.product_id
""")
print("Wide denormalized table created: gold.wide_sales_reporting")

# ── Same analysis, NO JOINS needed ───────────────────────────────
print("\nSame query on wide denormalized table (NO JOINS):")
t0 = time.time()

wide_query = spark.sql("""
    SELECT
        customer_segment,
        product_category,
        SUM(net_amount) AS total_revenue,
        COUNT(DISTINCT transaction_id) AS transaction_count,
        ROUND(AVG(amount), 2) AS avg_transaction_value
    FROM gold.wide_sales_reporting
    WHERE is_returned = 0
    GROUP BY customer_segment, product_category
    ORDER BY customer_segment, total_revenue DESC
""")

display(wide_query)
print(f"Wide table query completed in {time.time() - t0:.3f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Design Tradeoffs Summary

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                    TABLE DESIGN PATTERN TRADEOFFS                        ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  STAR SCHEMA (normalized)                                                ║
║  ├─ ✓ Storage-efficient (no duplicate dimension data)                   ║
║  ├─ ✓ Easy to update dimensions independently                            ║
║  ├─ ✓ Flexible — add dimensions without changing facts                  ║
║  ├─ ✓ Best for ETL patterns, complex transformations                    ║
║  └─ ✗ Every query needs JOINs — can be slow without optimization        ║
║                                                                          ║
║  WIDE DENORMALIZED                                                       ║
║  ├─ ✓ Lightning-fast reads (no JOINs)                                   ║
║  ├─ ✓ Simple queries — BI tools love wide tables                        ║
║  ├─ ✓ Best for dashboards, ad-hoc analytics                             ║
║  └─ ✗ Redundant data (customer name in every row)                       ║
║  └─ ✗ Expensive to update dimensions (must rewrite entire table)        ║
║  └─ ✗ Wide schemas hit the 32-column stats limit                        ║
║                                                                          ║
║  HYBRID (recommended for most lakehouses)                                ║
║  ├─ Gold layer: Wide reporting tables for BI consumers                  ║
║  ├─ Silver layer: Normalized star schema for transformations             ║
║  ├─ Bronze layer: Raw ingest (no design — fidelity matters)             ║
║  └─ Rebuild wide tables nightly/weekly; keep star schema as source      ║
║                                                                          ║
║  DESIGN HEURISTICS:                                                      ║
║  ├─ Put most-filtered columns in the FIRST 32 positions                 ║
║  ├─ Cluster/partition on DATE columns (most queries filter by time)     ║
║  ├─ Use DECIMAL for currency (not FLOAT/DOUBLE)                         ║
║  ├─ Avoid deeply nested STRUCTs in frequently queried columns           ║
║  └─ Use MERGE for dimension updates, INSERT for fact appends            ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 95 — Delta UniForm & Multi-Engine Interoperability [Medium]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC Your organization uses multiple engines — Spark for ETL, Snowflake for BI, BigQuery for ML, Trino for ad-hoc. Each engine wants its own table format (Delta, Iceberg, Hudi). Copying data between formats is expensive, slow, and creates consistency problems.
# MAGIC
# MAGIC **Delta UniForm** solves this: write once as Delta, and the table is automatically readable as Iceberg (and soon Hudi) — with no data copy.
# MAGIC
# MAGIC **NOTE:** UniForm requires Delta 3.0+ on full Databricks platform. This section explains the concept, configuration, and decision framework.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC | Approach | How It Works | Pros | Cons |
# MAGIC |---|---|---|---|
# MAGIC | **UniForm** | Single Delta table auto-generates Iceberg/Hudi metadata | One copy, all engines | Full platform only |
# MAGIC | **Delta Sharing** | Databricks shares data via open protocol | No copy, governed access | Recipient needs Databricks or open connector |
# MAGIC | **Federation** | Query external data in-place via Lakehouse Federation | No copy, no ingest | Query performance depends on remote system |
# MAGIC | **Manual Export** | Periodically copy data to another format | Works everywhere | Duplicate data, stale, expensive |
# MAGIC
# MAGIC ### How UniForm Works (Under the Hood)
# MAGIC
# MAGIC 1. You enable UniForm on a Delta table: `ENABLE UNIFORM(ICEBERG_COMPAT_VERSION=2)`
# MAGIC 2. On each Delta commit, Databricks automatically generates Iceberg metadata files alongside the Delta log
# MAGIC 3. Iceberg readers (Trino, Snowflake, BigQuery, Athena) see the same data files — they just read a different metadata path
# MAGIC 4. No data duplication — the Parquet files are the same

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Simulate UniForm Metadata Structure (Conceptual)

# COMMAND ----------

print("=" * 60)
print("CONCEPT 95: DELTA UNIFORM & MULTI-ENGINE INTEROPERABILITY")
print("=" * 60)

# Show what a UniForm-enabled table's directory looks like
print("""
 Delta Table with UniForm enabled:

   my_table/
   ├── _delta_log/           ← Delta transaction log (native)
   │   ├── 000000.json
   │   ├── 000001.json
   │   └── ...
   ├── metadata/             ← Iceberg metadata (auto-generated by UniForm)
   │   ├── v1.metadata.json
   │   ├── v2.metadata.json
   │   └── snap-*.avro
   ├── part-00000-xxx.parquet  ← Same data files for ALL engines
   ├── part-00001-xxx.parquet
   └── ...

 KEY INSIGHT: The .parquet data files are SHARED.
              Only the METADATA layer differs per format.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Decision Tree — UniForm vs Sharing vs Federation

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║          MULTI-ENGINE ACCESS: DECISION FRAMEWORK                         ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  START HERE:  Does the consumer use Databricks or an open connector?     ║
║                                                                          ║
║  ├─ YES → DELTA SHARING                                                  ║
║  │    └─ Recipient gets governed, live access. No data copy.             ║
║  │    └─ Best when: you control both sides, want centralized governance  ║
║  │                                                                       ║
║  ├─ Consumer uses ICEBERG-compatible engine (Trino, Snowflake, etc.)?    ║
║  │  ├─ YES → UNIFORM                                                     ║
║  │  │    └─ Single Delta table, readable as Iceberg                      ║
║  │  │    └─ Best when: one producer (Databricks), many consumers         ║
║  │  │    └─ Example: Write ETL in Databricks; BI team reads via Trino    ║
║  │  │                                                                    ║
║  │  └─ Consumer uses HUDI?                                               ║
║  │     └─ UniForm Hudi support (roadmap)                                 ║
║  │                                                                       ║
║  └─ Consumer needs to query data in ANOTHER system's native storage?     ║
║     └─ LAKEHOUSE FEDERATION                                              ║
║        └─ Query Snowflake/Postgres/MySQL tables directly from Databricks ║
║        └─ Best when: data lives elsewhere, occasional access needed      ║
║                                                                          ║
║  COST COMPARISON:                                                        ║
║  ├─ UniForm: Metadata generation cost (tiny). Storage = 1x data.         ║
║  ├─ Delta Sharing: No extra storage. Egress costs may apply.             ║
║  ├─ Federation: Query pushdown to source system. Source bears query cost.║
║  └─ Manual Export: 2x+ storage. Stale data. High maintenance.            ║
║                                                                          ║
║  COMPATIBILITY MATRIX:                                                   ║
║  Engine               Delta   Iceberg   Hudi      Notes                  ║
║  ──────────────────────────────────────────────────                      ║
║  Databricks             ✓       ✗*       ✗*      *Unless via UniForm     ║
║  Apache Spark           ✓       ✓         ✓       Depends on config      ║
║  Snowflake               ✗       ✓         ✗      Iceberg tables         ║
║  BigQuery                ✗       ✓         ✗      Iceberg via BigLake    ║
║  Trino/Presto            ✗       ✓         ✓      Plugins required       ║
║  Athena                  ✗       ✓         ✗      Iceberg via Glue       ║
║  Dremio                  ✓       ✓         ✗      Direct Delta + Iceberg ║
║  Redshift Spectrum       ✗       ✓         ✗      Iceberg only           ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Conceptual UniForm Configuration

# COMMAND ----------

# ── This is conceptual code — UniForm requires full platform ─────
print("""
── Configuring UniForm (Full Platform Only) ──

-- Enable UniForm on an existing table:
ALTER TABLE sales_facts
SET TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg'
);

-- Check UniForm status:
DESCRIBE EXTENDED sales_facts;
-- Look for: delta.universalFormat.enabledFormats

-- External engines connect to the Iceberg catalog path:
--   s3://bucket/table/metadata/

-- On Trino, for example:
--   CREATE TABLE trino_sales (...)
--   WITH (format = 'ICEBERG', location = 's3://bucket/table/');

── Considerations ──
• UniForm adds small metadata write overhead on each Delta commit
• Iceberg metadata is auto-generated — no manual sync needed
• Column mapping mode must be 'name' (not 'id') for Iceberg compat
• Partition evolution and schema evolution work natively
• VACUUM must respect both Delta and Iceberg retention requirements
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 96 — Compute Policies & Cluster Governance [Medium]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC Developers create clusters with 100 nodes and forget to terminate them. Costs spiral. You need guardrails that balance developer productivity with budget discipline — without requiring every cluster to go through an approval process.
# MAGIC
# MAGIC **Compute policies** define what cluster configurations are allowed. They act as templates with fixed values, allowed ranges, and mandatory tags.
# MAGIC
# MAGIC **NOTE:** Policies are managed at the admin level (full platform). This section shows the policy structure and governance concepts applicable in any Databricks environment.
# MAGIC
# MAGIC ### Key Concepts
# MAGIC
# MAGIC | Policy Element | Purpose | Example |
# MAGIC |---|---|---|
# MAGIC | **Fixed values** | Lock a setting (cannot be changed) | `autotermination_minutes: 30` |
# MAGIC | **Allowed ranges** | Limit a setting to a range | `num_workers: {min: 1, max: 10}` |
# MAGIC | **Default values** | Pre-fill a setting (can be changed) | `spark_version: "13.3.x-scala2.12"` |
# MAGIC | **Required tags** | Force tagging for cost tracking | `"cost_center": {"type": "fixed", "value": "required"}` |
# MAGIC | **Hidden fields** | Remove advanced options from UI | Spot instance config |
# MAGIC
# MAGIC ### Policy Types
# MAGIC - **Personal Compute (default):** Per-user, auto-terminates, limited size
# MAGIC - **Power User:** More workers, spot instances allowed, longer auto-termination
# MAGIC - **Job Compute:** Optimized for jobs, no interactive features, aggressive auto-termination
# MAGIC - **Restricted:** Only specific instance types and versions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Sample Compute Policy JSON

# COMMAND ----------

print("=" * 60)
print("CONCEPT 96: COMPUTE POLICIES & CLUSTER GOVERNANCE")
print("=" * 60)

# Define sample policies as Python dicts for analysis
general_policy = {
    "name": "Standard Developer",
    "description": "Balanced policy for daily development work",
    "definition": json.dumps({
        "autotermination_minutes": {
            "type": "range",
            "minValue": 15,
            "maxValue": 120,
            "defaultValue": 30,
            "isOptional": False
        },
        "num_workers": {
            "type": "range",
            "minValue": 2,
            "maxValue": 20,
            "defaultValue": 4,
            "isOptional": False
        },
        "node_type_id": {
            "type": "allowlist",
            "values": [
                "i3.xlarge",
                "i3.2xlarge",
                "m5d.xlarge",
                "m5d.2xlarge"
            ],
            "defaultValue": "i3.xlarge"
        },
        "custom_tags.cost_center": {
            "type": "regex",
            "pattern": "^CC-[0-9]{4}$",
            "defaultValue": "CC-0000"
        },
        "custom_tags.environment": {
            "type": "fixed",
            "value": "development"
        },
        "spark_conf.spark.sql.shuffle.partitions": {
            "type": "fixed",
            "value": "auto"
        },
        "driver_node_type_id": {
            "type": "fixed",
            "value": "i3.xlarge"
        }
    }, indent=2)
}

print("── Sample Policy: Standard Developer ──")
print(json.dumps(json.loads(general_policy["definition"]), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Cost-Effective Cluster Policy (Job Compute)

# COMMAND ----------

job_policy = {
    "name": "Production Jobs",
    "description": "Optimized for scheduled jobs — spot instances, tight auto-termination",
    "definition": json.dumps({
        "autotermination_minutes": {
            "type": "fixed",
            "value": 15,
            "isOptional": False
        },
        "num_workers": {
            "type": "range",
            "minValue": 2,
            "maxValue": 50,
            "defaultValue": 8,
            "isOptional": False
        },
        "node_type_id": {
            "type": "allowlist",
            "values": [
                "i3.xlarge",
                "i3.2xlarge",
                "i3.4xlarge",
                "i3en.xlarge",
                "i3en.2xlarge"
            ],
            "defaultValue": "i3.xlarge"
        },
        "enable_elastic_disk": {
            "type": "fixed",
            "value": True
        },
        "spark_conf.spark.sql.adaptive.enabled": {
            "type": "fixed",
            "value": "true"
        },
        "spark_conf.spark.sql.adaptive.coalescePartitions.enabled": {
            "type": "fixed",
            "value": "true"
        },
        "spark_conf.spark.databricks.delta.optimizeWrite.enabled": {
            "type": "fixed",
            "value": "true"
        },
        "custom_tags.cost_center": {
            "type": "regex",
            "pattern": "^CC-[0-9]{4}$",
            "defaultValue": "CC-0000"
        },
        "custom_tags.workload_type": {
            "type": "fixed",
            "value": "production_job"
        }
    }, indent=2)
}

print("── Sample Policy: Production Jobs ──")
print(json.dumps(json.loads(job_policy["definition"]), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Governance Framework Summary

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                  COMPUTE GOVERNANCE FRAMEWORK                             ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  THREE PILLARS OF COST CONTROL:                                          ║
║                                                                          ║
║  1. PREVENT (Policies)                                                   ║
║     ├─ Limit max workers (e.g., 20 for dev, 50 for prod)                 ║
║     ├─ Restrict instance types to cost-effective families (i3, m5d)      ║
║     ├─ Enforce auto-termination (15-30 min)                               ║
║     ├─ Require cost_center and environment tags                           ║
║     └─ Disable expensive features for dev (Photon, GPU)                   ║
║                                                                          ║
║  2. MONITOR (Usage Tracking)                                             ║
║     ├─ system.billing.usage table (full platform)                        ║
║     ├─ Tag-based cost allocation by team/department                       ║
║     ├─ Set up budgets and alerts                                         ║
║     └─ Weekly cost review with platform team                             ║
║                                                                          ║
║  3. OPTIMIZE (Right-Sizing)                                              ║
║     ├─ Review cluster utilization: < 50% util? Downsize.                 ║
║     ├─ Use Jobs Compute for pipelines (cheaper than All-Purpose)         ║
║     ├─ Serverless for bursty SQL workloads                               ║
║     ├─ Spot instances for batch jobs (30-50% cheaper)                    ║
║     └─ Instance pools for warm start (reduces start-up time, saves DBU) ║
║                                                                          ║
║  POLICY ESCALATION PATTERN:                                              ║
║                                                                          ║
║  Default (Personal Compute)                                              ║
║    ├─ Max 8 workers, 2 hr timeout, no spot                               ║
║    │                                                                      ║
║    ├─ Need more power? → Request "Power User" policy                     ║
║    │   └─ Max 20 workers, spot allowed, longer timeout                   ║
║    │                                                                      ║
║    ├─ Running production jobs? → Use "Job Compute"                       ║
║    │   └─ Auto-configured, spot-friendly, tight timeout                  ║
║    │                                                                      ║
║    └─ Special requirements? → Exception request (temporary, reviewed)    ║
║        └─ GPU, Photon, > 50 workers, specialized instance type           ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 97 — AI Functions for Data Engineers [Medium]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC You need to classify thousands of product reviews by sentiment, extract entities from contracts, or summarize support tickets — tasks that traditionally require custom ML models, regex spaghetti, or manual review. AI Functions bring LLM capabilities directly into SQL, making these tasks as simple as calling `ai_classify()`.
# MAGIC
# MAGIC **NOTE:** AI Functions require full Databricks platform with Foundation Model APIs and AI features enabled. Community Edition does not support them. This section demonstrates the concepts, syntax, and decision framework.
# MAGIC
# MAGIC ### Key Functions
# MAGIC
# MAGIC | Function | Purpose | Example Use |
# MAGIC |---|---|---|
# MAGIC | `ai_query()` | Send a prompt to a model | "Summarize this ticket" |
# MAGIC | `ai_classify()` | Classify text into categories | Sentiment, priority, department |
# MAGIC | `ai_extract()` | Extract structured data from text | Entities, dates, amounts |
# MAGIC | `ai_generate()` | Generate text content | Product descriptions, email drafts |
# MAGIC | `ai_mask()` | Mask PII in text | Redact names, emails, SSNs |
# MAGIC
# MAGIC ### When AI Functions Replace Traditional Logic
# MAGIC
# MAGIC | Task | Traditional Approach | AI Function Approach |
# MAGIC |---|---|---|
# MAGIC | **Sentiment analysis** | Custom ML model, training data, deployment | `ai_classify(text, ['positive','negative','neutral'])` |
# MAGIC | **Entity extraction** | Regex patterns, NER models, multiple passes | `ai_extract(text, ['company', 'date', 'amount'])` |
# MAGIC | **Ticket routing** | Keyword matching, fragile rules | `ai_classify(ticket, departments)` |
# MAGIC | **Data cleansing** | Complex UDFs, lookup tables | `ai_generate("Standardize: " + raw_address)` |
# MAGIC | **Text summarization** | Extractive summarization models | `ai_query("Summarize: " + long_text)` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Traditional Regex Approach vs AI Function (Conceptual)

# COMMAND ----------

print("=" * 60)
print("CONCEPT 97: AI FUNCTIONS FOR DATA ENGINEERS")
print("=" * 60)

# ── Create sample support ticket data ─────────────────────────────
tickets_data = [
    (1, "I cannot log into my account. The password reset email never arrives.", "open"),
    (2, "Your product is amazing! We increased sales by 40% since switching.", "resolved"),
    (3, "Billing charged me twice this month. Need immediate refund of $299.99.", "open"),
    (4, "The API returns 503 errors after the latest update. Our production is down.", "open"),
    (5, "Just wanted to say thank you for the great support. Everything works perfectly.", "closed"),
    (6, "I need to update our company address to 123 Main St, Springfield, IL 62701.", "open"),
    (7, "Can you send the Q4 invoice to john.doe@example.com? Purchase order #PO-8823.", "open"),
    (8, "Data pipeline has been failing since 2024-11-15. Error: Spark memory exceeded.", "open"),
    (9, "Feature request: please add dark mode to your dashboard. Many users requested this.", "open"),
    (10, "Urgent: Security vulnerability found in version 2.4.1. Please patch immediately.", "open"),
]

tickets_df = spark.createDataFrame(tickets_data, ["ticket_id", "description", "status"])
tickets_df.createOrReplaceTempView("support_tickets")

print("Support tickets created:")
display(tickets_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Traditional Approach — Regex + UDF for Classification

# COMMAND ----------

# ── Traditional: manually classify with regex patterns ─────────────
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

def classify_ticket_regex(description):
    """Classify ticket using fragile regex patterns."""
    description = description.lower()
    
    if any(w in description for w in ["billing", "charge", "refund", "invoice", "payment"]):
        return "Billing"
    elif any(w in description for w in ["login", "password", "account", "cannot log"]):
        return "Account Access"
    elif any(w in description for w in ["error", "bug", "fail", "down", "503", "500", "broken"]):
        return "Technical Issue"
    elif any(w in description for w in ["security", "vulnerability", "patch", "breach"]):
        return "Security"
    elif any(w in description for w in ["feature", "request", "add", "enhancement"]):
        return "Feature Request"
    elif any(w in description for w in ["thank", "great", "amazing", "love"]):
        return "Positive Feedback"
    else:
        return "General Inquiry"

classify_udf = udf(classify_ticket_regex, StringType())

print("── Traditional Regex Classification ──")
tickets_df \
    .withColumn("classification", classify_udf(col("description"))) \
    .select("ticket_id", "classification", "description") \
    .show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: AI Function Approach (Conceptual — Full Platform Only)

# COMMAND ----------

print("""
── AI Function Approach (Full Platform Only) ──

-- Replace 30+ lines of regex with ONE SQL call:

SELECT
    ticket_id,
    description,
    ai_classify(
        description,
        ARRAY('Billing', 'Account Access', 'Technical Issue',
              'Security', 'Feature Request', 'Positive Feedback',
              'General Inquiry')
    ) AS classification
FROM support_tickets;

-- Additional AI-powered enrichment:

SELECT
    ticket_id,
    description,
    ai_classify(description,
        ARRAY('urgent', 'high', 'medium', 'low')) AS priority,
    ai_extract(description,
        ARRAY('email', 'amount', 'date', 'version_number')) AS extracted_entities,
    ai_generate(
        'Write a 1-sentence summary: ' || description
    ) AS summary
FROM support_tickets;

── Key Differences from Regex ──

╔══════════════════════════════════════════════════════════════════╗
║  ASPECT            REGEX/UDF              AI FUNCTION            ║
╠══════════════════════════════════════════════════════════════════╣
║  Accuracy          70-80% (misses nuances) 90-95%+               ║
║  Maintenance       Fragile rules to update  Prompt engineering    ║
║  New categories    Rewrite pattern files   Add to category array  ║
║  Languages         One per language        Multilingual by default║
║  Edge cases        Handles known patterns  Handles novel patterns ║
║  Latency           Milliseconds            1-3 seconds per row    ║
║  Cost per row      Free (compute only)     ~$0.01-0.05 per 1k    ║
║  Infrastructure    Just Spark              Foundation Model API   ║
╚══════════════════════════════════════════════════════════════════╝

── When to Use Which ──

USE AI FUNCTIONS when:
• Text is unstructured / free-form (tickets, reviews, emails)
• Patterns are too varied for regex
• You need semantic understanding (sentiment, intent)
• Multi-language support needed
• Accuracy matters more than latency

USE REGEX/UDF when:
• Patterns are predictable (log parsing, error codes, URLs)
• Latency must be < 100ms per row
• Processing 100M+ rows (AI costs add up)
• Deterministic output required (same input → same output always)
• No LLM dependency desired
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Cost Estimation Framework

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                    AI FUNCTION COST ESTIMATION                           ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  TOKEN PRICING (approximate, varies by model):                           ║
║  ├─ Input:  ~$0.50 per million tokens                                    ║
║  └─ Output: ~$1.50 per million tokens                                    ║
║                                                                          ║
║  ESTIMATION FORMULA:                                                     ║
║  Cost = num_rows * (avg_input_tokens * input_rate +                      ║
║                     avg_output_tokens * output_rate)                     ║
║                                                                          ║
║  EXAMPLE CALCULATION:                                                    ║
║  ├─ 1 million support tickets                                            ║
║  ├─ avg 200 input tokens per ticket (description)                        ║
║  ├─ avg 10 output tokens (category label)                                ║
║  ├─ Input cost:  1M * 200/1M * $0.50 = $100                             ║
║  ├─ Output cost: 1M * 10/1M  * $1.50 = $15                              ║
║  └─ Total: ~$115 for 1M classifications                                  ║
║                                                                          ║
║  COMPARISON with manual review:                                          ║
║  ├─ AI classify 1M tickets: $115, ~1 hour compute time                   ║
║  └─ Manual review 1M tickets: ~$10,000+, weeks of effort                 ║
║                                                                          ║
║  OPTIMIZATION STRATEGIES:                                                ║
║  ├─ Batch processing during off-peak hours (lower token pricing)         ║
║  ├─ Pre-filter: only send ambiguous cases to AI, use regex for obvious   ║
║  ├─ Cache results: re-process only new/updated rows                      ║
║  └─ Use smaller/faster models for simple classifications                 ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 98 — Multi-Workspace Architecture [Hard]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC A single Databricks workspace works for small teams, but enterprises face challenges: different teams need isolation, data must stay in specific regions, compliance mandates separation of duties. When do you split into multiple workspaces, and how do you maintain governance across them?
# MAGIC
# MAGIC ### Key Drivers for Multi-Workspace
# MAGIC
# MAGIC | Driver | Example | Impact |
# MAGIC |---|---|---|
# MAGIC | **Team isolation** | Data Engineering vs Data Science teams shouldn't impact each other's compute | Separate workspaces per team |
# MAGIC | **Environment separation** | Dev/Staging/Prod | Separate workspaces per environment |
# MAGIC | **Regional deployment** | EU data must stay in EU; US data in US | Workspace per region |
# MAGIC | **Compliance boundary** | PCI-DSS workloads vs general analytics | Separate workspace with strict controls |
# MAGIC | **Cost allocation** | Each business unit manages its own budget | Workspace per BU with per-workspace budgets |
# MAGIC | **Blast radius** | Limit impact of misconfiguration or runaway jobs | Smaller workspaces = smaller blast radius |
# MAGIC
# MAGIC ### When One Workspace Suffices
# MAGIC - Small-to-medium team (< 50 users)
# MAGIC - Single region, single compliance regime
# MAGIC - Unity Catalog provides sufficient isolation (schemas/catalogs)
# MAGIC - No regulatory requirement for physical separation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Multi-Workspace Architecture Diagram

# COMMAND ----------

print("=" * 60)
print("CONCEPT 98: MULTI-WORKSPACE ARCHITECTURE")
print("=" * 60)

print("""
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                           ENTERPRISE MULTI-WORKSPACE ARCHITECTURE                            ║
╠══════════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                              ║
║  ┌─────────────────────────────────────────────┐                                             ║
║  │        ACCOUNT LEVEL (Databricks Account)    │                                             ║
║  │  ┌───────────────────────────────────────┐   │                                             ║
║  │  │     Unity Catalog (Metastore)         │   │ ← Shared across ALL workspaces             ║
║  │  │  ┌──────────┐ ┌──────────┐ ┌───────┐ │   │                                             ║
║  │  │  │ dev_cat  │ │ stg_cat  │ │prd_cat│ │   │ ← Catalogs per environment                 ║
║  │  │  └──────────┘ └──────────┘ └───────┘ │   │                                             ║
║  │  └───────────────────────────────────────┘   │                                             ║
║  │  ┌─────────────┐  ┌─────────────┐  ┌───────┐ │                                             ║
║  │  │ Account     │  │ Account     │  │Account│ │ ← Admin: Users, Groups, Service Principals  ║
║  │  │ Users/Groups│  │ Policies    │  │ Budgets│ │                                             ║
║  │  └─────────────┘  └─────────────┘  └───────┘ │                                             ║
║  └─────────────────────────────────────────────┘                                             ║
║                         │                        │                        │                   ║
║            ┌────────────┼────────────┬───────────┼────────────┬───────────┼───────────┐       ║
║            ▼            ▼            ▼           ▼            ▼           ▼           ▼       ║
║  ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐ ┌─────────────────┐ ┌─────────────┐ ║
║  │   WORKSPACE     │ │   WORKSPACE     │ │  WORKSPACE   │ │   WORKSPACE     │ │  WORKSPACE  │ ║
║  │   US-East       │ │   US-East       │ │  US-East     │ │   EU-West       │ │  EU-West    │ ║
║  │   Development   │ │   Staging       │ │  Production  │ │   Production    │ │  Analytics  │ ║
║  │                 │ │                 │ │              │ │                 │ │             │ ║
║  │ [DE Team]      │ │ [Integration]   │ │ [Prod Jobs]  │ │ [EU Data Team] │ │ [BI Users]  │ ║
║  │ [DS Team]      │ │ [QA Testing]    │ │ [Dashboards] │ │ [Compliance]   │ │ [Ad-hoc]    │ ║
║  │ [Sandboxes]    │ │                 │ │ [Monitoring] │ │                │ │             │ ║
║  │                 │ │                 │ │              │ │                │ │             │ ║
║  │ Compute:       │ │ Compute:        │ │ Compute:     │ │ Compute:       │ │ Compute:    │ ║
║  │ All-Purpose    │ │ Jobs + SQL WH  │ │ Jobs Compute │ │ All-Purpose    │ │ SQL WH      │ ║
║  │                 │ │                 │ │              │ │                │ │             │ ║
║  │ Tags: env=dev  │ │ Tags: env=stg  │ │ Tags: env=prd│ │ Tags: env=prd  │ │ Tags: env=… │ ║
║  │       team=de  │ │       team=qa  │ │       team=de│ │       team=eu  │ │       team=bi│ ║
║  └─────────────────┘ └─────────────────┘ └──────────────┘ └─────────────────┘ └─────────────┘ ║
║                                                                                              ║
║  ─────────────────────────────────────────────────────────────────────────────────────────  ║
║                                                                                              ║
║  CROSS-WORKSPACE ORCHESTRATION:                                                              ║
║  ┌────────────────────────────────────────────────────────────────┐                         ║
║  │ Workspace A (Dev) ──deploy──▶ Workspace B (Staging)             │                         ║
║  │   DABs validate + deploy     │ tests pass?                     │                         ║
║  │                              │   ──promote──▶ Workspace C (Prod)│                         ║
║  │                                                               │                         ║
║  │ Cross-workspace job triggers via:                              │                         ║
║  │ • Databricks Workflows (cross-workspace trigger)               │                         ║
║  │ • External orchestrator (Airflow, Dagster, Prefect)            │                         ║
║  │ • CI/CD pipeline (GitHub Actions, Jenkins, Azure DevOps)       │                         ║
║  │ • Terraform/Pulumi for infra-as-code                          │                         ║
║  └────────────────────────────────────────────────────────────────┘                         ║
║                                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Splitting Decision Framework

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║              WHEN TO SPLIT: DECISION FRAMEWORK                           ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  SPLIT BY TEAM when:                                                    ║
║  ├─ Teams have different compute needs (DE needs many small clusters;    ║
║  │  DS needs GPU)                                                        ║
║  ├─ Teams manage their own budgets                                       ║
║  ├─ Risk of one team's jobs impacting another's                          ║
║  └─ Teams use different cloud accounts/regions                           ║
║                                                                          ║
║  SPLIT BY ENVIRONMENT when:                                              ║
║  ├─ Strict change control required (prod changes need approval)          ║
║  ├─ Prod data must be isolated from dev access                           ║
║  ├─ Different security policies per environment                          ║
║  └─ You need environment-specific cluster policies                       ║
║                                                                          ║
║  SPLIT BY REGION when:                                                   ║
║  ├─ Data residency requirements (GDPR, CCPA, local laws)                 ║
║  ├─ Latency-sensitive workloads (compute near data)                      ║
║  ├─ Different cloud providers per region                                 ║
║  └─ Disaster recovery across regions                                     ║
║                                                                          ║
║  SPLIT BY COMPLIANCE when:                                               ║
║  ├─ PCI-DSS, HIPAA, or SOX workloads need isolation                      ║
║  ├─ Audit requires clear boundaries between regulated/unregulated data   ║
║  └─ Different retention/deletion policies                                ║
║                                                                          ║
║  KEEP SINGLE WORKSPACE when:                                             ║
║  ├─ Team < 50 users, < 3 distinct team functions                         ║
║  ├─ Unity Catalog provides sufficient isolation                          ║
║  ├─ No regulatory need for physical separation                           ║
║  └─ Simplicity wins: fewer workspaces = less operational overhead        ║
║                                                                          ║
║  ANTI-PATTERNS:                                                          ║
║  ├─ One workspace per person (way too many!)                              ║
║  ├─ Splitting early before you have a governance model                   ║
║  ├─ Different workspaces for the SAME purpose (fragmented data)          ║
║  └─ Creating workspaces manually (use Terraform/Pulumi!)                 ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 99 — Performance Troubleshooting Methodology [Hard]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC A pipeline that ran in 10 minutes now takes 2 hours. A dashboard query times out. Without a structured diagnostic approach, you waste hours guessing — adding partitions, throwing more hardware at it, or rewriting code blindly.
# MAGIC
# MAGIC This concept provides a **repeatable methodology** for diagnosing and fixing slow Spark/Delta workloads, backed by the tools and techniques from Concepts #1–#98.
# MAGIC
# MAGIC ### The Diagnostic Checklist
# MAGIC
# MAGIC 1. **Check the Spark UI** — Stage breakdown, task durations, skew
# MAGIC 2. **Identify shuffle/spill bottlenecks** — Spill to disk, exchange sizes
# MAGIC 3. **Examine the query plan** — Full table scans, broadcast vs sort-merge join
# MAGIC 4. **Verify clustering/statistics** — Are files being pruned?
# MAGIC 5. **Check cluster utilization** — CPU idle, GC pressure, disk I/O

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a Deliberately Slow Query

# COMMAND ----------

print("=" * 60)
print("CONCEPT 99: PERFORMANCE TROUBLESHOOTING METHODOLOGY")
print("=" * 60)

# Create a larger dataset to make performance issues visible
large_sales = spark.range(0, 200000).select(
    col("id").alias("transaction_id"),
    concat(lit("cust_"), lpad((col("id") % 1000).cast("string"), 4, "0")).alias("customer_id"),
    concat(lit("prod_"), lpad((col("id") % 200).cast("string"), 3, "0")).alias("product_id"),
    (rand(42) * 5000).cast(DecimalType(10, 2)).alias("amount"),
    concat(lit("store_"), ((col("id") % 20) + 1).cast("string")).alias("store_id"),
    col("id").cast("timestamp").alias("transaction_date")
).repartition(50)

large_sales_path = f"{base_dir}/perf_diag/large_sales"
large_sales.write.format("delta").mode("overwrite").save(large_sales_path)
spark.sql(f"""
    CREATE OR REPLACE TABLE perf_sales
    USING DELTA
    LOCATION '{large_sales_path}'
""")

# Create a large dimension table (simulating a poorly designed join)
large_dim = spark.range(0, 50000).select(
    concat(lit("cust_"), lpad((col("id") % 1000).cast("string"), 4, "0")).alias("customer_id"),
    concat(lit("Customer_"), (col("id") % 1000).cast("string")).alias("customer_name"),
    array("Enterprise", "SMB", "Startup", "Government", "Education")
        .getItem((col("id") % 5).cast("int")).alias("segment"),
    (rand(99) * 100000).cast(DecimalType(12, 2)).alias("lifetime_value"),
    col("id").cast("string").alias("filler_1"),
    col("id").cast("string").alias("filler_2"),
    col("id").cast("string").alias("filler_3")
).repartition(30)

large_dim_path = f"{base_dir}/perf_diag/large_dim"
large_dim.write.format("delta").mode("overwrite").save(large_dim_path)
spark.sql(f"""
    CREATE OR REPLACE TABLE perf_customers
    USING DELTA
    LOCATION '{large_dim_path}'
""")

print(f"perf_sales: {spark.table('perf_sales').count():,} rows")
print(f"perf_customers: {spark.table('perf_customers').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Diagnostic Check #1 — Examine the Query Plan

# COMMAND ----------

print("── CHECKLIST ITEM 1: Examine the Query Plan ──\n")

# The "slow" query: join with no optimization
slow_query = """
    SELECT
        c.segment,
        COUNT(*) AS transaction_count,
        SUM(s.amount) AS total_revenue
    FROM perf_sales s
    JOIN perf_customers c ON s.customer_id = c.customer_id
    GROUP BY c.segment
    ORDER BY total_revenue DESC
"""

# Show the EXPLAIN plan
print("Physical Plan for the 'slow' query:")
spark.sql(f"EXPLAIN EXTENDED {slow_query}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Diagnostic Check #2 — Run the Query and Time It

# COMMAND ----------

print("── CHECKLIST ITEM 2: Run and Time the Query ──\n")

t0 = time.time()
result_slow = spark.sql(slow_query)
row_count = result_slow.count()
t_slow = time.time() - t0

display(result_slow)
print(f"\nSLOW query completed in {t_slow:.2f}s")
print(f"Result rows: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Diagnostic Check #3 — Check Shuffle and Spill

# COMMAND ----------

print("── CHECKLIST ITEM 3: Shuffle & Spill Analysis ──\n")

shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"spark.sql.shuffle.partitions = {shuffle_partitions}")

aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
print(f"spark.sql.adaptive.enabled = {aqe_enabled}")

broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"spark.sql.autoBroadcastJoinThreshold = {broadcast_threshold}")

print("\n── Analyzing JOIN strategy ──")
join_plan = spark.sql(f"""
    EXPLAIN EXTENDED
    SELECT s.*, c.segment
    FROM perf_sales s
    JOIN perf_customers c ON s.customer_id = c.customer_id
    LIMIT 100
""").collect()

for row in join_plan:
    line = row[0]
    if any(kw in line for kw in ["Join", "Broadcast", "SortMerge", "Shuffle"]):
        print(f"  {line}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Diagnostic Check #4 — Apply Fixes Step by Step

# COMMAND ----------

print("── CHECKLIST ITEM 4: Apply Fixes ──\n")

# FIX 1: Broadcast hint for the smaller dimension table
print("FIX 1: Adding BROADCAST hint for dimension table\n")

optimized_query_1 = """
    SELECT /*+ BROADCAST(c) */
        c.segment,
        COUNT(*) AS transaction_count,
        SUM(s.amount) AS total_revenue
    FROM perf_sales s
    JOIN perf_customers c ON s.customer_id = c.customer_id
    GROUP BY c.segment
    ORDER BY total_revenue DESC
"""

t0 = time.time()
result_opt1 = spark.sql(optimized_query_1)
result_opt1.count()
t_opt1 = time.time() - t0
print(f"  SLOW query:  {t_slow:.2f}s")
print(f"  BROADCAST:   {t_opt1:.2f}s")
if t_slow > 0:
    print(f"  Improvement: {((t_slow - t_opt1) / t_slow * 100):.0f}% faster")

# FIX 2: ANALYZE TABLE for statistics
print(f"\nFIX 2: ANALYZE TABLE for column statistics\n")
spark.sql("ANALYZE TABLE perf_sales COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE perf_customers COMPUTE STATISTICS FOR ALL COLUMNS")
print("  Statistics computed.")

stats = spark.sql("DESCRIBE EXTENDED perf_sales").filter(
    col("col_name").rlike("Statistics|numRows|sizeInBytes")
)
print("\n  Table statistics:")
stats.show(truncate=False)

# FIX 3: OPTIMIZE with ZORDER on join key
print(f"\nFIX 3: OPTIMIZE with ZORDER on join key\n")
spark.sql("OPTIMIZE perf_sales ZORDER BY (customer_id)")
spark.sql("OPTIMIZE perf_customers ZORDER BY (customer_id)")
print("  Z-ORDER applied on customer_id for both tables.")

# Re-run the optimized query
t0 = time.time()
result_opt3 = spark.sql(optimized_query_1)
result_opt3.count()
t_opt3 = time.time() - t0

print(f"\n  SLOW query:          {t_slow:.2f}s")
print(f"  + BROADCAST:         {t_opt1:.2f}s")
print(f"  + STATS + Z-ORDER:   {t_opt3:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Reusable Diagnostic Function

# COMMAND ----------

def diagnose_query(spark, query_name, sql_query):
    """Reusable diagnostic function for query troubleshooting.
    
    Prints: execution plan summary, timing, row count, and optimization hints.
    Reference: Run this on any slow query to get a structured diagnosis.
    """
    print(f"\n{'='*70}")
    print(f" DIAGNOSTIC REPORT: {query_name}")
    print(f"{'='*70}")
    
    plan_rows = spark.sql(f"EXPLAIN EXTENDED {sql_query}").collect()
    plan_text = "\n".join([r[0] for r in plan_rows])
    
    has_broadcast = "BroadcastHashJoin" in plan_text or "BroadcastNestedLoopJoin" in plan_text
    has_sortmerge = "SortMergeJoin" in plan_text
    has_cartesian = "CartesianProduct" in plan_text
    has_full_scan = "Scan parquet" in plan_text
    
    print(f"\n  JOIN STRATEGY:")
    if has_broadcast:
        print(f"    ✓ Broadcast Join (efficient for small tables)")
    elif has_sortmerge:
        print(f"    ⚠ Sort-Merge Join (shuffle-heavy — consider BROADCAST hint)")
    elif has_cartesian:
        print(f"    ✗ Cartesian Product (CRITICAL — add join condition!)")
    else:
        print(f"    ? Unknown join type")
    
    if has_full_scan:
        print(f"    ⚠ Full table scan detected — check partition/ZORDER/pruning")
    
    shuffle_parts = spark.conf.get("spark.sql.shuffle.partitions")
    aqe = spark.conf.get("spark.sql.adaptive.enabled")
    
    print(f"\n  CONFIGURATION CHECK:")
    print(f"    shuffle.partitions:    {shuffle_parts}")
    print(f"    adaptive.enabled:      {aqe}")
    
    t0 = time.time()
    result = spark.sql(sql_query)
    num_rows = result.count()
    elapsed = time.time() - t0
    
    print(f"\n  QUERY STATS:")
    print(f"    Rows returned: {num_rows:,}")
    print(f"    Time elapsed:  {elapsed:.2f}s")
    
    print(f"\n  RECOMMENDATIONS:")
    if has_sortmerge and spark.conf.get("spark.sql.autoBroadcastJoinThreshold") != "-1":
        print(f"    → Try /*+ BROADCAST(small_table) */ hint")
    if has_full_scan:
        print(f"    → Run ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS")
        print(f"    → Consider ZORDER on filter columns")
    if elapsed > 10:
        print(f"    → Check for data skew in the Spark UI")
        print(f"    → Verify no unnecessary columns in SELECT *")
    
    print(f"\n{'='*70}")
    
    return {
        "query_name": query_name,
        "rows": num_rows,
        "time_seconds": elapsed,
        "join_type": "broadcast" if has_broadcast else "sort_merge" if has_sortmerge else "unknown",
        "has_full_scan": has_full_scan,
        "recommendations": [
            r for r in [
                "Try BROADCAST hint" if has_sortmerge else None,
                "Run ANALYZE TABLE" if has_full_scan else None,
                "Check data skew" if elapsed > 10 else None
            ] if r
        ]
    }

# ── Test the diagnostic function ──────────────────────────────────
diagnosis = diagnose_query(spark, "Segment Revenue Analysis",
    """
    SELECT c.segment,
           COUNT(*) AS cnt,
           SUM(s.amount) AS total
    FROM perf_sales s
    JOIN perf_customers c ON s.customer_id = c.customer_id
    GROUP BY c.segment
    ORDER BY total DESC
    """
)

print("\nDiagnostic report object:")
for k, v in diagnosis.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Troubleshooting Decision Tree

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║              PERFORMANCE TROUBLESHOOTING DECISION TREE                   ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  QUERY IS SLOW ──▶                                                       ║
║  │                                                                       ║
║  ├─ Check Spark UI: Are tasks skewed?                                    ║
║  │  ├─ YES → Data skew?                                                  ║
║  │  │  ├─ Salted join (add random prefix to keys)                        ║
║  │  │  └─ AQE skew join (spark.sql.adaptive.skewJoin.enabled=true)       ║
║  │  └─ NO → Continue                                                     ║
║  │                                                                       ║
║  ├─ Check Spark UI: Spill to disk?                                       ║
║  │  ├─ YES → Memory pressure                                             ║
║  │  │  ├─ Increase spark.sql.shuffle.partitions                          ║
║  │  │  ├─ Increase executor memory                                       ║
║  │  │  └─ Reduce data per task (increase parallelism)                    ║
║  │  └─ NO → Continue                                                     ║
║  │                                                                       ║
║  ├─ Check EXPLAIN: SortMergeJoin on large tables?                        ║
║  │  ├─ YES → Can small table be broadcast?                               ║
║  │  │  ├─ YES → Use /*+ BROADCAST(small_table) */ hint                   ║
║  │  │  └─ NO  → Ensure join keys are Z-ORDERed                           ║
║  │  │          → Check if Liquid Clustering can help (full platform)      ║
║  │  └─ NO → Continue                                                     ║
║  │                                                                       ║
║  ├─ Check EXPLAIN: Full table scan?                                      ║
║  │  ├─ YES → Are files being pruned?                                     ║
║  │  │  ├─ NO  → Run ANALYZE TABLE (stats for first 32 cols)              ║
║  │  │  ├─ NO  → Reorder columns (filter cols in first 32 positions)     ║
║  │  │  └─ NO  → Apply Z-ORDER or Liquid Clustering                       ║
║  │  └─ YES → Good (files are being pruned)                               ║
║  │                                                                       ║
║  ├─ Check Cluster Utilization: Low CPU?                                   ║
║  │  ├─ YES → I/O bound → Check file sizes (too many small files?)        ║
║  │  │  └─ Run OPTIMIZE to compact small files                            ║
║  │  └─ NO → CPU bound → Add workers or use larger instances              ║
║  │                                                                       ║
║  └─ CHECK WRITES: Is MERGE slow? (Concepts #59-#60)                      ║
║     ├─ low_shuffle_merge enabled?                                         ║
║     ├─ ZORDER on merge key?                                              ║
║     ├─ Too many small files before merge?                                 ║
║     └─ optimizeWrite enabled?                                            ║
║                                                                          ║
║  REMEMBER: Performance tuning is a CYCLE, not a checklist:               ║
║  1. Measure baseline                                                      ║
║  2. Form hypothesis                                                       ║
║  3. Apply ONE change                                                      ║
║  4. Measure again                                                         ║
║  5. Keep or revert                                                        ║
║  6. Repeat until performance target met                                   ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 100 — Testing Patterns for Data Pipelines [Hard]
# MAGIC
# MAGIC ### What Problem It Solves
# MAGIC
# MAGIC "The pipeline succeeded but the data is wrong." Without testing, data quality issues propagate downstream silently. Testing data pipelines is harder than testing application code — data changes over time, schemas evolve, and edge cases (nulls, duplicates, late arrivals) are the norm, not the exception.
# MAGIC
# MAGIC This concept establishes a **comprehensive testing framework** including unit tests for transformations, integration tests for pipelines, data quality assertions, and schema evolution tests.
# MAGIC
# MAGIC ### Testing Pyramid for Data Pipelines
# MAGIC
# MAGIC ```
# MAGIC         ╱  E2E Tests ╲           (Full pipeline, real data)
# MAGIC        ╱──────────────╲
# MAGIC       ╱ Integration     ╲         (Component interactions, subset data)
# MAGIC      ╱────────────────────╲
# MAGIC     ╱   Unit Tests          ╲       (Individual transforms, sample data)
# MAGIC    ╱──────────────────────────╲
# MAGIC ```
# MAGIC
# MAGIC ### What We Test
# MAGIC
# MAGIC | Level | What | Example |
# MAGIC |---|---|---|
# MAGIC | **Unit** | Single transformation function | Does `clean_amount()` handle negative values? |
# MAGIC | **Unit** | Schema validation | Does the output have the expected columns? |
# MAGIC | **Integration** | Join correctness | Do fact-dimension joins produce correct results? |
# MAGIC | **Integration** | Aggregation accuracy | Are SUM/COUNT/AVG correct? |
# MAGIC | **Assertions** | Business rules | No transaction amount > $1M? |
# MAGIC | **Assertions** | Data quality | No nulls in required fields? |
# MAGIC | **Edge cases** | Robustness | Empty input, all nulls, schema changes |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create Test Fixtures

# COMMAND ----------

print("=" * 60)
print("CONCEPT 100: TESTING PATTERNS FOR DATA PIPELINES")
print("=" * 60)

# ── Test Fixture: Small, controlled DataFrames ────────────────────

# Fixture 1: Normal sales data (happy path)
normal_sales = spark.createDataFrame([
    (1, "cust_001", "prod_001", 100.00, "store_1", "completed"),
    (2, "cust_002", "prod_002", 200.00, "store_2", "completed"),
    (3, "cust_001", "prod_001", 150.00, "store_1", "completed"),
], ["transaction_id", "customer_id", "product_id", "amount", "store_id", "status"])

# Fixture 2: Edge cases
edge_sales = spark.createDataFrame([
    (4, None, "prod_001", 0.00, None, "completed"),       # null IDs, zero amount
    (5, "cust_003", None, -50.00, "store_1", "returned"),  # null product, negative
    (6, "cust_001", "prod_001", 100.00, "store_1", "completed"),  # duplicate!
], ["transaction_id", "customer_id", "product_id", "amount", "store_id", "status"])

# Fixture 3: Empty data
empty_sales = spark.createDataFrame([], normal_sales.schema)

# Fixture 4: Schema evolution scenario
evolved_sales = spark.createDataFrame([
    (7, "cust_004", "prod_003", 300.00, "store_3", "pending", "web", 5),
    (8, "cust_005", "prod_004", 400.00, "store_4", "completed", "mobile", 3),
], ["transaction_id", "customer_id", "product_id", "amount", "store_id", "status", "channel", "discount_pct"])

print("Test fixtures created:")
print(f"  normal_sales:  {normal_sales.count()} rows, {len(normal_sales.columns)} cols")
print(f"  edge_sales:    {edge_sales.count()} rows (nulls, negatives, duplicates)")
print(f"  empty_sales:   {empty_sales.count()} rows (empty)")
print(f"  evolved_sales: {evolved_sales.count()} rows (new columns: channel, discount_pct)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Define Transformation Functions to Test

# COMMAND ----------

# ── Transformation: Clean and validate sales data ──────────────────
from pyspark.sql.functions import when

def transform_sales(df):
    """Clean and enrich raw sales data. Pure function — no side effects."""
    return df \
        .withColumn("amount",
            when(col("amount").isNull(), lit(0.0))
            .otherwise(col("amount"))
        ) \
        .withColumn("amount",
            when(col("amount") < 0, lit(0.0))
            .otherwise(col("amount"))
        ) \
        .withColumn("customer_id",
            when(col("customer_id").isNull(), lit("UNKNOWN"))
            .otherwise(col("customer_id"))
        ) \
        .withColumn("product_id",
            when(col("product_id").isNull(), lit("UNKNOWN"))
            .otherwise(col("product_id"))
        ) \
        .withColumn("sales_channel",
            when(col("amount") > 500, lit("high_value"))
            .when(col("amount") > 100, lit("medium"))
            .otherwise(lit("low"))
        )

# ── Transformation: Aggregate by customer ─────────────────────────
def aggregate_by_customer(sales_df):
    """Aggregate sales by customer. Returns summary DataFrame."""
    return sales_df \
        .groupBy("customer_id") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            round(avg("amount"), 2).alias("avg_transaction_value"),
            max("amount").alias("max_transaction")
        )

# ── Transformation: Calculate revenue by segment ──────────────────
def revenue_by_segment(sales_df, customer_df):
    """Join sales with customer dimension, calculate segment revenue."""
    return sales_df \
        .join(customer_df, "customer_id", "left") \
        .groupBy("segment") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_revenue")
        )

print("Transformation functions defined.")
print("  transform_sales(df)         → Clean & enrich")
print("  aggregate_by_customer(df)   → Customer-level summary")
print("  revenue_by_segment(s, c)    → Segment-level revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: UNIT TESTS — Transformations

# COMMAND ----------

print("=" * 60)
print("UNIT TESTS: Transformation Functions")
print("=" * 60)

test_results = []

# ── Test 1: Happy Path ─────────────────────────────────────────────
def test_transform_sales_normal():
    """Normal data should be enriched with sales_channel column."""
    result = transform_sales(normal_sales)
    
    assert "sales_channel" in result.columns, "Missing sales_channel column"
    assert result.count() == 3, f"Expected 3 rows, got {result.count()}"
    
    channels = [r.sales_channel for r in result.select("sales_channel").distinct().collect()]
    assert "low" in channels, "Missing 'low' channel"
    assert "medium" in channels, "Missing 'medium' channel"
    
    return True, "Normal data transforms correctly"

# ── Test 2: Edge Cases (nulls, negatives) ──────────────────────────
def test_transform_sales_edge_cases():
    """Null IDs should become 'UNKNOWN', negative amounts should become 0."""
    result = transform_sales(edge_sales)
    
    assert result.filter(col("customer_id") == "UNKNOWN").count() > 0, \
        "Null customer_id not replaced with UNKNOWN"
    assert result.filter(col("product_id") == "UNKNOWN").count() > 0, \
        "Null product_id not replaced with UNKNOWN"
    
    negative_count = result.filter(col("amount") < 0).count()
    assert negative_count == 0, f"Found {negative_count} negative amounts (should be 0)"
    
    zero_count = result.filter(col("amount") == 0.0).count()
    assert zero_count >= 1, f"Expected at least 1 zero amount, got {zero_count}"
    
    return True, "Edge cases handled correctly"

# ── Test 3: Empty Input ────────────────────────────────────────────
def test_transform_sales_empty():
    """Empty input should produce empty output (not error)."""
    result = transform_sales(empty_sales)
    assert result.count() == 0, f"Expected 0 rows from empty input, got {result.count()}"
    return True, "Empty input handled gracefully"

# ── Test 4: Idempotency ────────────────────────────────────────────
def test_transform_sales_idempotent():
    """Running transform twice should produce identical results."""
    first = transform_sales(normal_sales)
    second = transform_sales(first)
    assert first.count() == second.count(), "Row counts differ"
    assert set(first.columns) == set(second.columns), "Column sets differ"
    return True, "Transform is idempotent"

# ── Run all unit tests ─────────────────────────────────────────────
tests = [
    ("Happy Path", test_transform_sales_normal),
    ("Edge Cases", test_transform_sales_edge_cases),
    ("Empty Input", test_transform_sales_empty),
    ("Idempotency", test_transform_sales_idempotent),
]

for test_name, test_fn in tests:
    try:
        passed, msg = test_fn()
        status = "✓ PASS"
        test_results.append((test_name, "PASS", msg))
    except AssertionError as e:
        status = "✗ FAIL"
        test_results.append((test_name, "FAIL", str(e)))
    except Exception as e:
        status = "✗ ERROR"
        test_results.append((test_name, "ERROR", str(e)))
    print(f"  {status}  {test_name:<20}  {test_results[-1][2]}")

passed = sum(1 for r in test_results if r[1] == "PASS")
total = len(test_results)
print(f"\n  Unit Tests: {passed}/{total} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: INTEGRATION TESTS — Aggregation & Join Correctness

# COMMAND ----------

print("=" * 60)
print("INTEGRATION TESTS: Aggregations & Joins")
print("=" * 60)

integration_results = []

# ── Test 5: Aggregation Accuracy ───────────────────────────────────
def test_aggregate_by_customer():
    """Customer aggregation should produce correct totals."""
    clean = transform_sales(normal_sales)
    result = aggregate_by_customer(clean)
    
    cust_001 = result.filter(col("customer_id") == "cust_001").collect()[0]
    assert cust_001.transaction_count == 2, \
        f"Expected 2 transactions, got {cust_001.transaction_count}"
    assert abs(float(cust_001.total_amount) - 250.00) < 0.01, \
        f"Expected 250.00, got {cust_001.total_amount}"
    
    cust_002 = result.filter(col("customer_id") == "cust_002").collect()[0]
    assert cust_002.transaction_count == 1
    assert abs(float(cust_002.total_amount) - 200.00) < 0.01
    
    return True, "Aggregation totals verified"

# ── Test 6: Column Types After Transform ──────────────────────────
def test_column_types():
    """Verify expected column types after transformation."""
    clean = transform_sales(normal_sales)
    schema = {f.name: f.dataType.simpleString() for f in clean.schema.fields}
    
    assert schema["transaction_id"] == "bigint", f"Expected bigint, got {schema['transaction_id']}"
    assert schema["customer_id"] == "string", f"Expected string, got {schema['customer_id']}"
    assert "sales_channel" in schema, "Missing sales_channel column"
    
    return True, "Column types verified"

# ── Test 7: No Duplicate Keys ──────────────────────────────────────
def test_no_duplicate_transaction_ids():
    """Transaction IDs should be unique after processing."""
    clean = transform_sales(normal_sales)
    total = clean.count()
    distinct_ids = clean.select("transaction_id").distinct().count()
    assert total == distinct_ids, \
        f"Duplicate transaction_ids: {total} rows but {distinct_ids} distinct"
    return True, "No duplicate transaction IDs"

# ── Test 8: Business Rule — Amount Bounds ──────────────────────────
def test_amount_bounds():
    """Amounts should be non-negative and within expected range."""
    clean = transform_sales(normal_sales)
    
    negative = clean.filter(col("amount") < 0).count()
    assert negative == 0, f"Found {negative} negative amounts"
    
    out_of_range = clean.filter((col("amount") < 0) | (col("amount") > 1000)).count()
    assert out_of_range == 0, f"Found {out_of_range} amounts outside [0, 1000]"
    
    return True, "Amount bounds valid"

# ── Run all integration tests ──────────────────────────────────────
integration_tests = [
    ("Aggregation Accuracy", test_aggregate_by_customer),
    ("Column Types", test_column_types),
    ("No Duplicate Keys", test_no_duplicate_transaction_ids),
    ("Amount Bounds", test_amount_bounds),
]

for test_name, test_fn in integration_tests:
    try:
        passed, msg = test_fn()
        status = "✓ PASS"
        integration_results.append((test_name, "PASS", msg))
    except AssertionError as e:
        status = "✗ FAIL"
        integration_results.append((test_name, "FAIL", str(e)))
    except Exception as e:
        status = "✗ ERROR"
        integration_results.append((test_name, "ERROR", str(e)))
    print(f"  {status}  {test_name:<25}  {integration_results[-1][2]}")

passed_int = sum(1 for r in integration_results if r[1] == "PASS")
total_int = len(integration_results)
print(f"\n  Integration Tests: {passed_int}/{total_int} passed")
# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: SCHEMA EVOLUTION TESTS

# COMMAND ----------

print("=" * 60)
print("SCHEMA EVOLUTION TESTS")
print("=" * 60)

evolution_results = []

# ── Test 9: Handle New Columns ─────────────────────────────────────
def test_handle_new_columns():
    """Transformation should handle input with additional columns."""
    result = transform_sales(evolved_sales)
    assert result.count() == 2, f"Expected 2 rows, got {result.count()}"
    assert "sales_channel" in result.columns, "Missing sales_channel"
    return True, "Evolved schema handled (extra columns)"

# ── Test 10: Missing Columns Detection ────────────────────────────
def test_detect_missing_columns():
    """Should detect when required columns are missing."""
    incomplete = spark.createDataFrame([
        (9, "cust_006", "prod_005", "store_5", "completed"),
    ], ["transaction_id", "customer_id", "product_id", "store_id", "status"])
    
    required_cols = {"transaction_id", "customer_id", "product_id", "amount", "store_id", "status"}
    actual_cols = set(incomplete.columns)
    missing = required_cols - actual_cols
    
    assert missing == {"amount"}, f"Expected missing {{'amount'}}, got {missing}"
    return True, f"Missing columns detected: {missing}"

# ── Test 11: NULL-Heavy Data ──────────────────────────────────────
def test_null_heavy_data():
    """Pipeline should handle data where most fields are NULL."""
    null_heavy = spark.createDataFrame([
        (10, None, None, None, None, None),
        (11, "cust_007", None, 50.00, None, "completed"),
    ], ["transaction_id", "customer_id", "product_id", "amount", "store_id", "status"])
    
    result = transform_sales(null_heavy)
    assert result.count() == 2, "Row count changed"
    
    unknown_count = result.filter(col("customer_id") == "UNKNOWN").count()
    assert unknown_count == 1, f"Expected 1 UNKNOWN customer, got {unknown_count}"
    
    zero_amount_count = result.filter(col("amount") == 0.0).count()
    assert zero_amount_count >= 1, "Expected zero amount, got none"
    
    return True, "NULL-heavy data handled correctly"

# ── Run schema evolution tests ─────────────────────────────────────
evol_tests = [
    ("New Columns", test_handle_new_columns),
    ("Missing Columns Detection", test_detect_missing_columns),
    ("NULL-Heavy Data", test_null_heavy_data),
]

for test_name, test_fn in evol_tests:
    try:
        passed, msg = test_fn()
        status = "✓ PASS"
        evolution_results.append((test_name, "PASS", msg))
    except AssertionError as e:
        status = "✗ FAIL"
        evolution_results.append((test_name, "FAIL", str(e)))
    except Exception as e:
        status = "✗ ERROR"
        evolution_results.append((test_name, "ERROR", str(e)))
    print(f"  {status}  {test_name:<25}  {evolution_results[-1][2]}")

passed_evol = sum(1 for r in evolution_results if r[1] == "PASS")
total_evol = len(evolution_results)
print(f"\n  Schema Evolution Tests: {passed_evol}/{total_evol} passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Comprehensive Test Runner

# COMMAND ----------

class DataPipelineTestRunner:
    """Reusable test runner for data pipeline testing.
    
    Usage:
        runner = DataPipelineTestRunner("Sales Pipeline Tests")
        runner.add_test("Happy Path", test_fn_1)
        runner.add_test("Edge Cases", test_fn_2)
        runner.run()
        runner.summary()
    """
    
    def __init__(self, suite_name):
        self.suite_name = suite_name
        self.tests = []
        self.results = []
    
    def add_test(self, name, test_function, category="General"):
        self.tests.append({"name": name, "fn": test_function, "category": category})
    
    def run(self):
        print(f"\n{'='*70}")
        print(f" TEST SUITE: {self.suite_name}")
        print(f"{'='*70}")
        
        for test in self.tests:
            try:
                passed, msg = test["fn"]()
                self.results.append({
                    "name": test["name"],
                    "category": test["category"],
                    "status": "PASS",
                    "message": msg
                })
                print(f"  ✓ PASS  [{test['category']}] {test['name']:<30} {msg}")
            except AssertionError as e:
                self.results.append({
                    "name": test["name"],
                    "category": test["category"],
                    "status": "FAIL",
                    "message": str(e)
                })
                print(f"  ✗ FAIL  [{test['category']}] {test['name']:<30} {str(e)}")
            except Exception as e:
                self.results.append({
                    "name": test["name"],
                    "category": test["category"],
                    "status": "ERROR",
                    "message": str(e)
                })
                print(f"  ✗ ERROR [{test['category']}] {test['name']:<30} {str(e)}")
        
        self._print_summary()
    
    def _print_summary(self):
        passed = sum(1 for r in self.results if r["status"] == "PASS")
        failed = sum(1 for r in self.results if r["status"] == "FAIL")
        errors = sum(1 for r in self.results if r["status"] == "ERROR")
        total = len(self.results)
        
        print(f"\n{'─'*70}")
        print(f" SUMMARY: {passed}/{total} PASSED")
        if failed > 0:
            print(f"          {failed} FAILED")
        if errors > 0:
            print(f"          {errors} ERRORS")
        print(f"{'─'*70}")
        
        rating = "✦✦✦✦✦" if passed == total else ("✦✦✦✧✧" if passed >= total * 0.8 else "✦✧✧✧✧")
        print(f" QUALITY RATING: {rating}")
    
    def get_failures(self):
        return [r for r in self.results if r["status"] != "PASS"]
    
    def assert_all_passed(self):
        """Fail if any test didn't pass. Raises AssertionError."""
        failures = self.get_failures()
        if failures:
            msg = f"{len(failures)} test(s) failed:\n"
            for f in failures:
                msg += f"  - {f['name']}: {f['message']}\n"
            raise AssertionError(msg)
        return True

# ── Create and run the full test suite ─────────────────────────────
full_runner = DataPipelineTestRunner("Full Pipeline Test Suite")

full_runner.add_test("Happy Path Transform", test_transform_sales_normal, "Unit")
full_runner.add_test("Edge Cases Transform", test_transform_sales_edge_cases, "Unit")
full_runner.add_test("Empty Input", test_transform_sales_empty, "Unit")
full_runner.add_test("Idempotency", test_transform_sales_idempotent, "Unit")
full_runner.add_test("Aggregation Accuracy", test_aggregate_by_customer, "Integration")
full_runner.add_test("Column Types", test_column_types, "Integration")
full_runner.add_test("No Duplicates", test_no_duplicate_transaction_ids, "Integration")
full_runner.add_test("Amount Bounds", test_amount_bounds, "Integration")
full_runner.add_test("New Columns", test_handle_new_columns, "Schema Evolution")
full_runner.add_test("Missing Columns", test_detect_missing_columns, "Schema Evolution")
full_runner.add_test("NULL-Heavy Data", test_null_heavy_data, "Schema Evolution")

full_runner.run()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: CI/CD Integration Pattern (Conceptual)

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                    CI/CD FOR DATA PIPELINES (DABs)                       ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  TESTING IN CI/CD:                                                      ║
║                                                                          ║
║  ┌──────────────────────────────────────────────────────────────┐       ║
║  │  PR Opened ──▶ CI Pipeline                                    │       ║
║  │  ├─ 1. Lint: ruff/black (Python), sqlfluff (SQL)              │       ║
║  │  ├─ 2. Unit Tests: test transforms with sample data           │       ║
║  │  ├─ 3. Integration Tests: run on dev workspace with subset    │       ║
║  │  ├─ 4. DABs Validate: databricks bundle validate              │       ║
║  │  └─ 5. Plan: terraform plan / bundle plan                     │       ║
║  └──────────────────────────────────────────────────────────────┘       ║
║                         │                                                ║
║                         ▼                                                ║
║  ┌──────────────────────────────────────────────────────────────┐       ║
║  │  Merge to main ──▶ CD Pipeline                                │       ║
║  │  ├─ 1. Deploy to Staging: databricks bundle deploy -t stg     │       ║
║  │  ├─ 2. Smoke Tests: run on staging with prod-like data        │       ║
║  │  ├─ 3. Data Quality Checks: run expectation suite             │       ║
║  │  ├─ 4. Approval Gate: manual or automated                     │       ║
║  │  └─ 5. Deploy to Prod: databricks bundle deploy -t prod       │       ║
║  └──────────────────────────────────────────────────────────────┘       ║
║                                                                          ║
║  TEST DATA STRATEGY:                                                    ║
║  ├─ Unit tests: hardcoded fixtures (like this notebook)                 ║
║  ├─ Integration: 1% sample of production data (anonymized)              ║
║  ├─ E2E: synthetic data matching production distributions               ║
║  └─ NEVER use real production PII in test suites!                       ║
║                                                                          ║
║  ASSERTION PATTERNS:                                                     ║
║  ├─ Row counts: expected = actual (after filter/join)                   ║
║  ├─ Column types: verify schema matches expected                        ║
║  ├─ Value ranges: min/max within bounds                                 ║
║  ├─ Uniqueness: no duplicate keys                                       ║
║  ├─ Referential integrity: FKs exist in dimension tables                ║
║  ├─ Business rules: revenue > 0, dates in range, statuses valid         ║
║  └─ No data loss: source count = target count (accounting for filters)  ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")
# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---
# MAGIC # GRAND FINALE
# MAGIC
# MAGIC ## Congratulations! You've Completed All 100 Concepts
# MAGIC
# MAGIC If you've worked through all 10 notebooks in this series, you've covered the full spectrum of Databricks and Delta Lake engineering — from the fundamentals of the transaction log to multi-workspace enterprise architecture.
# MAGIC
# MAGIC ### What You've Built
# MAGIC
# MAGIC Through these notebooks, you've created and worked with:
# MAGIC
# MAGIC - **Delta Lake tables** with ACID transactions, time travel, schema evolution, and deletion vectors
# MAGIC - **Streaming pipelines** with Auto Loader, Structured Streaming, and Change Data Feed
# MAGIC - **Performance-optimized** workloads with Z-ORDER, Liquid Clustering, Photon, and Predictive Optimization
# MAGIC - **Secure and governed** data with Unity Catalog, row filters, column masks, and data lineage
# MAGIC - **Orchestrated workflows** with Delta Live Tables, Databricks Workflows, and DABs
# MAGIC - **ML-integrated pipelines** with Feature Store, Model Registry, and MLflow
# MAGIC - **Production-grade testing** frameworks with unit, integration, and data quality assertions
# MAGIC - **Enterprise architecture** patterns for multi-team, multi-region deployments
# MAGIC
# MAGIC ### Skills You Can Now Claim
# MAGIC
# MAGIC - Delta Lake Architecture & Optimization
# MAGIC - Apache Spark Performance Tuning
# MAGIC - Data Pipeline Testing & CI/CD
# MAGIC - Lakehouse Table Design (Star Schema, OBT, Data Vault)
# MAGIC - Enterprise Security & Governance
# MAGIC - Multi-Engine Interoperability
# MAGIC - Cost Optimization & Cluster Governance
# MAGIC - Production Operations & Troubleshooting

# COMMAND ----------

# MAGIC %md
# MAGIC ## 100-Concept Self-Assessment Scorecard
# MAGIC
# MAGIC Rate your confidence on each concept (1–5) to identify areas for further study.

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════════════════════╗
║                          100-CONCEPT SELF-ASSESSMENT SCORECARD                           ║
╠══════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                          ║
║  RATING GUIDE:                                                                           ║
║  1 — Heard of it                  3 — Understand, can use           5 — Can teach it      ║
║  2 — Basic understanding          4 — Can debug & optimize                                ║
║                                                                                          ║
║  ┌─────┬─────────────────────────────────────────┬───┐  ┌─────┬────────────────────────┬───┐
║  │  #  │ Concept                                  │ ★ │  │  #  │ Concept                 │ ★ │
║  ├─────┼─────────────────────────────────────────┼───┤  ├─────┼────────────────────────┼───┤
║  │  1  │ ACID Transactions                        │   │  │  51 │ Serverless Economics    │   │
║  │  2  │ Transaction Log (_delta_log)             │   │  │  52 │ Predicate Pushdown      │   │
║  │  3  │ Time Travel & RESTORE                    │   │  │  53 │ Predictive Optimization │   │
║  │  4  │ Schema Enforcement vs Evolution          │   │  │  54 │ Column Statistics       │   │
║  │  5  │ Liquid Clustering                        │   │  │  55 │ Write Optimization      │   │
║  │  6  │ OPTIMIZE & File Compaction               │   │  │  56 │ Cluster Sizing          │   │
║  │  7  │ VACUUM & Storage Lifecycle               │   │  │  57 │ DBU Cost Model          │   │
║  │  8  │ Change Data Feed (CDF)                   │   │  │  58 │ Query Profile           │   │
║  │  9  │ Deletion Vectors                         │   │  │  59 │ Join Performance        │   │
║  │ 10  │ Table Properties & Config               │   │  │  60 │ MERGE Amplification     │   │
║  ├─────┼─────────────────────────────────────────┼───┤  ├─────┼────────────────────────┼───┤
║  │ 11  │ Spark Architecture Overview             │   │  │  61 │ Unity Catalog Metastore │   │
║  │ 12  │ DAG Execution & Lazy Evaluation         │   │  │  62 │ Table ACLs              │   │
║  │ 13  │ Shuffle & Partition Mechanics           │   │  │  63 │ Row Filters             │   │
║  │ 14  │ Data Skew Diagnosis                     │   │  │  64 │ Column Masks            │   │
║  │ 15  │ Tungsten & Whole-Stage CodeGen          │   │  │  65 │ Data Lineage            │   │
║  │ 16  │ Broadcast vs Sort-Merge Joins           │   │  │  66 │ External Locations      │   │
║  │ 17  │ Adaptive Query Execution (AQE)          │   │  │  67 │ Credential Passthrough  │   │
║  │ 18  │ Memory Management (on-heap/off-heap)    │   │  │  68 │ Information Schema      │   │
║  │ 19  │ Partition Coalescing                    │   │  │  69 │ Audit Logging           │   │
║  │ 20  │ Stage-Level Scheduling                  │   │  │  70 │ Data Classification     │   │
║  ├─────┼─────────────────────────────────────────┼───┤  ├─────┼────────────────────────┼───┤
║  │ 21  │ DataFrame vs SQL: When Each Wins        │   │  │  71 │ Delta Live Tables (DLT) │   │
║  │ 22  │ Temporary Views & CTEs                  │   │  │  72 │ DLT CDC/SCD Type 2      │   │
║  │ 23  │ Window Functions for Analytics          │   │  │  73 │ Databricks Workflows    │   │
║  │ 24  │ Pivot/Unpivot Operations                │   │  │  74 │ Job Clusters vs Pools   │   │
║  │ 25  │ Subqueries & Correlated Queries         │   │  │  75 │ Trigger Types           │   │
║  │ 26  │ UDFs vs Built-in Functions              │   │  │  76 │ Job Parameters          │   │
║  │ 27  │ Spark Catalyst Optimizer                │   │  │  77 │ Repair & Retry Logic    │   │
║  │ 28  │ Higher-Order Functions                  │   │  │  78 │ DABs (CI/CD Bundles)    │   │
║  │ 29  │ Photon Engine                           │   │  │  79 │ DABs with Terraform     │   │
║  │ 30  │ Query Federation                        │   │  │  80 │ Monitoring & SLA Alerts │   │
║  ├─────┼─────────────────────────────────────────┼───┤  ├─────┼────────────────────────┼───┤
║  │ 31  │ Auto Loader (file notification)         │   │  │  81 │ MLflow Tracking         │   │
║  │ 32  │ COPY INTO for batch ingestion           │   │  │  82 │ MLflow Model Registry   │   │
║  │ 33  │ Incremental Ingestion Patterns          │   │  │  83 │ Feature Store Eng.      │   │
║  │ 34  │ Bronze Table Design (Raw Zone)          │   │  │  84 │ Feature Lookup          │   │
║  │ 35  │ Schema Inference & Evolution            │   │  │  85 │ Online vs Offline Store │   │
║  │ 36  │ File Formats: Parquet, Avro, CSV        │   │  │  86 │ Hyperopt Tuning         │   │
║  │ 37  │ Ingestion from Kafka/Event Hubs         │   │  │  87 │ Autologging             │   │
║  │ 38  │ Slowly Changing Dimensions (SCD)        │   │  │  88 │ Serving Endpoints       │   │
║  │ 39  │ Data Quality: Lakeflow Expectations     │   │  │  89 │ Lakehouse Monitoring    │   │
║  │ 40  │ Streaming ForeachBatch Patterns         │   │  │  90 │ Model Lifecycle Mgmt    │   │
║  ├─────┼─────────────────────────────────────────┼───┤  ├─────┼────────────────────────┼───┤
║  │ 41  │ Structured Streaming Overview           │   │  │  91 │ Shallow vs Deep Clones  │   │
║  │ 42  │ Watermarking & State Management         │   │  │  92 │ Managed vs External     │   │
║  │ 43  │ Output Modes & Triggers                 │   │  │  93 │ Legacy Partition/Z-ORDER│   │
║  │ 44  │ Streaming Aggregations                  │   │  │  94 │ Lakehouse Design Patt.  │   │
║  │ 45  │ Streaming Joins (Stream-Stream)         │   │  │  95 │ Delta UniForm & Interop │   │
║  │ 46  │ Change Data Feed for Streaming          │   │  │  96 │ Compute Policies        │   │
║  │ 47  │ Streaming Checkpoint Recovery           │   │  │  97 │ AI Functions for DE     │   │
║  │ 48  │ Idempotent Writes for Streaming         │   │  │  98 │ Multi-Workspace Arch    │   │
║  │ 49  │ Rate Limiting & Back-Pressure           │   │  │  99 │ Performance Troubleshoot│   │
║  │ 50  │ Delta Live Tables Streaming             │   │  │ 100 │ Testing Patterns        │   │
║  └─────┴─────────────────────────────────────────┴───┘  └─────┴────────────────────────┴───┘
║                                                                                          ║
║  ┌─────────────────────────────────────────────────────────────────────────────────┐    ║
║  │  SCORING:                                                                         │    ║
║  │  ├─ 400-500  ✦✦✦✦✦  Expert — ready for certification and production leadership   │    ║
║  │  ├─ 300-399  ✦✦✦✦✧  Advanced — strong practitioner, fill a few gaps               │    ║
║  │  ├─ 200-299  ✦✦✦✧✧  Intermediate — solid foundation, continue building            │    ║
║  │  ├─ 100-199  ✦✦✧✧✧  Beginner — keep learning, revisit Notebooks 01-03            │    ║
║  │  └─ <100     ✦✧✧✧✧  Just starting — welcome! Work through sequentially            │    ║
║  └─────────────────────────────────────────────────────────────────────────────────┘    ║
║                                                                                          ║
╚══════════════════════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps: Where to Go From Here
# MAGIC
# MAGIC ### Certifications
# MAGIC
# MAGIC | Certification | Level | Focus | Recommended After |
# MAGIC |---|---|---|---|
# MAGIC | **Databricks Data Engineer Associate** | Entry | Delta, Spark, ETL basics | Completing notebooks 1–3 |
# MAGIC | **Databricks Data Engineer Professional** | Advanced | DLT, streaming, optimization, DABs | All 10 notebooks |
# MAGIC | **Databricks ML Engineer Associate** | Entry | MLflow, Feature Store, model serving | Notebooks 1–3 + 9 |
# MAGIC | **Databricks SQL Analyst** | Entry | SQL warehouses, dashboards, BI | Notebooks 1–3 |
# MAGIC | **Databricks Platform Administrator** | Advanced | Workspaces, policies, Unity Catalog | Notebooks 6–8, 10 |
# MAGIC
# MAGIC ### Hands-On Projects
# MAGIC
# MAGIC | Project | Concepts Applied | Difficulty |
# MAGIC |---|---|---|
# MAGIC | **Build a Medallion Pipeline** | #1-#10, #31-#40 | Intermediate |
# MAGIC | **Real-Time Fraud Detection** | #41-#50, #81-#90 | Advanced |
# MAGIC | **Enterprise Data Platform** | #61-#70, #91-#100 | Expert |
# MAGIC | **Cost Optimization Audit** | #51-#60, #96 | Intermediate |
# MAGIC | **Multi-Engine Lakehouse** | #92, #95, #98 | Expert |
# MAGIC
# MAGIC ### Community & Resources
# MAGIC
# MAGIC | Resource | URL | Purpose |
# MAGIC |---|---|---|
# MAGIC | **Databricks Documentation** | docs.databricks.com | Official reference |
# MAGIC | **Delta Lake OSS** | delta.io | Open-source Delta |
# MAGIC | **Apache Spark Docs** | spark.apache.org/docs | Spark internals |
# MAGIC | **Databricks Community** | community.databricks.com | Q&A, discussions |
# MAGIC | **Databricks Blog** | databricks.com/blog | Best practices, releases |
# MAGIC | **Databricks Academy** | partner-academy.databricks.com | Free courses |
# MAGIC | **Delta Lake GitHub** | github.com/delta-io/delta | Source code, examples |
# MAGIC
# MAGIC ### Learning Path Continuation
# MAGIC
# MAGIC ```
# MAGIC 100-Concept Series (You Are Here)
# MAGIC       │
# MAGIC       ├─▶ Databricks Associate Certification
# MAGIC       │
# MAGIC       ├─▶ Databricks Professional Certification
# MAGIC       │
# MAGIC       ├─▶ Real-World Project Portfolio (3-5 projects)
# MAGIC       │
# MAGIC       └─▶ Community Contribution (blog posts, talks, open source)
# MAGIC ```

# COMMAND ----------

# CLEANUP: Drop all tables created in this notebook
print("Cleaning up...")
tables_to_drop = [
    "clone_source", "clone_shallow", "clone_deep",
    "sales_managed", "sales_external",
    "drop_test_managed", "drop_test_external",
    "gold.fact_sales", "gold.dim_customer", "gold.dim_product",
    "gold.dim_date", "gold.wide_sales_reporting",
    "perf_sales", "perf_customers"
]
for t in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    except:
        pass
dbutils.fs.rm(base_dir, recurse=True)
print("Cleanup complete.")

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                                                                          ║
║                         END OF NOTEBOOK SERIES                           ║
║                                                                          ║
║                   10 Architecture & Advanced Patterns                    ║
║                                                                          ║
║          Concepts #91-#100  |  Capstone  |  Grand Finale                 ║
║                                                                          ║
║   "The best time to start learning was yesterday.                         ║
║    The second best time is now."                                          ║
║                                                                          ║
║   Thank you for completing the 100-Concept Databricks                    ║
║   Professional Learning series. Go build something amazing.              ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

print("End of 10_Architecture_Advanced_Patterns notebook. All 10 concepts (#91-#100) covered. GRAND FINALE complete.")
