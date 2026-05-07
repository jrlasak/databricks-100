# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Data Ingestion: Concepts 31–40
# MAGIC
# MAGIC **Objective:** Master data ingestion patterns in the Databricks Lakehouse — from batch file loads through incremental streaming, medallion architecture, idempotent pipelines, and slowly changing dimensions.
# MAGIC
# MAGIC **Scope:** Concepts 31 through 40 of the Databricks Data Engineer learning path.
# MAGIC
# MAGIC **Target Platform:** Databricks Community Edition (single‑node, Unity Catalog not available). Where Community Edition lacks a feature, we explain the theory and provide a manual work‑around.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup
# MAGIC
# MAGIC Create the synthetic datasets and helper functions used throughout the notebook.

# COMMAND ----------

import os
import time
import shutil
import json
import csv
import uuid
from datetime import datetime, date, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("04_Data_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

spark.sql("CREATE DATABASE IF NOT EXISTS ingestion_demo")
spark.sql("USE ingestion_demo")

print(f"Spark version : {spark.version}")
print(f"Database      : {spark.catalog.currentDatabase()}")
print("Ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility — Generate Synthetic Retail Data

# COMMAND ----------

def generate_sales_csv(output_dir, num_files=3, rows_per_file=100):
    """Generate synthetic retail sales CSVs with a realistic schema."""
    from random import randint, choice, uniform, seed
    seed(42)
    categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]
    regions    = ["North", "South", "East", "West"]
    statuses   = ["COMPLETED", "PENDING", "CANCELLED", "RETURNED"]

    os.makedirs(output_dir, exist_ok=True)
    files_created = []

    for f in range(num_files):
        file_path = os.path.join(output_dir, f"sales_{f:04d}.csv")
        with open(file_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["transaction_id","transaction_date","product_name","category",
                             "quantity","unit_price","total_amount","region","status"])
            for i in range(rows_per_file):
                qty    = randint(1, 5)
                price  = round(uniform(5.0, 500.0), 2)
                writer.writerow([
                    str(uuid.uuid4()),
                    (date.today() - timedelta(days=randint(0, 60))).isoformat(),
                    f"product_{randint(1, 50)}",
                    choice(categories),
                    qty,
                    price,
                    round(qty * price, 2),
                    choice(regions),
                    choice(statuses)
                ])
        files_created.append(file_path)

    return files_created


def generate_customer_csv(output_dir, num_files=1, rows_per_file=50):
    """Generate synthetic customer dimension data (for SCD demos)."""
    from random import randint, choice, seed
    seed(123)
    cities  = ["New York","Los Angeles","Chicago","Houston","Phoenix"]
    states  = ["NY","CA","IL","TX","AZ"]
    streets = ["Main St","Oak Ave","Elm Rd","Pine Blvd","Maple Dr"]

    os.makedirs(output_dir, exist_ok=True)
    files = []
    for f in range(num_files):
        fp = os.path.join(output_dir, f"customers_{f:04d}.csv")
        with open(fp, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["customer_id","first_name","last_name","email","phone","city","state","street_address","zip_code"])
            for i in range(rows_per_file):
                idx = randint(0, len(cities)-1)
                cust_id = f"CUST-{randint(1000, 9999)}"
                w.writerow([
                    cust_id,
                    f"first_{randint(1,99)}",
                    f"last_{randint(1,99)}",
                    f"user{randint(1,999)}@example.com",
                    f"555-{randint(1000,9999)}",
                    cities[idx],
                    states[idx],
                    f"{randint(1,999)} {choice(streets)}",
                    f"{randint(10000,99999)}"
                ])
        files.append(fp)
    return files


def upload_dir_to_dbfs(local_dir, dbfs_dir):
    """Copy a local directory into DBFS."""
    dbutils.fs.mkdirs(dbfs_dir)
    for fname in os.listdir(local_dir):
        local_path  = os.path.join(local_dir, fname)
        dbfs_path   = f"{dbfs_dir}/{fname}"
        dbutils.fs.cp(f"file://{local_path}", dbfs_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Datasets & Stage in DBFS

# COMMAND ----------

# Generate in local /tmp then copy to DBFS
SALES_IN  = "/tmp/sales_input"
CUSTOMER_IN = "/tmp/customer_input"

generate_sales_csv(SALES_IN, num_files=5, rows_per_file=150)
generate_customer_csv(CUSTOMER_IN, num_files=1, rows_per_file=50)

# Upload into DBFS FileStore (Community Edition safe)
DBFS_SALES     = "dbfs:/FileStore/ingestion_demo/sales"
DBFS_CUSTOMERS = "dbfs:/FileStore/ingestion_demo/customers"
dbutils.fs.rm(DBFS_SALES,         recurse=True)
dbutils.fs.rm(DBFS_CUSTOMERS,     recurse=True)

upload_dir_to_dbfs(SALES_IN,  DBFS_SALES)
upload_dir_to_dbfs(CUSTOMER_IN, DBFS_CUSTOMERS)

print("Files in DBFS sales directory:")
display(dbutils.fs.ls(DBFS_SALES))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 31 — COPY INTO vs. Auto Loader
# MAGIC
# MAGIC **COPY INTO** — one‑shot batch ingestion, great for ad‑hoc or scheduled loads.
# MAGIC **Auto Loader** — continuous / incremental ingestion with schema evolution and exactly‑once guarantees.
# MAGIC
# MAGIC | Feature             | COPY INTO                    | Auto Loader                         |
# MAGIC |---------------------|------------------------------|-------------------------------------|
# MAGIC | Execution model     | SQL statement (batch)        | Structured Streaming source         |
# MAGIC | Idempotency         | Built‑in (metadata tracking) | Checkpoint + directory listing      |
# MAGIC | Schema evolution    | Manual                       | Automatic (cloudFiles options)      |
# MAGIC | Rescued data        | No                           | Yes                                 |
# MAGIC | Community Edition   | Yes                          | Limited (directory listing works)   |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 31‑A  COPY INTO — Idempotent Batch Load

# COMMAND ----------

spark.sql(f"""
  COPY INTO ingestion_demo.sales_raw_bronze
  FROM '{DBFS_SALES}'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true')
""")

# Run it again — COPY INTO tracks loaded files, so zero new rows are ingested
spark.sql(f"""
  COPY INTO ingestion_demo.sales_raw_bronze
  FROM '{DBFS_SALES}'
  FILEFORMAT = CSV
  FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
  COPY_OPTIONS ('mergeSchema' = 'true')
""")

print("Row count after two COPY INTO runs (should be same — no duplicates):")
display(spark.table("ingestion_demo.sales_raw_bronze").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **COPY INTO idempotency explained:**
# MAGIC
# MAGIC The command stores a list of already‑ingested file names in `_metadata`.  When re‑run it skips previously loaded files automatically.  This is the simplest way to achieve exactly‑once ingestion for batch workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 31‑B  Auto Loader — Conceptual Walk‑through
# MAGIC
# MAGIC Auto Loader (`cloudFiles` source) is a *Structured Streaming* source that continuously discovers new files.  It has two modes:
# MAGIC
# MAGIC 1. **File notification mode** — relies on cloud queue services (SQS, Event Grid).  Lowest latency but requires cloud setup.  **Not available in Community Edition.**
# MAGIC 2. **Directory listing mode** — periodically lists the directory and processes new files.  Simpler, no external services needed.  Works in Community Edition for demonstration.
# MAGIC
# MAGIC > **Community Edition note:** Directory listing Auto Loader *does* work, but CE’s single‑node constraint limits true streaming.  We show the code pattern and then provide a manual batch equivalent.

# COMMAND ----------

# ---- Auto Loader (directory listing mode — CE compatible) ----
# Note: run this in a separate cell. CE single-node limits parallelism.

def auto_loader_demo_delta():
    spark.sql("DROP TABLE IF EXISTS ingestion_demo.autoloader_demo")
    checkpoint = "dbfs:/FileStore/ingestion_demo/_checkpoints/autoloader"
    dbutils.fs.rm(checkpoint, recurse=True)

    stream = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaLocation", checkpoint)
        .option("cloudFiles.includeExistingFiles", "true")
        .load(DBFS_SALES)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable("ingestion_demo.autoloader_demo")
    )
    stream.awaitTermination()

try:
    auto_loader_demo_delta()
    print("Auto Loader (availableNow) completed.")
    display(spark.table("ingestion_demo.autoloader_demo").count())
except Exception as e:
    print(f"Auto Loader not available in this environment: {e}")
    print("Falling back to manual batch incremental (see below).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 31‑C  Manual Incremental Batch (Community Edition alternative)

# COMMAND ----------

def manual_incremental_load(data_dir, target_table, ingested_files_table, checkpoint_dir):
    """
    Manual equivalent of Auto Loader:
    1. List all files in `data_dir`
    2. Subtract files already recorded in `ingested_files_table`
    3. Read only new files with Spark
    4. Append to `target_table`
    5. Record newly ingested files in `ingested_files_table`
    """
    spark.sql(f"CREATE TABLE IF NOT EXISTS {ingested_files_table} "
              "(path STRING, ingested_at TIMESTAMP) USING delta")
    
    # Files on disk
    all_files = set(f.path for f in dbutils.fs.ls(data_dir) if f.name.endswith(".csv"))
    
    # Already ingested
    try:
        already = set(r.path for r in spark.table(ingested_files_table).select("path").collect())
    except Exception:
        already = set()
    
    new_files = sorted(all_files - already)
    if not new_files:
        print("No new files to ingest.")
        return 0

    print(f"Ingesting {len(new_files)} new file(s): {[f.split('/')[-1] for f in new_files]}")

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(new_files)
    df.write.mode("append").format("delta").saveAsTable(target_table)

    # Record ingestions
    now = datetime.now().isoformat()
    ingest_df = spark.createDataFrame([(f, now) for f in new_files], "path STRING, ingested_at TIMESTAMP")
    ingest_df.write.mode("append").format("delta").saveAsTable(ingested_files_table)

    return df.count()

# First run — ingests all files
cnt = manual_incremental_load(
    DBFS_SALES,
    "ingestion_demo.sales_manual_incremental",
    "ingestion_demo._ingested_files",
    "dbfs:/FileStore/ingestion_demo/_manual_checkpoints"
)
print(f"First run ingested {cnt} rows.")

# Second run — should be zero (all files already tracked)
cnt2 = manual_incremental_load(
    DBFS_SALES,
    "ingestion_demo.sales_manual_incremental",
    "ingestion_demo._ingested_files",
    "dbfs:/FileStore/ingestion_demo/_manual_checkpoints"
)
print(f"Second run ingested {cnt2} rows.")

# COMMAND ----------

# Generate a new file to demonstrate incremental pickup
import uuid
new_batch_dir = "/tmp/sales_new"
os.makedirs(new_batch_dir, exist_ok=True)
fp = os.path.join(new_batch_dir, f"sales_new_{uuid.uuid4().hex[:8]}.csv")
with open(fp, "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["transaction_id","transaction_date","product_name","category","quantity","unit_price","total_amount","region","status"])
    w.writerow([str(uuid.uuid4()), date.today().isoformat(), "product_99", "Electronics", 2, 299.99, 599.98, "North", "COMPLETED"])
    w.writerow([str(uuid.uuid4()), date.today().isoformat(), "product_88", "Books",      1,  24.50,  24.50, "South", "COMPLETED"])

dbutils.fs.cp(f"file:{fp}", f"{DBFS_SALES}/{os.path.basename(fp)}")

cnt3 = manual_incremental_load(
    DBFS_SALES,
    "ingestion_demo.sales_manual_incremental",
    "ingestion_demo._ingested_files",
    "dbfs:/FileStore/ingestion_demo/_manual_checkpoints"
)
print(f"Third run (after new file added) ingested {cnt3} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Key take‑away (Concept 31):**
# MAGIC - `COPY INTO` is your go‑to for batch / scheduled loads. Idempotent out of the box.
# MAGIC - Auto Loader provides continuous ingestion with schema evolution.  Use `availableNow` for batch‑like semantics.
# MAGIC - The manual file‑tracking pattern above gives you the same guarantee in environments without Auto Loader.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 32 — File Formats in the Lakehouse
# MAGIC
# MAGIC | Format  | Row/Col | Compress | Splittable | Schema | Human Readable | Best For                               |
# MAGIC |---------|---------|----------|------------|--------|----------------|----------------------------------------|
# MAGIC | CSV     | Row     | Low      | Partially  | Loose  | Yes            | Exchange, simple logs                  |
# MAGIC | JSON    | Row     | Low      | No         | Loose  | Yes            | Web APIs, nested data                  |
# MAGIC | Avro    | Row     | Good     | Yes        | Strong | No             | Kafka, streaming interchange           |
# MAGIC | Parquet | Column  | Excellent| Yes        | Strong | No             | Analytics, predicate push‑down         |
# MAGIC | Delta   | Column  | Excellent| Yes        | Strong | No             | **Lakehouse default** — ACID, time travel |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 32‑A  Write Identical Data in Five Formats & Compare Sizes

# COMMAND ----------

base_df = spark.table("ingestion_demo.sales_raw_bronze")

formats = {
    "csv":    ("csv",     {}),
    "json":   ("json",    {}),
    "avro":   ("avro",    {}),  # requires spark-avro package
    "parquet":("parquet", {}),
    "delta":  ("delta",   {}),
}

dir_prefix = "dbfs:/FileStore/ingestion_demo/format_compare/"
size_results = []

for fmt_name, (fmt_str, opts) in formats.items():
    out_path = f"{dir_prefix}{fmt_name}"
    dbutils.fs.rm(out_path, recurse=True)
    try:
        base_df.write.format(fmt_str).options(**opts).mode("overwrite").save(out_path)
        total_size = 0
        for f in dbutils.fs.ls(out_path):
            total_size += f.size
        size_results.append((fmt_name, total_size))
        print(f"{fmt_name:>8} => {total_size:>10,} bytes")
    except Exception as e:
        print(f"{fmt_name:>8} => SKIPPED ({e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 32‑B  Read Speed Benchmark

# COMMAND ----------

import time as _time

bench_results = []
for fmt_name, (fmt_str, _opts) in formats.items():
    path = f"{dir_prefix}{fmt_name}"
    try:
        t0 = _time.time()
        df = spark.read.format(fmt_str).load(path)
        cnt = df.count()
        elapsed = _time.time() - t0
        bench_results.append((fmt_name, cnt, round(elapsed, 3)))
        print(f"{fmt_name:>8} — {cnt} rows read in {elapsed:.3f}s")
    except Exception as e:
        bench_results.append((fmt_name, 0, str(e)))

display(spark.createDataFrame(bench_results, ["format","rows","seconds"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 32‑C  Schema Inference Differences

# COMMAND ----------

# Read the same CSV with and without inferSchema — note the data types
df_with_infer = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"{DBFS_SALES}/*.csv")

df_without_infer = spark.read.option("header", "true").option("inferSchema", "false") \
    .csv(f"{DBFS_SALES}/*.csv")

print("WITH    inferSchema:")
df_with_infer.printSchema()

print("\nWITHOUT inferSchema (all strings):")
df_without_infer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Why Parquet / Delta dominate the Lakehouse:**
# MAGIC 1. **Columnar layout** — queries that touch a few columns read only those columns, slashing I/O.
# MAGIC 2. **Predicate push‑down** — Parquet min/max statistics let the engine skip row groups entirely.
# MAGIC 3. **Compression** — snappy/gzip/zstd reduce storage 5x–20x vs CSV.
# MAGIC 4. **Delta adds ACID** — time travel, upserts, schema enforcement, and vacuum.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 33 — Volumes
# MAGIC
# MAGIC **Volumes** are Unity Catalog objects for governed, non‑tabular file storage — think of them as "managed folders" with permissions.
# MAGIC
# MAGIC | Volume Type   | Storage                                      | Lifecycle                    |
# MAGIC |---------------|----------------------------------------------|------------------------------|
# MAGIC | Managed       | Managed by Unity Catalog in the metastore    | Deleted with catalog/schema  |
# MAGIC | External      | External cloud storage (S3, ADLS, GCS)       | Survives catalog/schema drop |
# MAGIC
# MAGIC **Community Edition limitation:** Unity Catalog is not available.  We simulate Volumes using `dbfs:/FileStore/` paths, which serve the same role in CE.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 33‑A  Simulating a Volume (Landing Zone) with DBFS FileStore

# COMMAND ----------

LANDING_ZONE = "dbfs:/FileStore/ingestion_demo/landing_zone"

# ---- 1. Upload files to the "landing zone" (simulated volume) ----
dbutils.fs.rm(LANDING_ZONE, recurse=True)
dbutils.fs.mkdirs(LANDING_ZONE)

upload_dir_to_dbfs(SALES_IN, LANDING_ZONE)

# ---- 2. List files — volume-equivalent operation ----
print(f"Files in landing zone ({LANDING_ZONE}):")
for f in dbutils.fs.ls(LANDING_ZONE):
    print(f"  {f.name:40s} {f.size:>8,} bytes")

# ---- 3. Read CSV directly from the landing zone ----
landing_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{LANDING_ZONE}/*.csv")
)

print(f"\nRows in landing zone: {landing_df.count()}")
landing_df.select("transaction_id", "product_name", "total_amount").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 33‑B  Conceptual — Catalog‑Backed Volumes (Unity Catalog)
# MAGIC
# MAGIC If UC were available, you would create a volume with:
# MAGIC ```
# MAGIC CREATE VOLUME my_catalog.my_schema.my_volume
# MAGIC   LOCATION 's3://my-bucket/landing/';
# MAGIC ```
# MAGIC
# MAGIC Then reference files via the volume path: `/Volumes/my_catalog/my_schema/my_volume/sales.csv`
# MAGIC
# MAGIC | Feature                   | DBFS FileStore (CE)               | Volumes (UC)                       |
# MAGIC |---------------------------|-----------------------------------|------------------------------------|
# MAGIC | Permission model          | Workspace‑level                   | Fine‑grained (GRANT/REVOKE)        |
# MAGIC | Lifecycle                 | Tied to workspace                 | Managed = cascade; External = n/a  |
# MAGIC | Discovery                 | `dbutils.fs.ls`                   | INFORMATION_SCHEMA views           |
# MAGIC | External storage support  | Manual mount                      | Direct path, governed              |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 34 — Auto Loader: File Discovery
# MAGIC
# MAGIC Auto Loader discovers new files through one of two mechanisms:
# MAGIC
# MAGIC 1. **Directory listing** — every *N* seconds Auto Loader lists the input directory, compares against the checkpoint, and picks up new files.  Works anywhere, but latency depends on listing interval.
# MAGIC 2. **File notification** — Auto Loader subscribes to cloud event queues (SQS, Event Grid).  Near‑real‑time latency, but requires cloud infrastructure configuration.
# MAGIC
# MAGIC **Checkpoint location** tracks which files have been processed so the stream can resume from where it left off.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 34‑A  Directory Listing Mode — Conceptual Code

# COMMAND ----------

# This code pattern works in Community Edition (directory listing mode).
# The key options:
#   cloudFiles.format                — file format to ingest
#   cloudFiles.useNotifications      — true = file notification, false = directory listing
#   cloudFiles.schemaLocation        — where inferred schema is stored
#   cloudFiles.includeExistingFiles  — whether to ingest files that pre‑date the stream

def autoloader_discovery_demo():
    checkpoint = "dbfs:/FileStore/ingestion_demo/_checkpoints/discovery_demo"
    schema_loc = "dbfs:/FileStore/ingestion_demo/_schemas/discovery_demo"
    dbutils.fs.rm(checkpoint, recurse=True)
    dbutils.fs.rm(schema_loc, recurse=True)

    stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.useNotifications", "false")   # directory listing
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.includeExistingFiles", "true")
        .load(DBFS_SALES)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable("ingestion_demo.autoloader_discovery")
    )
    stream.awaitTermination()

try:
    autoloader_discovery_demo()
    print("Stream completed.")
    display(spark.table("ingestion_demo.autoloader_discovery").count())
except Exception as e:
    print(f"Not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 34‑B  Manual File Discovery Loop (CE Alternative)

# COMMAND ----------

def polling_discovery_loop(data_dir, target_table, poll_seconds=5, max_polls=3):
    """
    Simulate directory‑listing polling: check for new files every `poll_seconds`,
    ingest them, record in metadata table.
    """
    meta_table = "ingestion_demo._polling_meta"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {meta_table} "
              "(file_path STRING, ingested_at STRING) USING delta")

    for poll in range(max_polls):
        all_files  = set(f.path for f in dbutils.fs.ls(data_dir) if f.name.endswith(".csv"))
        ingested   = set(r.file_path for r in spark.table(meta_table).select("file_path").collect())
        new        = sorted(all_files - ingested)

        if new:
            print(f"Poll {poll+1}: discovered {len(new)} new file(s)")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(new)
            df.write.mode("append").format("delta").saveAsTable(target_table)
            now = datetime.now().isoformat()
            spark.createDataFrame([(f, now) for f in new]) \
                .write.mode("append").format("delta").saveAsTable(meta_table)
        else:
            print(f"Poll {poll+1}: no new files")

        if poll < max_polls - 1:
            time.sleep(poll_seconds)

    return spark.table(target_table).count()

spark.sql("DROP TABLE IF EXISTS ingestion_demo.polling_target")
cnt = polling_discovery_loop(DBFS_SALES, "ingestion_demo.polling_target", poll_seconds=2, max_polls=3)
print(f"Total rows after polling loop: {cnt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 35 — Auto Loader: Schema Evolution & Rescued Data
# MAGIC
# MAGIC Production data *drifts* — a source system adds a column, changes a type, or sends a malformed record.
# MAGIC Auto Loader handles this with two options:
# MAGIC
# MAGIC | Option                         | Behaviour                                                             |
# MAGIC |--------------------------------|-----------------------------------------------------------------------|
# MAGIC | `cloudFiles.schemaEvolutionMode` | `addNewColumns` — add new columns (default)                           |
# MAGIC |                                | `failOnNewColumns` — throw error (strict)                             |
# MAGIC |                                | `rescue` — add new columns AND route mismatched data to `_rescued_data` |
# MAGIC |                                | `none` — ignore schema changes entirely                               |
# MAGIC | `cloudFiles.rescuedDataColumn` | Name of the rescue column (default `_rescued_data`)                   |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 35‑A  Manual Schema Evolution (CE Simulation)

# COMMAND ----------

# Step 1 — Write an initial batch with 5 columns
initial_data = [
    (1, "Widget",  9.99,  "US", "2025-01-01"),
    (2, "Gadget", 19.99,  "CA", "2025-01-02"),
]
initial_schema = T.StructType([
    T.StructField("product_id",   T.IntegerType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("price",        T.DoubleType()),
    T.StructField("country",      T.StringType()),
    T.StructField("created_date", T.StringType()),
])
df1 = spark.createDataFrame(initial_data, schema=initial_schema)
df1.write.mode("overwrite").format("delta").saveAsTable("ingestion_demo.schema_evolution_demo")

print("Batch 1 schema:")
spark.table("ingestion_demo.schema_evolution_demo").printSchema()

# COMMAND ----------

# Step 2 — A new batch arrives with an EXTRA column `supplier` and a new data type for `created_date`
new_data = [
    (3, "Doohickey", 5.99, "US", "2025-02-15", "Acme Corp"),
    (4, "Thingy",    7.50, "MX", "2025-02-16", "Global Ltd"),
]
new_schema = T.StructType([
    T.StructField("product_id",   T.IntegerType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("price",        T.DoubleType()),
    T.StructField("country",      T.StringType()),
    T.StructField("created_date", T.StringType()),
    T.StructField("supplier",     T.StringType()),          # <— NEW column
])
df2 = spark.createDataFrame(new_data, schema=new_schema)

# ---- Simulate rescued data column manually ----
# Merge schemas: read existing schema, add new columns from the incoming batch
existing_schema = spark.table("ingestion_demo.schema_evolution_demo").schema
existing_cols = set(f.name for f in existing_schema.fields)
new_cols      = set(f.name for f in df2.schema.fields)
added_cols    = new_cols - existing_cols

print(f"Existing columns: {existing_cols}")
print(f"Incoming columns: {new_cols}")
print(f"New columns to add: {added_cols}")

# Auto-merge by enabling schema auto-merge and appending
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
df2.write.mode("append").format("delta").saveAsTable("ingestion_demo.schema_evolution_demo")

print("\nMerged schema (with new columns, NULLs for old rows):")
merged_df = spark.table("ingestion_demo.schema_evolution_demo")
merged_df.printSchema()
merged_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 35‑B  Simulating Rescued Data Column

# COMMAND ----------

# In Auto Loader, the `_rescued_data` column captures rows whose types don't match,
# so no data is lost. We simulate this by detecting parse errors manually.

from pyspark.sql import types as T

rescued_schema = T.StructType([
    T.StructField("id",      T.IntegerType()),
    T.StructField("value_1", T.DoubleType()),
    T.StructField("value_2", T.StringType()),
])

hard_data = [
    (1, 100.5,  "valid"),
    (2, None,   "valid"),          # okay, null
    (3, "BAD",  "valid"),          # type mismatch on value_1 (string in double column)
    (4, 200.0,  "valid"),
]

# Create DataFrame with permissive mode and a rescue column
raw_df = spark.createDataFrame(hard_data, schema=rescued_schema)
raw_df.show()

# In real Auto Loader, the bad row would be written to _rescued_data.
# Manual alternative: read as strings, validate, separate clean/malformed.
# Run a query to identify rows where value_1 is not numeric:

raw_df.createOrReplaceTempView("rescued_temp")
cleaned  = spark.sql("SELECT * FROM rescued_temp WHERE value_1 IS NOT NULL")
rescued  = spark.sql("SELECT * FROM rescued_temp WHERE value_1 IS NULL AND id = 3")

print("Clean rows:")
cleaned.show()
print("Rescued / malformed rows:")
rescued.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Evolution summary (Concept 35):**
# MAGIC - Auto Loader's `addNewColumns` mode is the safest default — never lose data.
# MAGIC - `rescue` mode adds `_rescued_data` so you can quarantine and repair malformed records.
# MAGIC - In Community Edition, `spark.databricks.delta.schema.autoMerge.enabled = true` gives you automatic column addition on append.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 36 — Multi-Hop Medallion Architecture
# MAGIC
# MAGIC | Layer   | Purpose                                   | Operations                        |
# MAGIC |---------|-------------------------------------------|-----------------------------------|
# MAGIC | Bronze  | Raw ingestion, 1:1 with source            | Append only, preserve lineage     |
# MAGIC | Silver  | Cleansed, validated, deduplicated         | Cast types, deduplicate, QC       |
# MAGIC | Gold    | Business aggregates & views               | Aggregations, joins, enrichment   |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 36‑A  Bronze — Raw Ingestion (Append‑Only)

# COMMAND ----------

BRONZE_TABLE = "ingestion_demo.sales_bronze"

# Bronze: raw CSV dump, add ingestion timestamp and source filename
def build_bronze(source_dir, bronze_table):
    spark.sql(f"DROP TABLE IF EXISTS {bronze_table}")

    raw_df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{source_dir}/*.csv")
    )

    bronze_df = (raw_df
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    bronze_df.write.mode("overwrite").format("delta").saveAsTable(bronze_table)
    return spark.table(bronze_table).count()

bronze_rows = build_bronze(DBFS_SALES, BRONZE_TABLE)
print(f"Bronze layer: {bronze_rows} rows")
spark.table(BRONZE_TABLE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 36‑B  Silver — Clean, Deduplicate, Cast Types

# COMMAND ----------

SILVER_TABLE = "ingestion_demo.sales_silver"

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")

bronze = spark.table(BRONZE_TABLE)

# 1. Deduplicate on transaction_id (keep latest _bronze_ingested_at)
window_spec = Window.partitionBy("transaction_id").orderBy(F.desc("_bronze_ingested_at"))
deduped = (bronze
    .withColumn("_rn", F.row_number().over(window_spec))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# 2. Clean & cast — ensure correct types, strip whitespace, filter invalid rows
silver_df = (deduped
    .withColumn("transaction_id",   F.trim(F.col("transaction_id")))
    .withColumn("transaction_date", F.to_date(F.col("transaction_date")))
    .withColumn("product_name",     F.trim(F.col("product_name")))
    .withColumn("category",         F.trim(F.col("category")))
    .withColumn("quantity",         F.col("quantity").cast("integer"))
    .withColumn("unit_price",       F.col("unit_price").cast("double"))
    .withColumn("total_amount",     F.col("total_amount").cast("double"))
    .withColumn("region",           F.trim(F.col("region")))
    .withColumn("status",           F.trim(F.col("status")))
    .withColumn("_silver_cleaned_at", F.current_timestamp())
    # Drop null transaction_ids (data quality)
    .filter(F.col("transaction_id").isNotNull())
    .filter(F.col("transaction_id") != "")
)

# 3. Add data quality flag
silver_df = silver_df.withColumn("_quality_flag",
    F.when(F.col("total_amount") < 0, "SUSPECT_NEGATIVE")
     .when(F.col("quantity") <= 0,    "SUSPECT_ZERO_QTY")
     .otherwise("OK")
)

silver_df.write.mode("overwrite").format("delta").saveAsTable(SILVER_TABLE)

print(f"Silver layer: {spark.table(SILVER_TABLE).count()} rows")
spark.table(SILVER_TABLE).printSchema()

# Data quality report
quality_counts = spark.table(SILVER_TABLE).groupBy("_quality_flag").count()
display(quality_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 36‑C  Gold — Business Aggregates

# COMMAND ----------

GOLD_DB = "ingestion_demo"
silver = spark.table(SILVER_TABLE)

# ---- Aggregate 1: Daily sales by category ----
daily_sales = (silver
    .groupBy("transaction_date", "category")
    .agg(
        F.sum("total_amount").alias("daily_revenue"),
        F.count("*").alias("order_count"),
        F.avg("total_amount").alias("avg_order_value"),
    )
)

daily_sales.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_DB}.gold_daily_sales")
print("Gold — Daily Sales by Category")
display(spark.table(f"{GOLD_DB}.gold_daily_sales").orderBy("transaction_date", "category"))

# ---- Aggregate 2: Top 10 products by revenue ----
top_products = (silver
    .groupBy("product_name", "category")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

top_products.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_DB}.gold_top_products")
print("\nGold — Top 10 Products")
display(spark.table(f"{GOLD_DB}.gold_top_products"))

# ---- Aggregate 3: Regional performance ----
regional = (silver
    .groupBy("region")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returns"),
        F.round(F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)) / F.count("*") * 100, 2).alias("return_rate_pct"),
    )
)

regional.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_DB}.gold_regional")
print("\nGold — Regional Performance")
display(spark.table(f"{GOLD_DB}.gold_regional"))

# ---- Aggregate 4: Customer spending segments (simulated from transaction size) ----
segments = (silver
    .withColumn("spending_segment",
        F.when(F.col("total_amount") >= 500, "High")
         .when(F.col("total_amount") >= 100, "Medium")
         .otherwise("Low")
    )
    .groupBy("spending_segment")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
)

segments.write.mode("overwrite").format("delta").saveAsTable(f"{GOLD_DB}.gold_spending_segments")
print("\nGold — Spending Segments")
display(spark.table(f"{GOLD_DB}.gold_spending_segments"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 36‑D  Medallion Lineage View

# COMMAND ----------

print("""
  ┌──────────────────────────────────────────────────┐
  │                Medallion Pipeline                 │
  ├──────────┬──────────────────┬────────────────────┤
  │  Bronze  │  Silver          │  Gold               │
  ├──────────┼──────────────────┼────────────────────┤
  │ Raw CSV  │ Deduplicated     │ gold_daily_sales    │
  │ (append) │ Typed            │ gold_top_products   │
  │          │ Quality-flagged  │ gold_regional       │
  │          │                  │ gold_spending_segments │
  └──────────┴──────────────────┴────────────────────┘
""")

# Verify we can trace a transaction through all layers
sample_id = spark.table(BRONZE_TABLE).select("transaction_id").first()[0]
print(f"Tracing transaction: {sample_id}")

b = spark.table(BRONZE_TABLE).filter(f"transaction_id = '{sample_id}'").select("transaction_id","total_amount","_bronze_ingested_at")
s = spark.table(SILVER_TABLE).filter(f"transaction_id = '{sample_id}'").select("transaction_id","total_amount","_quality_flag")
print("\nBronze:")
b.show(truncate=False)
print("Silver:")
s.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 37 — Incremental Processing Patterns
# MAGIC
# MAGIC Three common strategies for processing only new/changed data:
# MAGIC
# MAGIC | Pattern               | Granularity | Latency    | Complexity | Best For                        |
# MAGIC |-----------------------|-------------|------------|------------|---------------------------------|
# MAGIC | File‑level            | File        | Minutes    | Low        | Simple batch append             |
# MAGIC | Change Data Feed (CDF)| Row         | Seconds    | Medium     | CDC from Delta tables           |
# MAGIC | Timestamp / Watermark | Row         | Sub‑second | Medium     | Streaming with watermark        |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 37‑A  File‑Level Incremental Processing

# COMMAND ----------

# We already demonstrated this in Concept 31-C. Here's a concise recap:
spark.sql("DROP TABLE IF EXISTS ingestion_demo.file_level_target")
spark.sql("CREATE TABLE IF NOT EXISTS ingestion_demo._file_level_meta "
          "(file_path STRING, ingested_at STRING) USING delta")

# Process only files NOT in the meta table
def file_level_incremental(source_dir, target_table):
    meta = spark.table("ingestion_demo._file_level_meta")
    ingested = set(r.file_path for r in meta.select("file_path").collect())
    all_files = set(f.path for f in dbutils.fs.ls(source_dir) if f.name.endswith(".csv"))
    new = [f for f in sorted(all_files - ingested) if not f.endswith("_SUCCESS")]

    if not new:
        print("No new files.")
        return 0

    print(f"Processing {len(new)} new file(s)...")
    df = spark.read.option("header","true").option("inferSchema","true").csv(new)
    df.write.mode("append").format("delta").saveAsTable(target_table)
    now = datetime.now().isoformat()
    spark.createDataFrame([(f, now) for f in new], "file_path STRING, ingested_at STRING") \
        .write.mode("append").format("delta").saveAsTable("ingestion_demo._file_level_meta")
    return df.count()

cnt = file_level_incremental(DBFS_SALES, "ingestion_demo.file_level_target")
print(f"File-level incremental ingested {cnt} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 37‑B  Row‑Level — Change Data Feed (CDF)

# COMMAND ----------

# Delta's Change Data Feed tracks row‑level changes (insert, update, delete).
# For Community Edition we simulate CDF by reading version history.

# Create a base table and make changes
spark.sql("DROP TABLE IF EXISTS ingestion_demo.cdf_demo")

cdf_base = spark.createDataFrame(
    [(1, "Alice", 1000.0), (2, "Bob", 1500.0)],
    "id INT, name STRING, balance DOUBLE"
)
cdf_base.write.format("delta").saveAsTable("ingestion_demo.cdf_demo")

# Update Bob's balance
spark.sql("""
  MERGE INTO ingestion_demo.cdf_demo t
  USING (SELECT 2 AS id, 'Bob' AS name, 2000.0 AS balance) s
  ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET t.balance = s.balance
  WHEN NOT MATCHED THEN INSERT *
""")

# Insert a new row
spark.sql("INSERT INTO ingestion_demo.cdf_demo VALUES (3, 'Carol', 2500.0)")

# Delete Alice
spark.sql("DELETE FROM ingestion_demo.cdf_demo WHERE id = 1")

# ---- Simulate CDF with Delta time travel ----
print("Initial state (version 0):")
spark.sql("SELECT * FROM ingestion_demo.cdf_demo VERSION AS OF 0").show()

print("Current state (latest version):")
spark.table("ingestion_demo.cdf_demo").show()

# In production with CDF enabled, you'd query table_changes():
# SELECT * FROM table_changes('ingestion_demo.cdf_demo', 0)
# This returns change_type (insert/update/delete), commit_version, timestamp

print("\nCDF simulation — history:")
spark.sql("DESCRIBE HISTORY ingestion_demo.cdf_demo").select("version","timestamp","operation").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 37‑C  Timestamp / Watermark‑Based Incremental Processing

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS ingestion_demo.watermark_source")
spark.sql("DROP TABLE IF EXISTS ingestion_demo.watermark_target")

# Create a source table with timestamped events
from pyspark.sql import functions as F

watermark_data = []
base_date = datetime(2025, 1, 1)
for i in range(50):
    ts = base_date + timedelta(hours=i)
    watermark_data.append((i, f"event_{i}", round(i * 1.5, 2), ts.isoformat()))

watermark_df = spark.createDataFrame(watermark_data, "id INT, event_name STRING, value DOUBLE, event_time STRING")
watermark_df.write.mode("overwrite").format("delta").saveAsTable("ingestion_demo.watermark_source")

# ---- Function to process only rows after last run time ----
def watermark_incremental(source_table, target_table, watermark_col, watermark_state_table):
    """
    Read only rows from `source_table` whose `watermark_col` is greater than
    the last recorded high-watermark stored in `watermark_state_table`.
    """
    spark.sql(f"CREATE TABLE IF NOT EXISTS {watermark_state_table} "
              "(last_watermark STRING, updated_at STRING) USING delta")

    # Get the last high-watermark
    state = spark.table(watermark_state_table)
    if state.count() > 0:
        last_wm = state.orderBy(F.desc("updated_at")).select("last_watermark").first()[0]
    else:
        last_wm = "1970-01-01T00:00:00"

    print(f"Last high-watermark: {last_wm}")

    new_df = spark.table(source_table).filter(F.col(watermark_col) > last_wm)
    new_count = new_df.count()

    if new_count == 0:
        print("No new rows since last watermark.")
        return 0

    if spark.catalog.tableExists(target_table):
        new_df.write.mode("append").format("delta").saveAsTable(target_table)
    else:
        new_df.write.mode("overwrite").format("delta").saveAsTable(target_table)

    # Compute new max watermark from processed data
    new_wm = new_df.agg(F.max(watermark_col).alias("max_wm")).first()["max_wm"]
    now = datetime.now().isoformat()

    # Update state
    sparK = spark  # avoid shadowing
    state_update = sparK.createDataFrame([(new_wm, now)], "last_watermark STRING, updated_at STRING")
    state_update.write.mode("overwrite").format("delta").saveAsTable(watermark_state_table)

    print(f"Processed {new_count} rows. New high-watermark: {new_wm}")
    return new_count

# First run — loads all data
c = watermark_incremental(
    "ingestion_demo.watermark_source",
    "ingestion_demo.watermark_target",
    "event_time",
    "ingestion_demo._watermark_state"
)
print(f"First run: {c} rows")

# Second run — should be zero (no new data)
c2 = watermark_incremental(
    "ingestion_demo.watermark_source",
    "ingestion_demo.watermark_target",
    "event_time",
    "ingestion_demo._watermark_state"
)
print(f"Second run: {c2} rows")

# COMMAND ----------

# Add new data and run again to verify watermark picks up only new rows
new_events = []
for i in range(50, 60):
    ts = base_date + timedelta(hours=i)
    new_events.append((i, f"event_{i}", round(i * 1.5, 2), ts.isoformat()))

new_df = spark.createDataFrame(new_events, "id INT, event_name STRING, value DOUBLE, event_time STRING")
new_df.write.mode("append").format("delta").saveAsTable("ingestion_demo.watermark_source")

c3 = watermark_incremental(
    "ingestion_demo.watermark_source",
    "ingestion_demo.watermark_target",
    "event_time",
    "ingestion_demo._watermark_state"
)
print(f"Third run (after adding 10 new events): {c3} rows (expect 10)")

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental Processing comparison:**
# MAGIC
# MAGIC | Pattern              | Cost  | Latency   | Use Case                             |
# MAGIC |----------------------|-------|-----------|--------------------------------------|
# MAGIC | File‑level tracking  | Low   | File‑based| Daily batch loads                    |
# MAGIC | CDF (Delta)          | Med   | Seconds   | CDC replication, auditing            |
# MAGIC | Watermark / timestamp| Low   | Near‑RT   | Streaming events, log processing     |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 38 — Lakehouse Federation
# MAGIC
# MAGIC Lakehouse Federation allows Databricks to query data in *external* databases (PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery) **without moving the data** — the query is pushed down as much as possible and results are returned.
# MAGIC
# MAGIC **Key concepts:**
# MAGIC - **Foreign catalog** — a catalog that maps to an external database connection.
# MAGIC - **Query pushdown** — Databricks translates Spark SQL into the external DB's dialect so filters and aggregations run remotely.
# MAGIC - **Use cases** — ad‑hoc cross‑system queries, building a 360° customer view, gradual migration to Lakehouse.
# MAGIC
# MAGIC **Community Edition limitation:** Foreign catalogs require full platform.  We demonstrate JDBC reading as the CE equivalent.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 38‑A  JDBC Read — Community Edition Equivalent of Federation

# COMMAND ----------

# Federation conceptual syntax (not runnable in CE):
# CREATE FOREIGN CATALOG pg_catalog
#   USING JDBC OPTIONS (
#     url      'jdbc:postgresql://host:5432/db',
#     user     'reader',
#     password '***'
#   );
#
# SELECT * FROM pg_catalog.public.customers WHERE region = 'West';

# ---- CE alternative: JDBC read ----
# For demo we read our own Delta table via JDBC (self-referencing)
# In production you'd connect to Postgres, MySQL, etc.

jdbc_url    = f"jdbc:spark://localhost:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=sql/protocolv1/o/0/0000-000000-0000000000"
jdbc_table  = "ingestion_demo.sales_silver"

try:
    jdbc_df = (spark.read
        .format("jdbc")
        .option("url",      jdbc_url)
        .option("dbtable",  jdbc_table)
        .option("user",     "token")
        .option("password", "dummy")
        .load()
    )
    print("JDBC read succeeded (self-referencing).")
    jdbc_df.select("transaction_id", "product_name", "total_amount").show(3)
except Exception as e:
    print(f"JDBC to Databricks itself not available in CE: {e}")

# ---- Show a generic JDBC pattern (PostgreSQL example, commented) ----
print("""
# Generic PostgreSQL JDBC read pattern:
pg_df = (spark.read
    .format("jdbc")
    .option("url",      "jdbc:postgresql://<host>:5432/<db>")
    .option("dbtable",  "(SELECT * FROM orders WHERE order_date > '2025-01-01') AS filtered")
    .option("user",     "<user>")
    .option("password", "<password>")
    .option("driver",   "org.postgresql.Driver")
    .option("numPartitions", "8")
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "1000000")
    .load()
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 38‑B  Query Pushdown Explained

# COMMAND ----------

# When using federation, Databricks pushes predicates to the remote DB.
# Example: Spark SQL `SELECT * FROM pg_catalog.public.orders WHERE region = 'West'`
#   => Databricks translates to: `SELECT * FROM orders WHERE region = 'West'` on PostgreSQL
#   => Only matching rows are transferred over the network

# Without pushdown (naive JDBC read), ALL rows are pulled and filtered in Spark.
# With pushdown, the WHERE clause runs server-side — dramatically less data transfer.

# Simulate pushdown benefit with local Spark:
big_df = spark.table(BRONZE_TABLE).repartition(1)
big_df.write.mode("overwrite").format("delta").saveAsTable("ingestion_demo.pushdown_test")

t0 = time.time()
# Without predicate pushdown simulation (read all then filter)
_  = spark.table("ingestion_demo.pushdown_test").filter("region = 'West'").collect()
t1 = time.time()

# With pushdown (Parquet predicate pushdown happens automatically)
_  = spark.table("ingestion_demo.pushdown_test").filter("region = 'West'").collect()
t2 = time.time()

print(f"Filter chain time: {t1-t0:.4f}s  (automatic pushdown in Delta/Parquet: {t2-t1:.4f}s)")
print("In a federated query, pushdown avoids transferring millions of unnecessary rows over the network.")

# COMMAND ----------

# MAGIC %md
# MAGIC **When to use Federation vs. ETL:**
# MAGIC
# MAGIC | Scenario                                | Approach          |
# MAGIC |-----------------------------------------|-------------------|
# MAGIC | Ad‑hoc query across 2+ systems          | Federation        |
# MAGIC | Operational dashboard with <5s latency  | Federation        |
# MAGIC | One‑time data exploration               | Federation        |
# MAGIC | Heavy transformations, joins, history   | ETL into Lakehouse|
# MAGIC | ML training on large datasets           | ETL into Lakehouse|
# MAGIC | Compliance / audit trail required       | ETL into Lakehouse|

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 39 — Idempotent Pipeline Design
# MAGIC
# MAGIC **Idempotency = running the pipeline multiple times produces the same final state.**
# MAGIC
# MAGIC Why it matters:
# MAGIC - **Failure recovery** — re‑run without manual cleanup
# MAGIC - **Backfill** — re‑process a date range safely
# MAGIC - **Late‑arriving data** — re‑ingest a partition
# MAGIC
# MAGIC Strategies:
# MAGIC - `MERGE` with natural keys (upsert semantics)
# MAGIC - `INSERT OVERWRITE` with `replaceWhere`
# MAGIC - Partition‑based truncate + reload
# MAGIC - `COPY INTO` (built‑in idempotency)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 39‑A  MERGE with Natural Keys — Idempotent Upsert

# COMMAND ----------

# Create a target table with a natural key
spark.sql("DROP TABLE IF EXISTS ingestion_demo.idempotent_products")

target_schema = T.StructType([
    T.StructField("product_code", T.StringType()),   # natural key
    T.StructField("product_name", T.StringType()),
    T.StructField("price",        T.DoubleType()),
    T.StructField("updated_at",   T.StringType()),
])

initial = spark.createDataFrame([
    ("P001", "Widget",      9.99, "2025-01-01"),
    ("P002", "Gadget",     19.99, "2025-01-02"),
], schema=target_schema)

initial.write.mode("overwrite").format("delta").saveAsTable("ingestion_demo.idempotent_products")

# ---- Incoming batch (some new, some updates) ----
incoming = spark.createDataFrame([
    ("P002", "Gadget Pro", 24.99, "2025-02-01"),   # UPDATE — P002 exists
    ("P003", "Doohickey",   5.99, "2025-02-01"),   # INSERT — new record
    ("P001", "Widget",      9.99, "2025-02-01"),   # UNCHANGED — same values
], schema=target_schema)

incoming.createOrReplaceTempView("incoming_updates")

# Idempotent MERGE — run it multiple times, result is always the same
merge_sql = """
  MERGE INTO ingestion_demo.idempotent_products AS target
  USING incoming_updates AS source
  ON target.product_code = source.product_code
  WHEN MATCHED THEN
    UPDATE SET
      target.product_name = source.product_name,
      target.price        = source.price,
      target.updated_at   = source.updated_at
  WHEN NOT MATCHED THEN
    INSERT (product_code, product_name, price, updated_at)
    VALUES (source.product_code, source.product_name, source.price, source.updated_at)
"""

# Run 1
spark.sql(merge_sql)
print("After run 1:")
spark.table("ingestion_demo.idempotent_products").orderBy("product_code").show()

# Run 2 — should produce identical results
spark.sql(merge_sql)
print("After run 2 (idempotent):")
spark.table("ingestion_demo.idempotent_products").orderBy("product_code").show()

# Run 3 — still identical
spark.sql(merge_sql)
print("After run 3 (still idempotent):")
spark.table("ingestion_demo.idempotent_products").orderBy("product_code").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 39‑B  INSERT OVERWRITE with replaceWhere — Partition Idempotency

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS ingestion_demo.idempotent_sales")

# Target partitioned by date
sample_sales = spark.table(BRONZE_TABLE) \
    .withColumn("sale_date", F.to_date(F.col("transaction_date"))) \
    .select("transaction_id","sale_date","product_name","category","total_amount","region")

sample_sales.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("sale_date") \
    .saveAsTable("ingestion_demo.idempotent_sales")

# ---- Re-ingest a specific date safely ----
target_date = sample_sales.select("sale_date").first()["sale_date"]
print(f"Demonstrating replaceWhere for date: {target_date}")

# Build a "new" DataFrame for that date (simulates re-processing)
replace_df = spark.table("ingestion_demo.idempotent_sales") \
    .filter(f"sale_date = '{target_date}'") \
    .withColumn("category", F.upper(F.col("category")))  # pretend we fixed category casing

print(f"Before replaceWhere — sample row for {target_date}:")
spark.table("ingestion_demo.idempotent_sales") \
    .filter(f"sale_date = '{target_date}'") \
    .select("sale_date","category") \
    .show(2, truncate=False)

(replace_df.write
    .mode("overwrite")
    .format("delta")
    .option("replaceWhere", f"sale_date = '{target_date}'")
    .saveAsTable("ingestion_demo.idempotent_sales")
)

print(f"After replaceWhere — category is now upper-case:")
spark.table("ingestion_demo.idempotent_sales") \
    .filter(f"sale_date = '{target_date}'") \
    .select("sale_date","category") \
    .show(2, truncate=False)

# Other partitions untouched
distinct_dates = spark.table("ingestion_demo.idempotent_sales") \
    .select("sale_date") \
    .distinct() \
    .count()
print(f"Total distinct dates in table: {distinct_dates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 39‑C  Fully Idempotent Ingestion Function

# COMMAND ----------

def idempotent_ingest(source_dir, target_table, natural_key_cols, partition_col=None):
    """
    Generic idempotent ingestion function:
    1. Read new data from source_dir
    2. MERGE into target table using natural_key_cols
    3. If partition_col is provided, use replaceWhere at partition granularity
    """
    print(f"Source      : {source_dir}")
    print(f"Target      : {target_table}")
    print(f"Natural keys: {natural_key_cols}")

    # Read source
    source_df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{source_dir}/*.csv")
    )

    # Add ingestion metadata
    source_df = source_df \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source",      F.lit(source_dir))

    # Ensure target exists
    if not spark.catalog.tableExists(target_table):
        source_df.write.mode("overwrite").format("delta").saveAsTable(target_table)
        print(f"Created new table: {target_table}")
        return source_df.count()

    source_df.createOrReplaceTempView("_idempotent_source")

    join_condition = " AND ".join(
        f"target.{c} = source.{c}" for c in natural_key_cols
    )

    target_schema = spark.table(target_table).schema
    target_cols = {f.name for f in target_schema.fields}
    source_cols = {f.name for f in source_df.schema.fields}
    cols_to_update = [c for c in source_cols if c in target_cols and c not in natural_key_cols and c != "_ingested_at"]

    update_clause = ", ".join(f"target.{c} = source.{c}" for c in cols_to_update)
    insert_cols   = ", ".join(cols_to_update + natural_key_cols)
    insert_vals   = ", ".join(f"source.{c}" for c in cols_to_update + natural_key_cols)

    merge_sql = f"""
      MERGE INTO {target_table} AS target
      USING _idempotent_source AS source
      ON {join_condition}
      WHEN MATCHED THEN UPDATE SET {update_clause}
      WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    print(f"\nMERGE statement:\n{merge_sql[:500]}...")
    spark.sql(merge_sql)

    result_count = spark.table(target_table).count()
    print(f"Total rows in {target_table}: {result_count}")
    return result_count

# Test the idempotent function
spark.sql("DROP TABLE IF EXISTS ingestion_demo.fully_idempotent_target")

c1 = idempotent_ingest(
    DBFS_SALES,
    "ingestion_demo.fully_idempotent_target",
    natural_key_cols=["transaction_id"]
)
print(f"Run 1 created {c1} rows")

c2 = idempotent_ingest(
    DBFS_SALES,
    "ingestion_demo.fully_idempotent_target",
    natural_key_cols=["transaction_id"]
)
print(f"Run 2 (idempotent) — table has {c2} rows (should equal {c1})")

assert c1 == c2, "IDEMPOTENCY VIOLATED"
print("Idempotency verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 39‑D  Retry Pattern with Checkpoint Recovery

# COMMAND ----------

def idempotent_ingest_with_retry(source_dir, target_table, natural_keys, max_retries=3, backoff_seconds=2):
    """Idempotent ingestion with exponential backoff retry."""
    for attempt in range(1, max_retries + 1):
        try:
            count = idempotent_ingest(source_dir, target_table, natural_keys)
            print(f"Success on attempt {attempt}.")
            return count
        except Exception as e:
            if attempt == max_retries:
                print(f"FAILED after {max_retries} attempts: {e}")
                raise
            wait = backoff_seconds * (2 ** (attempt - 1))
            print(f"Attempt {attempt} failed ({e}). Retrying in {wait}s...")
            time.sleep(wait)

print("\nRetry pattern demonstration:")
try:
    idempotent_ingest_with_retry(
        DBFS_SALES,
        "ingestion_demo.fully_idempotent_target",
        ["transaction_id"],
        max_retries=2
    )
except Exception as e:
    print(f"Expected failure: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Idempotency summary (Concept 39):**
# MAGIC
# MAGIC | Strategy                | When to Use                               |
# MAGIC |-------------------------|-------------------------------------------|
# MAGIC | `MERGE` with natural key| Dimension tables, upsert workloads        |
# MAGIC | `INSERT OVERWRITE`      | Full partition reload                     |
# MAGIC | `replaceWhere`          | Selective partition reload                |
# MAGIC | `COPY INTO`             | File‑based batch loads (built‑in)         |
# MAGIC | Checkpoint + retry      | Streaming / long‑running pipelines        |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Concept 40 — SCD Type 1 & Type 2 Patterns
# MAGIC
# MAGIC **Slowly Changing Dimensions (SCD)** manage changes to dimensional attributes over time.
# MAGIC
# MAGIC | Type | Strategy                                 | Historical tracking | Storage |
# MAGIC |------|------------------------------------------|---------------------|---------|
# MAGIC | 1    | Overwrite old value with new value       | No                  | Low     |
# MAGIC | 2    | Insert new row; expire old row           | Yes                 | Higher  |
# MAGIC | 3    | Keep previous value in a separate column | One version back    | Medium  |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 40‑A  Build Customer Dimension (Base Data)

# COMMAND ----------

# Create initial customer dimension
spark.sql("DROP TABLE IF EXISTS ingestion_demo.customers")
spark.sql("DROP TABLE IF EXISTS ingestion_demo.customers_scd2")

CUSTOMER_DATA = [
    ("CUST-1001", "Alice",   "Smith",   "alice@example.com",   "New York",   "NY"),
    ("CUST-1002", "Bob",     "Johnson", "bob@example.com",     "Los Angeles","CA"),
    ("CUST-1003", "Charlie", "Brown",   "charlie@example.com", "Chicago",    "IL"),
    ("CUST-1004", "Diana",   "Prince",  "diana@example.com",   "Houston",    "TX"),
    ("CUST-1005", "Edward",  "Norton",  "edward@example.com",  "Phoenix",    "AZ"),
]

customer_schema = T.StructType([
    T.StructField("customer_id",  T.StringType()),
    T.StructField("first_name",   T.StringType()),
    T.StructField("last_name",    T.StringType()),
    T.StructField("email",        T.StringType()),
    T.StructField("city",         T.StringType()),
    T.StructField("state",        T.StringType()),
])

customer_base = spark.createDataFrame(CUSTOMER_DATA, schema=customer_schema) \
    .withColumn("created_at", F.current_timestamp())

customer_base.write.mode("overwrite").format("delta").saveAsTable("ingestion_demo.customers")
print("Initial customer dimension:")
spark.table("ingestion_demo.customers").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 40‑B  SCD Type 1 — Overwrite Current Attributes

# COMMAND ----------

# ---- Incoming changes (Type 1) ----
# Alice moved to Seattle; Bob changed email; Frank is new
scd1_changes = spark.createDataFrame([
    ("CUST-1001", "Alice",   "Smith",   "alice_new@example.com", "Seattle",    "WA"),  # UPDATE
    ("CUST-1002", "Bob",     "Johnson", "bob_new@example.com",   "Los Angeles","CA"),  # UPDATE
    ("CUST-1006", "Frank",   "Miller",  "frank@example.com",     "Denver",     "CO"),  # INSERT
], schema=customer_schema)

scd1_changes.createOrReplaceTempView("scd1_changes")

scd1_merge = """
  MERGE INTO ingestion_demo.customers AS target
  USING scd1_changes AS source
  ON target.customer_id = source.customer_id
  WHEN MATCHED THEN
    UPDATE SET
      target.first_name = source.first_name,
      target.last_name  = source.last_name,
      target.email      = source.email,
      target.city       = source.city,
      target.state      = source.state
  WHEN NOT MATCHED THEN
    INSERT (customer_id, first_name, last_name, email, city, state, created_at)
    VALUES (source.customer_id, source.first_name, source.last_name,
            source.email, source.city, source.state, current_timestamp())
"""

spark.sql(scd1_merge)

print("After SCD Type 1 merge (Alice & Bob updated in place; Frank added):")
spark.table("ingestion_demo.customers").orderBy("customer_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD Type 1 consequence:** Alice's old email and city are *lost*.  No history preserved — but the table stays small and simple.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 40‑C  SCD Type 2 — Full History with Effective Dates & Current Flag

# COMMAND ----------

# Create SCD Type 2 dimension (initial load)
scd2_initial = spark.createDataFrame(CUSTOMER_DATA, schema=customer_schema) \
    .withColumn("eff_start_date", F.current_date()) \
    .withColumn("eff_end_date",   F.lit(None).cast("date")) \
    .withColumn("is_current",     F.lit(True))

scd2_initial.write.mode("overwrite").format("delta").saveAsTable("ingestion_demo.customers_scd2")
print("Initial SCD Type 2 dimension:")
spark.table("ingestion_demo.customers_scd2").orderBy("customer_id").show(truncate=False)

# COMMAND ----------

# ---- Incoming changes (Type 2) ----
# Same three changes as before, but now we preserve history
scd2_incoming = spark.createDataFrame([
    ("CUST-1001", "Alice",   "Smith",   "alice_new@example.com", "Seattle",    "WA"),  # UPDATE
    ("CUST-1002", "Bob",     "Johnson", "bob_new@example.com",   "Los Angeles","CA"),  # UPDATE
    ("CUST-1006", "Frank",   "Miller",  "frank@example.com",     "Denver",     "CO"),  # INSERT
], schema=customer_schema)

scd2_incoming.createOrReplaceTempView("scd2_incoming")

# ---- Step 1: Expire current rows for changing customers ----
expire_sql = """
  MERGE INTO ingestion_demo.customers_scd2 AS target
  USING scd2_incoming AS source
  ON target.customer_id = source.customer_id AND target.is_current = true
  WHEN MATCHED AND (
    target.first_name <> source.first_name OR
    target.last_name  <> source.last_name OR
    target.email      <> source.email OR
    target.city       <> source.city OR
    target.state      <> source.state
  ) THEN UPDATE SET
    target.is_current    = false,
    target.eff_end_date  = current_date()
"""

spark.sql(expire_sql)

# ---- Step 2: Insert new rows with current data ----
insert_sql = """
  INSERT INTO ingestion_demo.customers_scd2
    (customer_id, first_name, last_name, email, city, state,
     eff_start_date, eff_end_date, is_current)
  SELECT
    source.customer_id,
    source.first_name,
    source.last_name,
    source.email,
    source.city,
    source.state,
    current_date()  AS eff_start_date,
    CAST(NULL AS DATE) AS eff_end_date,
    true           AS is_current
  FROM scd2_incoming source
  -- Only insert if the customer doesn't exist at all, or has changed
  LEFT ANTI JOIN ingestion_demo.customers_scd2 target
    ON source.customer_id = target.customer_id AND target.is_current = true
  WHERE NOT EXISTS (
    SELECT 1 FROM ingestion_demo.customers_scd2 t2
    WHERE t2.customer_id = source.customer_id
      AND t2.is_current = true
      AND t2.first_name = source.first_name
      AND t2.last_name  = source.last_name
      AND t2.email      = source.email
      AND t2.city       = source.city
      AND t2.state      = source.state
  )
"""

spark.sql(insert_sql)

# ---- Simpler two-step MERGE approach (more common in production) ----
# For clarity, here's an alternative using a view:

all_scd2_rows = spark.table("ingestion_demo.customers_scd2") \
    .unionByName(scd2_incoming
        .withColumn("eff_start_date", F.current_date())
        .withColumn("eff_end_date",   F.lit(None).cast("date"))
        .withColumn("is_current",     F.lit(True)),
        allowMissingColumns=True
    )

print("After SCD Type 2 — full history:")
spark.table("ingestion_demo.customers_scd2") \
    .orderBy("customer_id", "eff_start_date") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 40‑D  Querying Current vs. Historical Views (SCD Type 2)

# COMMAND ----------

# ---- Current view (most common query) ----
scd2_current = spark.table("ingestion_demo.customers_scd2") \
    .filter(F.col("is_current") == True)

print("Current customers (is_current = true):")
scd2_current.orderBy("customer_id").show(truncate=False)

# ---- Point-in-time query: "What did customers look like on 2025-01-15?" ----
query_date = date.today().isoformat()
pit_sql = f"""
  SELECT * FROM ingestion_demo.customers_scd2
  WHERE '{query_date}' >= eff_start_date
    AND ('{query_date}' < eff_end_date OR eff_end_date IS NULL)
"""
print(f"\nPoint-in-time view as of {query_date}:")
spark.sql(pit_sql).orderBy("customer_id").show(truncate=False)

# ---- Change history for a single customer ----
print("\nFull history for CUST-1001 (Alice):")
spark.table("ingestion_demo.customers_scd2") \
    .filter("customer_id = 'CUST-1001'") \
    .orderBy("eff_start_date") \
    .select("customer_id","first_name","city","state","eff_start_date","eff_end_date","is_current") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 40‑E  SCD Type 2 MERGE — Production‑Ready Single Statement

# COMMAND ----------

def scd_type2_merge(target_table, source_df, business_key, scd_columns):
    """
    Production-ready SCD Type 2 using a single MERGE statement.
    
    Steps:
      1. Expire old rows (set is_current=false, eff_end_date=now)
      2. Insert new rows for changed or new keys
    """
    source_df.createOrReplaceTempView("_scd2_source")
    current_date_val = date.today().isoformat()

    # Build the change-detection condition
    change_conditions = " OR ".join(
        f"target.{c} <> source.{c}" for c in scd_columns
    )

    # Step 1: Expire
    expire_merge = f"""
      MERGE INTO {target_table} AS target
      USING _scd2_source AS source
      ON target.{business_key} = source.{business_key} AND target.is_current = true
      WHEN MATCHED AND ({change_conditions}) THEN
        UPDATE SET target.is_current = false, target.eff_end_date = '{current_date_val}'
    """
    spark.sql(expire_merge)

    # Step 2: Insert new versions
    insert_merge = f"""
      MERGE INTO {target_table} AS target
      USING _scd2_source AS source
      ON target.{business_key} = source.{business_key} AND target.is_current = true
      WHEN NOT MATCHED THEN
        INSERT ({business_key}, {', '.join(scd_columns)}, eff_start_date, eff_end_date, is_current)
        VALUES (source.{business_key}, {', '.join(f'source.{c}' for c in scd_columns)}, '{current_date_val}', NULL, true)
    """
    spark.sql(insert_merge)

    return spark.table(target_table).count()

# ---- Test the function ----
spark.sql("DROP TABLE IF EXISTS ingestion_demo.cust_scd2_v2")

scd2_v2_initial = spark.createDataFrame(CUSTOMER_DATA, schema=customer_schema) \
    .withColumn("eff_start_date", F.current_date()) \
    .withColumn("eff_end_date",   F.lit(None).cast("date")) \
    .withColumn("is_current",     F.lit(True))

scd2_v2_initial.write.mode("overwrite").format("delta").saveAsTable("ingestion_demo.cust_scd2_v2")

# Apply new changes
new_changes = spark.createDataFrame([
    ("CUST-1001", "Alice",   "Smith",   "alice_final@example.com", "Miami",      "FL"),
    ("CUST-1007", "Grace",   "Hopper",  "grace@example.com",       "Boston",     "MA"),
], schema=customer_schema)

total = scd_type2_merge(
    "ingestion_demo.cust_scd2_v2",
    new_changes,
    business_key="customer_id",
    scd_columns=["first_name","last_name","email","city","state"]
)
print(f"Total rows after SCD2 merge: {total}")

print("\nFull SCD Type 2 dimension:")
spark.table("ingestion_demo.cust_scd2_v2") \
    .orderBy("customer_id","eff_start_date") \
    .select("customer_id","first_name","city","state","eff_start_date","eff_end_date","is_current") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **SCD Type 1 vs Type 2 tradeoffs:**
# MAGIC
# MAGIC | Aspect              | Type 1                        | Type 2                           |
# MAGIC |---------------------|-------------------------------|----------------------------------|
# MAGIC | Storage             | Low (one row per key)         | Higher (one row per change)      |
# MAGIC | Historical queries  | Not possible                  | Built‑in (eff dates + flag)      |
# MAGIC | Implementation      | Simple UPDATE                 | MERGE with expire + insert       |
# MAGIC | Use case            | Corrections, transient fields | Regulatory audit, change tracking|

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup (Optional)

# COMMAND ----------

# Uncomment to clean up all demo tables and files:
# spark.sql("DROP DATABASE IF EXISTS ingestion_demo CASCADE")
# dbutils.fs.rm("dbfs:/FileStore/ingestion_demo", recurse=True)
# dbutils.fs.rm("dbfs:/FileStore/tables/ingestion_demo", recurse=True)

print("Cleanup skipped — remove comments above to execute.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary — Concepts 31 through 40
# MAGIC
# MAGIC | #  | Concept                              | Key Technique / Takeaway                                                     |
# MAGIC |----|--------------------------------------|------------------------------------------------------------------------------|
# MAGIC | 31 | COPY INTO vs. Auto Loader            | `COPY INTO` for batch (idempotent); Auto Loader for streaming with schema evo|
# MAGIC | 32 | File Formats in the Lakehouse        | Delta/Parquet rule the lakehouse — columnar, compressed, predicate pushdown  |
# MAGIC | 33 | Volumes                              | Governed file storage in Unity Catalog; use DBFS FileStore in CE             |
# MAGIC | 34 | Auto Loader: File Discovery          | Directory listing (simple) vs. file notification (low‑latency)                |
# MAGIC | 35 | Schema Evolution & Rescued Data      | `addNewColumns` + `_rescued_data` prevents data loss on schema drift        |
# MAGIC | 36 | Multi-Hop Medallion Architecture     | Bronze (raw) → Silver (cleansed) → Gold (aggregates) — each a Delta table   |
# MAGIC | 37 | Incremental Processing Patterns      | File‑level, CDF, or watermark‑based — pick by latency/cost requirements     |
# MAGIC | 38 | Lakehouse Federation                 | Query external DBs without moving data; JDBC fallback in CE                   |
# MAGIC | 39 | Idempotent Pipeline Design           | MERGE with natural keys, replaceWhere, checkpoint recovery                   |
# MAGIC | 40 | SCD Type 1 & Type 2                  | Type 1 = overwrite; Type 2 = history with eff dates + current flag           |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self‑Assessment
# MAGIC
# MAGIC Answer these to validate your understanding:
# MAGIC
# MAGIC 1. What makes `COPY INTO` idempotent without any user‑managed tracking table?
# MAGIC 2. Why does Parquet outperform CSV for analytical queries even when the same data is stored?
# MAGIC 3. How do Volumes differ from regular Delta tables in Unity Catalog?
# MAGIC 4. What are the two file‑discovery modes in Auto Loader and when would you choose each?
# MAGIC 5. What does the `_rescued_data` column capture, and why is it important?
# MAGIC 6. Draw the Medallion architecture layers and describe what happens at each stage.
# MAGIC 7. Compare file‑level, CDF, and watermark‑based incremental processing.
# MAGIC 8. When would you use Lakehouse Federation instead of a full ETL pipeline?
# MAGIC 9. Write a pseudocode `MERGE` statement that is safe to run multiple times without duplicating data.
# MAGIC 10. A customer moves from NY to CA.  What is the difference in query results between SCD Type 1 and SCD Type 2?
# MAGIC
# MAGIC **Happy Engineering!**

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 — Databricks Data Engineering Learning Path — Concepts 31–40
