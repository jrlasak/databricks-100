
# Delta Lake Fundamentals: Concepts #1-#10

## Notebook Overview

This notebook covers the **10 core concepts** of Delta Lake, from ACID transactions to advanced table configuration. Each concept includes a problem statement, real-world use case, hands-on code demo, key takeaways, and a self-assessment question.

**Environment:** Designed for Databricks Community Edition (free tier) — single node, no Unity Catalog, no Photon, no serverless.

**Concepts Covered:**
1. ACID Transactions [Easy]
2. Transaction Log (_delta_log) [Easy]
3. Time Travel & RESTORE [Easy]
4. Schema Enforcement vs. Evolution [Medium]
5. Liquid Clustering [Medium]
6. OPTIMIZE & File Compaction [Medium]
7. VACUUM & Storage Lifecycle [Medium]
8. Change Data Feed (CDF) [Medium]
9. Deletion Vectors [Medium]
10. Delta Table Properties & Configuration [Hard]

---


## Setup: Create Working Directory & Clean Up

We'll use a dedicated directory under `/tmp/` for all our Delta tables. This keeps things organised and makes cleanup easy.

```python

# Define base working directory
import os

base_dir = "/tmp/delta_lake_fundamentals"

# Clean up any previous runs
dbutils.fs.rm(base_dir, recurse=True)

# Create fresh directory — DBFS paths don't need explicit mkdir, but we'll log it
print(f"Working directory set to: {base_dir}")
print("Cleaned up any previous tables in this location.")

```


## Concept #1: ACID Transactions [Easy]

### What Problem It Solves

In traditional data lakes (Parquet, CSV, JSON), a failed write can leave the data in a corrupt or incomplete state. If a Spark job crashes mid-write, you might have partial files that downstream readers interpret as valid data. There's no built-in mechanism to ensure that a set of operations either all succeed or all fail as one unit.

Delta Lake solves this by providing **full ACID transactions** on your data lake:
- **Atomicity:** Multiple writes are committed together or fully rolled back.
- **Consistency:** Readers always see a consistent snapshot of the data.
- **Isolation:** Concurrent writes are serialised via optimistic concurrency control.
- **Durability:** Once committed, data persists.

### Real-World Use Case

An e-commerce platform writes order data and updates inventory levels in the same transaction. If the inventory update fails, the order write must also be rolled back to prevent data inconsistency.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 1: ACID Transactions")
print("=" * 60)

# Clean up from any previous runs
spark.sql(f"DROP TABLE IF EXISTS delta_orders")

# Create a Delta table with synthetic sales order data
orders_data = [
    (1001, "Laptop", 1, 1200.00, "pending"),
    (1002, "Monitor", 2, 300.00, "pending"),
    (1003, "Keyboard", 5, 45.00, "pending"),
    (1004, "Mouse", 10, 25.00, "pending"),
    (1005, "Headset", 3, 80.00, "pending"),
]

orders_df = spark.createDataFrame(
    orders_data, ["order_id", "product", "quantity", "price", "status"]
)

orders_df.write.format("delta").mode("overwrite").save(f"{base_dir}/orders_acid")

# Register as a table for SQL access
spark.sql(
    f"CREATE TABLE IF NOT EXISTS delta_orders USING DELTA LOCATION '{base_dir}/orders_acid'"
)

print("\nInitial orders table:")
spark.sql("SELECT * FROM delta_orders").show()

```


#### Atomic Write: Multiple Operations in One Transaction

We'll perform an INSERT and an UPDATE as a single atomic transaction using `MERGE`. If anything fails, nothing gets committed.

```python

# Demonstrate atomicity: MERGE performs INSERT + UPDATE atomically
spark.sql("""
    MERGE INTO delta_orders AS target
    USING (
        SELECT 1006 AS order_id, 'Webcam' AS product, 4 AS quantity,
               60.00 AS price, 'confirmed' AS status
    ) AS source
    ON target.order_id = source.order_id
    WHEN MATCHED THEN
        UPDATE SET status = 'confirmed', quantity = source.quantity
    WHEN NOT MATCHED THEN
        INSERT (order_id, product, quantity, price, status)
        VALUES (source.order_id, source.product, source.quantity,
                source.price, source.status)
""")

print("After atomic MERGE (INSERT + UPDATE in one transaction):")
spark.sql("SELECT * FROM delta_orders ORDER BY order_id").show()

```


#### Demonstrating Rollback on Failure

What happens if a write fails? With raw Parquet, partial files are left behind. With Delta, the transaction is rolled back completely. Let's demonstrate by writing to a raw Parquet directory at the same path to show the difference, then show Delta's safe behaviour.

```python

# Check current row count
current_count = spark.sql("SELECT COUNT(*) AS cnt FROM delta_orders").collect()[0]["cnt"]
print(f"Current row count: {current_count}")

# Attempt a failed write — this INSERT will fail because of a schema mismatch
# (we deliberately use a wrong column name to trigger failure)
try:
    bad_data = [(9999, "Test Product", 1, 99.99)]
    bad_df = spark.createDataFrame(bad_data, ["order_id", "product", "quantity", "price"])
    # This won't fail on schema — let's do a proper failure scenario:
    # Write a DataFrame with a column that violates constraints
    bad_df.write.format("delta").mode("append").save(f"{base_dir}/orders_acid_fail")
    print("Write succeeded (unexpected)")
except Exception as e:
    print(f"Write failed as expected: {str(e)[:200]}")

# IMPORTANT: Demonstrate that when you write directly with Delta and a failure
# occurs, no partial data is committed. Let's do an intentional failure inside a transaction.
# We'll use overwrite mode with a schema that's incompatible — but that's actually fine.
# Better approach: create a table and attempt to insert NULL into a NOT NULL column

# Create a table with a NOT NULL constraint
spark.sql(f"""
    DROP TABLE IF EXISTS delta_constrained
""")
spark.sql(f"""
    CREATE TABLE delta_constrained (id INT, name STRING NOT NULL)
    USING DELTA
    LOCATION '{base_dir}/delta_constrained'
""")

spark.sql("INSERT INTO delta_constrained VALUES (1, 'Alice')")
print("Before failed insert:")
spark.sql("SELECT * FROM delta_constrained").show()

# Now attempt to insert NULL — this should fail and roll back
try:
    spark.sql("INSERT INTO delta_constrained VALUES (2, CAST(NULL AS STRING))")
except Exception as e:
    print(f"Expected failure — NULL constraint violated: {str(e)[:200]}")

# Verify no partial data was committed
print("After failed insert (should still have only 1 row):")
spark.sql("SELECT * FROM delta_constrained").show()
row_count_after = spark.sql("SELECT COUNT(*) AS cnt FROM delta_constrained").collect()[0]["cnt"]
assert row_count_after == 1, f"Expected 1 row, got {row_count_after} — ACID failure!"
print(f"ACID property verified: Row count is {row_count_after} (rollback worked)")

```


#### Demonstrating Optimistic Concurrency Control

Delta uses optimistic concurrency by detecting conflicts at commit time. If two writers attempt conflicting modifications, one will be rejected with a `ConcurrentAppendException` or `ConcurrentDeleteReadException`.

```python

print("\n=== Optimistic Concurrency Control Demo ===")

# Read the current table for two concurrent "writers"
df_writer1 = spark.sql("SELECT * FROM delta_orders")
df_writer2 = spark.sql("SELECT * FROM delta_orders")

# Simulate writer 1 appending data (this will succeed)
new_order1 = [(2001, "Tablet", 2, 350.00, "confirmed")]
new_df1 = spark.createDataFrame(new_order1, ["order_id", "product", "quantity", "price", "status"])
new_df1.write.format("delta").mode("append").save(f"{base_dir}/orders_acid")
print("Writer 1 committed successfully.")

# Simulate writer 2 ALSO appending data (this should also succeed — appends are non-conflicting)
new_order2 = [(2002, "Dock", 1, 120.00, "pending")]
new_df2 = spark.createDataFrame(new_order2, ["order_id", "product", "quantity", "price", "status"])
new_df2.write.format("delta").mode("append").save(f"{base_dir}/orders_acid")
print("Writer 2 committed successfully (both appends are non-conflicting).")

print("\nFinal table after both concurrent appends:")
spark.sql("SELECT * FROM delta_orders ORDER BY order_id").show()

print("\nKEY INSIGHT: Delta uses optimistic concurrency — appends don't conflict,")
print("but if two writers try to update the SAME row, the second commit is rejected.")
print("This avoids expensive locking while maintaining correctness.")

```


### Key Takeaways
- Delta Lake guarantees **atomicity**: all operations in a transaction succeed or fail together.
- **Optimistic concurrency control** allows high-throughput concurrent writes without locking.
- Unlike raw Parquet/CSV, a failed Delta write leaves **no partial or corrupt data**.

### Self-Assessment Question

*Q: What happens if you have a `MERGE` statement that inserts 100 rows and updates 50 rows, and it fails halfway through?*

<details><summary>Click for answer</summary>
**A:** With Delta Lake, nothing is committed. All 100 inserts and 50 updates are rolled back. With raw Parquet, you could end up with partial files written, corrupting your downstream reads.
</details>


---
## Concept #2: Transaction Log (_delta_log) [Easy]

### What Problem It Solves

How does Delta know which files belong to which version of the table? Without a transaction log, there's no way to track which files were added or removed over time, no way to support time travel, and no way to ensure snapshot isolation for concurrent readers and writers.

The `_delta_log` directory is the **source of truth** for a Delta table. It contains ordered JSON files (and checkpoint Parquet files) that record every operation performed on the table.

### Real-World Use Case

A data engineering team needs to audit who modified a critical financial table and when. The transaction log provides a complete, immutable audit trail of every write operation.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 2: Transaction Log (_delta_log)")
print("=" * 60)

# Create a fresh Delta table and build up some history
table_path = f"{base_dir}/tlog_demo"

spark.sql(f"DROP TABLE IF EXISTS tlog_table")

# Create initial data
initial_data = [
    (1, "Alice", "Engineering", 95000),
    (2, "Bob", "Sales", 72000),
    (3, "Charlie", "Engineering", 105000),
]
df = spark.createDataFrame(initial_data, ["id", "name", "dept", "salary"])
df.write.format("delta").mode("overwrite").save(table_path)

# Register table
spark.sql(f"CREATE TABLE IF NOT EXISTS tlog_table USING DELTA LOCATION '{table_path}'")

# Perform multiple operations to build up log entries
spark.sql("INSERT INTO tlog_table VALUES (4, 'Diana', 'Marketing', 88000)")
spark.sql("INSERT INTO tlog_table VALUES (5, 'Eve', 'Engineering', 110000)")
spark.sql("UPDATE tlog_table SET salary = 98000 WHERE id = 1")
spark.sql("INSERT INTO tlog_table VALUES (6, 'Frank', 'Sales', 76000)")
spark.sql("DELETE FROM tlog_table WHERE id = 6")
spark.sql("INSERT INTO tlog_table VALUES (7, 'Grace', 'Marketing', 92000)")

print("Performed 6 operations on the table (creates 6 new versions beyond v0).")

```


#### Inspect the `_delta_log` Directory

Every Delta table has a `_delta_log` subdirectory containing JSON transaction log entries and, after enough operations, checkpoint Parquet files.

```python

# List the contents of the _delta_log directory
print("Contents of _delta_log directory:")
log_entries = dbutils.fs.ls(f"{table_path}/_delta_log")
for entry in log_entries:
    size_kb = entry.size / 1024 if entry.size > 0 else 0
    print(f"  {entry.name:<40} {size_kb:>8.2f} KB")

print(f"\nTotal log entries: {len(log_entries)}")

```


#### Read the Latest JSON Transaction Log Entry

Each JSON file contains a sequence of actions: `add`, `remove`, `metaData`, `commitInfo`, etc.

```python

# Read and display the most recent JSON log entry
import json

# Get the last .json file (most recent transaction)
json_files = sorted([e.name for e in log_entries if e.name.endswith(".json")])
latest_json = json_files[-1]
print(f"Reading latest transaction log entry: {latest_json}")
content = spark.read.text(f"{table_path}/_delta_log/{latest_json}").collect()
for row in content[:5]:  # Show first 5 lines
    parsed = json.loads(row.value)
    action_type = list(parsed.keys())[0]
    print(f"\nAction: {action_type}")
    print(json.dumps(parsed[action_type], indent=2)[:500])

```


#### View Transaction History with `DESCRIBE HISTORY`

```python

# DESCRIBE HISTORY shows the full transaction log in human-readable form
print("\nTransaction History (DESCRIBE HISTORY):")
spark.sql("DESCRIBE HISTORY tlog_table").select(
    "version", "operation", "operationMetrics", "timestamp"
).show(truncate=False)

```


#### Checkpoint Creation

After many operations (default: every 10 commits), Delta creates a **checkpoint** — a Parquet file that aggregates all previous JSON entries. This speeds up table state reconstruction by not requiring replay of the entire log.

```python

# Perform enough operations to trigger a checkpoint (10 more commits)
for i in range(10):
    spark.sql(f"INSERT INTO tlog_table VALUES ({100 + i}, 'TestUser{i}', 'QA', {50000 + i * 1000})")

print("Performed 10 more INSERTs to trigger checkpoint creation.")

# Re-list the _delta_log directory
updated_log_entries = dbutils.fs.ls(f"{table_path}/_delta_log")
print("\nUpdated _delta_log contents:")
for entry in updated_log_entries:
    size_kb = entry.size / 1024 if entry.size > 0 else 0
    print(f"  {entry.name:<40} {size_kb:>8.2f} KB")

has_checkpoint = any(e.name.endswith(".checkpoint.parquet") for e in updated_log_entries)
print(f"\nCheckpoint file present: {has_checkpoint}")
print("Checkpoints dramatically speed up state reconstruction by avoiding full log replay.")

```


### Key Takeaways
- The **`_delta_log` directory** is the single source of truth for a Delta table's state.
- JSON log files record every **add**, **remove**, and **metadata change** as ordered transactions.
- **Checkpoints** (Parquet files) are created every 10 commits by default, enabling fast snapshot reads without replaying the entire log.

### Self-Assessment Question

*Q: If you delete all `.json` files from the `_delta_log` but keep the `.checkpoint.parquet` file, can the table still be read?*

<details><summary>Click for answer</summary>
**A:** You can read up to the checkpoint version, but any versions AFTER the checkpoint will be lost because the JSON files containing those incremental changes are gone. Always treat the `_delta_log` directory as immutable — never manually delete entries!
</details>


---
## Concept #3: Time Travel & RESTORE [Easy]

### What Problem It Solves

Data engineers often need to query data "as it was" at a specific point in the past — for auditing, debugging bad writes, reproducing reports, or rolling back accidental changes. Without built-in versioning, you'd have to maintain costly snapshot copies of your data.

Delta Lake stores a complete version history, allowing you to query any past table version or restore the table to a previous state.

### Real-World Use Case

A scheduled job accidentally runs a `DELETE` that removes 2 million valid customer records. Instead of restoring from a backup, the team simply runs `RESTORE TABLE` to roll back to the version just before the bad job ran.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 3: Time Travel & RESTORE")
print("=" * 60)

# Create a table with versioned data
tt_path = f"{base_dir}/timetravel_demo"
spark.sql(f"DROP TABLE IF EXISTS timetravel_sales")

sales_init = [
    ("2024-01-01", "Product-A", 10, 150.00),
    ("2024-01-01", "Product-B", 5, 200.00),
    ("2024-01-02", "Product-A", 8, 160.00),
]
df = spark.createDataFrame(sales_init, ["sale_date", "product", "quantity", "amount"])
df.write.format("delta").mode("overwrite").save(tt_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS timetravel_sales USING DELTA LOCATION '{tt_path}'")
print("Version 0 created (3 records).")

# Version 1: Insert new data
spark.sql("INSERT INTO timetravel_sales VALUES ('2024-01-03', 'Product-C', 12, 300.00)")

# Version 2: Update prices
spark.sql("UPDATE timetravel_sales SET amount = amount * 1.1 WHERE product = 'Product-A'")

# Version 3: Delete a product
spark.sql("DELETE FROM timetravel_sales WHERE product = 'Product-B'")

# Version 4: More inserts
spark.sql("INSERT INTO timetravel_sales VALUES ('2024-01-04', 'Product-D', 3, 450.00)")

print("Versions 1-4 created (inserts, updates, deletes).")

# View history
print("\nFull history:")
spark.sql("DESCRIBE HISTORY timetravel_sales").select(
    "version", "operation", "operationMetrics", "timestamp"
).show(truncate=False)

```


#### Query with `VERSION AS OF` and `TIMESTAMP AS OF`

```python

# Query current version
print("Current table (latest version):")
spark.sql("SELECT * FROM timetravel_sales ORDER BY sale_date, product").show()

# Query Version 0 (original data)
print("Version 0 (original create):")
spark.sql("SELECT * FROM timetravel_sales VERSION AS OF 0 ORDER BY sale_date, product").show()

# Query Version 2 (after price update, before delete)
print("Version 2 (after price update, before delete of Product-B):")
spark.sql("SELECT * FROM timetravel_sales VERSION AS OF 2 ORDER BY sale_date, product").show()

# Count rows at each version
print("Row counts across versions:")
for v in range(5):
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM timetravel_sales VERSION AS OF {v}").collect()[0]["cnt"]
    print(f"  Version {v}: {cnt} rows")

```


#### Demonstrate `RESTORE TABLE` to Roll Back

```python

# Before restore — check current state
print("BEFORE RESTORE (latest version):")
spark.sql("SELECT * FROM timetravel_sales ORDER BY sale_date, product").show()

# Restore to version 2
spark.sql("RESTORE TABLE timetravel_sales TO VERSION AS OF 2")
print("\nRESTORE to version 2 completed.")

print("AFTER RESTORE (should match version 2):")
spark.sql("SELECT * FROM timetravel_sales ORDER BY sale_date, product").show()

# Verify history now shows RESTORE operation
print("\nHistory after RESTORE:")
spark.sql("DESCRIBE HISTORY timetravel_sales").select(
    "version", "operation", "operationMetrics", "timestamp"
).show(truncate=False)

```


#### Time Travel with `TIMESTAMP AS OF`

```python

# Get a timestamp from the history to use for time travel
history_df = spark.sql("DESCRIBE HISTORY timetravel_sales")
version_1_ts = history_df.filter("version = 1").select("timestamp").collect()[0][0]
print(f"Querying table as of timestamp: {version_1_ts}")

spark.sql(f"""
    SELECT * FROM timetravel_sales
    TIMESTAMP AS OF '{version_1_ts}'
    ORDER BY sale_date, product
""").show()

```


#### Retention Period and VACUUM Interaction

Delta's default retention for time travel is **7 days** (controlled by `delta.logRetentionDuration`). After that, VACUUM can remove old files, and time travel beyond the retention window will fail. We'll explore this further in Concept #7.

```python

# Check the table's log retention setting
props = spark.sql("SHOW TBLPROPERTIES timetravel_sales").filter("key like '%retention%' OR key like '%deleted%'")
print("Relevant table properties:")
props.show(truncate=False)

print("\nDefault log retention: 30 days (delta.logRetentionDuration)")
print("Default file retention: 7 days (delta.deletedFileRetentionDuration)")
print("NOTE: In Community Edition with single node, VACUUM still works but won't have distributed cleanup.")

```


### Key Takeaways
- `VERSION AS OF N` lets you query **any past version** of a Delta table without maintaining snapshots.
- `RESTORE TABLE` rolls the table back to a previous version, creating a **new version** that mirrors the old state.
- Time travel is limited by the **retention period** (default 7 days for files); VACUUM removes expired files permanently.

### Self-Assessment Question

*Q: You run `RESTORE TABLE orders TO VERSION AS OF 5`. Is version 6 lost forever?*

<details><summary>Click for answer</summary>
**A:** No! `RESTORE` creates a **new version** (e.g., version 7) that copies the state from version 5. Version 6 still exists in the transaction log and you can still time-travel to it — unless VACUUM removes the underlying data files after the retention period expires.
</details>


---
## Concept #4: Schema Enforcement vs. Evolution [Medium]

### What Problem It Solves

Traditional data lakes (Parquet, CSV) have **no schema enforcement**: you can accidentally write malformed data, wrong column types, or extra/missing columns. This creates silent data quality issues that are discovered only when downstream jobs fail.

Delta Lake **enforces schema** on write by default, preventing bad data from entering the table. At the same time, Delta provides **schema evolution** (`mergeSchema`) so you can explicitly add new columns without breaking existing pipelines.

### Real-World Use Case

A new field (`customer_tier`) is added to the source system. The engineering team enables `mergeSchema` to let the new column be added automatically to the Delta table, avoiding pipeline failures while preserving all existing data.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 4: Schema Enforcement vs. Evolution")
print("=" * 60)

# Create a table with a strict schema
schema_path = f"{base_dir}/schema_demo"
spark.sql(f"DROP TABLE IF EXISTS schema_patients")

# Initial data with a known schema
patients = [
    (1, "John Doe", 35, "A+"),
    (2, "Jane Smith", 28, "O-"),
    (3, "Bob Johnson", 42, "B+"),
]
df_patients = spark.createDataFrame(patients, ["id", "name", "age", "blood_type"])
df_patients.write.format("delta").mode("overwrite").save(schema_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS schema_patients USING DELTA LOCATION '{schema_path}'")
print("Initial table created with schema: id INT, name STRING, age INT, blood_type STRING")

```


#### Schema Enforcement: Rejecting Bad Data

```python

print("\n=== Schema Enforcement Demo ===")

# Attempt 1: Write data with a MISMATCHED column type (string where INT expected)
try:
    bad_type_data = [(4, "Bad Guy", "thirty-five", "AB+")]
    bad_type_df = spark.createDataFrame(bad_type_data, ["id", "name", "age", "blood_type"])
    bad_type_df.write.format("delta").mode("append").save(schema_path)
    print("Write succeeded (unexpected — schema enforcement failed)")
except Exception as e:
    error_msg = str(e)
    print(f"Schema enforcement BLOCKED write: type mismatch detected")
    print(f"Error snippet: {error_msg[:200]}")

print("\n")

# Attempt 2: Write data with an EXTRA column (no mergeSchema)
try:
    extra_col_data = [(5, "Extra Person", 30, "A-", "extradata")]
    extra_col_df = spark.createDataFrame(extra_col_data, ["id", "name", "age", "blood_type", "extra"])
    extra_col_df.write.format("delta").mode("append").save(schema_path)
    print("Write succeeded (unexpected)")
except Exception as e:
    error_msg = str(e)
    print(f"Schema enforcement BLOCKED write: extra column detected")
    print(f"Error snippet: {error_msg[:200]}")

print("\n")

# Attempt 3: Write data with a MISSING column
try:
    missing_col_data = [(6, "Missing Data", 22)]
    missing_col_df = spark.createDataFrame(missing_col_data, ["id", "name", "age"])
    missing_col_df.write.format("delta").mode("append").save(schema_path)
    print("Write succeeded (unexpected)")
except Exception as e:
    error_msg = str(e)
    print(f"Schema enforcement BLOCKED write: missing column detected")
    print(f"Error snippet: {error_msg[:200]}")

print("\nTable is unchanged:")
spark.sql("SELECT * FROM schema_patients").show()

```


#### Schema Evolution: `mergeSchema`

```python

print("\n=== Schema Evolution Demo ===")

# Now add a new column using mergeSchema
new_data_with_email = [
    (7, "Alice Wonder", 29, "O+", "alice@example.com"),
    (8, "Charlie Brown", 33, "B-", "charlie@example.com"),
]
new_df = spark.createDataFrame(new_data_with_email, ["id", "name", "age", "blood_type", "email"])

new_df.write.format("delta").mode("append").option("mergeSchema", "true").save(schema_path)

print("Data with NEW column 'email' written successfully using mergeSchema!")

# Show the evolved table
print("\nTable now has the 'email' column:")
spark.sql("SELECT * FROM schema_patients ORDER BY id").show()

# Check the schema
print("\nUpdated schema:")
spark.sql("DESCRIBE schema_patients").show(truncate=False)

```


#### `overwriteSchema` — Full Schema Replacement

```python

# overwriteSchema replaces the entire schema when you want a structural change
replacement_data = [
    (1, "John Doe", 35, "A+", "john@example.com", "VIP"),
    (2, "Jane Smith", 28, "O-", "jane@example.com", "Regular"),
]
replacement_df = spark.createDataFrame(
    replacement_data, ["id", "name", "age", "blood_type", "email", "tier"]
)

replacement_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(schema_path)

print("overwriteSchema applied — data AND schema replaced.")
print("Note: This REPLACES all data, unlike mergeSchema which is non-destructive.")
spark.sql("SELECT * FROM schema_patients").show()

spark.sql("DESCRIBE schema_patients").show(truncate=False)

```


#### `autoMerge` in Writes

You can set `spark.databricks.delta.schema.autoMerge.enabled = true` at the SparkSession level to make schema evolution opt-out instead of opt-in.

```python

# Demonstrate autoMerge
auto_path = f"{base_dir}/schema_auto_demo"
spark.sql(f"DROP TABLE IF EXISTS schema_auto")

base_df = spark.createDataFrame([(1, "Base")], ["id", "name"])
base_df.write.format("delta").mode("overwrite").save(auto_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS schema_auto USING DELTA LOCATION '{auto_path}'")
print("Base table created with columns: id, name")

# Set autoMerge at session level
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

expanded_df = spark.createDataFrame([(2, "Expanded", "NewCol")], ["id", "name", "new_column"])
expanded_df.write.format("delta").mode("append").save(auto_path)
print("Appended with new column 'new_column' — autoMerge accepted it!")

spark.sql("SELECT * FROM schema_auto").show()

# Reset the config
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

```


### Key Takeaways
- **Schema enforcement** prevents bad data (wrong types, extra/missing columns) from entering your Delta table — a critical data quality guard.
- **`mergeSchema`** allows intentional schema evolution by adding new columns without losing data.
- **`overwriteSchema`** replaces the entire schema (and data) — use with caution.

### Self-Assessment Question

*Q: Your upstream source adds a new field `loyalty_points`. What's the safest way to add this column to your existing Delta table without breaking downstream consumers?*

<details><summary>Click for answer</summary>
**A:** Use `mergeSchema` (or set `autoMerge`). This adds the new column with NULL values for existing rows. Downstream consumers that don't reference the new column are unaffected. `overwriteSchema` would be dangerous because it replaces ALL data.
</details>


---
## Concept #5: Liquid Clustering [Medium]

### What Problem It Solves

Traditional data partitioning requires you to choose a partition column upfront (e.g., `date`). If you choose wrong (too many small partitions, or data skew), you get terrible performance with no easy fix. Z-Ordering helps with multi-dimensional clustering but requires manual OPTIMIZE commands.

**Liquid Clustering** is Delta's next-generation clustering technique that replaces both partitioning and Z-Ordering. It incrementally clusters data as it's written and dynamically adjusts to changing data patterns — no manual maintenance.

**IMPORTANT NOTE FOR COMMUNITY EDITION:** Liquid Clustering requires `delta.minReaderVersion >= 3` and `delta.minWriterVersion >= 7`, which may not be available in all Community Edition deployments. If the syntax fails, this section explains the concept and shows traditional Z-Ordering as the closest equivalent.

### Real-World Use Case

An IoT sensor table receives 500M rows/hour with timestamps, device IDs, and sensor types. Queries often filter by `sensor_type` AND `device_id`. Partitioning by either alone causes skew. Liquid Clustering clusters on both columns automatically, providing efficient file skipping for all query patterns.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 5: Liquid Clustering")
print("=" * 60)

# Create a large synthetic dataset to demonstrate clustering benefits
import random
import datetime

print("Generating synthetic IoT sensor data...")

sensor_types = ["temperature", "humidity", "pressure", "light", "motion"]
device_ids = [f"device-{i:04d}" for i in range(1, 201)]
regions = ["us-east", "us-west", "eu-west", "ap-south"]

data = []
base_date = datetime.date(2024, 1, 1)
for i in range(50000):
    ts = datetime.datetime(2024, 1, 1, 0, 0, 0) + datetime.timedelta(
        minutes=random.randint(0, 525600)
    )
    data.append((
        ts,
        random.choice(sensor_types),
        random.choice(device_ids),
        random.choice(regions),
        round(random.uniform(0, 100), 2),
    ))

sensor_df = spark.createDataFrame(
    data, ["timestamp", "sensor_type", "device_id", "region", "reading"]
)

print(f"Generated {sensor_df.count()} sensor readings.")
sensor_df.show(5, truncate=False)

```


#### Traditional Z-Ordering Approach (Available in Community Edition)

```python

# Write with traditional Z-Ordering
cluster_path_zorder = f"{base_dir}/cluster_traditional"

# Write data normally first
sensor_df.write.format("delta").mode("overwrite").save(cluster_path_zorder)

spark.sql(f"CREATE TABLE IF NOT EXISTS cluster_traditional USING DELTA LOCATION '{cluster_path_zorder}'")

print("Before Z-Order OPTIMIZE:")
detail_before = spark.sql("DESCRIBE DETAIL cluster_traditional").select(
    "numFiles", "sizeInBytes", "minReaderVersion", "minWriterVersion"
)
detail_before.show(truncate=False)

# Apply Z-Order optimization on frequently queried columns
spark.sql(f"OPTIMIZE cluster_traditional ZORDER BY (sensor_type, device_id)")

print("\nAfter Z-Order OPTIMIZE:")
detail_after = spark.sql("DESCRIBE DETAIL cluster_traditional").select(
    "numFiles", "sizeInBytes", "minReaderVersion", "minWriterVersion"
)
detail_after.show(truncate=False)

```


#### Liquid Clustering Syntax (if available in your environment)

The `CLUSTER BY` syntax creates a table with Liquid Clustering enabled. If your Community Edition supports it, this cell will execute. If not, it will fail gracefully.

```python

cluster_path_liquid = f"{base_dir}/cluster_liquid"
spark.sql(f"DROP TABLE IF EXISTS cluster_liquid")

try:
    spark.sql(f"""
        CREATE TABLE cluster_liquid
        CLUSTER BY (sensor_type, device_id)
        USING DELTA
        LOCATION '{cluster_path_liquid}'
    """)

    # Insert data
    sensor_df.write.format("delta").mode("append").save(cluster_path_liquid)

    print("Liquid Clustering table created successfully!")
    print("CLUSTER BY (sensor_type, device_id) enables incremental clustering.")

    spark.sql("DESCRIBE DETAIL cluster_liquid").select(
        "numFiles", "sizeInBytes", "clusteringColumns"
    ).show(truncate=False)

except Exception as e:
    print(f"Liquid Clustering not available in this environment: {str(e)[:200]}")
    print("\nLiquid Clustering requires:")
    print("  - delta.minReaderVersion >= 3")
    print("  - delta.minWriterVersion >= 7")
    print("  - Databricks Runtime 13.3 LTS or higher")
    print("\nFalling back to traditional Z-Ordering (already demonstrated above).")

```


#### Comparing Query Performance (File Skipping)

```python

# Demonstrate file skipping with Z-Ordering
# Without optimization — scan all files
# With Z-Order on sensor_type — skip irrelevant files

print("Query: Filter by sensor_type='temperature' AND device_id='device-0050'")

# Measure on Z-Ordered table
import time

start = time.time()
result = spark.sql(f"""
    SELECT COUNT(*) FROM cluster_traditional
    WHERE sensor_type = 'temperature' AND device_id = 'device-0050'
""").collect()[0][0]
elapsed = time.time() - start
print(f"Z-Ordered table — {result} rows found in {elapsed:.3f}s")

# Let's also create an un-optimized table for comparison
unopt_path = f"{base_dir}/cluster_unoptimized"
sensor_df.write.format("delta").mode("overwrite").save(unopt_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS cluster_unoptimized USING DELTA LOCATION '{unopt_path}'")

start = time.time()
result2 = spark.sql(f"""
    SELECT COUNT(*) FROM cluster_unoptimized
    WHERE sensor_type = 'temperature' AND device_id = 'device-0050'
""").collect()[0][0]
elapsed2 = time.time() - start
print(f"Unoptimized table — {result2} rows found in {elapsed2:.3f}s")

print("\nKEY INSIGHT: Z-Ordering (and Liquid Clustering) co-locates related data in the same files,")
print("enabling Delta to skip irrelevant files during reads — dramatically reducing I/O.")

```


### Key Takeaways
- **Liquid Clustering** replaces traditional partitioning and Z-Ordering with incremental, maintenance-free clustering.
- Use `CLUSTER BY (col1, col2)` on table creation (requires compatible Runtime version).
- In environments without Liquid Clustering, **Z-Ordering via `OPTIMIZE ZORDER BY`** provides similar multi-dimensional clustering benefits.

### Self-Assessment Question

*Q: When would you use Liquid Clustering over traditional `PARTITIONED BY`?*

<details><summary>Click for answer</summary>
**A:** Use Liquid Clustering when you have high-cardinality columns (e.g., device IDs, user IDs) that would create too many small partitions, or when query patterns filter by multiple different columns. Liquid Clustering handles multi-dimensional clustering automatically without manual partition tuning.
</details>


---
## Concept #6: OPTIMIZE & File Compaction [Medium]

### What Problem It Solves

Streaming and frequent small-batch writes create many **small files**. This "small file problem" degrades read performance because the query engine must open, read metadata from, and close thousands of tiny files.

`OPTIMIZE` consolidates small files into larger ones (~1 GB by default), dramatically improving read performance. Combined with `ZORDER BY`, it also clusters related data together for file skipping.

### Real-World Use Case

A Kafka streaming pipeline writes 50MB micro-batches every minute into a Delta table. After 24 hours, there are 1,440 small files. Running `OPTIMIZE` hourly consolidates them into larger files, keeping query performance predictable.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 6: OPTIMIZE & File Compaction")
print("=" * 60)

# Simulate many small appends to create small files
opt_path = f"{base_dir}/optimize_demo"
spark.sql(f"DROP TABLE IF EXISTS optimize_sales")

# Create initial data
schema_opt = spark.createDataFrame([], "id INT, customer STRING, amount DOUBLE, region STRING")
schema_opt.write.format("delta").mode("overwrite").save(opt_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS optimize_sales USING DELTA LOCATION '{opt_path}'")

print("Simulating 50 small-batch writes (streaming/micro-batch scenario)...")

for batch in range(50):
    batch_data = [
        (batch * 10 + i,
         f"Customer-{random.randint(1, 100)}",
         round(random.uniform(10, 500), 2),
         random.choice(["North", "South", "East", "West"]))
        for i in range(10)
    ]
    batch_df = spark.createDataFrame(batch_data, ["id", "customer", "amount", "region"])
    batch_df.write.format("delta").mode("append").save(opt_path)

total_rows = spark.sql("SELECT COUNT(*) FROM optimize_sales").collect()[0][0]
print(f"Total rows written: {total_rows}")

```


#### File Count Before OPTIMIZE

```python

print("BEFORE OPTIMIZE — DESCRIBE DETAIL:")
detail_before_opt = spark.sql("DESCRIBE DETAIL optimize_sales").select(
    "numFiles", "sizeInBytes", "numRecords"
)
detail_before_opt.show(truncate=False)

num_files_before = detail_before_opt.collect()[0]["numFiles"]
print(f"\nSmall files BEFORE optimize: {num_files_before}")

# Also list the actual files
print("\nActual files in the table directory:")
all_files = dbutils.fs.ls(opt_path + "/")
parquet_files = [f.name for f in all_files if f.name.endswith(".parquet") or f.name.endswith(".snappy.parquet")]
print(f"Parquet files found: {len(parquet_files)}")
for f in parquet_files[:10]:
    print(f"  {f}")
if len(parquet_files) > 10:
    print(f"  ... and {len(parquet_files) - 10} more")

```


#### Run OPTIMIZE

```python

# Set a smaller target file size for demonstration (default is 1 GB)
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 134217728)  # 128 MB
spark.conf.set("spark.databricks.delta.optimize.minFileSize", 33554432)   # 32 MB

print("Running OPTIMIZE to compact small files...\n")
optimize_result = spark.sql("OPTIMIZE optimize_sales")
optimize_result.show(truncate=False)

```


#### File Count After OPTIMIZE

```python

print("AFTER OPTIMIZE — DESCRIBE DETAIL:")
detail_after_opt = spark.sql("DESCRIBE DETAIL optimize_sales").select(
    "numFiles", "sizeInBytes", "numRecords"
)
detail_after_opt.show(truncate=False)

num_files_after = detail_after_opt.collect()[0]["numFiles"]
files_reduced = num_files_before - num_files_after
print(f"\nSmall files AFTER optimize: {num_files_after}")
print(f"Files reduced by: {files_reduced} ({(files_reduced/num_files_before*100):.1f}% reduction)")

# List remaining files
all_files_after = dbutils.fs.ls(opt_path + "/")
parquet_files_after = [f.name for f in all_files_after if f.name.endswith(".parquet")]
print(f"\nRemaining parquet files: {len(parquet_files_after)}")
print("The small files have been consolidated into fewer, larger files.")

```


#### OPTIMIZE with ZORDER BY

```python

# Z-Ordering clusters related data together in the same files
print("Running OPTIMIZE with ZORDER BY region...")
opt_zorder_result = spark.sql("OPTIMIZE optimize_sales ZORDER BY (region)")
opt_zorder_result.show(truncate=False)

detail_with_zorder = spark.sql("DESCRIBE DETAIL optimize_sales").select(
    "numFiles", "sizeInBytes", "numRecords"
)
detail_with_zorder.show(truncate=False)

```


#### Read Performance Impact

After OPTIMIZE, queries that filter on the Z-Ordered column benefit from file skipping — only relevant files are read, reducing scan overhead significantly.

```python

# Compare read performance: scan all files vs filter on Z-Ordered column
print("Comparing scan performance...\n")

# Full table scan
import time as time_mod
t0 = time_mod.time()
full_count = spark.sql("SELECT COUNT(*) FROM optimize_sales").collect()[0][0]
t1 = time_mod.time()
print(f"Full scan: {t1 - t0:.4f}s ({full_count} rows)")

# Filtered scan on Z-Ordered column
t0 = time_mod.time()
filtered_count = spark.sql("SELECT COUNT(*) FROM optimize_sales WHERE region = 'North'").collect()[0][0]
t1 = time_mod.time()
print(f"Filtered scan (region='North'): {t1 - t0:.4f}s ({filtered_count} rows)")

print("\nZ-Ordering enables Delta to skip files that don't contain 'North' region data.")

```


### Key Takeaways
- `OPTIMIZE` consolidates small files into **larger files** (target ~1 GB), reducing metadata overhead and improving read performance.
- `OPTIMIZE ... ZORDER BY (col)` additionally **co-locates related data** in the same files, enabling file skipping.
- In production, run OPTIMIZE **periodically** (e.g., hourly or daily) to keep the file count manageable — it's idempotent and safe to run on live tables.

### Self-Assessment Question

*Q: Your table has 10,000 small files after a day of streaming writes. Running `OPTIMIZE` consolidates them to 10 files. What happens to the old small files?*

<details><summary>Click for answer</summary>
**A:** The old small files are marked as "removed" in the transaction log. They are NOT deleted immediately — they remain until VACUUM runs after the retention period (default 7 days). This is how Delta supports time travel even after OPTIMIZE.
</details>


---
## Concept #7: VACUUM & Storage Lifecycle [Medium]

### What Problem It Solves

Over time, Delta tables accumulate old data files that are no longer referenced by the latest table version (from OPTIMIZE, DELETE, UPDATE operations). These orphaned files waste storage and cost money.

`VACUUM` removes unreferenced files older than the retention period (default 7 days). This is the Delta equivalent of garbage collection — essential for managing storage costs.

### Real-World Use Case

A data engineering team runs weekly `OPTIMIZE` on a 10 TB sales table. Without VACUUM, the unreferenced files from each week's OPTIMIZE would accumulate, doubling or tripling storage costs. With `VACUUM`, only the current and recent versions' files are retained.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 7: VACUUM & Storage Lifecycle")
print("=" * 60)

# Create a table and perform operations that create orphaned files
vac_path = f"{base_dir}/vacuum_demo"
spark.sql(f"DROP TABLE IF EXISTS vacuum_sales")

vac_data = [(i, f"Product-{i % 10}", random.randint(1, 100), round(random.uniform(10, 500), 2))
            for i in range(1000)]
vac_df = spark.createDataFrame(vac_data, ["id", "product", "qty", "amount"])
vac_df.write.format("delta").mode("overwrite").save(vac_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS vacuum_sales USING DELTA LOCATION '{vac_path}'")
print(f"Initial table: {spark.sql('SELECT COUNT(*) FROM vacuum_sales').collect()[0][0]} rows")

# Perform operations that create old file versions
spark.sql("UPDATE vacuum_sales SET amount = amount * 1.1 WHERE id > 500")
spark.sql("DELETE FROM vacuum_sales WHERE id < 200")
spark.sql("OPTIMIZE vacuum_sales")

print("\nPerformed UPDATE, DELETE, and OPTIMIZE — these create unreferenced files.")

```


#### `DESCRIBE DETAIL` — See Storage Statistics

```python

# DESCRIBE DETAIL shows file-level stats
print("DESCRIBE DETAIL before VACUUM:")
detail_vac = spark.sql("DESCRIBE DETAIL vacuum_sales").select(
    "numFiles", "sizeInBytes", "numRecords", "lastModified"
)
detail_vac.show(truncate=False)

# List actual files
print("\nFiles in table directory:")
vac_files = dbutils.fs.ls(vac_path + "/")
data_files = [f for f in vac_files if f.name.endswith(".parquet")]
for f in data_files[:10]:
    print(f"  {f.name:<50} {f.size:>10} bytes")
if len(data_files) > 10:
    print(f"  ... and {len(data_files) - 10} more files")
print(f"Total data files in directory: {len(data_files)}")

```


#### Check Retention Period and Version History

```python

# Show current retention settings
print("Retention-related properties:")
retention_props = spark.sql("SHOW TBLPROPERTIES vacuum_sales").filter(
    "key like '%retention%' OR key like '%logRetention%'"
)
retention_props.show(truncate=False)

print("\nCurrent history (number of versions):")
history_vac = spark.sql("DESCRIBE HISTORY vacuum_sales")
print(f"Total versions: {history_vac.count()}")
history_vac.select("version", "operation", "timestamp").show(truncate=False)

```


#### Run VACUUM (with Dry Run)

`VACUUM` with `DRY RUN` shows which files would be removed without actually deleting them. Always run a dry run first in production!

```python

# Dry run first — safe preview
print("VACUUM DRY RUN (preview mode — no files deleted):")
try:
    spark.sql("VACUUM vacuum_sales DRY RUN").show(truncate=False)
    print("\nDry run completed. No files were actually deleted.")
except Exception as e:
    print(f"DRY RUN not supported in this version: {str(e)[:100]}")

```


#### Run VACUUM (Retain 0 Hours for Demo)

**WARNING:** Setting retention to 0 hours disables time travel to previous versions. This is for demonstration only. In production, use the default 7-day retention.

```python

# Count files before VACUUM
files_before = len(dbutils.fs.ls(vac_path + "/"))
print(f"Total files/directories before VACUUM: {files_before}")

# Run VACUUM with RETAIN 0 HOURS for demo (in production, keep the default 168 hours / 7 days)
print("\nRunning VACUUM with RETAIN 0 HOURS (for demo only)...")
spark.sql("VACUUM vacuum_sales RETAIN 0 HOURS")

# Count files after VACUUM
files_after = len(dbutils.fs.ls(vac_path + "/"))
print(f"Total files/directories after VACUUM: {files_after}")
print(f"Files removed: {files_before - files_after}")

```


#### Time Travel AFTER VACUUM — What Happens?

```python

# Try to time travel to an older version AFTER VACUUM
print("Attempting time travel after VACUUM (0 hour retention)...")

# Get old version numbers
old_versions = spark.sql("DESCRIBE HISTORY vacuum_sales").select("version").collect()
if len(old_versions) > 1:
    try:
        old_version = old_versions[1][0]  # Version 1
        spark.sql(f"SELECT COUNT(*) FROM vacuum_sales VERSION AS OF {old_version}").show()
        print(f"Time travel to version {old_version} succeeded (files still present).")
    except Exception as e:
        print(f"Time travel to version {old_version} FAILED: {str(e)[:200]}")
        print("The data files for that version have been physically removed by VACUUM.")
        print("This is why you should keep a reasonable retention period in production!")

# Current table is still intact
print(f"\nCurrent table data is intact: {spark.sql('SELECT COUNT(*) FROM vacuum_sales').collect()[0][0]} rows")

```


### Key Takeaways
- `VACUUM` removes old data files that are no longer referenced within the **retention period** (default 7 days).
- Always run `DRY RUN` first to preview which files will be deleted.
- After VACUUM, **time travel** to versions older than the retention period will fail. Plan your retention window based on audit/compliance needs.

### Self-Assessment Question

*Q: You set `delta.deletedFileRetentionDuration = 1 hour` and run VACUUM. Two hours later, can you query the table as it was 30 minutes ago?*

<details><summary>Click for answer</summary>
**A:** No. VACUUM would have removed files older than 1 hour. After 2 hours, the 30-minutes-ago version is within the retention window, but the version from 2.5 hours ago would be gone. Always set retention to cover your maximum time-travel needs plus a safety margin.
</details>


---
## Concept #8: Change Data Feed (CDF) [Medium]

### What Problem It Solves

How do you efficiently capture row-level changes (inserts, updates, deletes) from a Delta table for downstream incremental processing? Without CDF, you'd have to do expensive full-table comparisons or rely on unreliable "last modified" columns.

**Change Data Feed (CDF)** records every row-level change made to a Delta table, including the type of change (`insert`, `update_preimage`, `update_postimage`, `delete`) and the version at which it occurred. This unlocks efficient CDC (Change Data Capture) pipelines.

### Real-World Use Case

A data warehouse team needs to sync a Delta table to a downstream analytics database (e.g., PostgreSQL) in near-real-time. Instead of re-syncing the entire table, they read only the changes since the last sync via CDF.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 8: Change Data Feed (CDF)")
print("=" * 60)

# Create a table with CDF enabled
cdf_path = f"{base_dir}/cdf_demo"
spark.sql(f"DROP TABLE IF EXISTS cdf_products")

# Enable CDF on the table
cdf_init_data = [
    (1, "Widget A", "Gadgets", 9.99, 100),
    (2, "Widget B", "Gadgets", 14.99, 50),
    (3, "Bolt M6", "Hardware", 0.99, 1000),
    (4, "Nut M6", "Hardware", 0.49, 1000),
    (5, "Spring XL", "Hardware", 2.99, 200),
]
cdf_df = spark.createDataFrame(cdf_init_data, ["product_id", "name", "category", "price", "stock"])

cdf_df.write.format("delta").mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save(cdf_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS cdf_products USING DELTA LOCATION '{cdf_path}'")

print("CDF table created with change data feed ENABLED.")
print(f"Initial data: {spark.sql('SELECT COUNT(*) FROM cdf_products').collect()[0][0]} rows")
spark.sql("SELECT * FROM cdf_products").show()

```


#### Perform Multiple Changes to Generate CDF Entries

```python

# Version 1: INSERT new products
spark.sql("""
    INSERT INTO cdf_products VALUES
        (6, 'Widget C', 'Gadgets', 19.99, 75),
        (7, 'Washer M6', 'Hardware', 0.25, 2000)
""")

# Version 2: UPDATE prices (increase by 10%)
spark.sql("UPDATE cdf_products SET price = price * 1.10 WHERE category = 'Gadgets'")

# Version 3: UPDATE stock levels
spark.sql("UPDATE cdf_products SET stock = stock + 50 WHERE product_id IN (1, 2)")

# Version 4: DELETE a product
spark.sql("DELETE FROM cdf_products WHERE product_id = 3")

# Show current table state
print("Current table state (after all changes):")
spark.sql("SELECT * FROM cdf_products ORDER BY product_id").show()

# Show history
print("\nTable history:")
spark.sql("DESCRIBE HISTORY cdf_products").select("version", "operation", "operationMetrics", "timestamp").show(truncate=False)

```


#### Read Change Data Feed with `table_changes()`

```python

print("\n=== Reading Change Data Feed ===")

# Read all changes from version 0 to the latest
print("All CDF changes (version 0 to latest):")
cdf_all = spark.sql("SELECT * FROM table_changes('cdf_products', 0)")
cdf_all.select("_change_type", "_commit_version", "_commit_timestamp", "product_id", "name", "price", "stock").show(truncate=False)

# Read changes from a specific version range
print("\nCDF changes from version 1 to version 3:")
cdf_range = spark.sql("SELECT * FROM table_changes('cdf_products', 1, 3)")
cdf_range.select("_change_type", "_commit_version", "product_id", "name", "price", "stock").show(truncate=False)

```


#### Understanding `_change_type` Values

```python

# Show distribution of change types
print("Change type distribution:")
spark.sql("""
    SELECT _change_type, COUNT(*) AS change_count
    FROM table_changes('cdf_products', 0)
    GROUP BY _change_type
    ORDER BY _change_type
""").show()

# Demonstrate the preimage/postimage for UPDATEs
print("\nUPDATE change details (update_preimage + update_postimage):")
cdf_updates = spark.sql("""
    SELECT _change_type, _commit_version, product_id, name, price, stock
    FROM table_changes('cdf_products', 0)
    WHERE _change_type LIKE 'update_%'
    ORDER BY _commit_version, product_id, _change_type
""")
cdf_updates.show(truncate=False)

```


#### Incremental Processing Pattern with CDF

```python

print("\n=== Incremental Processing with CDF ===")

# Simulate an incremental pipeline that reads only new changes since last processed version
processed_version = 2  # We last processed up to version 2

print(f"Last processed version: {processed_version}")
print(f"Reading changes from version {processed_version} to latest...")

new_changes = spark.sql(f"""
    SELECT * FROM table_changes('cdf_products', {processed_version})
""")

print(f"New changes to process: {new_changes.count()} rows")

# Typical incremental processing: apply changes to downstream system
# - INSERT records → insert into target
# - update_preimage + update_postimage → update target record
# - DELETE records → delete from target

# Collect by change type
for change_type in ["insert", "update_preimage", "update_postimage", "delete"]:
    cnt = new_changes.filter(f"_change_type = '{change_type}'").count()
    if cnt > 0:
        print(f"  {change_type}: {cnt} rows")

# Update our "processed" tracker
latest_version = spark.sql("SELECT MAX(version) AS max_ver FROM (DESCRIBE HISTORY cdf_products)").collect()[0][0]
print(f"\nUpdated processed version to: {latest_version}")
print("Next incremental run will read from version", latest_version)

```


### Key Takeaways
- CDF records every row-level change with `_change_type`, `_commit_version`, and `_commit_timestamp`.
- Use `table_changes('table_name', start_version, end_version)` to query changes efficiently for incremental processing.
- UPDATE operations produce **both** `update_preimage` (old values) and `update_postimage` (new values) — essential for accurate CDC replication.

### Self-Assessment Question

*Q: You enable CDF on an existing table. Can you query changes that happened BEFORE CDF was enabled?*

<details><summary>Click for answer</summary>
**A:** No. CDF only captures changes from the version where it was enabled and onwards. If you need change data for historical versions, you'd need to use time travel to compare versions manually, which is possible but less efficient.
</details>


---
## Concept #9: Deletion Vectors [Medium]

### What Problem It Solves

In traditional Delta Lake, a `DELETE` operation physically rewrites all files containing matching rows — a costly, I/O-intensive operation. For a 1 TB table, deleting 10 rows could require rewriting hundreds of GB of data just to exclude those rows.

**Deletion Vectors** (DVs) solve this by using **soft-delete markers** stored in separate, small binary files. When a DELETE runs, instead of rewriting data files, Delta records which rows to skip in a deletion vector file. This makes DELETE operations near-instant, regardless of table size.

**NOTE FOR COMMUNITY EDITION:** Deletion Vectors require `delta.minReaderVersion >= 3` and `delta.minWriterVersion >= 7`. If these features aren't available in your Community Edition deployment, this section demonstrates the concept and shows the performance difference by simulating the before/after behavior.

### Real-World Use Case

A GDPR compliance job needs to delete specific user records from a 5 TB events table. Without deletion vectors, each DELETE would take hours and cost hundreds of dollars in compute. With deletion vectors, the DELETE completes in seconds and the file rewrite cost is avoided — the soft-deleted rows are physically removed later during OPTIMIZE.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 9: Deletion Vectors")
print("=" * 60)

# First, demonstrate the COST of DELETE without deletion vectors (traditional behavior)
# by running a DELETE and observing how files are rewritten

dv_path_traditional = f"{base_dir}/dv_traditional"
spark.sql(f"DROP TABLE IF EXISTS dv_traditional")

# Create moderate-size data
dv_data = [(i, f"User-{i%100}", f"Event-{i%5}", random.randint(1, 10000))
           for i in range(5000)]
dv_df = spark.createDataFrame(dv_data, ["event_id", "user_id", "event_type", "value"])
dv_df.write.format("delta").mode("overwrite").save(dv_path_traditional)

spark.sql(f"CREATE TABLE IF NOT EXISTS dv_traditional USING DELTA LOCATION '{dv_path_traditional}'")

# Count files before DELETE
files_before_del = len([f for f in dbutils.fs.ls(dv_path_traditional + "/") if f.name.endswith(".parquet")])
print(f"Parquet files before DELETE: {files_before_del}")

# Run a small DELETE
spark.sql("DELETE FROM dv_traditional WHERE event_id < 100")
print("DELETE completed (traditional — files were rewritten).")

# Count files after DELETE
files_after_del = len([f for f in dbutils.fs.ls(dv_path_traditional + "/") if f.name.endswith(".parquet")])
print(f"Parquet files after DELETE: {files_after_del}")

# EXPLAIN: Without deletion vectors, files containing deleted rows are rewritten
print(f"\nINSIGHT: Without deletion vectors, {files_before_del} files were touched")
print("even though only ~100 rows were deleted from a 5000-row table.")
print("For large tables, this rewrite cost is enormous.")

```


#### Deletion Vectors — Enable and Demonstrate (if available)

```python

# Try to enable deletion vectors
dv_path = f"{base_dir}/dv_demo"
spark.sql(f"DROP TABLE IF EXISTS dv_trades")

dv_init_data = [
    (1, "AAPL", "BUY", 100, 175.50),
    (2, "GOOGL", "SELL", 50, 140.25),
    (3, "MSFT", "BUY", 200, 380.00),
    (4, "AAPL", "SELL", 75, 178.00),
    (5, "AMZN", "BUY", 150, 185.30),
    (6, "GOOGL", "BUY", 100, 141.00),
    (7, "META", "SELL", 25, 505.75),
    (8, "MSFT", "SELL", 100, 382.50),
]
dv_init_df = spark.createDataFrame(dv_init_data, ["trade_id", "ticker", "action", "quantity", "price"])

try:
    # Enable deletion vectors
    dv_init_df.write.format("delta").mode("overwrite") \
        .option("delta.enableDeletionVectors", "true") \
        .save(dv_path)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS dv_trades USING DELTA LOCATION '{dv_path}'
    """)

    # Verify property
    props = spark.sql("SHOW TBLPROPERTIES dv_trades").filter("key = 'delta.enableDeletionVectors'")
    if props.count() > 0:
        has_dv = props.collect()[0]["value"] == "true"
        print(f"Deletion Vectors enabled: {has_dv}")
    else:
        print("Deletion Vectors property not found — may not be supported in this environment.")

except Exception as e:
    error_msg = str(e)
    print(f"Deletion Vectors not supported in this environment: {error_msg[:200]}")
    print("\nDeletion Vectors require:")
    print("  - delta.minReaderVersion >= 3")
    print("  - delta.minWriterVersion >= 7")
    print("  - delta.enableDeletionVectors = true table property")
    print("\nCreating fallback table for architectural explanation...")
    
    # Fallback: create without DVs
    dv_init_df.write.format("delta").mode("overwrite").save(dv_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS dv_trades USING DELTA LOCATION '{dv_path}'")

```


#### How Deletion Vectors Work — Architectural Overview

**Without Deletion Vectors (Traditional):**
1. DELETE identifies files containing matching rows
2. Each matching file is **fully rewritten** minus the deleted rows
3. Old files are removed from the log, new files are added
4. Cost: O(file_size) for each file touched

**With Deletion Vectors:**
1. DELETE identifies specific row positions in each file
2. A small **deletion vector** (binary file) is created, recording which rows to skip
3. Data files are **NOT rewritten** — they remain in place
4. Readers filter out soft-deleted rows at read time
5. Cost: O(1) for the DELETE, O(deleted_rows) to filter during reads
6. **Physical compaction** happens during OPTIMIZE (reclaims space)


#### Demonstrate Soft-Delete vs Physical Delete Behavior

```python

print("Current state of trades table:")
spark.sql("SELECT * FROM dv_trades ORDER BY trade_id").show()

# Count files before the operation
files_before = len([f for f in dbutils.fs.ls(dv_path + "/") if f.name.endswith(".parquet")])

# Perform DELETE
deleted_count = spark.sql("SELECT COUNT(*) FROM dv_trades WHERE action = 'SELL'").collect()[0][0]
print(f"\nDeleting {deleted_count} trades where action = 'SELL'...")
spark.sql("DELETE FROM dv_trades WHERE action = 'SELL'")

# Count files after
files_after = len([f for f in dbutils.fs.ls(dv_path + "/") if f.name.endswith(".parquet")])

print(f"\nFiles before DELETE: {files_before}")
print(f"Files after DELETE: {files_after}")

# If DVs are working, files won't change much; if traditional, files are rewritten
if has_dv if 'has_dv' in dir() else False:
    print("\nWith Deletion Vectors: Files NOT rewritten (soft-delete markers used).")
    print("The rows are logically deleted but still physically present in files.")
    print("OPTIMIZE will later physically remove them and reclaim space.")
else:
    print("\nWithout Deletion Vectors: Files were rewritten to exclude deleted rows.")
    print("This is the traditional behavior — costly for large tables.")

# Verify data is gone from the logical view
print("\nAfter DELETE (logical view — deleted rows are gone):")
spark.sql("SELECT * FROM dv_trades ORDER BY trade_id").show()

```


#### View Deletion Vector Stats (if available)

```python

try:
    detail_dv = spark.sql("DESCRIBE DETAIL dv_trades").select(
        "numDeletionVectors", "numFiles", "numRecords"
    )
    detail_dv.show(truncate=False)

    num_dvs = detail_dv.collect()[0]["numDeletionVectors"]
    if num_dvs is not None and num_dvs > 0:
        print(f"Table has {num_dvs} deletion vector(s) — soft-deletes in effect.")
        print("Run OPTIMIZE to physically remove soft-deleted rows.")
    else:
        print("No deletion vectors found — traditional physical deletes were used.")
except Exception as e:
    print(f"'numDeletionVectors' field not available in this Runtime version.")
    print("This metric is available in DBR 14.0+ with deletion vectors enabled.")

```


### Key Takeaways
- **Deletion Vectors** enable near-instant DELETEs by using soft-delete markers instead of rewriting files.
- Without DVs, a DELETE rewrites **every file** that contains matching rows — costly for large tables.
- Soft-deleted rows are **filtered at read time** and physically removed during OPTIMIZE/VACUUM.

### Self-Assessment Question

*Q: You enable deletion vectors and run `DELETE FROM transactions WHERE user_id = 'GDPR_Subject_123'`. The user queries the table and doesn't see their data. Is the data still on disk?*

<details><summary>Click for answer</summary>
**A:** Yes, the data is still physically on disk (soft-deleted) and may remain there until OPTIMIZE physically removes it. The deletion vector ensures the data is filtered out at read time, so the user cannot see it — but for true GDPR compliance, you should ensure the data is physically removed via OPTIMIZE + VACUUM.
</details>


---
## Concept #10: Delta Table Properties & Configuration [Hard]

### What Problem It Solves

Delta Lake has dozens of configurable properties that control behaviour: auto-optimization, column mapping, protocol versions, retention, isolation levels, and more. Without understanding these properties, you can't tune Delta for production workloads — you might miss out on auto-compaction, column renaming, or proper isolation configuration.

### Real-World Use Case

A team needs to rename a column in a 10 TB Delta table without rewriting any data. By enabling `delta.columnMapping.mode = 'name'`, they can rename columns via `ALTER TABLE ... RENAME COLUMN` instantly — a metadata-only operation.

### Hands-On Demo

```python

print("=" * 60)
print("CONCEPT 10: Delta Table Properties & Configuration")
print("=" * 60)

# Create a fully configured table to explore properties
props_path = f"{base_dir}/props_demo"
spark.sql(f"DROP TABLE IF EXISTS props_table")

props_data = [(1, "Alice", "HR", 75000), (2, "Bob", "Engineering", 95000)]
props_df = spark.createDataFrame(props_data, ["id", "name", "dept", "salary"])

props_df.write.format("delta").mode("overwrite") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .option("delta.autoOptimize.autoCompact", "true") \
    .save(props_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS props_table USING DELTA LOCATION '{props_path}'")

```


#### 1. `SHOW TBLPROPERTIES` — View All Properties

```python

print("All table properties (SHOW TBLPROPERTIES):")
spark.sql("SHOW TBLPROPERTIES props_table").show(100, truncate=False)

```


#### 2. Key Production Properties Explained

```python

# === Property 1: autoOptimize.optimizeWrite ===
print("=" * 60)
print("Property: delta.autoOptimize.optimizeWrite")
print("=" * 60)
print("When true, Delta automatically optimizes the file sizes of writes.")
print("Each write produces fewer, optimally-sized files instead of many small ones.")
print("")
print("Use when: You have streaming or micro-batch workloads.")
print("Trade-off: Slight increase in write latency for better read performance.")

props_opt = spark.sql("SHOW TBLPROPERTIES props_table").filter("key = 'delta.autoOptimize.optimizeWrite'")
props_opt.show(truncate=False)

# === Property 2: autoOptimize.autoCompact ===
print("\nProperty: delta.autoOptimize.autoCompact")
print("After each write, Delta checks if small files exist and automatically compacts them.")
print("This is like running OPTIMIZE automatically after each write — no manual maintenance.")
print("Use when: You want hands-free file management.")
props_ac = spark.sql("SHOW TBLPROPERTIES props_table").filter("key = 'delta.autoOptimize.autoCompact'")
props_ac.show(truncate=False)

```


#### 3. Protocol Versions: `minReaderVersion` & `minWriterVersion`

```python

print("=" * 60)
print("Protocol Versions")
print("=" * 60)

# DESCRIBE EXTENDED shows protocol versions
print("DESCRIBE EXTENDED props_table (truncated to show protocol):")
extended = spark.sql("DESCRIBE EXTENDED props_table").filter(
    "col_name IN ('Provider', 'Type', 'Location', 'Table Properties')"
)
extended.show(truncate=False)

# More useful: DESCRIBE DETAIL
print("\nDESCRIBE DETAIL (includes minReaderVersion, minWriterVersion):")
detail_props = spark.sql("DESCRIBE DETAIL props_table").select(
    "format", "minReaderVersion", "minWriterVersion", "properties"
)
detail_props.show(truncate=False)

print("\nProtocol version summary:")
print("  minReaderVersion = 1: Basic Delta features (time travel, ACID)")
print("  minReaderVersion = 2: Column invariants, GENERATED columns")
print("  minReaderVersion = 3: CHECK constraints, column mapping, deletion vectors")
print("  minWriterVersion = 2: Basic writes")
print("  minWriterVersion = 3: CHECK constraints")
print("  minWriterVersion = 4: Change Data Feed, GENERATED columns")
print("  minWriterVersion = 5: Column invariants")
print("  minWriterVersion = 6: Identity columns")
print("  minWriterVersion = 7: Deletion vectors, column mapping (name mode), Liquid Clustering")

```


#### 4. Column Mapping — Enabling Column Renames

```python

print("=" * 60)
print("Column Mapping: Renaming Columns Without Rewriting Data")
print("=" * 60)

colmap_path = f"{base_dir}/colmap_demo"
spark.sql(f"DROP TABLE IF EXISTS colmap_table")

# Create table WITH column mapping enabled (uses 'name' mode)
colmap_data = [(1, "Alice", "HR"), (2, "Bob", "Engineering")]
colmap_df = spark.createDataFrame(colmap_data, ["employee_id", "full_name", "department"])

colmap_df.write.format("delta").mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .option("delta.minReaderVersion", "2") \
    .option("delta.minWriterVersion", "5") \
    .save(colmap_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS colmap_table USING DELTA LOCATION '{colmap_path}'")

print("Table created with column mapping mode = 'name'")

# Show original names
print("\nOriginal schema:")
spark.sql("DESCRIBE colmap_table").show()

# Rename a column (this is a metadata-only operation with column mapping!)
print("\nRenaming column 'full_name' to 'employee_name'...")
spark.sql("ALTER TABLE colmap_table RENAME COLUMN full_name TO employee_name")

print("Column renamed (no data rewritten — metadata-only operation):")
spark.sql("DESCRIBE colmap_table").show()

spark.sql("SELECT * FROM colmap_table").show()

print("\nColumn mapping modes:")
print("  'none' — Default. Column names are physical (in Parquet). Cannot rename without rewrite.")
print("  'name' — Column names are logical IDs in metadata. Renames are metadata-only. BEST for evolving schemas.")
print("  'id' — Also supports renaming. Columns are tracked by stable IDs.")
print("\nNOTE: Column mapping requires minReaderVersion >= 2 and minWriterVersion >= 5.")

```


#### 5. Table Properties Catalogue

```python

print("=" * 60)
print("Essential Delta Table Properties Reference")
print("=" * 60)

properties_summary = [
    ("delta.appendOnly", "true/false", "Prevents DELETE/UPDATE — table is append-only"),
    ("delta.autoOptimize.optimizeWrite", "true/false", "Auto-optimize file sizes on write"),
    ("delta.autoOptimize.autoCompact", "true/false", "Auto-compact small files after writes"),
    ("delta.columnMapping.mode", "none/name/id", "Enable column rename support"),
    ("delta.dataSkippingNumIndexedCols", "int (default 32)", "Number of columns to collect stats for skipping"),
    ("delta.deletedFileRetentionDuration", "interval (default 7 days)", "How long VACUUM retains deleted files"),
    ("delta.enableChangeDataFeed", "true/false", "Enable Change Data Feed"),
    ("delta.enableDeletionVectors", "true/false", "Enable deletion vectors for fast DELETEs"),
    ("delta.logRetentionDuration", "interval (default 30 days)", "How long to keep transaction log entries"),
    ("delta.minReaderVersion", "int", "Minimum Delta protocol reader version required"),
    ("delta.minWriterVersion", "int", "Minimum Delta protocol writer version required"),
    ("delta.randomizeFilePrefixes", "true/false", "Randomize file prefixes to reduce hotspotting"),
    ("delta.targetFileSize", "size (default 1gb)", "Target file size for OPTIMIZE"),
    ("delta.isolationLevel", "Serializable/WriteSerializable", "Transaction isolation level"),
]

for name, default_val, description in properties_summary:
    print(f"  {name:<45} | Default: {default_val:<20}")
    print(f"  {'':45} | {description}")
    print()

```


#### 6. `ALTER TABLE SET TBLPROPERTIES` — Modify Properties

```python

# Set and verify properties
print("Setting table properties...")
spark.sql("ALTER TABLE props_table SET TBLPROPERTIES ('delta.appendOnly' = 'true')")

print("\nVerifying properties:")
spark.sql("SHOW TBLPROPERTIES props_table").filter(
    "key IN ('delta.appendOnly', 'delta.autoOptimize.optimizeWrite', 'delta.autoOptimize.autoCompact')"
).show(truncate=False)

# Test appendOnly — should succeed for INSERT
print("\nTesting appendOnly = true: INSERT should work...")
spark.sql("INSERT INTO props_table VALUES (3, 'Charlie', 'Finance', 85000)")
print("INSERT succeeded (appends are allowed).")

# Test appendOnly — DELETE should FAIL
print("\nTesting appendOnly = true: DELETE should fail...")
try:
    spark.sql("DELETE FROM props_table WHERE id = 1")
    print("DELETE succeeded (unexpected — appendOnly should block this)")
except Exception as e:
    print(f"DELETE correctly blocked: {str(e)[:150]}")
    print("appendOnly = true prevents accidental data deletions.")

# Reset for further use
spark.sql("ALTER TABLE props_table SET TBLPROPERTIES ('delta.appendOnly' = 'false')")

```


#### 7. `DESCRIBE DETAIL` — Complete Table Metadata

```python

print("DESCRIBE DETAIL — Complete table metadata at a glance:")
spark.sql("DESCRIBE DETAIL props_table").show(truncate=False)

# Highlight important fields
detail = spark.sql("DESCRIBE DETAIL props_table").collect()[0]
print("\nKey metrics extracted from DESCRIBE DETAIL:")
print(f"  Location:           {detail['location']}")
print(f"  Format:             {detail['format']}")
print(f"  Number of files:    {detail['numFiles']}")
print(f"  Size in bytes:      {detail['sizeInBytes']}")
print(f"  Number of records:  {detail['numRecords']}")
print(f"  Table properties:   {detail['properties']}")

```


### Key Takeaways
- `SHOW TBLPROPERTIES` lists all configured properties; `ALTER TABLE SET TBLPROPERTIES` modifies them.
- **Protocol versions** (`minReaderVersion`, `minWriterVersion`) gate access to features like column mapping, deletion vectors, and CDF.
- `delta.columnMapping.mode = 'name'` enables column **renames without data rewrites** — critical for schema evolution in large tables.

### Self-Assessment Question

*Q: You want to enable column renames and Change Data Feed on a new table. What properties must you set at creation time?*

<details><summary>Click for answer</summary>
**A:** Set `delta.columnMapping.mode = 'name'` (or `'id'`), `delta.minReaderVersion = 2`, `delta.minWriterVersion = 5` (minimum for column mapping), and `delta.enableChangeDataFeed = true`. These must be set BEFORE writing data — you can't enable column mapping on an existing table without rewriting it.
</details>


---
---
## Notebook Summary

### What You've Learned

| # | Concept | Key Skill |
|---|---------|-----------|
| 1 | **ACID Transactions** | Atomic writes, optimistic concurrency, rollback on failure |
| 2 | **Transaction Log** | Inspecting `_delta_log`, understanding checkpoints, `DESCRIBE HISTORY` |
| 3 | **Time Travel** | `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE TABLE` |
| 4 | **Schema Enforcement** | Type validation, `mergeSchema`, `overwriteSchema`, `autoMerge` |
| 5 | **Liquid Clustering** | `CLUSTER BY`, Z-Ordering, file skipping for queries |
| 6 | **OPTIMIZE & Compaction** | Small-file consolidation, `ZORDER BY`, read performance |
| 7 | **VACUUM & Lifecycle** | Storage cleanup, retention periods, DRY RUN, time travel impact |
| 8 | **Change Data Feed** | `table_changes()`, `_change_type`, incremental processing |
| 9 | **Deletion Vectors** | Soft-delete markers, physical vs logical deletes, GDPR implications |
| 10 | **Table Properties** | Protocol versions, column mapping, auto-optimize, `DESCRIBE DETAIL` |

### Delta Lake Commands You Can Now Use

```
-- Inspection
DESCRIBE HISTORY table_name
DESCRIBE DETAIL table_name
DESCRIBE EXTENDED table_name
SHOW TBLPROPERTIES table_name

-- Time Travel
SELECT * FROM table_name VERSION AS OF N
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01'
RESTORE TABLE table_name TO VERSION AS OF N

-- Maintenance
OPTIMIZE table_name
OPTIMIZE table_name ZORDER BY (col1, col2)
VACUUM table_name
VACUUM table_name DRY RUN
VACUUM table_name RETAIN 168 HOURS

-- Change Data Feed
SELECT * FROM table_changes('table_name', start_version)
SELECT * FROM table_changes('table_name', start, end)

-- Configuration
ALTER TABLE table_name SET TBLPROPERTIES ('key' = 'value')
ALTER TABLE table_name RENAME COLUMN old_name TO new_name
```

---


## Score Yourself: How Many Concepts Do You Understand?

Check off each concept you feel confident with after completing this notebook.

- [ ] **Concept 1: ACID Transactions** — I can explain atomic writes and optimistic concurrency
- [ ] **Concept 2: Transaction Log** — I understand `_delta_log` structure and checkpointing
- [ ] **Concept 3: Time Travel & RESTORE** — I can query past versions and roll back tables
- [ ] **Concept 4: Schema Enforcement vs. Evolution** — I know when to use mergeSchema vs overwriteSchema
- [ ] **Concept 5: Liquid Clustering** — I understand clustering vs partitioning and can use Z-Ordering
- [ ] **Concept 6: OPTIMIZE & File Compaction** — I can consolidate small files and ZORDER tables
- [ ] **Concept 7: VACUUM & Storage Lifecycle** — I know how to manage storage and retention
- [ ] **Concept 8: Change Data Feed** — I can read row-level changes with table_changes()
- [ ] **Concept 9: Deletion Vectors** — I understand soft-deletes and their performance benefits
- [ ] **Concept 10: Table Properties** — I can configure protocol versions, column mapping, and auto-optimize

**Score:** ___ / 10

### Next Steps

After mastering these fundamentals, continue to notebook `02_Delta_Lake_Advanced.py` for:
- MERGE operations and SCD Type 2
- Streaming with Delta (Structured Streaming)
- Generated columns and default values
- Partitioning strategies and predicate pushdown
- Data skipping and bloom filters
- Multi-cluster concurrent writes
- Delta Sharing basics

---
*Notebook created for Databricks Community Edition | Delta Lake Fundamentals #1-#10*

```python

print("=" * 60)
print("NOTEBOOK COMPLETE")
print("=" * 60)
print("\nAll 10 Delta Lake concepts have been demonstrated.")
print("\nCleanup note: All tables were created under /tmp/delta_lake_fundamentals/")
print("These will persist until you manually remove them or the cluster restarts.")
print("\nTo clean up manually, run:")
print("  dbutils.fs.rm('/tmp/delta_lake_fundamentals', recurse=True)")
```

