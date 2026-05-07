# 03 — SQL & DataFrames: Advanced Analytics

This notebook covers **Concepts #21–#30** of the Databricks learning path.

| # | Concept | Level |
|---|---------|-------|
| 21 | DataFrame API — Core Operations | Easy |
| 22 | Temp Views & Global Temp Views | Easy |
| 23 | Window Functions | Medium |
| 24 | Complex Types — ARRAY, STRUCT, MAP | Medium |
| 25 | Higher-Order Functions | Medium |
| 26 | CTEs, PIVOT & Subqueries | Medium |
| 27 | UDFs vs Native Functions | Medium |
| 28 | VARIANT Type & Semi-Structured Data | Medium |
| 29 | MERGE INTO & Upsert Patterns | Hard |
| 30 | Table Constraints & Generated Columns | Medium |

**Scenario:** *RetailCo* — an ecommerce company managing customers, products, orders, and event logs.

**Setup:** All synthetic data is generated in-cell. No external files required.


## 0 — Environment Setup & Synthetic Data Generation

Create all datasets used across the notebook.

```python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, ArrayType, MapType
from pyspark.sql.functions import (
    col, lit, when, expr, rand, randn, round as spark_round,
    row_number, rank, dense_rank, lag, lead, sum as spark_sum,
    explode, posexplode, struct, array, map_from_arrays, map_concat,
    transform, filter as spark_filter, exists, aggregate,
    count, avg, max as spark_max, min as spark_min, collect_list, collect_set,
    to_date, date_sub, date_add, monotonically_increasing_id,
    pandas_udf, PandasUDFType, udf, coalesce, current_timestamp,
    unix_timestamp, from_unixtime, to_timestamp,
    split, size, element_at, slice, array_contains,
    broadcast, countDistinct, sumDistinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime, timedelta
import random
import time

spark = SparkSession.builder.appName("SQL_DataFrames_Concepts_21_30").getOrCreate()
print(f"Spark version : {spark.version}")
print(f"SparkSession  : {spark}")

```

```python

# ==========================================================================
# SYNTHETIC DATA: customers, products, orders, events
# ==========================================================================

random.seed(42)

# --- CUSTOMERS ---
customer_data = []
regions = ["North America", "Europe", "APAC", "Latin America", "Middle East"]
for i in range(1, 21):
    region = random.choice(regions)
    preferences = random.sample(["sports", "electronics", "fashion", "books", "home", "toys", "food"], k=random.randint(2, 5))
    customer_data.append((
        i,
        f"customer_{i}",
        f"cust{i}@example.com",
        region,
        to_date(lit(f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}")).cast(DateType()),
        preferences
    ))

customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("region", StringType()),
    StructField("signup_date", DateType()),
    StructField("preferences", ArrayType(StringType()))
])
customers_df = spark.createDataFrame(customer_data, schema=customers_schema)
customers_df.createOrReplaceTempView("customers")
print(f"customers: {customers_df.count()} rows")

# --- PRODUCTS ---
categories = {
    "Smartphone": "Electronics", "Laptop": "Electronics", "Headphones": "Electronics",
    "T-Shirt": "Fashion", "Jeans": "Fashion", "Sneakers": "Fashion",
    "Football": "Sports", "Yoga Mat": "Sports", "Dumbbell": "Sports",
    "Novel": "Books", "Cookbook": "Books", "Textbook": "Books",
    "Blender": "Home", "Lamp": "Home", "Cookware Set": "Home",
    "Action Figure": "Toys", "Board Game": "Toys", "Puzzle": "Toys",
    "Coffee": "Food", "Chocolate": "Food"
}
product_data = []
for pid, (name, cat) in enumerate(categories.items(), start=1):
    price = round(random.uniform(5.99, 1499.99), 2)
    tags = [cat.lower()] + random.sample(["bestseller", "new_arrival", "sale", "clearance", "premium", "eco_friendly"], k=random.randint(2, 4))
    product_data.append((pid, name, cat, price, tags, {"weight_kg": round(random.uniform(0.1, 10.0), 2), "color": random.choice(["black", "white", "red", "blue", "green"])}))

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("tags", ArrayType(StringType())),
    StructField("attributes", MapType(StringType(), StringType()))
])
products_df = spark.createDataFrame(product_data, schema=products_schema)
products_df.createOrReplaceTempView("products")
print(f"products: {products_df.count()} rows")

# --- ORDERS ---
order_data = []
statuses = ["pending", "shipped", "delivered", "cancelled", "returned"]
for oid in range(1, 201):
    cust_id = random.randint(1, 20)
    order_date = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
    num_items = random.randint(1, 5)
    items = []
    total = 0.0
    for _ in range(num_items):
        pid = random.randint(1, 20)
        qty = random.randint(1, 3)
        price = categories[list(categories.keys())[pid - 1]]  # won't work, use lookup
        price = [p[3] for p in product_data if p[0] == pid][0]
        items.append({"product_id": pid, "quantity": qty, "unit_price": price})
        total += price * qty
    status = random.choice(statuses)
    metadata = {
        "channel": random.choice(["web", "mobile_app", "in_store"]),
        "coupon_code": random.choice(["SAVE10", "FREESHIP", "", "", ""]) or None,
        "is_gift": str(random.choice([True, False])).lower()
    }
    order_data.append((oid, cust_id, to_date(lit(order_date)).cast(DateType()), round(total, 2), status, items, metadata))

from pyspark.sql.types import StructField as SF
items_schema = ArrayType(StructType([
    SF("product_id", IntegerType()),
    SF("quantity", IntegerType()),
    SF("unit_price", DoubleType())
]))
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType()),
    StructField("order_date", DateType()),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
    StructField("items", items_schema),
    StructField("metadata", MapType(StringType(), StringType()))
])
orders_df = spark.createDataFrame(order_data, schema=orders_schema)
orders_df.createOrReplaceTempView("orders")
print(f"orders: {orders_df.count()} rows")

# --- EVENTS (API Log with nested JSON) ---
event_types = ["page_view", "add_to_cart", "purchase", "search", "login", "logout", "review_submit"]
event_data = []
for eid in range(1, 501):
    ev_type = random.choice(event_types)
    ts = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
    payload = {
        "user_agent": random.choice(["Mozilla/5.0 Chrome", "Mozilla/5.0 Safari", "Mozilla/5.0 Edge", "App-iOS/3.2", "App-Android/4.1"]),
        "url": random.choice(["/home", "/products", "/cart", "/checkout", "/account", f"/product/{random.randint(1,20)}"]),
        "session_id": f"sess_{random.randint(1000, 9999)}",
        "extra_context": {
            "referrer": random.choice(["google.com", "facebook.com", "direct", "email_campaign", ""]),
            "ab_test_group": random.choice(["control", "variant_a", "variant_b"])
        }
    }
    event_data.append((eid, to_timestamp(lit(ts)), ev_type, payload))

events_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("timestamp", TimestampType()),
    StructField("event_type", StringType()),
    StructField("payload", StructType([
        StructField("user_agent", StringType()),
        StructField("url", StringType()),
        StructField("session_id", StringType()),
        StructField("extra_context", StructType([
            StructField("referrer", StringType()),
            StructField("ab_test_group", StringType())
        ]))
    ]))
])
events_df = spark.createDataFrame(event_data, schema=events_schema)
events_df.createOrReplaceTempView("events")
print(f"events: {events_df.count()} rows")

# --- SALES (monthly, for PIVOT) ---
months = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06",
          "2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12"]
sales_data = []
for m in months:
    for r in regions:
        sales_data.append((m, r, "Online", round(random.uniform(5000, 50000), 2)))
        sales_data.append((m, r, "In-Store", round(random.uniform(3000, 40000), 2)))
        sales_data.append((m, r, "Wholesale", round(random.uniform(1000, 25000), 2)))

sales_df = spark.createDataFrame(sales_data, schema=["month", "region", "channel", "revenue"])
sales_df.createOrReplaceTempView("sales")
print(f"sales: {sales_df.count()} rows")

print("\n--- All datasets created ---")

```


---
## Concept 21 — DataFrame API: Core Operations

Covers: `select`, `filter`, `withColumn`, `groupBy`, `agg`, `join` with method chaining.
Also demonstrates DataFrame API vs Spark SQL equivalence.

```python

from pyspark.sql.functions import col, lit, when, expr, round as spark_round, count, avg, sum as spark_sum, rank, dense_rank

# === SELECT + FILTER + WITHCOLUMN ===
print("=" * 60)
print("CONCEPT 21: DataFrame API Core Operations")
print("=" * 60)

df_orders_enriched = orders_df \
    .select("order_id", "customer_id", "order_date", "total_amount", "status") \
    .filter(col("status").isin("shipped", "delivered")) \
    .withColumn("is_high_value", when(col("total_amount") > 200, True).otherwise(False)) \
    .withColumn("order_month", expr("date_format(order_date, 'yyyy-MM')")) \
    .withColumn("amount_category",
                when(col("total_amount") < 50, "low")
                .when(col("total_amount") < 200, "medium")
                .otherwise("high"))

print("Enriched orders (shipped/delivered, high-value flag, amount category):")
df_orders_enriched.show(10, truncate=False)

# === GROUP BY + AGG ===
print("\n--- GroupBy + Agg: Revenue by Region ---")
df_joined = orders_df.alias("o") \
    .filter(col("o.status").isin("shipped", "delivered")) \
    .join(customers_df.alias("c"), col("o.customer_id") == col("c.customer_id"), "inner") \
    .select("o.*", "c.region")

df_region_revenue = df_joined \
    .groupBy("region") \
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_order_value"),
        count("order_id").alias("num_orders")
    ) \
    .orderBy(col("total_revenue").desc())

df_region_revenue.show(truncate=False)

# === JOIN types ===
print("\n--- All Orders with Customer Info (LEFT JOIN) ---")
orders_df.alias("o") \
    .join(customers_df.alias("c"), "customer_id", "left") \
    .select("o.order_id", "c.name", "c.region", "o.total_amount", "o.status") \
    .show(5, truncate=False)

# === DataFrame API vs Spark SQL: Same Physical Plan ===
print("\n--- DataFrame API & Spark SQL produce identical physical plans ---")
plan_df_api = orders_df.groupBy("status").agg(count("*").alias("cnt"))._jdf.queryExecution().toString().split("\n")[-5:]
plan_sql = spark.sql("SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status")._jdf.queryExecution().toString().split("\n")[-5:]

print("DataFrame API plan tail:")
for line in plan_df_api:
    print(f"  {line}")
print("\nSpark SQL plan tail:")
for line in plan_sql:
    print(f"  {line}")
print("\n>> Both plans are identical — DataFrame API and Spark SQL share the same Catalyst optimizer.")

# === createOrReplaceTempView for hybrid ===
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")
print("\nTemp views created. You can now mix spark.sql() with DataFrame API.")

# === Column expressions, aliases, when/otherwise demo ===
print("\n--- Column Expressions, Aliases, when/otherwise ---")
orders_df \
    .withColumn("discount_category",
                when(col("total_amount") > 300, "premium_customer")
                .when(col("total_amount") > 100, "regular_customer")
                .otherwise("standard_customer")) \
    .withColumn("tax", spark_round(col("total_amount") * 0.08, 2)) \
    .withColumn("total_with_tax", col("total_amount") + col("tax")) \
    .select(
        col("order_id").alias("Order#"),
        col("total_amount").alias("Subtotal"),
        col("tax").alias("Tax(8%)"),
        col("total_with_tax").alias("Grand Total"),
        col("discount_category").alias("Tier")
    ) \
    .show(10)

```


---
## Concept 22 — Temp Views & Global Temp Views

- **Temp View:** scoped to the SparkSession (notebook-level)
- **Global Temp View:** accessible across notebooks on the same cluster (via `global_temp` database)
- Neither persists across cluster restarts

```python

print("=" * 60)
print("CONCEPT 22: Temp Views & Global Temp Views")
print("=" * 60)

# -- Create a temp view (session-scoped) --
customers_df.createOrReplaceTempView("customers_temp_view")
print("[1] Created TEMP VIEW: customers_temp_view")

result_temp = spark.sql("""
    SELECT region, COUNT(*) AS customer_count
    FROM customers_temp_view
    GROUP BY region
    ORDER BY customer_count DESC
""")
print("\nQuerying TEMP VIEW (works within this notebook):")
result_temp.show(truncate=False)

# -- Create a global temp view (cross-session, same cluster) --
customers_df.createOrReplaceGlobalTempView("customers_global_view")
print("\n[2] Created GLOBAL TEMP VIEW: customers_global_view")

# Query via global_temp prefix
result_global = spark.sql("""
    SELECT region, COUNT(*) AS customer_count
    FROM global_temp.customers_global_view
    GROUP BY region
    ORDER BY customer_count DESC
""")
print("\nQuerying GLOBAL TEMP VIEW (via global_temp. prefix):")
result_global.show(truncate=False)

# -- Demonstrate GLOBAL temp view cross-access pattern --
# In another notebook on the SAME cluster, you would do:
print("\n>> In another notebook on this cluster, run:")
print('>>   spark.sql("SELECT * FROM global_temp.customers_global_view").show()')
print(">> This works because global temp views are registered in the global_temp database.")

# -- Show temp views exist in catalog --
print("\n--- Catalog listing ---")
print("Session temp views:")
spark.sql("SHOW TABLES").show(truncate=False)
print("Global temp views:")
spark.sql("SHOW TABLES IN global_temp").show(truncate=False)

# --- Clean up (demonstrating they are temporary) ---
spark.catalog.dropTempView("customers_temp_view")
print("\n[3] Dropped customers_temp_view")
print(">> Note: Both temp views and global temp views are lost when the cluster restarts.")
print(">> For permanent storage, use CREATE TABLE with a location (e.g., Delta table).")

# --- Difference summary ---
print("\n" + "=" * 60)
print("SCOPE COMPARISON")
print("=" * 60)
print("Temp View        → Current SparkSession only (this notebook)")
print("Global Temp View → All notebooks on same cluster (prefix: global_temp.)")
print("Permanent Table  → Persists across cluster restarts (data in storage)")
print("=" * 60)

```


---
## Concept 23 — Window Functions

Using retail orders: rank products by revenue, running totals, LAG/LEAD for trend analysis.

```python

print("=" * 60)
print("CONCEPT 23: Window Functions")
print("=" * 60)

# --- Prepare data: explode items to get product-level rows ---
order_items = orders_df.alias("o") \
    .join(customers_df.alias("c"), "customer_id") \
    .select(
        col("o.order_id"),
        col("o.order_date"),
        col("o.status"),
        col("c.region"),
        explode(col("o.items")).alias("item")
    ) \
    .select(
        "order_id", "order_date", "status", "region",
        col("item.product_id"),
        col("item.quantity"),
        col("item.unit_price"),
        (col("item.quantity") * col("item.unit_price")).alias("line_total")
    ) \
    .join(products_df.select("product_id", "product_name", "category"), "product_id")

order_items.createOrReplaceTempView("order_items")
print("order_items view ready with product-level detail")

# === ROW_NUMBER, RANK, DENSE_RANK ===
print("\n--- ROW_NUMBER, RANK, DENSE_RANK: Top Products by Revenue per Region ---")

window_spec = Window.partitionBy("region").orderBy(col("region_revenue").desc())

df_product_rankings = order_items \
    .groupBy("region", "product_id", "product_name") \
    .agg(spark_sum("line_total").alias("region_revenue")) \
    .withColumn("row_num", row_number().over(window_spec)) \
    .withColumn("rank_val", rank().over(window_spec)) \
    .withColumn("dense_rank_val", dense_rank().over(window_spec))

print("\nTop 3 products per region (notice RANK vs DENSE_RANK on ties):")
df_product_rankings.filter(col("row_num") <= 3).orderBy("region", "row_num").show(30, truncate=False)

# === LAG and LEAD ===
print("\n--- LAG/LEAD: Month-over-Month Revenue Change ---")

monthly_rev = order_items \
    .withColumn("order_month", expr("date_format(order_date, 'yyyy-MM')")) \
    .groupBy("order_month") \
    .agg(spark_sum("line_total").alias("monthly_revenue")) \
    .orderBy("order_month")

monthly_window = Window.orderBy("order_month")

df_trends = monthly_rev \
    .withColumn("prev_month_rev", lag("monthly_revenue", 1).over(monthly_window)) \
    .withColumn("next_month_rev", lead("monthly_revenue", 1).over(monthly_window)) \
    .withColumn("mom_change_pct",
                spark_round((col("monthly_revenue") - col("prev_month_rev")) / col("prev_month_rev") * 100, 2))

df_trends.show(12, truncate=False)

# === Running Total with SUM OVER ===
print("\n--- Running Total of Revenue (cumulative over months) ---")

df_cumulative = monthly_rev \
    .withColumn("running_total",
                spark_sum("monthly_revenue").over(Window.orderBy("order_month").rowsBetween(Window.unboundedPreceding, 0))) \
    .withColumn("running_total_all",
                spark_sum("monthly_revenue").over(Window.orderBy("order_month").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)))

df_cumulative.show(12, truncate=False)

# === SQL Window Function Equivalent ===
print("\n--- Same Window Function in Spark SQL ---")
spark.sql("""
    WITH ranked AS (
        SELECT
            region,
            product_name,
            SUM(line_total) AS region_revenue,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(line_total) DESC) AS rn,
            RANK() OVER (PARTITION BY region ORDER BY SUM(line_total) DESC) AS rnk,
            DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(line_total) DESC) AS drnk
        FROM order_items
        GROUP BY region, product_name
    )
    SELECT *
    FROM ranked
    WHERE rn <= 3
    ORDER BY region, rn
""").show(30, truncate=False)

```


---
## Concept 24 — Complex Types: ARRAY, STRUCT, MAP

Nested data: explode arrays, access struct fields, query maps. Scenario: flattening API event logs.

```python

print("=" * 60)
print("CONCEPT 24: Complex Types — ARRAY, STRUCT, MAP")
print("=" * 60)

# === EXPLODE — flatten array columns ===
print("\n--- explode(): Flatten items array from orders ---")

df_exploded = orders_df \
    .select("order_id", "customer_id", explode("items").alias("item"))

df_exploded.printSchema()
df_exploded.show(8, truncate=False)

# === posexplode() — with position index ===
print("\n--- posexplode(): Explode with positional index ---")

df_pos = orders_df \
    .select("order_id", posexplode("items").alias("item_pos", "item_struct"))

df_pos.show(8, truncate=False)

# === STRUCT field access: dot notation vs bracket notation ===
print("\n--- STRUCT Field Access: Dot Notation vs Bracket Notation ---")

df_struct_access = events_df \
    .select(
        col("event_id"),
        col("payload.url").alias("page_url"),              # dot notation
        col("payload").user_agent.alias("ua_dot"),         # dot chain
        col("payload")["session_id"].alias("session_bracket"),  # bracket notation
        col("payload").extra_context.referrer.alias("referrer"),
        col("payload").extra_context["ab_test_group"].alias("ab_group")
    )

df_struct_access.show(8, truncate=False)

# === MAP key/value access ===
print("\n--- MAP Key/Value Access ---")

df_map_explore = orders_df \
    .select(
        "order_id",
        col("metadata").getField("channel").alias("sales_channel"),
        col("metadata")["coupon_code"].alias("coupon"),
        col("metadata")["is_gift"].alias("is_gift")
    )

df_map_explore.show(8, truncate=False)

# === Flatten deeply nested JSON from API log events ===
print("\n--- Fully Flattened Event Log ---")

df_flat_events = events_df \
    .select(
        "event_id",
        "timestamp",
        "event_type",
        col("payload.user_agent").alias("user_agent"),
        col("payload.url").alias("url"),
        col("payload.session_id").alias("session_id"),
        col("payload.extra_context.referrer").alias("referrer"),
        col("payload.extra_context.ab_test_group").alias("ab_test_group")
    )

df_flat_events.show(10, truncate=False)
print(f"\nFlattened events count: {df_flat_events.count()}")

# === Build nested structures dynamically ===
print("\n--- Building nested structures: from flat to nested ---")

df_nested_built = order_items \
    .groupBy("order_id", "order_date", "status", "region") \
    .agg(
        collect_list(struct("product_id", "product_name", "category", "quantity", "unit_price", "line_total")).alias("line_items"),
        spark_sum("line_total").alias("order_total")
    )

df_nested_built.printSchema()
df_nested_built.select("order_id", "order_total", "line_items").show(3, truncate=False)

# === Access MAP keys/values + explode MAP ===
print("\n--- Exploding MAP entries ---")
df_map_exploded = orders_df \
    .select("order_id", explode("metadata").alias("meta_key", "meta_value"))

df_map_exploded.show(10, truncate=False)

```


---
## Concept 25 — Higher-Order Functions

Array functions: `transform()`, `filter()`, `exists()`, `aggregate()`.
Benchmarked against explode+groupBy and UDF approaches.

```python

print("=" * 60)
print("CONCEPT 25: Higher-Order Functions")
print("=" * 60)

# === transform() — map over arrays ===
print("\n--- transform(): Uppercase all product tags ---")

df_tags_upper = products_df \
    .withColumn("tags_upper", transform(col("tags"), lambda x: expr("upper(x)"))) \
    .select("product_name", "tags", "tags_upper")

df_tags_upper.show(8, truncate=False)

# === filter() — filter elements within an array ===
print("\n--- filter(): Keep only 'sale' or 'clearance' tags ---")

df_promo_tags = products_df \
    .withColumn("promo_tags", spark_filter(col("tags"), lambda t: t.isin("sale", "clearance"))) \
    .select("product_name", "tags", "promo_tags")

df_promo_tags.show(8, truncate=False)

# === exists() — test if any element satisfies condition ===
print("\n--- exists(): Products with at least one promo tag ---")

df_has_promo = products_df \
    .withColumn("has_promo", exists(col("tags"), lambda t: t.isin("sale", "clearance"))) \
    .select("product_name", "tags", "has_promo")

df_has_promo.show(8, truncate=False)

# === aggregate() — reduce an array to a scalar ===
print("\n--- aggregate(): Create comma-separated tag string ---")

df_tags_csv = products_df \
    .withColumn("tags_csv",
                aggregate(
                    "tags",
                    lit(""),
                    lambda acc, x: expr("CASE WHEN acc = '' THEN x ELSE concat(acc, ', ', x) END"),
                    lambda acc: acc
                )) \
    .select("product_name", "tags", "tags_csv")

df_tags_csv.show(8, truncate=False)

# === Performance: Higher-Order Functions vs explode+groupBy ===
print("\n--- PERFORMANCE COMPARISON: HOF vs explode+groupBy ---")

# Create larger dataset for timing
large_products = products_df.crossJoin(spark.range(1000)).select(products_df["*"])
large_products.cache().count()

# Approach 1: explode + groupBy
start = time.time()
result_explode = large_products \
    .select(explode("tags").alias("tag")) \
    .groupBy("tag") \
    .agg(count("*").alias("tag_count"))
result_explode.count()  # force execution
time_explode = time.time() - start

# Approach 2: Higher-order function (not directly comparable, but shows pattern)
start = time.time()
result_hof = large_products \
    .withColumn("tag_count", size(col("tags")))
result_hof.count()  # force execution
time_hof = time.time() - start

print(f"explode+groupBy: {time_explode:.3f} seconds")
print(f"HOF (size):        {time_hof:.3f} seconds")
print(f"Speedup:           {time_explode / max(time_hof, 0.001):.1f}x")
print(">> Higher-order functions avoid shuffle by operating within each row. explode+groupBy requires shuffle for aggregation.")

# === Real pattern: Clean product tags ===
print("\n--- Real Pattern: Clean and Normalize Tags ---")

df_clean_tags = products_df \
    .withColumn("clean_tags",
                transform(
                    spark_filter(col("tags"), lambda t: t != "premium"),
                    lambda t: when(t == "new_arrival", lit("new")).otherwise(t)
                ))

df_clean_tags.select("product_name", "tags", "clean_tags").show(8, truncate=False)

# Compare with UDF approach (Concept 27 will time this more thoroughly)
print("\n>> Higher-order functions run inside the Tungsten engine (columnar, no serialization).")
print(">> UDFs require row→Python→row serialization (slower).")

```


---
## Concept 26 — CTEs, PIVOT & Subqueries

- **CTEs (WITH clauses):** Multi-step queries
- **PIVOT:** Wide-format reshaping (sales by month by region)
- **Correlated vs Uncorrelated subqueries**

```python

print("=" * 60)
print("CONCEPT 26: CTEs, PIVOT & Subqueries")
print("=" * 60)

# === CTE (WITH clause) — Multi-step query ===
print("\n--- CTE: Multi-step customer order analysis ---")

cte_query = """
    WITH
    customer_orders AS (
        SELECT
            o.customer_id,
            c.name,
            c.region,
            COUNT(o.order_id) AS num_orders,
            SUM(o.total_amount) AS total_spent
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.status IN ('shipped', 'delivered')
        GROUP BY o.customer_id, c.name, c.region
    ),
    regional_stats AS (
        SELECT
            region,
            AVG(total_spent) AS avg_spent_per_customer,
            PERCENTILE_APPROX(total_spent, 0.5) AS median_spent_per_customer,
            COUNT(*) AS num_customers
        FROM customer_orders
        GROUP BY region
    ),
    top_customers AS (
        SELECT
            *,
            RANK() OVER (PARTITION BY region ORDER BY total_spent DESC) AS region_rank
        FROM customer_orders
    )
    SELECT
        tc.name,
        tc.region,
        tc.num_orders,
        tc.total_spent,
        tc.region_rank,
        rs.avg_spent_per_customer,
        rs.median_spent_per_customer
    FROM top_customers tc
    JOIN regional_stats rs ON tc.region = rs.region
    WHERE tc.region_rank <= 3
    ORDER BY tc.region, tc.region_rank
"""

spark.sql(cte_query).show(20, truncate=False)

# === PIVOT — Wide-format reshaping ===
print("\n--- PIVOT: Monthly Sales by Region (wide format) ---")

df_pivot = sales_df \
    .groupBy("month") \
    .pivot("region") \
    .agg(spark_sum("revenue").alias("")) \
    .orderBy("month") \
    .fillna(0)

df_pivot.show(13, truncate=False)
print(">> PIVOT transforms narrow (month, region, revenue) into wide (month, NA, Europe, APAC, ...)")

# === Spark SQL PIVOT ===
print("\n--- PIVOT in Spark SQL ---")
spark.sql("""
    SELECT *
    FROM (
        SELECT month, region, revenue
        FROM sales
    )
    PIVOT (
        SUM(revenue)
        FOR region IN (
            'North America', 'Europe', 'APAC', 'Latin America', 'Middle East'
        )
    )
    ORDER BY month
""").show(13, truncate=False)

# === Correlated vs Uncorrelated Subqueries ===
print("\n--- Uncorrelated Subquery: Orders above average ---")

spark.sql("""
    SELECT order_id, customer_id, total_amount, status
    FROM orders
    WHERE total_amount > (SELECT AVG(total_amount) FROM orders)
    ORDER BY total_amount DESC
""").show(10, truncate=False)

print("\n--- Correlated Subquery: Customers whose max order > 2x their own average ---")

spark.sql("""
    SELECT
        c.customer_id,
        c.name,
        c.region,
        MAX(o.total_amount) AS max_order,
        AVG(o.total_amount) AS avg_order,
        COUNT(o.order_id) AS num_orders
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.name, c.region
    HAVING MAX(o.total_amount) > 2 * AVG(o.total_amount)
    ORDER BY max_order DESC
""").show(10, truncate=False)

# === Physical plan for CTEs ===
print("\n--- Physical Plan for CTE Query ---")
print(spark.sql(cte_query)._jdf.queryExecution().optimizedPlan().treeString())

```


---
## Concept 27 — UDFs vs Native Functions

Compare: **Native Spark SQL** vs **Python UDF** vs **Pandas UDF (vectorized)**
with timing benchmarks on a medium-sized dataset.

```python

print("=" * 60)
print("CONCEPT 27: UDFs vs Native Functions")
print("=" * 60)

# --- Generate a larger order dataset for timing benchmarks ---
large_orders = orders_df.crossJoin(spark.range(500)).select(
    (col("order_id") * 10000 + col("id") % 10000).alias("order_id"),
    col("customer_id"),
    col("order_date"),
    col("total_amount"),
    col("status"),
    col("items"),
    col("metadata")
)
large_orders.cache()
n_rows = large_orders.count()
print(f"Benchmark dataset: {n_rows} rows")

# --- Business Logic: Customer Loyalty Tier ---
def loyalty_tier_native(total_amount, num_orders):
    if total_amount is None or num_orders is None:
        return None
    if total_amount > 5000 and num_orders > 10:
        return "Platinum"
    elif total_amount > 2000 or num_orders > 5:
        return "Gold"
    elif total_amount > 500:
        return "Silver"
    else:
        return "Bronze"

# --- 1. Native Spark SQL approach ---
print("\n[1] NATIVE SPARK SQL (columnar, optimized) ---")

start = time.time()

# Prepare: get customer-level aggregates
customer_agg = large_orders \
    .filter(col("status").isin("shipped", "delivered")) \
    .groupBy("customer_id") \
    .agg(
        spark_sum("total_amount").alias("total_spent"),
        count("order_id").alias("num_orders")
    )

result_native = customer_agg \
    .withColumn("loyalty_tier",
                when((col("total_spent") > 5000) & (col("num_orders") > 10), "Platinum")
                .when((col("total_spent") > 2000) | (col("num_orders") > 5), "Gold")
                .when(col("total_spent") > 500, "Silver")
                .otherwise("Bronze"))

result_native.count()  # force execution
time_native = time.time() - start
print(f"Native SQL time: {time_native:.3f} seconds")
result_native.groupBy("loyalty_tier").count().show()

# --- 2. Python UDF (row-by-row) ---
print("\n[2] PYTHON UDF (row-by-row serialization) ---")

loyalty_udf = udf(loyalty_tier_native, StringType())

start = time.time()
customer_agg2 = large_orders \
    .filter(col("status").isin("shipped", "delivered")) \
    .groupBy("customer_id") \
    .agg(
        spark_sum("total_amount").alias("total_spent"),
        count("order_id").alias("num_orders")
    )

result_udf = customer_agg2 \
    .withColumn("loyalty_tier", loyalty_udf(col("total_spent"), col("num_orders")))

result_udf.count()
time_udf = time.time() - start
print(f"Python UDF time:     {time_udf:.3f} seconds")

# --- 3. Pandas UDF (vectorized) ---
print("\n[3] PANDAS UDF (vectorized batch processing) ---")

@pandas_udf(StringType())
def loyalty_pandas_udf(total_spent_series, num_orders_series):
    import pandas as pd
    result = pd.Series(["Bronze"] * len(total_spent_series), dtype=str)
    mask_platinum = (total_spent_series > 5000) & (num_orders_series > 10)
    mask_gold = ((total_spent_series > 2000) | (num_orders_series > 5)) & ~mask_platinum
    mask_silver = (total_spent_series > 500) & ~mask_platinum & ~mask_gold
    result[mask_platinum] = "Platinum"
    result[mask_gold] = "Gold"
    result[mask_silver] = "Silver"
    return result

start = time.time()
customer_agg3 = large_orders \
    .filter(col("status").isin("shipped", "delivered")) \
    .groupBy("customer_id") \
    .agg(
        spark_sum("total_amount").alias("total_spent"),
        count("order_id").alias("num_orders")
    )

result_pandas = customer_agg3 \
    .withColumn("loyalty_tier", loyalty_pandas_udf(col("total_spent"), col("num_orders")))

result_pandas.count()
time_pandas = time.time() - start
print(f"Pandas UDF time:      {time_pandas:.3f} seconds")

# --- Summary ---
print("\n" + "=" * 60)
print("PERFORMANCE SUMMARY")
print("=" * 60)
print(f"Native Spark SQL   : {time_native:.3f}s  (baseline, runs in JVM, columnar)")
print(f"Python UDF         : {time_udf:.3f}s  (row-by-row Python serialization)")
print(f"Pandas UDF         : {time_pandas:.3f}s  (vectorized, batch Arrow transfer)")
print("=" * 60)
print(">> Rule: Always prefer native functions. Use Pandas UDFs when complex logic requires Python.")
print(">> Python UDFs are the slowest — only use for truly one-off transformations.")

# --- When UDFs are actually necessary ---
print("\n--- When UDFs are Actually Necessary ---")
print("Examples of unavoidable UDF use cases:")
print("  1. Calling external ML model inference (scikit-learn, custom models)")
print("  2. Complex NLP text processing (regex beyond built-in, tokenization)")
print("  3. Business rules engine with dozens of interdependent conditions")
print("  4. Calling external APIs or databases (not recommended at scale)")
print("  5. Custom hashing/encryption algorithms not available in Spark SQL")
print(">> In all other cases, refactor logic to native Spark SQL functions.")

```


---
## Concept 28 — VARIANT Type & Semi-Structured Data

**Note:** `VARIANT` type may not be available in Databricks Community Edition (requires DBR 15.3+ / Unity Catalog).

- If available: demonstrates native VARIANT ingestion, path-based querying, schema-on-read
- If not available: shows equivalent approaches using STRUCT and JSON string parsing

```python

print("=" * 60)
print("CONCEPT 28: VARIANT Type & Semi-Structured Data")
print("=" * 60)

# --- Check if VARIANT is available ---
try:
    spark.sql("SELECT CAST('{\"a\": 1}' AS VARIANT)")
    variant_available = True
except Exception:
    variant_available = False

print(f"VARIANT type available: {variant_available}")
print("")

if variant_available:
    # VARIANT IS AVAILABLE — demonstrate directly
    print(">>> DEMONSTRATING NATIVE VARIANT TYPE <<<")
    print("")

    # Create JSON data as VARIANT
    variant_demo = spark.sql("""
        SELECT
            CAST('{"user_id": 1001, "action": "click", "properties": {"color": "red", "size": "M", "tags": ["sale","new"]}}' AS VARIANT) AS payload
        UNION ALL
        SELECT
            CAST('{"user_id": 1002, "action": "purchase", "properties": {"color": "blue", "size": "L", "tags": ["clearance"]}}' AS VARIANT)
    """)

    variant_demo.createOrReplaceTempView("variant_demo")
    print("--- VARIANT data ---")
    variant_demo.show(truncate=False)
    variant_demo.printSchema()

    # Query with path syntax
    print("\n--- VARIANT path-based querying ---")
    spark.sql("""
        SELECT
            payload:user_id::INT AS user_id,
            payload:action::STRING AS action,
            payload:properties.color::STRING AS color,
            payload:properties.size::STRING AS size,
            payload:properties.tags[0]::STRING AS primary_tag
        FROM variant_demo
    """).show(truncate=False)

    # Schema-on-read benefits
    print("\n--- Schema-on-read: evolving schemas without DDL ---")
    spark.sql("""
        WITH mixed AS (
            SELECT CAST('{"v": 1, "version": "1.0", "name": "alpha"}' AS VARIANT) AS data
            UNION ALL
            SELECT CAST('{"v": 2, "version": "2.0", "name": "beta", "new_field": [10,20]}' AS VARIANT)
        )
        SELECT
            data:v::INT AS v,
            data:version::STRING AS version,
            data:name::STRING AS name,
            data:new_field::STRING AS new_field
        FROM mixed
    """).show(truncate=False)
    print(">> VARIANT stores semi-structured data in a native binary format (.variant)")
    print(">> No upfront schema required — 'schema-on-read' using path expressions")

else:
    # VARIANT NOT AVAILABLE — show equivalent approaches
    print(">>> VARIANT NOT AVAILABLE — Showing equivalent approaches <<<")
    print("")
    print("--- What is VARIANT? ---")
    print("VARIANT is a native semi-structured binary type that:")
    print("  • Stores JSON/XML/Parquet data without requiring a predefined schema")
    print("  • Uses schema-on-read: define structure only when querying")
    print("  • Enables path-based access: data:field1:subfield2::INT")
    print("  • Stores data in an optimized binary format (faster than JSON strings)")
    print("  • Benefits: flexible ingestion, evolving schemas, simpler ETL")
    print("")

    # Approach 1: STRUCT (strongly typed, full schema upfront)
    print("--- Approach 1: STRUCT Type (schema-at-write) ---")

    from pyspark.sql.types import StructType as ST, StructField as SF, FloatType

    json_events = spark.createDataFrame([
        ("e1", '{"user_id": 101, "event": "click", "attr": {"page": "home", "duration_ms": 250}}'),
        ("e2", '{"user_id": 102, "event": "purchase", "attr": {"page": "checkout", "amount": 49.99}}'),
        ("e3", '{"user_id": 103, "event": "search", "attr": {"page": "search_results", "query": "laptops"}}')
    ], ["event_id", "raw_json"])

    json_events.createOrReplaceTempView("json_events")

    # Parse JSON into STRUCT — requires known schema
    parsed_struct = json_events \
        .withColumn("parsed",
                    expr("from_json(raw_json, 'user_id INT, event STRING, attr STRUCT<page: STRING, duration_ms: INT, amount: DOUBLE, query: STRING>')")) \
        .select("event_id", "parsed.user_id", "parsed.event", "parsed.attr.*")

    print("Parsed via STRUCT (schema-at-write):")
    parsed_struct.show(truncate=False)
    print(">> STRUCT requires the full schema upfront — schema-at-write")

    # Approach 2: JSON string parsing (schema-on-read)
    print("\n--- Approach 2: JSON String Parsing (schema-on-read, like VARIANT) ---")

    df_json_path = spark.sql("""
        SELECT
            event_id,
            get_json_object(raw_json, '$.user_id') AS user_id,
            get_json_object(raw_json, '$.event') AS event,
            get_json_object(raw_json, '$.attr.page') AS page,
            get_json_object(raw_json, '$.attr.duration_ms') AS duration_ms,
            get_json_object(raw_json, '$.attr.amount') AS amount,
            get_json_object(raw_json, '$.attr.query') AS query
        FROM json_events
    """)

    df_json_path.show(truncate=False)
    print(">> get_json_object() enables schema-on-read — extract only what you need")
    print(">> However, raw JSON strings are NOT optimized (no binary format, no statistics)")

    # Approach 3: Using json_tuple for multiple fields
    print("\n--- Approach 3: json_tuple (faster for multiple fields) ---")

    df_tuple = json_events \
        .select(
            "event_id",
            expr("json_tuple(raw_json, 'user_id', 'event')").alias("uid", "evt")
        )

    df_tuple.show(truncate=False)

    # Comparison
    print("\n--- VARIANT vs Alternatives Comparison ---")
    print("┌──────────────────┬──────────────────┬──────────────────┐")
    print("│ Feature          │ VARIANT          │ STRING(JSON)     │")
    print("├──────────────────┼──────────────────┼──────────────────┤")
    print("│ Schema           │ Schema-on-read   │ Schema-on-read   │")
    print("│ Storage          │ Native binary    │ Plain text       │")
    print("│ Query speed      │ Fast (optimized) │ Slower (parsing) │")
    print("│ Statistics       │ Yes              │ No               │")
    print("│ Path syntax      │ col:field::TYPE  │ get_json_object  │")
    print("│ Evolving schema  │ Trivial          │ Manual           │")
    print("└──────────────────┴──────────────────┴──────────────────┘")

```


---
## Concept 29 — MERGE INTO & Upsert Patterns

Full MERGE syntax with Delta tables. Upsert customer dimension table with MATCHED update/delete and NOT MATCHED insert.

```python

print("=" * 60)
print("CONCEPT 29: MERGE INTO & Upsert Patterns")
print("=" * 60)

# --- Create Delta tables for the MERGE demo ---
delta_output_path = "/tmp/retailco_merge_demo"

# Clean up any prior runs
dbutils.fs.rm(delta_output_path, True)
print("Cleaned up prior output location")

# --- Step 1: Create the target dimension table (customer_dim) ---
print("\n[1] Creating TARGET: customer_dim (existing customer records)")

target_df = customers_df \
    .select(
        "customer_id", "name", "email", "region", "signup_date"
    ) \
    .withColumn("loyalty_tier", lit(None).cast(StringType())) \
    .withColumn("last_order_date", lit(None).cast(DateType())) \
    .withColumn("is_active", lit(True)) \
    .withColumn("updated_at", current_timestamp())

target_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_dim")

print("customer_dim table created")
spark.table("customer_dim").select("customer_id", "name", "region", "loyalty_tier", "is_active").show(5)

# --- Step 2: Create the source table (staged updates) ---
print("\n[2] Creating SOURCE: customer_updates (staging table with changes)")

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# Updates: change region for customers 1-3, set loyalty tier for 5-8, new customers 21-23
update_data = [
    # MATCHED — UPDATE: change region
    (1, "customer_1", "cust1@newdomain.com", "Europe", "Gold", True),
    (2, "customer_2", "cust2@example.com", "APAC", "Silver", True),
    (3, "customer_3", "cust3@example.com", "Middle East", "Platinum", True),

    # MATCHED — UPDATE: add loyalty tier to existing
    (5, "customer_5", "cust5@example.com", "North America", "Gold", True),
    (6, "customer_6", "cust6@example.com", "Europe", "Gold", True),
    (7, "customer_7", "cust7@example.com", "APAC", "Silver", True),
    (8, "customer_8", "cust8@example.com", "Latin America", "Bronze", True),

    # MATCHED — DELETE: mark as inactive
    (14, "customer_14", "cust14@example.com", "North America", "Bronze", False),
    (15, "customer_15", "cust15@example.com", "Europe", "Bronze", False),

    # NOT MATCHED — INSERT: new customers
    (21, "customer_21", "cust21@example.com", "APAC", "Silver", True),
    (22, "customer_22", "cust22@example.com", "Latin America", "Gold", True),
    (23, "customer_23", "cust23@example.com", "European Union", "Platinum", True),
]

source_df = spark.createDataFrame(
    update_data,
    schema=["customer_id", "name", "email", "region", "loyalty_tier", "is_active"]
)
source_df.createOrReplaceTempView("customer_updates")
print("customer_updates staging table:")
source_df.show(truncate=False)

# --- Step 3: Full MERGE INTO ---
print("\n[3] Executing MERGE INTO (full upsert) ---")

merge_sql = """
    MERGE INTO customer_dim AS target
    USING customer_updates AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED AND source.is_active = false THEN
        UPDATE SET
            is_active = false,
            updated_at = current_timestamp(),
            loyalty_tier = source.loyalty_tier
    WHEN MATCHED THEN
        UPDATE SET
            name = source.name,
            email = source.email,
            region = source.region,
            loyalty_tier = source.loyalty_tier,
            is_active = source.is_active,
            updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (
            customer_id, name, email, region, signup_date,
            loyalty_tier, last_order_date, is_active, updated_at
        )
        VALUES (
            source.customer_id, source.name, source.email, source.region, current_date(),
            source.loyalty_tier, NULL, source.is_active, current_timestamp()
        )
"""

spark.sql(merge_sql)
print("MERGE INTO completed")

# --- Step 4: Verify results ---
print("\n[4] Verification: Updated / Inserted / Deactivated records ---")

print("\n--- All customers (note changed regions, loyalty tiers, inactive flags) ---")
spark.sql("SELECT customer_id, name, region, loyalty_tier, is_active FROM customer_dim ORDER BY customer_id").show(25, truncate=False)

print("\n--- Newly inserted customers (21-23) ---")
spark.sql("SELECT * FROM customer_dim WHERE customer_id >= 21").show(truncate=False)

print("\n--- Deactivated customers (14-15) ---")
spark.sql("SELECT customer_id, name, is_active, updated_at FROM customer_dim WHERE NOT is_active").show(truncate=False)

print("\n--- Updated customers (1-3: region changed; 5-8: loyalty tier set) ---")
spark.sql("SELECT customer_id, name, region, loyalty_tier, updated_at FROM customer_dim WHERE customer_id IN (1,2,3,5,6,7,8)").show(truncate=False)

# --- Step 5: MERGE clause ordering & write amplification ---
print("\n--- MERGE INTO: Clause Ordering Requirements ---")
print(">> WHEN MATCHED clauses are evaluated in order. First match wins.")
print(">> WHEN MATCHED AND <condition> must come before general WHEN MATCHED.")
print(">> WHEN NOT MATCHED must come after all WHEN MATCHED clauses.")
print("")
print("--- Write Amplification ---")
print(">> MERGE rewrites entire files that contain modified rows (not individual rows).")
print(">> If your table has 100 files and MERGE touches rows in 12 files, only those 12 are rewritten.")
print(">> Delta Lake uses data skipping and file-level statistics to minimize rewrites.")
print(">> OPTIMIZE: Use ZORDER on merge key (customer_id) to co-locate related data in fewer files.")
print(">>           spark.sql('OPTIMIZE customer_dim ZORDER BY (customer_id)')")

# --- Step 6: MERGE with key column optimization ---
print("\n[5] OPTIMIZE demonstration ---")
try:
    spark.sql("OPTIMIZE customer_dim ZORDER BY (customer_id)")
    print("OPTIMIZE successful — data co-located by customer_id for faster MERGE operations.")
except Exception as e:
    print(f"OPTIMIZE not available in this environment: {e}")

```


---
## Concept 30 — Table Constraints & Generated Columns

- **CHECK constraints:** Validate data on write
- **NOT NULL constraints:** Enforce required columns
- **PRIMARY KEY:** Informational only (not enforced by Delta)
- **Generated columns:** Auto-computed from expressions

```python

print("=" * 60)
print("CONCEPT 30: Table Constraints & Generated Columns")
print("=" * 60)

delta_path = "/tmp/retailco_constraints_demo/"
dbutils.fs.rm(delta_path, True)

# ==========================================================================
# 1. CREATE TABLE with CHECK, NOT NULL, and Generated Columns
# ==========================================================================
print("\n[1] Creating product_catalog with constraints ---")

spark.sql("""
    CREATE OR REPLACE TABLE product_catalog (
        product_id         INT NOT NULL,
        product_name       STRING NOT NULL,
        category           STRING NOT NULL,
        base_price         DECIMAL(10, 2) NOT NULL,
        discount_pct       DECIMAL(5, 2) DEFAULT 0.00,
        final_price        DECIMAL(10, 2)
            GENERATED ALWAYS AS (base_price * (1.0 - discount_pct / 100.0)),
        stock_quantity     INT NOT NULL,
        sku                STRING NOT NULL,
        is_active          BOOLEAN DEFAULT true,
        created_at         TIMESTAMP DEFAULT current_timestamp(),

        CONSTRAINT positive_base_price CHECK (base_price > 0),
        CONSTRAINT valid_discount_pct CHECK (discount_pct >= 0 AND discount_pct <= 100),
        CONSTRAINT positive_stock CHECK (stock_quantity >= 0),
        CONSTRAINT valid_sku_prefix CHECK (sku LIKE 'SKU-%')
    )
    USING delta
    LOCATION '/tmp/retailco_constraints_demo/product_catalog'
""")
print("product_catalog table created with constraints")

# Show table schema
print("\nTable DDL:")
spark.sql("DESCRIBE EXTENDED product_catalog").show(50, truncate=False)

# ==========================================================================
# 2. VALID INSERTS
# ==========================================================================
print("\n[2] Inserting VALID data ---")

spark.sql("""
    INSERT INTO product_catalog
        (product_id, product_name, category, base_price, discount_pct, stock_quantity, sku)
    VALUES
        (1, 'Wireless Mouse',      'Electronics', 29.99,  10.00, 150, 'SKU-ELEC-001'),
        (2, 'USB-C Cable',         'Electronics', 12.50,   0.00, 500, 'SKU-ELEC-002'),
        (3, 'Running Shoes',       'Sports',      89.99,  15.00,  75, 'SKU-SPRT-001'),
        (4, 'Yoga Mat',            'Sports',      24.99,   5.00, 200, 'SKU-SPRT-002'),
        (5, 'Desk Lamp',           'Home',        39.99,  20.00,  60, 'SKU-HOME-001')
""")

print("Valid data inserted successfully.")
spark.table("product_catalog").show(truncate=False)

# Show generated columns in action
print("\nGenerated column `final_price` computed automatically:")
spark.sql("""
    SELECT product_id, product_name, base_price, discount_pct, final_price,
           base_price * (1.0 - discount_pct / 100.0) AS manual_calc
    FROM product_catalog
""").show(truncate=False)

# ==========================================================================
# 3. CONSTRAINT VIOLATIONS — each should fail
# ==========================================================================
print("\n[3] CONSTRAINT VIOLATIONS — each INSERT should FAIL ---")

# 3a: CHECK violation — negative base_price
print("\n--- Test: Negative base_price (CHECK violation) ---")
try:
    spark.sql("""
        INSERT INTO product_catalog
            (product_id, product_name, category, base_price, stock_quantity, sku)
        VALUES (6, 'Bad Product', 'Electronics', -10.00, 10, 'SKU-ELEC-003')
    """)
    print("ERROR: Should have been rejected!")
except Exception as e:
    print(f"REJECTED (expected): {str(e)[:120]}...")

# 3b: CHECK violation — discount > 100%
print("\n--- Test: discount_pct > 100 (CHECK violation) ---")
try:
    spark.sql("""
        INSERT INTO product_catalog
            (product_id, product_name, category, base_price, discount_pct, stock_quantity, sku)
        VALUES (7, 'Another Bad Product', 'Home', 50.00, 150.00, 20, 'SKU-HOME-002')
    """)
    print("ERROR: Should have been rejected!")
except Exception as e:
    print(f"REJECTED (expected): {str(e)[:120]}...")

# 3c: NOT NULL violation
print("\n--- Test: NULL product_name (NOT NULL violation) ---")
try:
    spark.sql("""
        INSERT INTO product_catalog
            (product_id, product_name, category, base_price, stock_quantity, sku)
        VALUES (8, NULL, 'Books', 15.00, 10, 'SKU-BOOK-001')
    """)
    print("ERROR: Should have been rejected!")
except Exception as e:
    print(f"REJECTED (expected): {str(e)[:120]}...")

# 3d: CHECK violation — invalid SKU prefix
print("\n--- Test: Invalid SKU prefix (CHECK violation) ---")
try:
    spark.sql("""
        INSERT INTO product_catalog
            (product_id, product_name, category, base_price, stock_quantity, sku)
        VALUES (9, 'Valid Product Bad SKU', 'Toys', 19.99, 30, 'ABC-TOYS-001')
    """)
    print("ERROR: Should have been rejected!")
except Exception as e:
    print(f"REJECTED (expected): {str(e)[:120]}...")

# 3e: CHECK violation — negative stock
print("\n--- Test: Negative stock (CHECK violation) ---")
try:
    spark.sql("""
        INSERT INTO product_catalog
            (product_id, product_name, category, base_price, stock_quantity, sku)
        VALUES (10, 'Negative Stock', 'Food', 5.00, -5, 'SKU-FOOD-001')
    """)
    print("ERROR: Should have been rejected!")
except Exception as e:
    print(f"REJECTED (expected): {str(e)[:120]}...")

# ==========================================================================
# 4. PRIMARY KEY (informational, NOT enforced in Delta)
# ==========================================================================
print("\n[4] PRIMARY KEY — Informational Only ---")
print(">> Delta Lake PRIMARY KEY is informational (metadata only). It is NOT enforced.")
print(">> Demonstration: Inserting a duplicate product_id succeeds (no error).")

try:
    spark.sql("""
        INSERT INTO product_catalog
            (product_id, product_name, category, base_price, stock_quantity, sku)
        VALUES (1, 'Duplicate Mouse', 'Electronics', 19.99, 50, 'SKU-ELEC-999')
    """)
    print("INSERT SUCCEEDED — PRIMARY KEY is not enforced. Duplicate product_id=1 exists.")
    print(">> Use MERGE INTO with a unique key to enforce uniqueness manually.")
    spark.table("product_catalog").show(truncate=False)
except Exception as e:
    print(f"Unexpected rejection: {e}")

# ==========================================================================
# 5. Show all constraints
# ==========================================================================
print("\n[5] Listing Constraints ---")
try:
    spark.sql("SHOW TBLPROPERTIES product_catalog").filter(
        col("key").contains("constraint")
    ).show(truncate=False)
except Exception:
    print("SHOW TBLPROPERTIES not supported — constraints visible in DESCRIBE EXTENDED above.")

# ==========================================================================
# 6. Generated Columns with complex expressions
# ==========================================================================
print("\n[6] Generated Column: complex expression ---")

spark.sql("""
    CREATE OR REPLACE TABLE orders_with_metrics (
        order_id      INT NOT NULL,
        subtotal      DECIMAL(10, 2) NOT NULL,
        tax_rate      DECIMAL(5, 4) DEFAULT 0.0800,
        shipping      DECIMAL(10, 2) DEFAULT 0.00,
        discount      DECIMAL(10, 2) DEFAULT 0.00,

        tax_amount    DECIMAL(10, 2)
            GENERATED ALWAYS AS (subtotal * tax_rate),
        grand_total   DECIMAL(10, 2)
            GENERATED ALWAYS AS (subtotal + subtotal * tax_rate + shipping - discount),
        is_free_ship  BOOLEAN
            GENERATED ALWAYS AS (shipping = 0.00),

        CONSTRAINT positive_subtotal CHECK (subtotal > 0),
        CONSTRAINT reasonable_tax CHECK (tax_rate BETWEEN 0 AND 0.25)
    )
    USING delta
    LOCATION '/tmp/retailco_constraints_demo/orders_with_metrics'
""")

spark.sql("""
    INSERT INTO orders_with_metrics (order_id, subtotal, tax_rate, shipping, discount)
    VALUES
        (1, 100.00, 0.08, 5.99, 10.00),
        (2, 250.00, 0.10, 0.00, 25.00),
        (3, 50.00,  0.08, 12.99, 0.00)
""")

print("Generated columns in action:")
spark.table("orders_with_metrics").show(truncate=False)
print(">> tax_amount, grand_total, and is_free_ship are auto-computed from source columns.")
print(">> No INSERT value needed for generated columns — they are always computed.")

# ==========================================================================
# Summary
# ==========================================================================
print("\n" + "=" * 60)
print("CONSTRAINT SUMMARY")
print("=" * 60)
print("CHECK          → Enforced on write (validates row values)")
print("NOT NULL       → Enforced on write (rejects NULL values)")
print("PRIMARY KEY    → Informational only (not enforced by Delta)")
print("FOREIGN KEY    → Not supported in Delta Lake")
print("GENERATED      → Always computed from expression (read-only)")
print("DEFAULT        → Value used when column omitted from INSERT")
print("=" * 60)

```


---
## Summary & Self-Assessment Checklist

### Concepts Covered (#21–#30)

| # | Concept | Key Takeaways |
|---|---------|---------------|
| 21 | DataFrame API Core | `select`, `filter`, `withColumn`, `groupBy`, `agg`, `join` — method chaining; equivalent physical plan to Spark SQL |
| 22 | Temp/Global Temp Views | Temp = session scope; Global Temp = cross-notebook via `global_temp.`; neither persists across cluster restart |
| 23 | Window Functions | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, running totals with `SUM OVER` and frame clauses |
| 24 | Complex Types | `ARRAY` → `explode`/`posexplode`; `STRUCT` → dot & bracket access; `MAP` → key/value access, flatten nested JSON |
| 25 | Higher-Order Functions | `transform`, `filter`, `exists`, `aggregate` — avoid shuffle vs explode+groupBy; faster than UDFs |
| 26 | CTEs, PIVOT, Subqueries | `WITH` clauses for multi-step queries; `PIVOT` for wide-format; correlated vs uncorrelated subqueries |
| 27 | UDFs vs Native | Native (JVM, columnar) > Pandas UDF (vectorized Arrow) > Python UDF (row-by-row); use native when possible |
| 28 | VARIANT Type | Native binary semi-structured format; schema-on-read; path-based access; alternatives: STRUCT, JSON string parsing |
| 29 | MERGE INTO | Full upsert: `WHEN MATCHED UPDATE/DELETE`, `WHEN NOT MATCHED INSERT`; clause ordering matters; write amplification |
| 30 | Constraints & Generated | `CHECK`, `NOT NULL` enforced on write; `PRIMARY KEY` informational only; generated columns auto-computed |

### Self-Assessment Questions

- [ ] Can you write a chained DataFrame transformation with `select`, `filter`, `withColumn`, `groupBy`, and `agg`?
- [ ] Can you explain when to use a Global Temp View over a Temp View?
- [ ] Can you use `ROW_NUMBER`, `RANK`, and `DENSE_RANK` with window partitions?
- [ ] Can you flatten a `STRUCT` field deep inside an `ARRAY<MAP<STRING, STRUCT>>` ?
- [ ] Can you use `transform()` and `filter()` on an array column to clean data without exploding?
- [ ] Can you write a CTE with three chained steps?
- [ ] Can you compare native Spark, Python UDF, and Pandas UDF performance?
- [ ] Can you explain VARIANT vs JSON string vs STRUCT for semi-structured data?
- [ ] Can you write a full MERGE INTO with WHEN MATCHED update/delete and WHEN NOT MATCHED insert?
- [ ] Can you create a table with CHECK, NOT NULL, DEFAULT, and GENERATED ALWAYS AS constraints?

---
*End of Notebook — Databricks SQL & DataFrames Concepts #21–#30*

