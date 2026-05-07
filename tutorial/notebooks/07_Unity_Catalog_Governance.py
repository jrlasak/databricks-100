# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Unity Catalog & Governance
# MAGIC
# MAGIC **Concepts Covered:** #61–#70  
# MAGIC **Environment:** Databricks Community Edition (single-node, free, **no Unity Catalog**)  
# MAGIC **Goal:** Master data governance, security, and sharing principles on Databricks.
# MAGIC
# MAGIC **CRITICAL:** Databricks Community Edition uses the **legacy Hive Metastore**. Unity Catalog is available only on paid workspaces (Premium/Enterprise tiers on AWS, Azure, or GCP). Throughout this notebook, we explain what Unity Catalog provides and then show the Hive Metastore equivalent that runs in Community Edition.
# MAGIC
# MAGIC | # | Concept | Difficulty | Type |
# MAGIC |---|---------|------------|------|
# MAGIC | 61 | Three-Level Namespace | Easy | Conceptual + Hands-on |
# MAGIC | 62 | Permission Model & Inheritance | Medium | Conceptual + Hands-on |
# MAGIC | 63 | Dynamic Views for Security | Medium | Hands-on |
# MAGIC | 64 | Data Lineage | Medium | Conceptual + Hands-on |
# MAGIC | 65 | Information Schema & Metadata Queries | Medium | Hands-on |
# MAGIC | 66 | External Locations & Storage Credentials | Medium | Conceptual + Hands-on |
# MAGIC | 67 | Service Principals & Managed Identity | Medium | Conceptual |
# MAGIC | 68 | Audit Logging | Medium | Hands-on |
# MAGIC | 69 | Row Filters & Column Masks | Hard | Hands-on |
# MAGIC | 70 | Delta Sharing | Hard | Conceptual + Hands-on |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 61 — Three-Level Namespace
# MAGIC
# MAGIC ### Unity Catalog: `catalog.schema.table`
# MAGIC
# MAGIC Unity Catalog introduces a **three-level namespace** for organizing data assets:
# MAGIC
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC    │       │      │
# MAGIC    │       │      └── Table, View, Function, Model, Volume
# MAGIC    │       └───────── Schema (logical grouping, like a database)
# MAGIC    └───────────────── Catalog (top-level container, maps to an org unit)
# MAGIC ```
# MAGIC
# MAGIC **How catalogs map to organizational structure:**
# MAGIC
# MAGIC | Catalog | Purpose | Example Schemas |
# MAGIC |---------|---------|-----------------|
# MAGIC | `main` | Default catalog | `default`, `public` |
# MAGIC | `sales` | Sales department data | `revenue`, `pipeline`, `forecast` |
# MAGIC | `marketing` | Marketing data | `campaigns`, `analytics`, `segments` |
# MAGIC | `hr_prod` | Production HR data (restricted) | `employees`, `payroll`, `benefits` |
# MAGIC | `hr_dev` | HR development/sandbox data | `employees`, `payroll`, `benefits` |
# MAGIC
# MAGIC ### Hive Metastore: Two-Level Namespace (Community Edition)
# MAGIC
# MAGIC Community Edition uses the legacy two-level namespace: `database.table`
# MAGIC
# MAGIC ```
# MAGIC database.table
# MAGIC    │        │
# MAGIC    │        └── Table or View
# MAGIC    └─────────── Database (equivalent to UC "schema")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### Hive Metastore Equivalent — Namespace Navigation

# COMMAND ----------

# Show all databases (equivalent to UC "SHOW CATALOGS" + "SHOW SCHEMAS")
print("=" * 60)
print("DATABASES AVAILABLE (Hive Metastore)")
print("=" * 60)
display(spark.sql("SHOW DATABASES"))

# COMMAND ----------

# Use the default database and show its tables
spark.sql("USE default")
print("Tables in 'default' database:")
display(spark.sql("SHOW TABLES IN default"))

# COMMAND ----------

# Create sample tables to demonstrate namespace navigation
spark.sql("CREATE DATABASE IF NOT EXISTS sales_db")
spark.sql("CREATE DATABASE IF NOT EXISTS hr_db")
spark.sql("CREATE DATABASE IF NOT EXISTS marketing_db")

# Create tables in sales_db
spark.sql("USE sales_db")
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.revenue AS
    SELECT 1 AS order_id, 'Widget-A' AS product, 150.00 AS amount, '2025-01-01' AS order_date
    UNION ALL
    SELECT 2, 'Widget-B', 200.00, '2025-01-02'
    UNION ALL
    SELECT 3, 'Widget-A', 150.00, '2025-01-03'
    UNION ALL
    SELECT 4, 'Widget-C', 300.00, '2025-01-04'
""")

spark.sql("""
    CREATE OR REPLACE TABLE sales_db.pipeline AS
    SELECT 1 AS deal_id, 'Acme Corp' AS company, 50000 AS value, 'Negotiation' AS stage
    UNION ALL
    SELECT 2, 'Globex Inc', 75000, 'Proposal'
    UNION ALL
    SELECT 3, 'Initech', 25000, 'Closed Won'
""")

# Create tables in hr_db
spark.sql("""
    CREATE OR REPLACE TABLE hr_db.employees AS
    SELECT 101 AS emp_id, 'Alice' AS name, 'Engineering' AS dept, 95000 AS salary
    UNION ALL
    SELECT 102, 'Bob', 'Engineering', 105000
    UNION ALL
    SELECT 103, 'Carol', 'Sales', 85000
    UNION ALL
    SELECT 104, 'Dave', 'HR', 75000
""")

spark.sql("""
    CREATE OR REPLACE TABLE hr_db.payroll AS
    SELECT 101 AS emp_id, '3000-11-1111' AS account_num, 'Direct Deposit' AS method
    UNION ALL
    SELECT 102, '3000-22-2222', 'Direct Deposit'
    UNION ALL
    SELECT 103, '3000-33-3333', 'Wire Transfer'
""")

# Create tables in marketing_db
spark.sql("""
    CREATE OR REPLACE TABLE marketing_db.campaigns AS
    SELECT 'C001' AS campaign_id, 'Spring Launch' AS name, 'Email' AS channel, 0.05 AS conversion_rate
    UNION ALL
    SELECT 'C002', 'Summer Sale', 'Social', 0.08
    UNION ALL
    SELECT 'C003', 'Fall Promo', 'Search', 0.12
""")

print("Databases and tables created successfully.")
print("\nAll databases:")
display(spark.sql("SHOW DATABASES"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Show Tables Across Databases

# COMMAND ----------

for db in ["default", "sales_db", "hr_db", "marketing_db"]:
    print(f"\n{'=' * 50}")
    print(f"  Database: {db}")
    print(f"{'=' * 50}")
    try:
        display(spark.sql(f"SHOW TABLES IN {db}"))
    except:
        print(f"  (no tables or database not found)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Syntax Reference (Not Executable in CE)
# MAGIC
# MAGIC The following is the Unity Catalog syntax — **cannot run** in Community Edition but shows what you would use in production:
# MAGIC
# MAGIC ```sql
# MAGIC -- Unity Catalog: three-level references
# MAGIC SELECT * FROM main.default.my_table;
# MAGIC SELECT * FROM sales_db.revenue.orders;
# MAGIC
# MAGIC -- Set default catalog
# MAGIC USE CATALOG main;
# MAGIC
# MAGIC -- Set default schema within catalog
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC -- Show all catalogs
# MAGIC SHOW CATALOGS;
# MAGIC
# MAGIC -- Show schemas in a catalog
# MAGIC SHOW SCHEMAS IN main;
# MAGIC
# MAGIC -- Show tables in a schema
# MAGIC SHOW TABLES IN main.default;
# MAGIC
# MAGIC -- Fully-qualified create
# MAGIC CREATE TABLE sales.revenue.orders (
# MAGIC     order_id BIGINT,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     order_date DATE
# MAGIC );
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Design catalog topology** matching your organizational structure (departments, business units, data domains).
# MAGIC 2. **Use catalogs for data isolation** — separate `_prod` and `_dev` catalogs for the same domain.
# MAGIC 3. **Standardize naming conventions** — `catalog_schema_table` becomes the canonical identity of every data asset.
# MAGIC 4. **Set `USE CATALOG`** at the notebook level for cleaner references.
# MAGIC 5. **Avoid cross-catalog JOINs** unless necessary (they can incur egress costs on some clouds).
# MAGIC
# MAGIC | Community Edition | Production Unity Catalog |
# MAGIC |---|---|
# MAGIC | `database.table` (2-level) | `catalog.schema.table` (3-level) |
# MAGIC | `SHOW DATABASES` | `SHOW CATALOGS` + `SHOW SCHEMAS IN <catalog>` |
# MAGIC | One default database | One default catalog + one default schema per user |
# MAGIC | No catalog-level privileges | Full RBAC at all three levels |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 62 — Permission Model & Inheritance
# MAGIC
# MAGIC ### Unity Catalog Privilege Cascade
# MAGIC
# MAGIC Unity Catalog permissions follow a **cascading inheritance model** where child objects can have **more restrictive** (but never more permissive) privileges than their parent:
# MAGIC
# MAGIC ```
# MAGIC CATALOG                    ─── GRANT USAGE ON CATALOG sales TO `analysts`
# MAGIC   ├── schema: revenue      ─── GRANT CREATE TABLE, USAGE ON SCHEMA sales.revenue TO `analysts`
# MAGIC   │     ├── table: orders  ─── GRANT SELECT, MODIFY ON TABLE sales.revenue.orders TO `analysts`
# MAGIC   │     └── table: returns ─── GRANT SELECT ON TABLE sales.revenue.returns TO `analysts`
# MAGIC   └── schema: forecast     ─── GRANT USAGE ON SCHEMA sales.forecast TO `analysts`
# MAGIC         └── table: q1      ─── (inherits no table privileges; must be granted explicitly)
# MAGIC ```
# MAGIC
# MAGIC ### Key Unity Catalog Privileges
# MAGIC
# MAGIC | Privilege | Applies To | Meaning |
# MAGIC |-----------|-----------|---------|
# MAGIC | `USE CATALOG` | Catalog | Required to access anything in the catalog |
# MAGIC | `USE SCHEMA` | Schema | Required to list/access objects in the schema |
# MAGIC | `SELECT` | Table/View | Read data from the object |
# MAGIC | `MODIFY` | Table | INSERT, UPDATE, DELETE, MERGE |
# MAGIC | `CREATE TABLE` | Schema | Create new tables in the schema |
# MAGIC | `CREATE VIEW` | Schema | Create views in the schema |
# MAGIC | `CREATE FUNCTION` | Schema | Create UDFs |
# MAGIC | `ALL PRIVILEGES` | Any | Owner-like full access |
# MAGIC | `EXECUTE` | Function | Run a UDF |
# MAGIC
# MAGIC ### Ownership vs Granted Access
# MAGIC
# MAGIC - **Owner**: The principal (user/service principal/group) that created the object; has `ALL PRIVILEGES` by default.
# MAGIC - **Granted Access**: Explicit privileges given to other principals via `GRANT`.
# MAGIC - Ownership can be transferred with `ALTER <object> OWNER TO <principal>`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hive Metastore Equivalent — Limited GRANT/REVOKE in Community Edition
# MAGIC
# MAGIC Community Edition supports a limited subset of Hive-style privilege management.
# MAGIC The following demonstrates what *is* executable in CE:

# COMMAND ----------

# Hive Metastore: Show current user
print("Current user context:")
display(spark.sql("SELECT current_user() AS current_user"))

# Hive Metastore: GRANT SELECT on a table
# NOTE: In CE, this may succeed but enforcement is limited since there's no
#       full authentication/authorization layer in the free tier.
try:
    spark.sql("GRANT SELECT ON TABLE sales_db.revenue TO `user@example.com`")
    print("GRANT SELECT executed (note: enforcement is limited in CE)")
except Exception as e:
    print(f"GRANT result: {e}")

# Hive Metastore: SHOW GRANT on a table
print("Privileges on sales_db.revenue:")
try:
    display(spark.sql("SHOW GRANT ON TABLE sales_db.revenue"))
except Exception as e:
    print(f"SHOW GRANT result: {e}")

# Hive Metastore: REVOKE
try:
    spark.sql("REVOKE SELECT ON TABLE sales_db.revenue FROM `user@example.com`")
    print("REVOKE SELECT executed")
except Exception as e:
    print(f"REVOKE result: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog GRANT/REVOKE Reference (Not Executable in CE)
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant catalog-level access
# MAGIC GRANT USE CATALOG ON CATALOG sales TO `data_analysts`;
# MAGIC GRANT USE CATALOG ON CATALOG hr_prod TO `hr_team`;
# MAGIC
# MAGIC -- Grant schema-level access
# MAGIC GRANT USE SCHEMA ON SCHEMA sales.revenue TO `data_analysts`;
# MAGIC GRANT CREATE TABLE ON SCHEMA sales.revenue TO `data_engineers`;
# MAGIC GRANT CREATE VIEW ON SCHEMA sales.revenue TO `data_analysts`;
# MAGIC
# MAGIC -- Grant table-level access
# MAGIC GRANT SELECT ON TABLE sales.revenue.orders TO `data_analysts`;
# MAGIC GRANT MODIFY ON TABLE sales.revenue.orders TO `data_engineers`;
# MAGIC GRANT ALL PRIVILEGES ON TABLE sales.revenue.returns TO `data_owner`;
# MAGIC
# MAGIC -- Grant to a group (using backticks for account-level groups)
# MAGIC GRANT SELECT ON TABLE sales.revenue.orders TO `account groups/data_analysts`;
# MAGIC
# MAGIC -- Revoke privileges
# MAGIC REVOKE SELECT ON TABLE sales.revenue.orders FROM `data_analysts`;
# MAGIC REVOKE ALL PRIVILEGES ON TABLE sales.revenue.orders FROM `data_owner`;
# MAGIC
# MAGIC -- Show grants
# MAGIC SHOW GRANTS ON CATALOG sales;
# MAGIC SHOW GRANTS ON SCHEMA sales.revenue;
# MAGIC SHOW GRANTS ON TABLE sales.revenue.orders;
# MAGIC SHOW GRANTS TO `data_analysts`;   -- all grants for a principal
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permission Inheritance Pattern (Visual)
# MAGIC
# MAGIC ```
# MAGIC -- Best practice: grant at the highest appropriate level
# MAGIC
# MAGIC -- 1. Catalog level: entry point (everyone who needs ANY access needs USE CATALOG)
# MAGIC GRANT USE CATALOG ON CATALOG sales TO `sales_team`;
# MAGIC
# MAGIC -- 2. Schema level: broader access for trusted roles
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA sales.revenue TO `analysts`;
# MAGIC
# MAGIC -- 3. Table level: restrict sensitive tables only
# MAGIC GRANT SELECT ON TABLE hr_prod.employees.salaries TO `hr_admins`;
# MAGIC -- (analysts do NOT get this — salaries remain inaccessible to them)
# MAGIC ```
# MAGIC
# MAGIC **Principle of least privilege in UC:**
# MAGIC - Start with **no access** — everything is denied by default.
# MAGIC - Grant **minimum necessary** at the **highest level possible**.
# MAGIC - Use **groups** rather than individual users.
# MAGIC - Review grants regularly with `SHOW GRANTS`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Create account groups** (`account groups/<name>`) and assign users to groups.
# MAGIC 2. **Grant at the schema level** by default — avoid per-table grants unless specificity is required.
# MAGIC 3. **Use catalog-level `USE CATALOG`** for broad access, schema-level `USE SCHEMA` for team access.
# MAGIC 4. **Audit grants quarterly** — use `system.information_schema.table_privileges` to review.
# MAGIC 5. **Never grant to individual users** — always use groups for maintainability.
# MAGIC
# MAGIC | Community Edition | Production Unity Catalog |
# MAGIC |---|---|
# MAGIC | Limited Hive GRANT/REVOKE | Full RBAC with inheritance |
# MAGIC | No group support for privileges | Account groups + nested groups |
# MAGIC | No separation of duties | Owners, admins, and users distinct |
# MAGIC | No catalog-level governance | Catalog → Schema → Table privilege cascade |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 63 — Dynamic Views for Security
# MAGIC
# MAGIC ### What Are Dynamic Views?
# MAGIC
# MAGIC Dynamic views embed user-identity functions in their SQL definition to **automatically filter rows or mask columns** based on who is querying. Unlike static views, a single dynamic view serves different data to different users.
# MAGIC
# MAGIC **Key functions:**
# MAGIC - `CURRENT_USER()` — returns the Databricks user email running the query
# MAGIC - `IS_MEMBER(group_name)` — returns TRUE if the current user belongs to the specified group
# MAGIC
# MAGIC **Use cases:**
# MAGIC - **Row-level filtering**: Sales reps see only their own deals; managers see all deals.
# MAGIC - **Column-level masking**: HR sees full SSN; payroll sees last 4 digits; others see NULL.
# MAGIC - **Region-based access**: EU users see only EU customer data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition Demo — Dynamic View with Simulated User Context
# MAGIC
# MAGIC Since CE has limited `IS_MEMBER()` support, we simulate the pattern using a control table and `current_user()`.

# COMMAND ----------

# Create base HR data table
spark.sql("""
    CREATE OR REPLACE TABLE hr_db.employee_sensitive AS
    SELECT 101 AS emp_id, 'Alice' AS name, 'Engineering' AS dept, 
           95000 AS salary, '123-45-6789' AS ssn, '555-0101' AS phone
    UNION ALL
    SELECT 102, 'Bob', 'Engineering', 
           105000, '234-56-7890', '555-0102'
    UNION ALL
    SELECT 103, 'Carol', 'Sales', 
           85000, '345-67-8901', '555-0103'
    UNION ALL
    SELECT 104, 'Dave', 'HR', 
           75000, '456-78-9012', '555-0104'
    UNION ALL
    SELECT 105, 'Eve', 'Finance', 
           110000, '567-89-0123', '555-0105'
""")

print("Base employee_sensitive table:")
display(spark.sql("SELECT * FROM hr_db.employee_sensitive"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simulated Access Control Table
# MAGIC
# MAGIC In production, you'd use Unity Catalog groups + `IS_MEMBER()`. Here we simulate with a lookup table.

# COMMAND ----------

# Create a simulated access control table
spark.sql("""
    CREATE OR REPLACE TABLE hr_db.access_control AS
    SELECT 'HR' AS role, 'SELECT_FULL' AS access_level
    UNION ALL
    SELECT 'MANAGER', 'SELECT_MASKED'
    UNION ALL
    SELECT 'EMPLOYEE', 'SELECT_LIMITED'
""")

# Map the current user to a simulated role
current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")

# Simulate role assignment (normally this comes from UC group membership)
simulated_role = "HR"  # Change to "MANAGER" or "EMPLOYEE" to test different views
print(f"Simulated role for this demo: {simulated_role}")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW current_user_role AS
    SELECT '{simulated_role}' AS role
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pattern 1: Column-Level Masking with Dynamic View

# COMMAND ----------

# Create a dynamic view that masks SSN based on the user's role
# In Unity Catalog, you'd use IS_MEMBER('hr_team') instead of our simulated role lookup.

spark.sql("""
    CREATE OR REPLACE VIEW hr_db.employee_view AS
    SELECT
        emp_id,
        name,
        dept,
        CASE
            WHEN (SELECT role FROM current_user_role) = 'HR' THEN ssn
            WHEN (SELECT role FROM current_user_role) = 'MANAGER' THEN CONCAT('***-**-', RIGHT(ssn, 4))
            ELSE 'XXX-XX-XXXX'
        END AS ssn,
        CASE
            WHEN (SELECT role FROM current_user_role) IN ('HR', 'MANAGER') THEN salary
            ELSE NULL
        END AS salary,
        CASE
            WHEN (SELECT role FROM current_user_role) = 'HR' THEN phone
            ELSE '***'
        END AS phone
    FROM hr_db.employee_sensitive
""")

print(f"Employee View — Role: {simulated_role}")
display(spark.sql("SELECT * FROM hr_db.employee_view ORDER BY emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pattern 2: Row-Level Filtering with Dynamic View

# COMMAND ----------

# Create a view that filters rows based on the user's department
# In production, you'd use a mapping table or UC group membership.

spark.sql("""
    CREATE OR REPLACE VIEW hr_db.employee_by_dept AS
    SELECT
        e.emp_id,
        e.name,
        e.dept,
        e.salary,
        CASE
            WHEN (SELECT role FROM current_user_role) = 'HR' THEN e.ssn
            ELSE 'XXX-XX-XXXX'
        END AS ssn
    FROM hr_db.employee_sensitive e
    WHERE
        CASE
            WHEN (SELECT role FROM current_user_role) = 'HR' THEN TRUE           -- HR sees ALL rows
            WHEN (SELECT role FROM current_user_role) = 'MANAGER' THEN e.dept IN ('Engineering', 'Sales', 'HR')
            WHEN (SELECT role FROM current_user_role) = 'EMPLOYEE' THEN e.dept = 'Engineering'  -- only own dept
            ELSE FALSE
        END
""")

print(f"Employee by Dept View — Role: {simulated_role}")
display(spark.sql("SELECT * FROM hr_db.employee_by_dept ORDER BY emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test All Three Roles

# COMMAND ----------

import pyspark.sql.functions as F

roles = ["HR", "MANAGER", "EMPLOYEE"]

for role in roles:
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW current_user_role AS
        SELECT '{role}' AS role
    """)
    
    count = spark.sql("SELECT COUNT(*) AS cnt FROM hr_db.employee_view WHERE salary IS NOT NULL").collect()[0][0]
    total = spark.sql("SELECT COUNT(*) AS cnt FROM hr_db.employee_view").collect()[0][0]
    
    print(f"\n--- Role: {role} ---")
    print(f"  Visible rows: {total}")
    print(f"  Salary visible for: {count} employees")
    print(f"  SSN format: {'Full' if role == 'HR' else 'Masked (last 4)' if role == 'MANAGER' else 'All X'}")
    print(f"  Phone visible: {'Yes' if role == 'HR' else 'No'}")

# Reset to HR for remaining demos
spark.sql("""
    CREATE OR REPLACE TEMP VIEW current_user_role AS
    SELECT 'HR' AS role
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Dynamic View — Production Syntax
# MAGIC
# MAGIC ```sql
# MAGIC -- Production UC dynamic view with IS_MEMBER() and CURRENT_USER()
# MAGIC CREATE VIEW hr_prod.employees.employee_safe_view AS
# MAGIC SELECT
# MAGIC     emp_id,
# MAGIC     name,
# MAGIC     dept,
# MAGIC     CASE
# MAGIC         WHEN IS_MEMBER('hr_team') THEN ssn
# MAGIC         WHEN IS_MEMBER('managers') THEN CONCAT('***-**-', RIGHT(ssn, 4))
# MAGIC         ELSE 'XXX-XX-XXXX'
# MAGIC     END AS ssn,
# MAGIC     CASE
# MAGIC         WHEN IS_MEMBER('hr_team') OR IS_MEMBER('managers') THEN salary
# MAGIC         ELSE NULL
# MAGIC     END AS salary,
# MAGIC     CURRENT_USER() AS queried_by
# MAGIC FROM hr_prod.employees.employee_raw
# MAGIC WHERE
# MAGIC     IS_MEMBER('hr_team')
# MAGIC     OR (IS_MEMBER('managers') AND dept IN (SELECT dept FROM manager_departments WHERE manager = CURRENT_USER()))
# MAGIC     OR (IS_MEMBER('employees') AND name = (SELECT name FROM user_employee_map WHERE user = CURRENT_USER()));
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic Views vs Row Filters & Column Masks
# MAGIC
# MAGIC | Feature | Dynamic Views | Row Filters + Column Masks |
# MAGIC |---------|---------------|---------------------------|
# MAGIC | **How it works** | Logic embedded in VIEW definition | UDFs bound to TABLE via ALTER |
# MAGIC | **Apply to** | Specific view | Base table (affects ALL queries) |
# MAGIC | **Override possible?** | Create separate view | No — affects all direct table access |
# MAGIC | **Performance** | View overhead on every query | UDF overhead on every scan |
# MAGIC | **Best for** | Tailored access per audience | Uniform policy enforcement |
# MAGIC | **Introduced** | Older (GA since DBR 9.x) | Newer (UC 1.3+, DBR 14+) |
# MAGIC | **Manageability** | View-level, requires DDL | Table-level, centralized policy |
# MAGIC
# MAGIC **Recommendation:** Use **dynamic views** for role-specific representations. Use **row filters + column masks** (see Concept #69) for uniform, non-negotiable security policies on base tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. Define account groups in Unity Catalog: `hr_team`, `managers`, `sales_reps`, etc.
# MAGIC 2. Use `IS_MEMBER()` in view definitions for automated filtering.
# MAGIC 3. Add `current_user()` as a column for audit trails within the view.
# MAGIC 4. Combine row filtering AND column masking in the same view.
# MAGIC 5. Test views by impersonating different users (`EXECUTE AS` in SQL Warehouses).
# MAGIC 6. Deny direct table access — force all users through the secure view.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 64 — Data Lineage
# MAGIC
# MAGIC ### What Unity Catalog Lineage Provides
# MAGIC
# MAGIC Unity Catalog automatically captures **column-level lineage** across all workloads:
# MAGIC - Which notebook produced this table?
# MAGIC - Which upstream tables fed this view?
# MAGIC - Which downstream dashboards depend on this column?
# MAGIC - If I change column X, what breaks?
# MAGIC
# MAGIC Lineage is captured automatically — no manual instrumentation needed. It works across notebooks, Delta Live Tables pipelines, SQL queries, and jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition — Manual Lineage Tracking
# MAGIC
# MAGIC Since CE lacks automatic lineage, we build a manual tracking system to demonstrate the concept.

# COMMAND ----------

# Create a lineage tracking metadata table
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.data_lineage (
        lineage_id BIGINT GENERATED ALWAYS AS IDENTITY,
        target_table STRING,
        target_column STRING,
        source_table STRING,
        source_column STRING,
        transformation STRING,
        notebook_or_job STRING,
        recorded_at TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

# Function to record lineage entries
def record_lineage(target_table, target_column, source_table, source_column, transformation, notebook_or_job):
    """Simulate Unity Catalog lineage tracking by inserting records into a lineage table."""
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    from datetime import datetime
    
    df = spark.createDataFrame(
        [(target_table, target_column, source_table, source_column, transformation, notebook_or_job, datetime.now())],
        ["target_table", "target_column", "source_table", "source_column", "transformation", "notebook_or_job", "recorded_at"]
    )
    df.write.mode("append").saveAsTable("sales_db.data_lineage")

print("Lineage recording function defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build a Sample Pipeline and Map Its Lineage

# COMMAND ----------

# Step 1: Raw revenue ingestion (simulated source)
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.revenue_raw AS
    SELECT * FROM sales_db.revenue
""")
record_lineage("sales_db.revenue_raw", "*", "external_csv", "*", "Raw ingestion from CSV files", "Ingestion_Job_01")
print("Step 1: revenue_raw created from external source.")

# Step 2: Clean and standardize revenue data
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.revenue_clean AS
    SELECT
        order_id,
        UPPER(product) AS product,
        CAST(amount AS DECIMAL(10,2)) AS amount,
        CAST(order_date AS DATE) AS order_date,
        MONTH(order_date) AS order_month
    FROM sales_db.revenue_raw
    WHERE amount > 0
""")
record_lineage("sales_db.revenue_clean", "product", "sales_db.revenue_raw", "product", "UPPER() standardization", "Cleaning_Job_02")
record_lineage("sales_db.revenue_clean", "amount", "sales_db.revenue_raw", "amount", "CAST to DECIMAL, filter > 0", "Cleaning_Job_02")
record_lineage("sales_db.revenue_clean", "order_month", "sales_db.revenue_raw", "order_date", "MONTH() extraction", "Cleaning_Job_02")
print("Step 2: revenue_clean created with transformations.")

# Step 3: Aggregate to monthly sales summary
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.revenue_monthly AS
    SELECT
        order_month,
        product,
        SUM(amount) AS total_revenue,
        COUNT(*) AS order_count,
        AVG(amount) AS avg_order_value
    FROM sales_db.revenue_clean
    GROUP BY order_month, product
    ORDER BY order_month, product
""")
record_lineage("sales_db.revenue_monthly", "total_revenue", "sales_db.revenue_clean", "amount", "SUM() aggregation", "Aggregation_Job_03")
record_lineage("sales_db.revenue_monthly", "order_count", "sales_db.revenue_clean", "order_id", "COUNT() aggregation", "Aggregation_Job_03")
record_lineage("sales_db.revenue_monthly", "avg_order_value", "sales_db.revenue_clean", "amount", "AVG() aggregation", "Aggregation_Job_03")
print("Step 3: revenue_monthly aggregated.")

# Step 4: Create dashboard-ready view
spark.sql("""
    CREATE OR REPLACE VIEW sales_db.executive_dashboard AS
    SELECT
        order_month,
        SUM(total_revenue) AS monthly_revenue,
        SUM(order_count) AS total_orders,
        SUM(total_revenue) / NULLIF(SUM(order_count), 0) AS avg_order_value
    FROM sales_db.revenue_monthly
    GROUP BY order_month
""")
record_lineage("sales_db.executive_dashboard", "monthly_revenue", "sales_db.revenue_monthly", "total_revenue", "SUM() for dashboard", "Dashboard_Pipeline_04")
print("Step 4: executive_dashboard view created.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query the Lineage — Impact Analysis

# COMMAND ----------

print("=" * 70)
print("FULL LINEAGE GRAPH")
print("=" * 70)
display(spark.sql("SELECT * FROM sales_db.data_lineage ORDER BY lineage_id"))

# COMMAND ----------

# Impact analysis: "If I change revenue_raw.product, what downstream objects are affected?"
print("=" * 70)
print("IMPACT ANALYSIS: Changing 'sales_db.revenue_raw.product'")
print("=" * 70)

affected = spark.sql("""
    WITH RECURSIVE downstream AS (
        -- Direct dependents
        SELECT target_table, target_column, transformation, 1 AS depth
        FROM sales_db.data_lineage
        WHERE source_table = 'sales_db.revenue_raw' AND source_column = 'product'
        
        UNION ALL
        
        -- Indirect dependents
        SELECT l.target_table, l.target_column, l.transformation, d.depth + 1
        FROM sales_db.data_lineage l
        JOIN downstream d ON l.source_table = d.target_table
    )
    SELECT DISTINCT target_table, target_column, transformation, depth
    FROM downstream
    ORDER BY depth, target_table
""")

display(affected)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize the Pipeline Lineage
# MAGIC
# MAGIC ```
# MAGIC external_csv ──→ revenue_raw ──→ revenue_clean ──→ revenue_monthly ──→ executive_dashboard
# MAGIC                      │                   │                    │
# MAGIC                      │                   ├─ product (UPPER)    ├─ total_revenue (SUM)
# MAGIC                      │                   ├─ amount (CAST)      ├─ order_count (COUNT)
# MAGIC                      │                   └─ order_month (MONTH)└─ avg_order_value (AVG)
# MAGIC                      │
# MAGIC                      └─── * (raw ingestion)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Lineage Queries (Not Executable in CE)
# MAGIC
# MAGIC ```sql
# MAGIC -- UC: Query column-level lineage for a specific table
# MAGIC SELECT
# MAGIC     source_table_catalog,
# MAGIC     source_table_schema,
# MAGIC     source_table_name,
# MAGIC     source_column_name,
# MAGIC     target_table_catalog,
# MAGIC     target_table_schema,
# MAGIC     target_table_name,
# MAGIC     target_column_name,
# MAGIC     created_at
# MAGIC FROM system.lineage.column_lineage
# MAGIC WHERE target_table_name = 'revenue_monthly';
# MAGIC
# MAGIC -- UC: Find all downstream consumers of a column
# MAGIC SELECT DISTINCT
# MAGIC     target_table_name,
# MAGIC     notebook_path,
# MAGIC     job_name
# MAGIC FROM system.lineage.column_lineage
# MAGIC WHERE source_table_name = 'revenue_raw' AND source_column_name = 'product';
# MAGIC
# MAGIC -- UC: Find all upstream sources for a dashboard table
# MAGIC SELECT DISTINCT
# MAGIC     source_table_name,
# MAGIC     source_column_name
# MAGIC FROM system.lineage.column_lineage
# MAGIC WHERE target_table_name = 'executive_dashboard';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. Unity Catalog lineage is **automatic** — no manual instrumentation.
# MAGIC 2. Use **Lineage UI** in the Databricks workspace for visual graph exploration.
# MAGIC 3. Query `system.lineage.column_lineage` for programmatic impact analysis.
# MAGIC 4. Integrate lineage into your CI/CD to detect breaking changes before deployment.
# MAGIC 5. Tag all notebooks, jobs, and DLT pipelines with meaningful names for clear lineage.
# MAGIC 6. Lineage persists across retentions — you can trace history even after source tables are dropped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 65 — Information Schema & Metadata Queries
# MAGIC
# MAGIC ### Unity Catalog Information Schema
# MAGIC
# MAGIC UC provides `system.information_schema` with standardized metadata tables:
# MAGIC
# MAGIC | Table | What It Contains |
# MAGIC |-------|------------------|
# MAGIC | `system.information_schema.tables` | All tables and views across catalogs |
# MAGIC | `system.information_schema.columns` | All columns with types, nullable, defaults |
# MAGIC | `system.information_schema.table_privileges` | All GRANTs on tables |
# MAGIC | `system.information_schema.schemata` | All schemas/catalogs |
# MAGIC | `system.information_schema.views` | View definitions |
# MAGIC | `system.information_schema.routines` | UDFs and stored procedures |
# MAGIC
# MAGIC This is ANSI SQL standard compliant — the same queries work across Databricks, PostgreSQL, Snowflake, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition — Hive Metastore Metadata Queries
# MAGIC
# MAGIC CE uses Hive Metastore commands to discover metadata. While not a true `information_schema`, these serve the same purpose.

# COMMAND ----------

# Metadata Discovery — Show all databases
print("=" * 60)
print("ALL DATABASES")
print("=" * 60)
display(spark.sql("SHOW DATABASES"))

# Show tables in each database
for row in spark.sql("SHOW DATABASES").collect():
    db_name = row["databaseName"]
    print(f"\n{'=' * 60}")
    print(f"TABLES IN: {db_name}")
    print(f"{'=' * 60}")
    try:
        tables = spark.sql(f"SHOW TABLES IN {db_name}")
        display(tables)
    except Exception as e:
        print(f"  Error: {e}")

# COMMAND ----------

# Describe each table in sales_db
print("=" * 60)
print("SCHEMA DETAIL FOR: sales_db tables")
print("=" * 60)

for row in spark.sql("SHOW TABLES IN sales_db").collect():
    tbl = row["tableName"]
    is_temp = row.get("isTemporary", False)
    print(f"\n--- DESCRIBE TABLE sales_db.{tbl} {'(TEMP)' if is_temp else ''}---")
    try:
        display(spark.sql(f"DESCRIBE TABLE sales_db.{tbl}"))
    except Exception as e:
        print(f"  Error: {e}")

# COMMAND ----------

# Describe extended — shows table format, location, properties
print("=" * 60)
print("EXTENDED TABLE DETAILS: sales_db.revenue")
print("=" * 60)
try:
    display(spark.sql("DESCRIBE EXTENDED sales_db.revenue"))
except Exception as e:
    print(f"  Error: {e}")

# COMMAND ----------

# Show table properties (Delta-specific metadata)
print("=" * 60)
print("TABLE PROPERTIES: sales_db.revenue")
print("=" * 60)
try:
    display(spark.sql("SHOW TBLPROPERTIES sales_db.revenue"))
except Exception as e:
    print(f"  Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build a Compliance Report from Metadata

# COMMAND ----------

# Create a comprehensive metadata report
print("=" * 70)
print("COMPLIANCE REPORT — ALL TABLES & SCHEMAS")
print("=" * 70)

import pyspark.sql.functions as F

report_rows = []
for db_row in spark.sql("SHOW DATABASES").collect():
    db_name = db_row["databaseName"]
    
    for tbl_row in spark.sql(f"SHOW TABLES IN {db_name}").collect():
        tbl_name = tbl_row["tableName"]
        is_temp = tbl_row.get("isTemporary", False)
        
        if is_temp:
            continue
        
        try:
            cols_df = spark.sql(f"DESCRIBE TABLE {db_name}.{tbl_name}")
            col_rows = cols_df.collect()
            
            # Count columns and identify sensitive-sounding columns
            col_names = [r["col_name"].lower() for r in col_rows if r["col_name"] and not r["col_name"].startswith("#")]
            sensitive_cols = [c for c in col_names if any(kw in c for kw in ["ssn", "salary", "phone", "email", "address", "password", "credit", "account"])]
            
            # Get format and location from EXTENDED describe
            ext_df = spark.sql(f"DESCRIBE EXTENDED {db_name}.{tbl_name}")
            ext_map = {}
            for r in ext_df.collect():
                if r["col_name"] and r["data_type"]:
                    ext_map[r["col_name"].strip().lower()] = r["data_type"].strip()
            
            table_format = ext_map.get("provider", "unknown")
            location = ext_map.get("location", "unknown")
            
            report_rows.append((
                db_name, tbl_name, table_format, len(col_names),
                ", ".join(sensitive_cols) if sensitive_cols else "None",
                "RESTRICTED" if sensitive_cols else "OK",
                location
            ))
        except Exception as e:
            report_rows.append((db_name, tbl_name, "ERROR", 0, str(e)[:50], "ERROR", "N/A"))

# Build report DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("database", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("format", StringType(), True),
    StructField("column_count", IntegerType(), True),
    StructField("sensitive_columns", StringType(), True),
    StructField("status", StringType(), True),
    StructField("location", StringType(), True),
])

report_df = spark.createDataFrame(report_rows, schema)
display(report_df.orderBy("status", "database", "table_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Information Schema Queries (Reference)
# MAGIC
# MAGIC ```sql
# MAGIC -- UC: List all tables in the sales catalog
# MAGIC SELECT table_catalog, table_schema, table_name, table_type, data_source_format
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'sales'
# MAGIC ORDER BY table_schema, table_name;
# MAGIC
# MAGIC -- UC: Find all columns with PII-sounding names
# MAGIC SELECT table_catalog, table_schema, table_name, column_name, data_type
# MAGIC FROM system.information_schema.columns
# MAGIC WHERE LOWER(column_name) RLIKE 'ssn|salary|phone|email|address|password|credit'
# MAGIC ORDER BY table_catalog, table_schema, table_name;
# MAGIC
# MAGIC -- UC: Compliance audit — tables without descriptions
# MAGIC SELECT table_catalog, table_schema, table_name
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE comment IS NULL OR comment = ''
# MAGIC ORDER BY table_catalog, table_schema, table_name;
# MAGIC
# MAGIC -- UC: Review all grants on sensitive tables
# MAGIC SELECT grantee, privilege_type, table_catalog, table_schema, table_name
# MAGIC FROM system.information_schema.table_privileges
# MAGIC WHERE table_name IN ('salaries', 'employee_sensitive')
# MAGIC ORDER BY grantee, table_name;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Automate metadata scans** — weekly job that queries `system.information_schema.columns` for new PII columns.
# MAGIC 2. **Build a data catalog dashboard** listing all tables, owners, last updated, sensitivity level.
# MAGIC 3. **Enforce tagging** — use table comments and TBLPROPERTIES to tag data classification levels.
# MAGIC 4. **Monitor for "rogue tables"** — tables outside designated catalogs/schemas.
# MAGIC 5. **Integrate with external catalogs** — push UC metadata to enterprise data catalogs (Alation, Collibra, etc.).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 66 — External Locations & Storage Credentials
# MAGIC
# MAGIC ### Unity Catalog External Locations
# MAGIC
# MAGIC Unity Catalog governs access to cloud storage through **external locations** — named references to cloud storage paths:
# MAGIC
# MAGIC ```
# MAGIC Cloud Storage (S3/ADLS/GCS)
# MAGIC         │
# MAGIC         ▼
# MAGIC Storage Credential    ← IAM Role (AWS) / Service Principal (Azure) / Service Account (GCP)
# MAGIC         │
# MAGIC         ▼
# MAGIC External Location     ← s3://my-bucket/sales-data/ mapped to UC name "sales_data_loc"
# MAGIC         │
# MAGIC         ▼
# MAGIC Managed/External Tables ← Created under the external location
# MAGIC ```
# MAGIC
# MAGIC ### Two Types of Tables in UC
# MAGIC
# MAGIC | Type | Data Location | Schema Location | Dropping Table... |
# MAGIC |------|--------------|-----------------|-------------------|
# MAGIC | **Managed Table** | UC-managed storage (catalog-scoped) | UC-managed | Deletes DATA + SCHEMA |
# MAGIC | **External Table** | Your cloud storage (external location) | UC-metastore | Deletes SCHEMA only; DATA persists |
# MAGIC
# MAGIC **Key difference:** Managed tables give UC full lifecycle control. External tables let you own the data files in your cloud storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition — External Tables on DBFS
# MAGIC
# MAGIC CE does not have UC external locations, but the Hive Metastore supports external tables with a `LOCATION` clause. DBFS (`/FileStore/`, `/tmp/`) serves as the "external storage."

# COMMAND ----------

# Create a managed Delta table (default — data goes to Spark warehouse)
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.orders_managed
    USING DELTA
    AS
    SELECT 1 AS order_id, 'Widget-A' AS product, 150.00 AS amount
    UNION ALL
    SELECT 2, 'Widget-B', 250.00
""")

print("Managed table: data location chosen by Spark/Hive")
display(spark.sql("DESCRIBE EXTENDED sales_db.orders_managed"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Create an external table on DBFS** — the data lives at a path you control.

# COMMAND ----------

dbfs_path = "/FileStore/tables/sales_db/orders_external"

# Write data to an explicit DBFS location (simulates data landing in external storage)
external_df = spark.createDataFrame(
    [(3, "Widget-C", 100.00), (4, "Widget-D", 350.00), (5, "Widget-E", 225.00)],
    ["order_id", "product", "amount"]
)
external_df.write.format("delta").mode("overwrite").save(dbfs_path)
print(f"Data written to: {dbfs_path}")

# Now create an external table pointing to that location
spark.sql(f"""
    CREATE OR REPLACE TABLE sales_db.orders_external
    USING DELTA
    LOCATION '{dbfs_path}'
""")

print("\nExternal table created — schema in Hive Metastore, data at DBFS path.")
print(f"DBFS Path: {dbfs_path}")
display(spark.sql("DESCRIBE EXTENDED sales_db.orders_external"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate Managed vs External Behavior

# COMMAND ----------

print("=" * 60)
print("BEFORE DROP: Both tables exist")
print("=" * 60)
display(spark.sql("SHOW TABLES IN sales_db"))

# Query data from both
print("\nManaged table data:")
display(spark.sql("SELECT * FROM sales_db.orders_managed"))
print("\nExternal table data:")
display(spark.sql("SELECT * FROM sales_db.orders_external"))

# COMMAND ----------

# Drop the managed table — data is deleted
spark.sql("DROP TABLE IF EXISTS sales_db.orders_managed")
print("Dropped MANAGED table — data is gone.")

# Drop the external table — only schema is removed
spark.sql("DROP TABLE IF EXISTS sales_db.orders_external")
print("Dropped EXTERNAL table — schema removed, data still on DBFS.")

# Re-register the external table from the same data
spark.sql(f"""
    CREATE OR REPLACE TABLE sales_db.orders_external
    USING DELTA
    LOCATION '{dbfs_path}'
""")
print("Re-registered external table from the same data — data survived the drop!")
display(spark.sql("SELECT COUNT(*) AS surviving_rows FROM sales_db.orders_external"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog External Location Syntax (Reference)
# MAGIC
# MAGIC ```sql
# MAGIC -- UC: Create a storage credential (requires account admin)
# MAGIC CREATE STORAGE CREDENTIAL aws_sales_cred
# MAGIC USING 'arn:aws:iam::123456789:role/databricks-sales-access';
# MAGIC
# MAGIC -- UC: Create an external location bound to the credential
# MAGIC CREATE EXTERNAL LOCATION sales_data_loc
# MAGIC URL 's3://my-company-sales-bucket/'
# MAGIC WITH (
# MAGIC     CREDENTIAL aws_sales_cred
# MAGIC )
# MAGIC COMMENT 'Sales department raw data landing zone';
# MAGIC
# MAGIC -- UC: Grant access to the external location
# MAGIC GRANT CREATE TABLE, READ FILES, WRITE FILES
# MAGIC ON EXTERNAL LOCATION sales_data_loc
# MAGIC TO `data_engineers`;
# MAGIC
# MAGIC -- UC: Create an external table on the governed location
# MAGIC CREATE TABLE sales.revenue.invoices
# MAGIC LOCATION 's3://my-company-sales-bucket/invoices/';
# MAGIC
# MAGIC -- UC: Show all external locations
# MAGIC SHOW EXTERNAL LOCATIONS;
# MAGIC
# MAGIC -- UC: Describe an external location
# MAGIC DESCRIBE EXTERNAL LOCATION sales_data_loc;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Create external locations for all cloud storage paths** used by Databricks.
# MAGIC 2. **Use managed tables for derived/internal data** — let UC handle lifecycle.
# MAGIC 3. **Use external tables for shared/ingested data** — you control the source.
# MAGIC 4. **Grant READ FILES / WRITE FILES** at the external location level, not per-bucket.
# MAGIC 5. **Never use service credentials directly in notebooks** — always go through UC external locations.
# MAGIC 6. **Audit external locations** to ensure no unauthorized cloud storage access exists.
# MAGIC
# MAGIC | Community Edition | Production Unity Catalog |
# MAGIC |---|---|
# MAGIC | DBFS as "external" storage | S3/ADLS/GCS via external locations |
# MAGIC | No storage credential abstraction | IAM roles, service principals, service accounts |
# MAGIC | `LOCATION '/path/'` in Hive Metastore | `LOCATION 's3://...'` governed by UC |
# MAGIC | No access control on paths | GRANT READ/WRITE FILES on external locations |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 67 — Service Principals & Managed Identity
# MAGIC
# MAGIC ### What Are Service Principals?
# MAGIC
# MAGIC A **service principal** is a non-human identity — an application, CI/CD pipeline, or scheduled job — that authenticates to Databricks using its own credentials rather than a user's personal account.
# MAGIC
# MAGIC ```
# MAGIC HUMAN IDENTITY:                    MACHINE IDENTITY:
# MAGIC alice@company.com                 sp-sales-etl-prod
# MAGIC   │ OIDC / SSO                        │ OAuth2 client credentials
# MAGIC   │ Personal token                     │ Secret + Client ID
# MAGIC   ▼                                   ▼
# MAGIC Databricks Workspace ◄─────────── Databricks Workspace
# MAGIC ```
# MAGIC
# MAGIC ### Why NOT Use Personal Accounts for Production?
# MAGIC
# MAGIC | Risk | Consequence |
# MAGIC |------|------------|
# MAGIC | Employee leaves company | Job breaks; token deleted; panic |
# MAGIC | Personal token leaked | Full user access compromised |
# MAGIC | No separation of duties | Alice's prod job runs as Alice, not as "ETL Pipeline" |
# MAGIC | Audit confusion | Who really ran this — Alice or her script? |
# MAGIC | Credential rotation | Personal tokens aren't managed by security teams |
# MAGIC
# MAGIC **Production workloads MUST run as service principals.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Service Principal Architecture
# MAGIC
# MAGIC **On Azure:**
# MAGIC ```
# MAGIC Azure AD App Registration → Service Principal
# MAGIC         │
# MAGIC         ├── Client ID + Secret/Certificate (for OAuth)
# MAGIC         ├── Managed Identity (Azure resource-level, no secret management)
# MAGIC         └── Assigned to Databricks workspace via SCIM/API
# MAGIC ```
# MAGIC
# MAGIC **On AWS:**
# MAGIC ```
# MAGIC AWS IAM Role → Instance Profile
# MAGIC         │
# MAGIC         ├── Trust policy: allows Databricks to assume this role
# MAGIC         ├── Attached to Databricks clusters at launch time
# MAGIC         └── Provides S3 access without embedding keys
# MAGIC ```
# MAGIC
# MAGIC **On GCP:**
# MAGIC ```
# MAGIC GCP Service Account
# MAGIC         │
# MAGIC         ├── JSON key + service account email
# MAGIC         ├── Assigned to Databricks workspace
# MAGIC         └── Provides GCS/BigQuery access
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition — Conceptual Simulation
# MAGIC
# MAGIC We can't create real service principals in CE, but we can demonstrate the **principle** of machine vs human identity and best practices.

# COMMAND ----------

# Simulate a service principal identity pattern
from datetime import datetime

print("=" * 60)
print("SERVICE PRINCIPAL IDENTITY SIMULATION")
print("=" * 60)

# In production, this would be the service principal's identity
# obtained via OAuth client credentials grant
simulated_sp_identity = {
    "service_principal_id": "sp-sales-etl-prod-abc123",
    "display_name": "Sales ETL Production Pipeline",
    "application_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "type": "service_principal",
    "assigned_groups": ["etl_jobs", "sales_data_readers"],
    "created_by": "security_team@company.com",
    "notebook_owner": "alice@company.com",  # This is WRONG in production!
}

print(f"""
PRODUCTION BEST PRACTICE: Service Principal Configuration
{'─' * 55}
ID:              {simulated_sp_identity['service_principal_id']}
Name:            {simulated_sp_identity['display_name']}
Type:            {simulated_sp_identity['type']}
Groups:          {', '.join(simulated_sp_identity['assigned_groups'])}
Managed by:      {simulated_sp_identity['created_by']}

WHAT TO AVOID:
  Notebook Owner: {simulated_sp_identity['notebook_owner']} ← PERSONAL ACCOUNT!
  
CORRECT PATTERN:
  Schedule job "as" the service principal, NOT as Alice.
  If Alice leaves, the job continues running.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Best Practices for Service Principal Usage

# COMMAND ----------

best_practices = """
BEST PRACTICES FOR SERVICE PRINCIPALS
════════════════════════════════════════════════════════════════════

1. NAMING CONVENTION
   sp-{domain}-{purpose}-{environment}
   Examples: sp-sales-etl-prod, sp-marketing-analytics-dev, sp-finance-reports-prod

2. PRINCIPLE OF LEAST PRIVILEGE
   - Each service principal gets ONLY the minimum privileges needed.
   - One SP per logical workload (not one SP for everything).
   - Use Unity Catalog GRANTs scoped to specific catalogs/schemas/tables.

3. SECRET MANAGEMENT
   - NEVER hardcode secrets in notebooks or config files.
   - Use Databricks secrets API: dbutils.secrets.get()
   - Rotate secrets on a schedule (90-day maximum).
   - On Azure, prefer Managed Identity (no secret to manage at all).

4. SEPARATION OF DUTIES
   - DEV service principal: can create/modify tables in dev catalogs only.
   - PROD service principal: read-only on raw data, write to downstream tables.
   - CI/CD service principal: can create schemas/tables in all environments.
   - Human users: interactive development only.

5. AUDIT TRAIL
   - Every action taken by a service principal is logged in system.access.audit.
   - Use the service_principal_id to distinguish machine from human actions.
   - Set up alerts for unusual SP activity (e.g., SP running interactive notebooks).

6. MONITORING & ROTATION
   - Track: last used, token expiry, unused SPs (stale identities).
   - Rotate: OAuth secrets every 90 days or less.
   - Revoke: immediately disable SP secrets if compromise is suspected.

7. TOKEN SCOPE
   - Service principal tokens should have minimal scope.
   - Use ACLs on the token itself (not just the SP).
   - Prefer short-lived tokens (OAuth flow) over long-lived PATs.
"""

print(best_practices)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Register service principals in your identity provider** (Azure AD, AWS IAM, GCP IAM).
# MAGIC 2. **Add them to your Databricks account** via SCIM provisioning or Account API.
# MAGIC 3. **Assign Unity Catalog privileges** — `GRANT SELECT ON TABLE ... TO \`sp-sales-etl-prod\``.
# MAGIC 4. **Schedule ALL production jobs** to run as service principals, never as users.
# MAGIC 5. **Use Databricks secrets** for OAuth credentials: `sp_client_id` and `sp_client_secret`.
# MAGIC 6. **Implement automated rotation** — Azure Key Vault, AWS Secrets Manager, or GCP Secret Manager.
# MAGIC 7. **Alert on personal-account jobs** — detect and migrate any production jobs running as users.
# MAGIC
# MAGIC | Community Edition | Production Unity Catalog |
# MAGIC |---|---|
# MAGIC | No service principal support | Full SP lifecycle via Account API |
# MAGIC | Jobs run as the notebook owner | Jobs run as service principals |
# MAGIC | No managed identity | Azure Managed Identity / AWS Instance Profiles |
# MAGIC | Limited token management | OAuth2 client credentials with secrets rotation |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 68 — Audit Logging
# MAGIC
# MAGIC ### Unity Catalog Audit Logging
# MAGIC
# MAGIC UC provides `system.access.audit` — a comprehensive audit log table that captures every operation:
# MAGIC
# MAGIC | Field | Description |
# MAGIC |-------|-------------|
# MAGIC | `event_time` | When the operation occurred |
# MAGIC | `user_identity.email` | Who performed the operation (human or service principal) |
# MAGIC | `action_name` | What action (e.g., `selectTable`, `createTable`, `grant`) |
# MAGIC | `request_params.commandText` | The SQL statement (for queries) |
# MAGIC | `request_params.table_full_name` | The table affected |
# MAGIC | `response.status_code` | HTTP status (200 = success) |
# MAGIC | `source_ip_address` | Client IP address |
# MAGIC | `service_name` | Which service (e.g., `unity-catalog`, `databricks-sql`) |
# MAGIC
# MAGIC Audit logs are critical for:
# MAGIC - **Compliance** (SOC2, HIPAA, GDPR, PCI-DSS)
# MAGIC - **Forensic investigation** ("Who accessed this table at 3 AM?")
# MAGIC - **Anomaly detection** ("Why did the CI/CD SP drop a production table?")
# MAGIC - **Access review** ("Is ANYONE still using this deprecated table?")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition — Manual Audit Logging System
# MAGIC
# MAGIC CE does not have `system.access.audit`. We build a manual audit log to demonstrate the concept.

# COMMAND ----------

# Create an audit log table
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.audit_log (
        event_id BIGINT GENERATED ALWAYS AS IDENTITY,
        event_time TIMESTAMP,
        user_identity STRING,
        action_name STRING,
        object_type STRING,
        object_name STRING,
        query_text STRING,
        status STRING,
        row_count BIGINT,
        execution_time_ms BIGINT,
        notebook_name STRING,
        cluster_id STRING
    )
    USING DELTA
""")

print("Audit log table created.")

# COMMAND ----------

# Build a decorator-like audit logging function
from datetime import datetime, timedelta
import time

def log_audit(action_name, object_type, object_name, query_text, status, row_count=0, execution_time_ms=0):
    """Record an audit entry — simulates UC system.access.audit"""
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    
    # Get notebook info (may not be available in all contexts)
    try:
        notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse("unknown")
    except:
        notebook_name = "interactive_session"
    
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
    
    audit_df = spark.createDataFrame(
        [(datetime.now(), current_user, action_name, object_type, object_name, 
          query_text, status, row_count, execution_time_ms, notebook_name, cluster_id)],
        ["event_time", "user_identity", "action_name", "object_type", "object_name",
         "query_text", "status", "row_count", "execution_time_ms", "notebook_name", "cluster_id"]
    )
    audit_df.write.mode("append").saveAsTable("sales_db.audit_log")

def execute_and_log(query, action_name, object_type, object_name):
    """Execute a query and log it to the audit table."""
    start = time.time()
    try:
        result = spark.sql(query)
        elapsed_ms = int((time.time() - start) * 1000)
        
        if action_name in ("selectTable", "describeTable"):
            row_count = result.count()
        else:
            row_count = -1  # DDL statements don't return rows
        
        log_audit(action_name, object_type, object_name, query, "SUCCESS", row_count, elapsed_ms)
        return result
    except Exception as e:
        elapsed_ms = int((time.time() - start) * 1000)
        log_audit(action_name, object_type, object_name, query, f"FAILED: {str(e)[:100]}", 0, elapsed_ms)
        raise

print("Audit logging functions defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate Audit Logging in Action

# COMMAND ----------

# Execute several operations that will be logged
print("Executing operations with audit logging...\n")

# 1. SELECT
execute_and_log("SELECT * FROM sales_db.revenue", "selectTable", "TABLE", "sales_db.revenue")
print("  [1] SELECT from revenue — logged")

# 2. CREATE TABLE
execute_and_log(
    "CREATE OR REPLACE TABLE sales_db.audit_test_table AS SELECT * FROM sales_db.revenue WHERE amount > 100",
    "createTable", "TABLE", "sales_db.audit_test_table"
)
print("  [2] CREATE TABLE audit_test_table — logged")

# 3. DESCRIBE
execute_and_log("DESCRIBE TABLE sales_db.revenue", "describeTable", "TABLE", "sales_db.revenue")
print("  [3] DESCRIBE revenue — logged")

# 4. ALTER TABLE
try:
    execute_and_log(
        "ALTER TABLE sales_db.revenue SET TBLPROPERTIES ('classification' = 'sales_data')",
        "alterTable", "TABLE", "sales_db.revenue"
    )
    print("  [4] ALTER TABLE revenue — logged")
except Exception as e:
    print(f"  [4] ALTER TABLE — {e}")

# 5. SELECT (another read)
execute_and_log("SELECT order_id, product FROM sales_db.pipeline", "selectTable", "TABLE", "sales_db.pipeline")
print("  [5] SELECT from pipeline — logged")

print("\nAll operations logged to sales_db.audit_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the Audit Log

# COMMAND ----------

print("=" * 70)
print("AUDIT LOG — Complete History")
print("=" * 70)
display(spark.sql("""
    SELECT event_id, event_time, user_identity, action_name, 
           object_name, status, execution_time_ms
    FROM sales_db.audit_log
    ORDER BY event_id
"""))

# COMMAND ----------

# Audit summarization queries
print("=" * 70)
print("AUDIT SUMMARY BY ACTION")
print("=" * 70)
display(spark.sql("""
    SELECT 
        action_name,
        COUNT(*) AS event_count,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
        SUM(CASE WHEN status LIKE 'FAILED%' THEN 1 ELSE 0 END) AS failure_count,
        AVG(execution_time_ms) AS avg_execution_ms
    FROM sales_db.audit_log
    GROUP BY action_name
    ORDER BY event_count DESC
"""))

# COMMAND ----------

print("=" * 70)
print("AUDIT — TABLE ACCESS PATTERNS")
print("=" * 70)
display(spark.sql("""
    SELECT 
        object_name,
        MIN(event_time) AS first_access,
        MAX(event_time) AS last_access,
        COUNT(*) AS total_operations,
        COUNT(DISTINCT user_identity) AS distinct_users
    FROM sales_db.audit_log
    GROUP BY object_name
    ORDER BY last_access DESC
"""))

# COMMAND ----------

print("=" * 70)
print("AUDIT — RECENT FAILURES (Last 25 entries)")
print("=" * 70)
display(spark.sql("""
    SELECT event_time, user_identity, action_name, object_name, 
           status, execution_time_ms
    FROM sales_db.audit_log
    WHERE status LIKE 'FAILED%'
    ORDER BY event_time DESC
    LIMIT 25
"""))

# COMMAND ----------

# Clean up the test table
spark.sql("DROP TABLE IF EXISTS sales_db.audit_test_table")
log_audit("dropTable", "TABLE", "sales_db.audit_test_table", "DROP TABLE audit_test_table", "SUCCESS")
print("Cleaned up audit_test_table — logged the drop.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Production Audit Logs Would Contain
# MAGIC
# MAGIC ```sql
# MAGIC -- UC: Query production audit logs
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     user_identity.email,
# MAGIC     action_name,
# MAGIC     request_params.table_full_name,
# MAGIC     request_params.commandText,
# MAGIC     response.status_code,
# MAGIC     source_ip_address,
# MAGIC     service_name
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC   AND action_name IN ('selectTable', 'createTable', 'dropTable', 'grant')
# MAGIC ORDER BY event_time DESC;
# MAGIC
# MAGIC -- UC: Find tables that haven't been accessed in 30 days (cleanup candidates)
# MAGIC SELECT request_params.table_full_name, MAX(event_time) AS last_accessed
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name = 'selectTable'
# MAGIC GROUP BY request_params.table_full_name
# MAGIC HAVING MAX(event_time) < CURRENT_DATE() - INTERVAL 30 DAYS;
# MAGIC
# MAGIC -- UC: Anomaly detection — tables dropped outside business hours
# MAGIC SELECT event_time, user_identity.email, request_params.table_full_name
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name = 'dropTable'
# MAGIC   AND HOUR(event_time) NOT BETWEEN 7 AND 19;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Enable audit logging** — it's on by default in all paid tiers, retained for 365 days.
# MAGIC 2. **Create alert queries** that run daily and flag suspicious activity (e.g., `dropTable` by unusual users).
# MAGIC 3. **Build an audit dashboard** in Databricks SQL with time-series charts of operations.
# MAGIC 4. **Integrate with SIEM systems** — export audit logs to Splunk, Azure Sentinel, or ELK Stack.
# MAGIC 5. **Set up retention policies** — archive old audit logs to cold storage for compliance.
# MAGIC 6. **Use audit data for cost analysis** — identify heavy query users and optimize.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 69 — Row Filters & Column Masks
# MAGIC
# MAGIC ### Unity Catalog Row Filters & Column Masks (UC 1.3+)
# MAGIC
# MAGIC Row filters and column masks are **table-level security policies** enforced by UDFs:
# MAGIC
# MAGIC **Row Filter:** A SQL UDF that returns TRUE for rows the user is allowed to see.
# MAGIC ```sql
# MAGIC CREATE FUNCTION sales_filter(dept STRING)
# MAGIC RETURN IS_MEMBER('hr_team')
# MAGIC     OR (IS_MEMBER('managers') AND dept IN ('Engineering', 'Sales'))
# MAGIC     OR CURRENT_USER() = CONCAT(dept, '_dept_head@company.com');
# MAGIC
# MAGIC ALTER TABLE hr.employee.data SET ROW FILTER sales_filter ON (dept);
# MAGIC ```
# MAGIC
# MAGIC **Column Mask:** A SQL UDF that returns the masked value for the column.
# MAGIC ```sql
# MAGIC CREATE FUNCTION ssn_mask(ssn STRING)
# MAGIC RETURN CASE
# MAGIC     WHEN IS_MEMBER('hr_team') THEN ssn
# MAGIC     ELSE 'XXX-XX-XXXX'
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE hr.employee.data ALTER COLUMN ssn SET MASK ssn_mask;
# MAGIC ```
# MAGIC
# MAGIC **Critical difference from dynamic views:** These are bolted onto the **base table** — every query against the table (including direct `SELECT *`) is automatically filtered. There is no way to "accidentally" bypass the policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition — Row Filtering & Column Masking via Views
# MAGIC
# MAGIC In CE, we implement the same functionality using views (since we can't ALTER TABLE with row filters/column masks).

# COMMAND ----------

# Create a comprehensive HR dataset with various sensitivity levels
spark.sql("""
    CREATE OR REPLACE TABLE hr_db.employee_complete AS
    SELECT 201 AS emp_id, 'Alice' AS name, 'Engineering' AS dept, 
           95000 AS salary, '123-45-6789' AS ssn, '2020-03-15' AS hire_date,
           'F' AS gender, 'Seattle' AS location, '555-0101' AS phone,
           4 AS performance_rating
    UNION ALL
    SELECT 202, 'Bob', 'Engineering', 105000, '234-56-7890', '2019-01-10',
           'M', 'Seattle', '555-0102', 5
    UNION ALL
    SELECT 203, 'Carol', 'Sales', 85000, '345-67-8901', '2021-06-01',
           'F', 'New York', '555-0103', 4
    UNION ALL
    SELECT 204, 'Dave', 'HR', 75000, '456-78-9012', '2018-11-20',
           'M', 'Chicago', '555-0104', 3
    UNION ALL
    SELECT 205, 'Eve', 'Finance', 110000, '567-89-0123', '2022-02-28',
           'F', 'Chicago', '555-0105', 5
    UNION ALL
    SELECT 206, 'Frank', 'Marketing', 72000, '678-90-1234', '2021-09-15',
           'M', 'New York', '555-0106', 3
""")

print("Employee complete table created:")
display(spark.sql("SELECT * FROM hr_db.employee_complete ORDER BY emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Role-Based Access Functions (Simulating UDF Row Filters / Column Masks)

# COMMAND ----------

# Create a role mapping table (simulates Unity Catalog group membership)
spark.sql("""
    CREATE OR REPLACE TABLE hr_db.role_permissions AS
    SELECT 'hr_admin' AS role, 'ALL_DEPTS' AS dept_access, 'FULL' AS salary_visibility, 'FULL' AS ssn_visibility
    UNION ALL
    SELECT 'manager', 'OWN_DEPT', 'VISIBLE', 'MASKED'
    UNION ALL
    SELECT 'analyst', 'ALL_DEPTS', 'MASKED', 'HIDDEN'
    UNION ALL
    SELECT 'employee', 'OWN_DEPT', 'HIDDEN', 'HIDDEN'
    UNION ALL
    SELECT 'contractor', 'NONE', 'HIDDEN', 'HIDDEN'
""")

# Set the simulated role for this demo
simulated_role = "analyst"  # Try: hr_admin, manager, analyst, employee, contractor
simulated_dept = "Engineering"  # For "OWN_DEPT" roles

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW current_role_config AS
    SELECT '{simulated_role}' AS role, '{simulated_dept}' AS user_dept
""")

# Load role permissions
role_perms = spark.sql(f"""
    SELECT * FROM hr_db.role_permissions WHERE role = '{simulated_role}'
""").collect()[0]

print(f"Simulated Role: {simulated_role}")
print(f"  Department Access: {role_perms['dept_access']}")
print(f"  Salary Visibility: {role_perms['salary_visibility']}")
print(f"  SSN Visibility:    {role_perms['ssn_visibility']}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Row Filter Equivalent — A View That Automatically Filters Rows

# COMMAND ----------

# Create a view that simulates a table with a ROW FILTER applied
# In UC: ALTER TABLE hr_db.employee_complete SET ROW FILTER dept_filter ON (dept);
# In CE: We create a view that embeds the filter logic.

spark.sql("""
    CREATE OR REPLACE VIEW hr_db.employee_row_filtered AS
    SELECT e.*
    FROM hr_db.employee_complete e
    CROSS JOIN hr_db.role_permissions r
    WHERE r.role = (SELECT role FROM current_role_config)
      AND (
          -- hr_admin: sees all departments
          r.dept_access = 'ALL_DEPTS'
          OR
          -- manager / employee: sees only their own department
          (r.dept_access = 'OWN_DEPT' AND e.dept = (SELECT user_dept FROM current_role_config))
      )
      AND r.dept_access != 'NONE'  -- contractors see nothing
""")

print(f"Employee Row-Filtered View — Simulated Role: {simulated_role}")
display(spark.sql("SELECT emp_id, name, dept FROM hr_db.employee_row_filtered ORDER BY emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Column Mask Equivalent — A View That Automatically Masks Columns

# COMMAND ----------

# Create a view that simulates COLUMN MASKS on salary, ssn, and phone
spark.sql("""
    CREATE OR REPLACE VIEW hr_db.employee_column_masked AS
    SELECT
        e.emp_id,
        e.name,
        e.dept,
        -- Salary masking
        CASE r.salary_visibility
            WHEN 'FULL' THEN CAST(e.salary AS STRING)
            WHEN 'VISIBLE' THEN CAST(e.salary AS STRING)
            WHEN 'MASKED' THEN CONCAT('$', CAST(ROUND(e.salary / 1000) * 1000 AS STRING), ' (band)')
            ELSE 'REDACTED'
        END AS salary_display,
        -- SSN masking
        CASE r.ssn_visibility
            WHEN 'FULL' THEN e.ssn
            WHEN 'MASKED' THEN CONCAT('***-**-', RIGHT(e.ssn, 4))
            ELSE 'XXX-XX-XXXX'
        END AS ssn_display,
        -- Phone masking
        CASE r.ssn_visibility
            WHEN 'FULL' THEN e.phone
            WHEN 'MASKED' THEN CONCAT('***-', RIGHT(e.phone, 4))
            ELSE 'N/A'
        END AS phone_display,
        e.hire_date,
        e.location,
        -- Performance rating visible to managers and HR
        CASE 
            WHEN r.dept_access = 'ALL_DEPTS' THEN CAST(e.performance_rating AS STRING)
            WHEN r.dept_access = 'OWN_DEPT' THEN CAST(e.performance_rating AS STRING)
            ELSE 'HIDDEN'
        END AS performance_rating_display
    FROM hr_db.employee_complete e
    CROSS JOIN hr_db.role_permissions r
    WHERE r.role = (SELECT role FROM current_role_config)
""")

print(f"Employee Column-Masked View — Simulated Role: {simulated_role}")
display(spark.sql("SELECT * FROM hr_db.employee_column_masked ORDER BY emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Complete Security View — Row Filter + Column Mask Combined

# COMMAND ----------

# The "complete" security view: both row filtering AND column masking in one view.
# This is the pattern you'd typically enforce with UC row filters + column masks on the base table.

spark.sql("""
    CREATE OR REPLACE VIEW hr_db.employee_secure AS
    SELECT
        -- Non-sensitive: visible to all authorized users
        e.emp_id,
        e.name,
        e.dept,
        e.hire_date,
        e.location,
        e.gender,
        -- Sensitive: masked based on role
        CASE r.salary_visibility
            WHEN 'FULL' THEN CAST(e.salary AS STRING)
            WHEN 'VISIBLE' THEN CONCAT('$', FORMAT_NUMBER(e.salary, 0))
            WHEN 'MASKED' THEN CONCAT('$', CAST(ROUND(e.salary / 5000) * 5000 AS STRING), ' band')
            ELSE 'REDACTED'
        END AS salary,
        CASE r.ssn_visibility
            WHEN 'FULL' THEN e.ssn
            WHEN 'MASKED' THEN CONCAT('***-**-', RIGHT(e.ssn, 4))
            ELSE 'XXX-XX-XXXX'
        END AS ssn,
        CASE r.ssn_visibility
            WHEN 'FULL' THEN e.phone
            WHEN 'MASKED' THEN CONCAT('***-', RIGHT(e.phone, 4))
            ELSE 'N/A'
        END AS phone,
        CASE 
            WHEN r.salary_visibility IN ('FULL', 'VISIBLE') THEN CAST(e.performance_rating AS STRING)
            ELSE 'HIDDEN'
        END AS performance_rating
    FROM hr_db.employee_complete e
    CROSS JOIN hr_db.role_permissions r
    WHERE r.role = (SELECT role FROM current_role_config)
      AND (
          r.dept_access = 'ALL_DEPTS'
          OR (r.dept_access = 'OWN_DEPT' AND e.dept = (SELECT user_dept FROM current_role_config))
      )
      AND r.dept_access != 'NONE'
""")

print(f"=" * 70)
print(f"FULL SECURITY VIEW — Role: {simulated_role} | Dept: {simulated_dept}")
print(f"=" * 70)
display(spark.sql("SELECT * FROM hr_db.employee_secure ORDER BY emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test All Five Roles Through the Secure View

# COMMAND ----------

test_roles = [
    ("hr_admin", "Engineering"),
    ("manager", "Engineering"),
    ("analyst", "Engineering"),
    ("employee", "Engineering"),
    ("contractor", "Engineering"),
]

print("=" * 72)
print(f"{'Role':<16} {'Rows':>6} {'Salary':>12} {'SSN':>14} {'Phone':>14} {'Perf':>6}")
print("-" * 72)

for role, dept in test_roles:
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW current_role_config AS
        SELECT '{role}' AS role, '{dept}' AS user_dept
    """)
    
    rows = spark.sql("SELECT COUNT(*) FROM hr_db.employee_secure").collect()[0][0]
    salary_example = spark.sql("SELECT salary FROM hr_db.employee_secure WHERE emp_id = 201").collect()
    salary_val = salary_example[0][0] if salary_example else "N/A"
    ssn_example = spark.sql("SELECT ssn FROM hr_db.employee_secure WHERE emp_id = 201").collect()
    ssn_val = ssn_example[0][0] if ssn_example else "N/A"
    perf_example = spark.sql("SELECT performance_rating FROM hr_db.employee_secure WHERE emp_id = 201").collect()
    perf_val = perf_example[0][0] if perf_example else "N/A"
    
    print(f"{role:<16} {rows:>6} {str(salary_val)[:12]:>12} {str(ssn_val)[:14]:>14} {'***' if ssn_val == 'XXX-XX-XXXX' else ssn_val[:14]:>14} {str(perf_val)[:6]:>6}")

print("-" * 72)

# Reset to hr_admin for remaining demos
spark.sql("""
    CREATE OR REPLACE TEMP VIEW current_role_config AS
    SELECT 'hr_admin' AS role, 'Engineering' AS user_dept
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### When to Use Row Filters / Column Masks vs Dynamic Views
# MAGIC
# MAGIC | Scenario | Best Approach | Why |
# MAGIC |----------|--------------|-----|
# MAGIC | One audience needs tailored columns | Dynamic View | View is purpose-built for that audience |
# MAGIC | **All** queries must filter by region | **Row Filter on base table** | No way to bypass — even ad-hoc queries get filtered |
# MAGIC | SSN must **never** be exposed to non-HR | **Column Mask on base table** | Absolute enforcement at the table level |
# MAGIC | Multiple different rollups for different teams | Dynamic Views | Each team gets their own view |
# MAGIC | Regulatory compliance (GDPR, HIPAA) | **Row Filter + Column Mask** | Mandatory, universal, non-circumventable |
# MAGIC | Quick prototype or dashboard | Dynamic View | Simpler to create and iterate |
# MAGIC | Data scientists doing exploratory analysis | Row Filter on base table | They query the table directly but see only allowed data |
# MAGIC
# MAGIC **Rule of thumb:**
# MAGIC - **Regulatory / mandatory policies** → Row filters + column masks on base tables
# MAGIC - **Convenience / audience-specific views** → Dynamic views
# MAGIC - **Both can coexist** — base table has masks, views provide convenient rollups on top.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog Row Filter & Column Mask Syntax (Reference)
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Create a FILTER UDF (catalog-qualified)
# MAGIC CREATE OR REPLACE FUNCTION hr_prod.employees.region_filter(region STRING)
# MAGIC RETURN IS_MEMBER('hr_admins')
# MAGIC     OR (IS_MEMBER('us_managers') AND region = 'US')
# MAGIC     OR (IS_MEMBER('eu_managers') AND region = 'EU');
# MAGIC
# MAGIC -- Step 2: Apply the row filter to the table
# MAGIC ALTER TABLE hr_prod.employees.data
# MAGIC SET ROW FILTER hr_prod.employees.region_filter ON (region);
# MAGIC
# MAGIC -- Step 3: Create a MASK UDF
# MAGIC CREATE OR REPLACE FUNCTION hr_prod.employees.ssn_mask(ssn STRING)
# MAGIC RETURN CASE
# MAGIC     WHEN IS_MEMBER('hr_admins') THEN ssn
# MAGIC     WHEN IS_MEMBER('payroll') THEN CONCAT('***-**-', RIGHT(ssn, 4))
# MAGIC     ELSE NULL
# MAGIC END;
# MAGIC
# MAGIC -- Step 4: Apply the column mask
# MAGIC ALTER TABLE hr_prod.employees.data
# MAGIC ALTER COLUMN ssn SET MASK hr_prod.employees.ssn_mask;
# MAGIC
# MAGIC -- Show row filters on a table
# MAGIC SHOW ROW FILTERS ON TABLE hr_prod.employees.data;
# MAGIC
# MAGIC -- Remove a row filter
# MAGIC ALTER TABLE hr_prod.employees.data DROP ROW FILTER region_filter;
# MAGIC
# MAGIC -- Remove a column mask
# MAGIC ALTER TABLE hr_prod.employees.data ALTER COLUMN ssn DROP MASK;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Apply row filters on ALL tables containing regional or departmental data** — never rely on users to self-filter.
# MAGIC 2. **Apply column masks on ALL PII columns** — SSN, salary, phone, email, address, IP address.
# MAGIC 3. **Create the UDFs in the same schema as the tables** for clean organization.
# MAGIC 4. **Test with multiple user personas** before deploying to production.
# MAGIC 5. **Document each policy** — what it filters, why, and who is exempt.
# MAGIC 6. **Regularly review** with `SHOW ROW FILTERS` and `SHOW COLUMN MASKS` to ensure policies are still correct.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept 70 — Delta Sharing
# MAGIC
# MAGIC ### What Is Delta Sharing?
# MAGIC
# MAGIC Delta Sharing is an **open protocol** for securely sharing live data across organizations without copying it. It's built on Delta Lake and works with any platform that implements the sharing protocol (not just Databricks).
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────┐          ┌─────────────────────────┐
# MAGIC │  DATA PROVIDER       │          │  DATA RECIPIENT           │
# MAGIC │  (your company)      │          │  (partner/customer)       │
# MAGIC │                      │          │                           │
# MAGIC │  ┌─────────────┐    │  Share   │  ┌──────────────────┐    │
# MAGIC │  │ Unity Catalog│────┼──────────┼──│ Databricks        │    │
# MAGIC │  │   Tables     │    │ Activate │  │ (same/diff cloud) │    │
# MAGIC │  └─────────────┘    │  Link    │  │                   │    │
# MAGIC │                      │──────────┼──│ OR: Power BI      │    │
# MAGIC │  Recipients:         │          │  │ OR: pandas 🐼     │    │
# MAGIC │   - partner@acme.com│          │  │ OR: Spark         │    │
# MAGIC │   - client@globex.io│          │  │ OR: Trino/Presto  │    │
# MAGIC └─────────────────────┘          └─────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key properties:**
# MAGIC - **No data copy** — recipient queries LIVE data (table changes visible immediately)
# MAGIC - **Open protocol** — works with open-source Delta Sharing connectors
# MAGIC - **Cross-platform** — Spark, pandas, Power BI, Tableau, Trino all supported
# MAGIC - **Cross-cloud** — provider on AWS, recipient on Azure (or vice versa)
# MAGIC - **Granular** — share specific tables, partitions, or even row-filtered subsets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition — Simulation of Delta Sharing
# MAGIC
# MAGIC CE cannot create actual Delta Shares. We simulate the concept by:
# MAGIC 1. Creating a "shareable" dataset (read-only view)
# MAGIC 2. Simulating the provider/recipient flow
# MAGIC 3. Demonstrating what the production code would look like

# COMMAND ----------

# Create a clean dataset that could be shared externally
spark.sql("""
    CREATE OR REPLACE TABLE sales_db.shared_sales_summary AS
    SELECT
        product,
        SUM(amount) AS total_revenue,
        COUNT(*) AS order_count,
        AVG(amount) AS avg_order_value,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order
    FROM sales_db.revenue
    GROUP BY product
""")

print("Shared sales summary dataset: (provider's data that gets shared)")
display(spark.sql("SELECT * FROM sales_db.shared_sales_summary ORDER BY product"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simulate the Sharing Flow — Provider Side

# COMMAND ----------

print("=" * 70)
print("DELTA SHARING SIMULATION — Provider Side")
print("=" * 70)

# In production Databricks, the provider would do these steps:

provider_steps = """
PROVIDER STEPS (your company shares data):
──────────────────────────────────────────

[1] CREATE SHARE — define the share container:
    CREATE SHARE sales_insights_share
    COMMENT 'Monthly sales insights for partners';

[2] ADD TABLES TO THE SHARE:
    ALTER SHARE sales_insights_share ADD TABLE sales_db.shared_sales_summary;
    ALTER SHARE sales_insights_share ADD TABLE sales_db.revenue_monthly;

    -- Add with partition-based filtering (share only 2025 data):
    ALTER SHARE sales_insights_share ADD TABLE sales_db.revenue_monthly
    WITH RESTRICTION revenue_monthly.order_month >= '2025-01-01';

[3] ADD RECIPIENTS:
    CREATE RECIPIENT acme_corp USING ID 'acme-data-team@acme.com';
    CREATE RECIPIENT globex_inc USING ID 'analytics@globex.io';

[4] GRANT ACCESS to the share:
    GRANT SELECT ON SHARE sales_insights_share TO RECIPIENT acme_corp;
    GRANT SELECT ON SHARE sales_insights_share TO RECIPIENT globex_inc;

[5] RECIPIENT ACTIVATES:
    - Recipient receives an activation link via email
    - They click to download a credential file
    - They configure their Databricks workspace or open-source client
"""

print(provider_steps)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simulate the Sharing Flow — Recipient Side

# COMMAND ----------

# The "recipient" would get a credential file like this:
recipient_credential_example = """
RECIPIENT SIDE (partner consumes shared data):
─────────────────────────────────────────────

Received credential file: acme_databricks_share.config
{
  "shareCredentialsVersion": 1, 
  "endpoint": "https://adb-123456789.10.azuredatabricks.net/api/2.0/unity-catalog/shares",
  "bearerToken": "dapi1234abcd...",
  "expirationTime": "2026-05-07T00:00:00Z"
}

On their Databricks workspace (or any Delta Sharing client):

[1] Create a CATALOG from the share:
    CREATE CATALOG sales_from_partner
    USING SHARE acme_corp.sales_insights_share;

[2] Query the shared data AS IF IT WERE LOCAL:
    SELECT * FROM sales_from_partner.shared_sales_summary;
    SELECT product, SUM(total_revenue) 
    FROM sales_from_partner.revenue_monthly 
    GROUP BY product;

[3] Join shared data with their own data:
    SELECT 
        c.customer_name,
        s.product,
        s.total_revenue
    FROM my_catalog.accounts.customers c
    JOIN sales_from_partner.revenue_monthly s
      ON c.region = s.region;

WITH OPEN-SOURCE CLIENTS (Python):
    import delta_sharing
    client = delta_sharing.SharingClient(profile_file)
    table_url = profile_file + "#sales_db.shared_sales_summary"
    df = delta_sharing.load_as_pandas(table_url)
    print(df.head())
"""

print(recipient_credential_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate a "Shared" Read-Only View (CE Equivalent)

# COMMAND ----------

# In CE, the closest we can get to sharing is creating a view that
# represents what an external partner would be allowed to see.

# This view represents the data as the recipient would query it:
spark.sql("""
    CREATE OR REPLACE VIEW sales_db.partner_view AS
    SELECT
        product,
        total_revenue,
        order_count,
        CONCAT('$', FORMAT_NUMBER(avg_order_value, 2)) AS avg_order_value,
        first_order,
        last_order
    FROM sales_db.shared_sales_summary
    ORDER BY total_revenue DESC
    LIMIT 10
""")

print("SIMULATED PARTNER VIEW — What a Delta Share recipient would query:")
print("(In real Delta Sharing, they'd query the actual LIVE table, not a static view)\n")
display(spark.sql("SELECT * FROM sales_db.partner_view"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Live Tables vs Snapshots
# MAGIC
# MAGIC | Feature | Delta Sharing (Live) | Traditional Data Export (Snapshot) |
# MAGIC |---------|---------------------|-----------------------------------|
# MAGIC | **Data freshness** | Real-time (live table) | Stale (point-in-time copy) |
# MAGIC | **Data volume** | Recipient pays only for queries | Full copy transferred |
# MAGIC | **Storage duplication** | None — reads from provider | Duplicate storage needed |
# MAGIC | **Schema evolution** | Automatically visible | Manual re-export required |
# MAGIC | **Revocation** | Immediate — revoke share access | Cannot retract sent files |
# MAGIC | **Auditability** | Provider sees all recipient queries | No visibility after export |
# MAGIC | **Security** | Token-based, encrypted in transit | Files could be forwarded |
# MAGIC | **Governance** | UC policies enforced at read time | No enforcement after copy |
# MAGIC
# MAGIC **Winner for regulatory data sharing:** Delta Sharing — revocable, auditable, always up-to-date.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Sharing Architecture Diagram
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────── PROVIDER ──────────────────┐
# MAGIC │                                               │
# MAGIC │  Unity Catalog                                │
# MAGIC │  ┌──────────────────────────────────────┐    │
# MAGIC │  │ SHARE: sales_insights_share            │    │
# MAGIC │  │  ├── TABLE: shared_sales_summary       │    │
# MAGIC │  │  └── TABLE: revenue_monthly (>=2025)  │    │
# MAGIC │  └──────────────────────────────────────┘    │
# MAGIC │                                               │
# MAGIC │  Recipients:                                  │
# MAGIC │   ├── acme_corp (CREATE CATALOG ... USING)    │
# MAGIC │   └── globex_inc                              │
# MAGIC └───────────────────┬───────────────────────────┘
# MAGIC                     │
# MAGIC               Delta Sharing Protocol
# MAGIC               (REST API / gRPC)
# MAGIC                     │
# MAGIC ┌───────────────────┴───────────────────────────┐
# MAGIC │ RECIPIENT: acme_corp                           │
# MAGIC │                                                 │
# MAGIC │  Databricks Catalog: sales_from_partner         │
# MAGIC │   ├── shared_sales_summary ← LIVE QUERY        │
# MAGIC │   └── revenue_monthly      ← FILTERED (>=2025) │
# MAGIC │                                                 │
# MAGIC │  OR: Open-Source Client (pandas, Spark)         │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### What You'd Do in Production
# MAGIC
# MAGIC 1. **Identify shareable data** — clean, aggregated datasets (never raw PII, never unprocessed logs).
# MAGIC 2. **Create shares by audience** — one share per partner, or one share per data product.
# MAGIC 3. **Use partition restrictions** — share only agreed-upon date ranges or regions.
# MAGIC 4. **Monitor recipient usage** — track query patterns via `system.access.audit`.
# MAGIC 5. **Set up expiration** — recipient tokens have expiry; rotate regularly.
# MAGIC 6. **Document the data contract** — what columns mean, refresh frequency, SLAs.
# MAGIC 7. **Test with open-source Delta Sharing** — verify the share works from a non-Databricks client before handing to partners.
# MAGIC 8. **Revoke promptly** — if a partnership ends, `DROP RECIPIENT` / `REVOKE SELECT ON SHARE` immediately.
# MAGIC
# MAGIC | Step | Command |
# MAGIC |------|---------|
# MAGIC | Create share | `CREATE SHARE sales_insights_share;` |
# MAGIC | Add table | `ALTER SHARE ... ADD TABLE catalog.schema.table;` |
# MAGIC | Create recipient | `CREATE RECIPIENT partner USING ID 'partner@domain.com';` |
# MAGIC | Grant select | `GRANT SELECT ON SHARE sales_insights_share TO RECIPIENT partner;` |
# MAGIC | Recipient mounts | `CREATE CATALOG shared_data USING SHARE partner.sales_insights_share;` |
# MAGIC | Revoke | `REVOKE SELECT ON SHARE sales_insights_share FROM RECIPIENT partner;` |
# MAGIC | Remove table | `ALTER SHARE sales_insights_share REMOVE TABLE catalog.schema.table;` |
# MAGIC | Drop share | `DROP SHARE sales_insights_share;` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Unity Catalog & Governance
# MAGIC
# MAGIC ### Concepts Covered (#61–#70)
# MAGIC
# MAGIC | # | Concept | Key Takeaway |
# MAGIC |---|---------|-------------|
# MAGIC | 61 | **Three-Level Namespace** | `catalog.schema.table` organizes data; UC maps to org structure |
# MAGIC | 62 | **Permission Model** | Privileges cascade catalog→schema→table; use groups, not users |
# MAGIC | 63 | **Dynamic Views** | `IS_MEMBER()` and `CURRENT_USER()` embed security logic in views |
# MAGIC | 64 | **Data Lineage** | UC auto-tracks column-level lineage; critical for impact analysis |
# MAGIC | 65 | **Information Schema** | `system.information_schema.*` provides ANSI-standard metadata queries |
# MAGIC | 66 | **External Locations** | Governs cloud storage paths through UC; managed vs. external tables |
# MAGIC | 67 | **Service Principals** | Machine identities for production; NEVER use personal accounts for jobs |
# MAGIC | 68 | **Audit Logging** | `system.access.audit` records every operation for compliance |
# MAGIC | 69 | **Row Filters & Column Masks** | Table-level security policies; non-circumventable |
# MAGIC | 70 | **Delta Sharing** | Open protocol for live, cross-org data sharing without data copy |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Community Edition vs Production Unity Catalog — Comparison Matrix
# MAGIC
# MAGIC | Feature | Community Edition (Hive Metastore) | Production (Unity Catalog) |
# MAGIC |---------|----------------------------------|----------------------------|
# MAGIC | **Namespace** | `database.table` (2-level) | `catalog.schema.table` (3-level) |
# MAGIC | **Permissions** | Limited Hive GRANT/REVOKE | Full RBAC with inheritance |
# MAGIC | **Data lineage** | Manual tracking only | Automatic column-level lineage |
# MAGIC | **Metadata** | `SHOW`/`DESCRIBE` commands | `system.information_schema.*` (ANSI SQL) |
# MAGIC | **External locations** | Direct `LOCATION '/path/'` | UC-governed storage credentials + locations |
# MAGIC | **Service principals** | Not available | Full support via Account API |
# MAGIC | **Audit logging** | Must build manually | `system.access.audit` (automatic) |
# MAGIC | **Row-level security** | Views only | Row filters + column masks on tables |
# MAGIC | **Cross-org sharing** | Not available | Delta Sharing (open protocol) |
# MAGIC | **Cost** | Free (limited resources) | Paid DBU + cloud costs |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Self-Assessment — Test Your Understanding
# MAGIC
# MAGIC Answer these questions to validate your learning:

# COMMAND ----------

questions = r"""
GOVERNANCE SELF-ASSESSMENT
══════════════════════════════════════════════════════════

Q1: What is the three-level namespace in Unity Catalog?
    A) database.schema.table
    B) catalog.schema.table          ← CORRECT
    C) workspace.catalog.table
    D) project.dataset.table

Q2: What function checks if the current user belongs to a UC group?
    A) current_role()
    B) IS_MEMBER('group_name')       ← CORRECT
    C) user_in_group()
    D) has_permission()

Q3: In Unity Catalog, what happens when you DROP a MANAGED table?
    A) Only schema is deleted
    B) Schema AND data are deleted   ← CORRECT
    C) Table is renamed
    D) Table is shared

Q4: What is the key advantage of row filters over dynamic views?
    A) Better performance
    B) Cannot be bypassed (table-level) ← CORRECT
    C) Easier to write
    D) Support more SQL functions

Q5: What does Delta Sharing use for data transfer?
    A) SFTP
    B) Email attachments
    C) Open REST/gRPC protocol       ← CORRECT
    D) VPN tunnel

Q6: Why should production jobs use service principals?
    A) They are faster
    B) They survive employee departure   ← CORRECT
    C) They are free
    D) Required by Databricks

Q7: What system table provides audit logs in Unity Catalog?
    A) system.audit.logs
    B) system.access.audit           ← CORRECT
    C) system.security.events
    D) system.governance.trail

Q8: What is the difference between USE CATALOG and USE SCHEMA?
    A) No difference
    B) USE CATALOG sets the top-level container; USE SCHEMA sets the 
       namespace within it.                                      ← CORRECT
    C) USE CATALOG is for tables, USE SCHEMA is for views
    D) USE CATALOG requires admin privileges

Q9: What is the primary purpose of an external location in UC?
    A) Speed up queries
    B) Govern access to cloud storage paths   ← CORRECT
    C) Create temporary tables
    D) Share data with external partners

Q10: Can you apply BOTH a row filter and a column mask to the same table?
    A) No, tables can have only one security policy
    B) Yes, they coexist on the same table     ← CORRECT
    C) Only in Premium tier
    D) Only on external tables
"""

print(questions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Governance Principles — Cheat Sheet
# MAGIC
# MAGIC ```
# MAGIC 1. LEAST PRIVILEGE
# MAGIC    Grant minimum access needed. Start from zero. Expand only with justification.
# MAGIC
# MAGIC 2. GROUPS, NOT USERS
# MAGIC    Never grant privileges to individuals. Use account groups. Manage membership centrally.
# MAGIC
# MAGIC 3. MACHINE IDENTITIES
# MAGIC    Production jobs → Service principals. Interactive work → User accounts. Never mix.
# MAGIC
# MAGIC 4. DEFENSE IN DEPTH
# MAGIC    Dynamic views for UX, row filters + column masks for enforcement. Layer them.
# MAGIC
# MAGIC 5. AUDIT EVERYTHING
# MAGIC    If it's not logged, it didn't happen. Use system.access.audit for compliance.
# MAGIC
# MAGIC 6. CATALOG TOPOLOGY MATTERS
# MAGIC    Design catalog/schema hierarchy to match your org. Plan before creating tables.
# MAGIC
# MAGIC 7. MANAGED vs EXTERNAL
# MAGIC    Managed tables: UC handles lifecycle. External tables: you keep the files.
# MAGIC
# MAGIC 8. SHARE WITHOUT COPYING
# MAGIC    Delta Sharing for live, revocable data sharing. No more CSV exports.
# MAGIC
# MAGIC 9. METADATA IS POWER
# MAGIC    information_schema tells you everything about your data estate. Query it regularly.
# MAGIC
# MAGIC 10. LINEAGE IS AUTOMATIC
# MAGIC     UC traces column-level lineage without instrumentation. Use it for impact analysis.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup — Drop Objects Created in This Notebook

# COMMAND ----------

print("Cleaning up objects created in this notebook...\n")

# Drop tables created for demonstrations
tables_to_drop = [
    "sales_db.revenue",
    "sales_db.pipeline",
    "sales_db.revenue_raw",
    "sales_db.revenue_clean",
    "sales_db.revenue_monthly",
    "sales_db.shared_sales_summary",
    "sales_db.data_lineage",
    "sales_db.audit_log",
    "sales_db.orders_managed",
    "sales_db.orders_external",
    "sales_db.audit_test_table",
    "hr_db.employees",
    "hr_db.payroll",
    "hr_db.employee_sensitive",
    "hr_db.employee_complete",
    "hr_db.access_control",
    "hr_db.role_permissions",
    "marketing_db.campaigns",
]

for tbl in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        print(f"  DROPPED: {tbl}")
    except Exception as e:
        print(f"  SKIPPED: {tbl} — {str(e)[:60]}")

# Drop views
views_to_drop = [
    "sales_db.executive_dashboard",
    "sales_db.partner_view",
    "hr_db.employee_view",
    "hr_db.employee_by_dept",
    "hr_db.employee_row_filtered",
    "hr_db.employee_column_masked",
    "hr_db.employee_secure",
]

for view in views_to_drop:
    try:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
        print(f"  DROPPED VIEW: {view}")
    except Exception as e:
        print(f"  SKIPPED: {view} — {str(e)[:60]}")

# Drop databases (only if empty after table drops)
for db in ["sales_db", "hr_db", "marketing_db"]:
    try:
        spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
        print(f"  DROPPED DATABASE: {db}")
    except Exception as e:
        print(f"  SKIPPED DATABASE: {db} — {str(e)[:60]}")

print("\nCleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## End of Notebook — 07 Unity Catalog & Governance
# MAGIC
# MAGIC **What you learned:**
# MAGIC - The three-level catalog namespace and how it maps to organizational structure
# MAGIC - Unity Catalog's privilege model with cascade inheritance
# MAGIC - Building dynamic views for row-level and column-level security
# MAGIC - Automatic data lineage and manual lineage tracking techniques
# MAGIC - Querying information schema for metadata discovery and compliance
# MAGIC - External locations and the managed vs external table distinction
# MAGIC - Service principals as non-human identities for production workloads
# MAGIC - Audit logging for compliance and forensic investigation
# MAGIC - Row filters and column masks as table-level security enforcement
# MAGIC - Delta Sharing for live, cross-organization data sharing
# MAGIC
# MAGIC **Next:** Move to the capstone project — apply everything you've learned in a comprehensive end-to-end pipeline!

# COMMAND ----------
