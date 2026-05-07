# 07 — Unity Catalog & Governance

**Concepts Covered:** #61–#70  
**Environment:** Databricks Community Edition (single-node, free, **no Unity Catalog**)  
**Goal:** Master data governance, security, and sharing principles on Databricks.

**CRITICAL:** Databricks Community Edition uses the **legacy Hive Metastore**. Unity Catalog is available only on paid workspaces (Premium/Enterprise tiers on AWS, Azure, or GCP). Throughout this notebook, we explain what Unity Catalog provides and then show the Hive Metastore equivalent that runs in Community Edition.

| # | Concept | Difficulty | Type |
|---|---------|------------|------|
| 61 | Three-Level Namespace | Easy | Conceptual + Hands-on |
| 62 | Permission Model & Inheritance | Medium | Conceptual + Hands-on |
| 63 | Dynamic Views for Security | Medium | Hands-on |
| 64 | Data Lineage | Medium | Conceptual + Hands-on |
| 65 | Information Schema & Metadata Queries | Medium | Hands-on |
| 66 | External Locations & Storage Credentials | Medium | Conceptual + Hands-on |
| 67 | Service Principals & Managed Identity | Medium | Conceptual |
| 68 | Audit Logging | Medium | Hands-on |
| 69 | Row Filters & Column Masks | Hard | Hands-on |
| 70 | Delta Sharing | Hard | Conceptual + Hands-on |

```python

```
## Concept 61 — Three-Level Namespace

### Unity Catalog: `catalog.schema.table`

Unity Catalog introduces a **three-level namespace** for organizing data assets:

```
catalog.schema.table
   │       │      │
   │       │      └── Table, View, Function, Model, Volume
   │       └───────── Schema (logical grouping, like a database)
   └───────────────── Catalog (top-level container, maps to an org unit)
```

**How catalogs map to organizational structure:**

| Catalog | Purpose | Example Schemas |
|---------|---------|-----------------|
| `main` | Default catalog | `default`, `public` |
| `sales` | Sales department data | `revenue`, `pipeline`, `forecast` |
| `marketing` | Marketing data | `campaigns`, `analytics`, `segments` |
| `hr_prod` | Production HR data (restricted) | `employees`, `payroll`, `benefits` |
| `hr_dev` | HR development/sandbox data | `employees`, `payroll`, `benefits` |

### Hive Metastore: Two-Level Namespace (Community Edition)

Community Edition uses the legacy two-level namespace: `database.table`

```
database.table
   │        │
   │        └── Table or View
   └─────────── Database (equivalent to UC "schema")
```

```python

```
#### Hive Metastore Equivalent — Namespace Navigation

```python

# Show all databases (equivalent to UC "SHOW CATALOGS" + "SHOW SCHEMAS")
print("=" * 60)
print("DATABASES AVAILABLE (Hive Metastore)")
print("=" * 60)
display(spark.sql("SHOW DATABASES"))

```

```python

# Use the default database and show its tables
spark.sql("USE default")
print("Tables in 'default' database:")
display(spark.sql("SHOW TABLES IN default"))

```

```python

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

```

```python

```
#### Show Tables Across Databases

```python

for db in ["default", "sales_db", "hr_db", "marketing_db"]:
    print(f"\n{'=' * 50}")
    print(f"  Database: {db}")
    print(f"{'=' * 50}")
    try:
        display(spark.sql(f"SHOW TABLES IN {db}"))
    except:
        print(f"  (no tables or database not found)")

```

```python

```
### Unity Catalog Syntax Reference (Not Executable in CE)

The following is the Unity Catalog syntax — **cannot run** in Community Edition but shows what you would use in production:

```sql
-- Unity Catalog: three-level references
SELECT * FROM main.default.my_table;
SELECT * FROM sales_db.revenue.orders;

-- Set default catalog
USE CATALOG main;

-- Set default schema within catalog
USE SCHEMA default;

-- Show all catalogs
SHOW CATALOGS;

-- Show schemas in a catalog
SHOW SCHEMAS IN main;

-- Show tables in a schema
SHOW TABLES IN main.default;

-- Fully-qualified create
CREATE TABLE sales.revenue.orders (
    order_id BIGINT,
    amount DECIMAL(10,2),
    order_date DATE
);
```

```python

```
### What You'd Do in Production

1. **Design catalog topology** matching your organizational structure (departments, business units, data domains).
2. **Use catalogs for data isolation** — separate `_prod` and `_dev` catalogs for the same domain.
3. **Standardize naming conventions** — `catalog_schema_table` becomes the canonical identity of every data asset.
4. **Set `USE CATALOG`** at the notebook level for cleaner references.
5. **Avoid cross-catalog JOINs** unless necessary (they can incur egress costs on some clouds).

| Community Edition | Production Unity Catalog |
|---|---|
| `database.table` (2-level) | `catalog.schema.table` (3-level) |
| `SHOW DATABASES` | `SHOW CATALOGS` + `SHOW SCHEMAS IN <catalog>` |
| One default database | One default catalog + one default schema per user |
| No catalog-level privileges | Full RBAC at all three levels |

```python

```
## Concept 62 — Permission Model & Inheritance

### Unity Catalog Privilege Cascade

Unity Catalog permissions follow a **cascading inheritance model** where child objects can have **more restrictive** (but never more permissive) privileges than their parent:

```
CATALOG                    ─── GRANT USAGE ON CATALOG sales TO `analysts`
  ├── schema: revenue      ─── GRANT CREATE TABLE, USAGE ON SCHEMA sales.revenue TO `analysts`
  │     ├── table: orders  ─── GRANT SELECT, MODIFY ON TABLE sales.revenue.orders TO `analysts`
  │     └── table: returns ─── GRANT SELECT ON TABLE sales.revenue.returns TO `analysts`
  └── schema: forecast     ─── GRANT USAGE ON SCHEMA sales.forecast TO `analysts`
        └── table: q1      ─── (inherits no table privileges; must be granted explicitly)
```

### Key Unity Catalog Privileges

| Privilege | Applies To | Meaning |
|-----------|-----------|---------|
| `USE CATALOG` | Catalog | Required to access anything in the catalog |
| `USE SCHEMA` | Schema | Required to list/access objects in the schema |
| `SELECT` | Table/View | Read data from the object |
| `MODIFY` | Table | INSERT, UPDATE, DELETE, MERGE |
| `CREATE TABLE` | Schema | Create new tables in the schema |
| `CREATE VIEW` | Schema | Create views in the schema |
| `CREATE FUNCTION` | Schema | Create UDFs |
| `ALL PRIVILEGES` | Any | Owner-like full access |
| `EXECUTE` | Function | Run a UDF |

### Ownership vs Granted Access

- **Owner**: The principal (user/service principal/group) that created the object; has `ALL PRIVILEGES` by default.
- **Granted Access**: Explicit privileges given to other principals via `GRANT`.
- Ownership can be transferred with `ALTER <object> OWNER TO <principal>`.

```python

```
### Hive Metastore Equivalent — Limited GRANT/REVOKE in Community Edition

Community Edition supports a limited subset of Hive-style privilege management.
The following demonstrates what *is* executable in CE:

```python

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

```

```python

```
### Unity Catalog GRANT/REVOKE Reference (Not Executable in CE)

```sql
-- Grant catalog-level access
GRANT USE CATALOG ON CATALOG sales TO `data_analysts`;
GRANT USE CATALOG ON CATALOG hr_prod TO `hr_team`;

-- Grant schema-level access
GRANT USE SCHEMA ON SCHEMA sales.revenue TO `data_analysts`;
GRANT CREATE TABLE ON SCHEMA sales.revenue TO `data_engineers`;
GRANT CREATE VIEW ON SCHEMA sales.revenue TO `data_analysts`;

-- Grant table-level access
GRANT SELECT ON TABLE sales.revenue.orders TO `data_analysts`;
GRANT MODIFY ON TABLE sales.revenue.orders TO `data_engineers`;
GRANT ALL PRIVILEGES ON TABLE sales.revenue.returns TO `data_owner`;

-- Grant to a group (using backticks for account-level groups)
GRANT SELECT ON TABLE sales.revenue.orders TO `account groups/data_analysts`;

-- Revoke privileges
REVOKE SELECT ON TABLE sales.revenue.orders FROM `data_analysts`;
REVOKE ALL PRIVILEGES ON TABLE sales.revenue.orders FROM `data_owner`;

-- Show grants
SHOW GRANTS ON CATALOG sales;
SHOW GRANTS ON SCHEMA sales.revenue;
SHOW GRANTS ON TABLE sales.revenue.orders;
SHOW GRANTS TO `data_analysts`;   -- all grants for a principal
```

```python

```
### Permission Inheritance Pattern (Visual)

```
-- Best practice: grant at the highest appropriate level

-- 1. Catalog level: entry point (everyone who needs ANY access needs USE CATALOG)
GRANT USE CATALOG ON CATALOG sales TO `sales_team`;

-- 2. Schema level: broader access for trusted roles
GRANT USE SCHEMA, SELECT ON SCHEMA sales.revenue TO `analysts`;

-- 3. Table level: restrict sensitive tables only
GRANT SELECT ON TABLE hr_prod.employees.salaries TO `hr_admins`;
-- (analysts do NOT get this — salaries remain inaccessible to them)
```

**Principle of least privilege in UC:**
- Start with **no access** — everything is denied by default.
- Grant **minimum necessary** at the **highest level possible**.
- Use **groups** rather than individual users.
- Review grants regularly with `SHOW GRANTS`.

```python

```
### What You'd Do in Production

1. **Create account groups** (`account groups/<name>`) and assign users to groups.
2. **Grant at the schema level** by default — avoid per-table grants unless specificity is required.
3. **Use catalog-level `USE CATALOG`** for broad access, schema-level `USE SCHEMA` for team access.
4. **Audit grants quarterly** — use `system.information_schema.table_privileges` to review.
5. **Never grant to individual users** — always use groups for maintainability.

| Community Edition | Production Unity Catalog |
|---|---|
| Limited Hive GRANT/REVOKE | Full RBAC with inheritance |
| No group support for privileges | Account groups + nested groups |
| No separation of duties | Owners, admins, and users distinct |
| No catalog-level governance | Catalog → Schema → Table privilege cascade |

```python

```
## Concept 63 — Dynamic Views for Security

### What Are Dynamic Views?

Dynamic views embed user-identity functions in their SQL definition to **automatically filter rows or mask columns** based on who is querying. Unlike static views, a single dynamic view serves different data to different users.

**Key functions:**
- `CURRENT_USER()` — returns the Databricks user email running the query
- `IS_MEMBER(group_name)` — returns TRUE if the current user belongs to the specified group

**Use cases:**
- **Row-level filtering**: Sales reps see only their own deals; managers see all deals.
- **Column-level masking**: HR sees full SSN; payroll sees last 4 digits; others see NULL.
- **Region-based access**: EU users see only EU customer data.

```python

```
### Community Edition Demo — Dynamic View with Simulated User Context

Since CE has limited `IS_MEMBER()` support, we simulate the pattern using a control table and `current_user()`.

```python

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

```

```python

```
#### Simulated Access Control Table

In production, you'd use Unity Catalog groups + `IS_MEMBER()`. Here we simulate with a lookup table.

```python

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

```

```python

```
#### Pattern 1: Column-Level Masking with Dynamic View

```python

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

```

```python

```
#### Pattern 2: Row-Level Filtering with Dynamic View

```python

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

```

```python

```
### Test All Three Roles

```python

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

```

```python

```
### Unity Catalog Dynamic View — Production Syntax

```sql
-- Production UC dynamic view with IS_MEMBER() and CURRENT_USER()
CREATE VIEW hr_prod.employees.employee_safe_view AS
SELECT
    emp_id,
    name,
    dept,
    CASE
        WHEN IS_MEMBER('hr_team') THEN ssn
        WHEN IS_MEMBER('managers') THEN CONCAT('***-**-', RIGHT(ssn, 4))
        ELSE 'XXX-XX-XXXX'
    END AS ssn,
    CASE
        WHEN IS_MEMBER('hr_team') OR IS_MEMBER('managers') THEN salary
        ELSE NULL
    END AS salary,
    CURRENT_USER() AS queried_by
FROM hr_prod.employees.employee_raw
WHERE
    IS_MEMBER('hr_team')
    OR (IS_MEMBER('managers') AND dept IN (SELECT dept FROM manager_departments WHERE manager = CURRENT_USER()))
    OR (IS_MEMBER('employees') AND name = (SELECT name FROM user_employee_map WHERE user = CURRENT_USER()));
```

```python

```
### Dynamic Views vs Row Filters & Column Masks

| Feature | Dynamic Views | Row Filters + Column Masks |
|---------|---------------|---------------------------|
| **How it works** | Logic embedded in VIEW definition | UDFs bound to TABLE via ALTER |
| **Apply to** | Specific view | Base table (affects ALL queries) |
| **Override possible?** | Create separate view | No — affects all direct table access |
| **Performance** | View overhead on every query | UDF overhead on every scan |
| **Best for** | Tailored access per audience | Uniform policy enforcement |
| **Introduced** | Older (GA since DBR 9.x) | Newer (UC 1.3+, DBR 14+) |
| **Manageability** | View-level, requires DDL | Table-level, centralized policy |

**Recommendation:** Use **dynamic views** for role-specific representations. Use **row filters + column masks** (see Concept #69) for uniform, non-negotiable security policies on base tables.

```python

```
### What You'd Do in Production

1. Define account groups in Unity Catalog: `hr_team`, `managers`, `sales_reps`, etc.
2. Use `IS_MEMBER()` in view definitions for automated filtering.
3. Add `current_user()` as a column for audit trails within the view.
4. Combine row filtering AND column masking in the same view.
5. Test views by impersonating different users (`EXECUTE AS` in SQL Warehouses).
6. Deny direct table access — force all users through the secure view.

```python

```
## Concept 64 — Data Lineage

### What Unity Catalog Lineage Provides

Unity Catalog automatically captures **column-level lineage** across all workloads:
- Which notebook produced this table?
- Which upstream tables fed this view?
- Which downstream dashboards depend on this column?
- If I change column X, what breaks?

Lineage is captured automatically — no manual instrumentation needed. It works across notebooks, Delta Live Tables pipelines, SQL queries, and jobs.

```python

```
### Community Edition — Manual Lineage Tracking

Since CE lacks automatic lineage, we build a manual tracking system to demonstrate the concept.

```python

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

```

```python

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

```

```python

```
#### Build a Sample Pipeline and Map Its Lineage

```python

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

```

```python

```
#### Query the Lineage — Impact Analysis

```python

print("=" * 70)
print("FULL LINEAGE GRAPH")
print("=" * 70)
display(spark.sql("SELECT * FROM sales_db.data_lineage ORDER BY lineage_id"))

```

```python

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

```

```python

```
### Visualize the Pipeline Lineage

```
external_csv ──→ revenue_raw ──→ revenue_clean ──→ revenue_monthly ──→ executive_dashboard
                     │                   │                    │
                     │                   ├─ product (UPPER)    ├─ total_revenue (SUM)
                     │                   ├─ amount (CAST)      ├─ order_count (COUNT)
                     │                   └─ order_month (MONTH)└─ avg_order_value (AVG)
                     │
                     └─── * (raw ingestion)
```

```python

```
### Unity Catalog Lineage Queries (Not Executable in CE)

```sql
-- UC: Query column-level lineage for a specific table
SELECT
    source_table_catalog,
    source_table_schema,
    source_table_name,
    source_column_name,
    target_table_catalog,
    target_table_schema,
    target_table_name,
    target_column_name,
    created_at
FROM system.lineage.column_lineage
WHERE target_table_name = 'revenue_monthly';

-- UC: Find all downstream consumers of a column
SELECT DISTINCT
    target_table_name,
    notebook_path,
    job_name
FROM system.lineage.column_lineage
WHERE source_table_name = 'revenue_raw' AND source_column_name = 'product';

-- UC: Find all upstream sources for a dashboard table
SELECT DISTINCT
    source_table_name,
    source_column_name
FROM system.lineage.column_lineage
WHERE target_table_name = 'executive_dashboard';
```

```python

```
### What You'd Do in Production

1. Unity Catalog lineage is **automatic** — no manual instrumentation.
2. Use **Lineage UI** in the Databricks workspace for visual graph exploration.
3. Query `system.lineage.column_lineage` for programmatic impact analysis.
4. Integrate lineage into your CI/CD to detect breaking changes before deployment.
5. Tag all notebooks, jobs, and DLT pipelines with meaningful names for clear lineage.
6. Lineage persists across retentions — you can trace history even after source tables are dropped.

```python

```
## Concept 65 — Information Schema & Metadata Queries

### Unity Catalog Information Schema

UC provides `system.information_schema` with standardized metadata tables:

| Table | What It Contains |
|-------|------------------|
| `system.information_schema.tables` | All tables and views across catalogs |
| `system.information_schema.columns` | All columns with types, nullable, defaults |
| `system.information_schema.table_privileges` | All GRANTs on tables |
| `system.information_schema.schemata` | All schemas/catalogs |
| `system.information_schema.views` | View definitions |
| `system.information_schema.routines` | UDFs and stored procedures |

This is ANSI SQL standard compliant — the same queries work across Databricks, PostgreSQL, Snowflake, etc.

```python

```
### Community Edition — Hive Metastore Metadata Queries

CE uses Hive Metastore commands to discover metadata. While not a true `information_schema`, these serve the same purpose.

```python

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

```

```python

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

```

```python

# Describe extended — shows table format, location, properties
print("=" * 60)
print("EXTENDED TABLE DETAILS: sales_db.revenue")
print("=" * 60)
try:
    display(spark.sql("DESCRIBE EXTENDED sales_db.revenue"))
except Exception as e:
    print(f"  Error: {e}")

```

```python

# Show table properties (Delta-specific metadata)
print("=" * 60)
print("TABLE PROPERTIES: sales_db.revenue")
print("=" * 60)
try:
    display(spark.sql("SHOW TBLPROPERTIES sales_db.revenue"))
except Exception as e:
    print(f"  Error: {e}")

```

```python

```
### Build a Compliance Report from Metadata

```python

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

```

```python

```
### Unity Catalog Information Schema Queries (Reference)

```sql
-- UC: List all tables in the sales catalog
SELECT table_catalog, table_schema, table_name, table_type, data_source_format
FROM system.information_schema.tables
WHERE table_catalog = 'sales'
ORDER BY table_schema, table_name;

-- UC: Find all columns with PII-sounding names
SELECT table_catalog, table_schema, table_name, column_name, data_type
FROM system.information_schema.columns
WHERE LOWER(column_name) RLIKE 'ssn|salary|phone|email|address|password|credit'
ORDER BY table_catalog, table_schema, table_name;

-- UC: Compliance audit — tables without descriptions
SELECT table_catalog, table_schema, table_name
FROM system.information_schema.tables
WHERE comment IS NULL OR comment = ''
ORDER BY table_catalog, table_schema, table_name;

-- UC: Review all grants on sensitive tables
SELECT grantee, privilege_type, table_catalog, table_schema, table_name
FROM system.information_schema.table_privileges
WHERE table_name IN ('salaries', 'employee_sensitive')
ORDER BY grantee, table_name;
```

```python

```
### What You'd Do in Production

1. **Automate metadata scans** — weekly job that queries `system.information_schema.columns` for new PII columns.
2. **Build a data catalog dashboard** listing all tables, owners, last updated, sensitivity level.
3. **Enforce tagging** — use table comments and TBLPROPERTIES to tag data classification levels.
4. **Monitor for "rogue tables"** — tables outside designated catalogs/schemas.
5. **Integrate with external catalogs** — push UC metadata to enterprise data catalogs (Alation, Collibra, etc.).

```python

```
## Concept 66 — External Locations & Storage Credentials

### Unity Catalog External Locations

Unity Catalog governs access to cloud storage through **external locations** — named references to cloud storage paths:

```
Cloud Storage (S3/ADLS/GCS)
        │
        ▼
Storage Credential    ← IAM Role (AWS) / Service Principal (Azure) / Service Account (GCP)
        │
        ▼
External Location     ← s3://my-bucket/sales-data/ mapped to UC name "sales_data_loc"
        │
        ▼
Managed/External Tables ← Created under the external location
```

### Two Types of Tables in UC

| Type | Data Location | Schema Location | Dropping Table... |
|------|--------------|-----------------|-------------------|
| **Managed Table** | UC-managed storage (catalog-scoped) | UC-managed | Deletes DATA + SCHEMA |
| **External Table** | Your cloud storage (external location) | UC-metastore | Deletes SCHEMA only; DATA persists |

**Key difference:** Managed tables give UC full lifecycle control. External tables let you own the data files in your cloud storage.

```python

```
### Community Edition — External Tables on DBFS

CE does not have UC external locations, but the Hive Metastore supports external tables with a `LOCATION` clause. DBFS (`/FileStore/`, `/tmp/`) serves as the "external storage."

```python

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

```

```python

```
**Create an external table on DBFS** — the data lives at a path you control.

```python

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

```

```python

```
### Demonstrate Managed vs External Behavior

```python

print("=" * 60)
print("BEFORE DROP: Both tables exist")
print("=" * 60)
display(spark.sql("SHOW TABLES IN sales_db"))

# Query data from both
print("\nManaged table data:")
display(spark.sql("SELECT * FROM sales_db.orders_managed"))
print("\nExternal table data:")
display(spark.sql("SELECT * FROM sales_db.orders_external"))

```

```python

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

```

```python

```
### Unity Catalog External Location Syntax (Reference)

```sql
-- UC: Create a storage credential (requires account admin)
CREATE STORAGE CREDENTIAL aws_sales_cred
USING 'arn:aws:iam::123456789:role/databricks-sales-access';

-- UC: Create an external location bound to the credential
CREATE EXTERNAL LOCATION sales_data_loc
URL 's3://my-company-sales-bucket/'
WITH (
    CREDENTIAL aws_sales_cred
)
COMMENT 'Sales department raw data landing zone';

-- UC: Grant access to the external location
GRANT CREATE TABLE, READ FILES, WRITE FILES
ON EXTERNAL LOCATION sales_data_loc
TO `data_engineers`;

-- UC: Create an external table on the governed location
CREATE TABLE sales.revenue.invoices
LOCATION 's3://my-company-sales-bucket/invoices/';

-- UC: Show all external locations
SHOW EXTERNAL LOCATIONS;

-- UC: Describe an external location
DESCRIBE EXTERNAL LOCATION sales_data_loc;
```

```python

```
### What You'd Do in Production

1. **Create external locations for all cloud storage paths** used by Databricks.
2. **Use managed tables for derived/internal data** — let UC handle lifecycle.
3. **Use external tables for shared/ingested data** — you control the source.
4. **Grant READ FILES / WRITE FILES** at the external location level, not per-bucket.
5. **Never use service credentials directly in notebooks** — always go through UC external locations.
6. **Audit external locations** to ensure no unauthorized cloud storage access exists.

| Community Edition | Production Unity Catalog |
|---|---|
| DBFS as "external" storage | S3/ADLS/GCS via external locations |
| No storage credential abstraction | IAM roles, service principals, service accounts |
| `LOCATION '/path/'` in Hive Metastore | `LOCATION 's3://...'` governed by UC |
| No access control on paths | GRANT READ/WRITE FILES on external locations |

```python

```
## Concept 67 — Service Principals & Managed Identity

### What Are Service Principals?

A **service principal** is a non-human identity — an application, CI/CD pipeline, or scheduled job — that authenticates to Databricks using its own credentials rather than a user's personal account.

```
HUMAN IDENTITY:                    MACHINE IDENTITY:
alice@company.com                 sp-sales-etl-prod
  │ OIDC / SSO                        │ OAuth2 client credentials
  │ Personal token                     │ Secret + Client ID
  ▼                                   ▼
Databricks Workspace ◄─────────── Databricks Workspace
```

### Why NOT Use Personal Accounts for Production?

| Risk | Consequence |
|------|------------|
| Employee leaves company | Job breaks; token deleted; panic |
| Personal token leaked | Full user access compromised |
| No separation of duties | Alice's prod job runs as Alice, not as "ETL Pipeline" |
| Audit confusion | Who really ran this — Alice or her script? |
| Credential rotation | Personal tokens aren't managed by security teams |

**Production workloads MUST run as service principals.**

```python

```
### Service Principal Architecture

**On Azure:**
```
Azure AD App Registration → Service Principal
        │
        ├── Client ID + Secret/Certificate (for OAuth)
        ├── Managed Identity (Azure resource-level, no secret management)
        └── Assigned to Databricks workspace via SCIM/API
```

**On AWS:**
```
AWS IAM Role → Instance Profile
        │
        ├── Trust policy: allows Databricks to assume this role
        ├── Attached to Databricks clusters at launch time
        └── Provides S3 access without embedding keys
```

**On GCP:**
```
GCP Service Account
        │
        ├── JSON key + service account email
        ├── Assigned to Databricks workspace
        └── Provides GCS/BigQuery access
```

```python

```
### Community Edition — Conceptual Simulation

We can't create real service principals in CE, but we can demonstrate the **principle** of machine vs human identity and best practices.

```python

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

```

```python

```
### Best Practices for Service Principal Usage

```python

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

```

```python

```
### What You'd Do in Production

1. **Register service principals in your identity provider** (Azure AD, AWS IAM, GCP IAM).
2. **Add them to your Databricks account** via SCIM provisioning or Account API.
3. **Assign Unity Catalog privileges** — `GRANT SELECT ON TABLE ... TO \`sp-sales-etl-prod\``.
4. **Schedule ALL production jobs** to run as service principals, never as users.
5. **Use Databricks secrets** for OAuth credentials: `sp_client_id` and `sp_client_secret`.
6. **Implement automated rotation** — Azure Key Vault, AWS Secrets Manager, or GCP Secret Manager.
7. **Alert on personal-account jobs** — detect and migrate any production jobs running as users.

| Community Edition | Production Unity Catalog |
|---|---|
| No service principal support | Full SP lifecycle via Account API |
| Jobs run as the notebook owner | Jobs run as service principals |
| No managed identity | Azure Managed Identity / AWS Instance Profiles |
| Limited token management | OAuth2 client credentials with secrets rotation |

```python

```
## Concept 68 — Audit Logging

### Unity Catalog Audit Logging

UC provides `system.access.audit` — a comprehensive audit log table that captures every operation:

| Field | Description |
|-------|-------------|
| `event_time` | When the operation occurred |
| `user_identity.email` | Who performed the operation (human or service principal) |
| `action_name` | What action (e.g., `selectTable`, `createTable`, `grant`) |
| `request_params.commandText` | The SQL statement (for queries) |
| `request_params.table_full_name` | The table affected |
| `response.status_code` | HTTP status (200 = success) |
| `source_ip_address` | Client IP address |
| `service_name` | Which service (e.g., `unity-catalog`, `databricks-sql`) |

Audit logs are critical for:
- **Compliance** (SOC2, HIPAA, GDPR, PCI-DSS)
- **Forensic investigation** ("Who accessed this table at 3 AM?")
- **Anomaly detection** ("Why did the CI/CD SP drop a production table?")
- **Access review** ("Is ANYONE still using this deprecated table?")

```python

```
### Community Edition — Manual Audit Logging System

CE does not have `system.access.audit`. We build a manual audit log to demonstrate the concept.

```python

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

```

```python

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

```

```python

```
### Demonstrate Audit Logging in Action

```python

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

```

```python

```
### Query the Audit Log

```python

print("=" * 70)
print("AUDIT LOG — Complete History")
print("=" * 70)
display(spark.sql("""
    SELECT event_id, event_time, user_identity, action_name, 
           object_name, status, execution_time_ms
    FROM sales_db.audit_log
    ORDER BY event_id
"""))

```

```python

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

```

```python

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

```

```python

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

```

```python

# Clean up the test table
spark.sql("DROP TABLE IF EXISTS sales_db.audit_test_table")
log_audit("dropTable", "TABLE", "sales_db.audit_test_table", "DROP TABLE audit_test_table", "SUCCESS")
print("Cleaned up audit_test_table — logged the drop.")

```

```python

```
### What Production Audit Logs Would Contain

```sql
-- UC: Query production audit logs
SELECT
    event_time,
    user_identity.email,
    action_name,
    request_params.table_full_name,
    request_params.commandText,
    response.status_code,
    source_ip_address,
    service_name
FROM system.access.audit
WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND action_name IN ('selectTable', 'createTable', 'dropTable', 'grant')
ORDER BY event_time DESC;

-- UC: Find tables that haven't been accessed in 30 days (cleanup candidates)
SELECT request_params.table_full_name, MAX(event_time) AS last_accessed
FROM system.access.audit
WHERE action_name = 'selectTable'
GROUP BY request_params.table_full_name
HAVING MAX(event_time) < CURRENT_DATE() - INTERVAL 30 DAYS;

-- UC: Anomaly detection — tables dropped outside business hours
SELECT event_time, user_identity.email, request_params.table_full_name
FROM system.access.audit
WHERE action_name = 'dropTable'
  AND HOUR(event_time) NOT BETWEEN 7 AND 19;
```

```python

```
### What You'd Do in Production

1. **Enable audit logging** — it's on by default in all paid tiers, retained for 365 days.
2. **Create alert queries** that run daily and flag suspicious activity (e.g., `dropTable` by unusual users).
3. **Build an audit dashboard** in Databricks SQL with time-series charts of operations.
4. **Integrate with SIEM systems** — export audit logs to Splunk, Azure Sentinel, or ELK Stack.
5. **Set up retention policies** — archive old audit logs to cold storage for compliance.
6. **Use audit data for cost analysis** — identify heavy query users and optimize.

```python

```
## Concept 69 — Row Filters & Column Masks

### Unity Catalog Row Filters & Column Masks (UC 1.3+)

Row filters and column masks are **table-level security policies** enforced by UDFs:

**Row Filter:** A SQL UDF that returns TRUE for rows the user is allowed to see.
```sql
CREATE FUNCTION sales_filter(dept STRING)
RETURN IS_MEMBER('hr_team')
    OR (IS_MEMBER('managers') AND dept IN ('Engineering', 'Sales'))
    OR CURRENT_USER() = CONCAT(dept, '_dept_head@company.com');

ALTER TABLE hr.employee.data SET ROW FILTER sales_filter ON (dept);
```

**Column Mask:** A SQL UDF that returns the masked value for the column.
```sql
CREATE FUNCTION ssn_mask(ssn STRING)
RETURN CASE
    WHEN IS_MEMBER('hr_team') THEN ssn
    ELSE 'XXX-XX-XXXX'
END;

ALTER TABLE hr.employee.data ALTER COLUMN ssn SET MASK ssn_mask;
```

**Critical difference from dynamic views:** These are bolted onto the **base table** — every query against the table (including direct `SELECT *`) is automatically filtered. There is no way to "accidentally" bypass the policy.

```python

```
### Community Edition — Row Filtering & Column Masking via Views

In CE, we implement the same functionality using views (since we can't ALTER TABLE with row filters/column masks).

```python

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

```

```python

```
#### Define Role-Based Access Functions (Simulating UDF Row Filters / Column Masks)

```python

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

```

```python

```
#### Row Filter Equivalent — A View That Automatically Filters Rows

```python

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

```

```python

```
#### Column Mask Equivalent — A View That Automatically Masks Columns

```python

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

```

```python

```
#### Complete Security View — Row Filter + Column Mask Combined

```python

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

```

```python

```
### Test All Five Roles Through the Secure View

```python

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

```

```python

```
### When to Use Row Filters / Column Masks vs Dynamic Views

| Scenario | Best Approach | Why |
|----------|--------------|-----|
| One audience needs tailored columns | Dynamic View | View is purpose-built for that audience |
| **All** queries must filter by region | **Row Filter on base table** | No way to bypass — even ad-hoc queries get filtered |
| SSN must **never** be exposed to non-HR | **Column Mask on base table** | Absolute enforcement at the table level |
| Multiple different rollups for different teams | Dynamic Views | Each team gets their own view |
| Regulatory compliance (GDPR, HIPAA) | **Row Filter + Column Mask** | Mandatory, universal, non-circumventable |
| Quick prototype or dashboard | Dynamic View | Simpler to create and iterate |
| Data scientists doing exploratory analysis | Row Filter on base table | They query the table directly but see only allowed data |

**Rule of thumb:**
- **Regulatory / mandatory policies** → Row filters + column masks on base tables
- **Convenience / audience-specific views** → Dynamic views
- **Both can coexist** — base table has masks, views provide convenient rollups on top.

```python

```
### Unity Catalog Row Filter & Column Mask Syntax (Reference)

```sql
-- Step 1: Create a FILTER UDF (catalog-qualified)
CREATE OR REPLACE FUNCTION hr_prod.employees.region_filter(region STRING)
RETURN IS_MEMBER('hr_admins')
    OR (IS_MEMBER('us_managers') AND region = 'US')
    OR (IS_MEMBER('eu_managers') AND region = 'EU');

-- Step 2: Apply the row filter to the table
ALTER TABLE hr_prod.employees.data
SET ROW FILTER hr_prod.employees.region_filter ON (region);

-- Step 3: Create a MASK UDF
CREATE OR REPLACE FUNCTION hr_prod.employees.ssn_mask(ssn STRING)
RETURN CASE
    WHEN IS_MEMBER('hr_admins') THEN ssn
    WHEN IS_MEMBER('payroll') THEN CONCAT('***-**-', RIGHT(ssn, 4))
    ELSE NULL
END;

-- Step 4: Apply the column mask
ALTER TABLE hr_prod.employees.data
ALTER COLUMN ssn SET MASK hr_prod.employees.ssn_mask;

-- Show row filters on a table
SHOW ROW FILTERS ON TABLE hr_prod.employees.data;

-- Remove a row filter
ALTER TABLE hr_prod.employees.data DROP ROW FILTER region_filter;

-- Remove a column mask
ALTER TABLE hr_prod.employees.data ALTER COLUMN ssn DROP MASK;
```

```python

```
### What You'd Do in Production

1. **Apply row filters on ALL tables containing regional or departmental data** — never rely on users to self-filter.
2. **Apply column masks on ALL PII columns** — SSN, salary, phone, email, address, IP address.
3. **Create the UDFs in the same schema as the tables** for clean organization.
4. **Test with multiple user personas** before deploying to production.
5. **Document each policy** — what it filters, why, and who is exempt.
6. **Regularly review** with `SHOW ROW FILTERS` and `SHOW COLUMN MASKS` to ensure policies are still correct.

```python

```
## Concept 70 — Delta Sharing

### What Is Delta Sharing?

Delta Sharing is an **open protocol** for securely sharing live data across organizations without copying it. It's built on Delta Lake and works with any platform that implements the sharing protocol (not just Databricks).

```
┌─────────────────────┐          ┌─────────────────────────┐
│  DATA PROVIDER       │          │  DATA RECIPIENT           │
│  (your company)      │          │  (partner/customer)       │
│                      │          │                           │
│  ┌─────────────┐    │  Share   │  ┌──────────────────┐    │
│  │ Unity Catalog│────┼──────────┼──│ Databricks        │    │
│  │   Tables     │    │ Activate │  │ (same/diff cloud) │    │
│  └─────────────┘    │  Link    │  │                   │    │
│                      │──────────┼──│ OR: Power BI      │    │
│  Recipients:         │          │  │ OR: pandas 🐼     │    │
│   - partner@acme.com│          │  │ OR: Spark         │    │
│   - client@globex.io│          │  │ OR: Trino/Presto  │    │
└─────────────────────┘          └─────────────────────────┘
```

**Key properties:**
- **No data copy** — recipient queries LIVE data (table changes visible immediately)
- **Open protocol** — works with open-source Delta Sharing connectors
- **Cross-platform** — Spark, pandas, Power BI, Tableau, Trino all supported
- **Cross-cloud** — provider on AWS, recipient on Azure (or vice versa)
- **Granular** — share specific tables, partitions, or even row-filtered subsets

```python

```
### Community Edition — Simulation of Delta Sharing

CE cannot create actual Delta Shares. We simulate the concept by:
1. Creating a "shareable" dataset (read-only view)
2. Simulating the provider/recipient flow
3. Demonstrating what the production code would look like

```python

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

```

```python

```
#### Simulate the Sharing Flow — Provider Side

```python

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

```

```python

```
#### Simulate the Sharing Flow — Recipient Side

```python

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

```

```python

```
### Simulate a "Shared" Read-Only View (CE Equivalent)

```python

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

```

```python

```
#### Live Tables vs Snapshots

| Feature | Delta Sharing (Live) | Traditional Data Export (Snapshot) |
|---------|---------------------|-----------------------------------|
| **Data freshness** | Real-time (live table) | Stale (point-in-time copy) |
| **Data volume** | Recipient pays only for queries | Full copy transferred |
| **Storage duplication** | None — reads from provider | Duplicate storage needed |
| **Schema evolution** | Automatically visible | Manual re-export required |
| **Revocation** | Immediate — revoke share access | Cannot retract sent files |
| **Auditability** | Provider sees all recipient queries | No visibility after export |
| **Security** | Token-based, encrypted in transit | Files could be forwarded |
| **Governance** | UC policies enforced at read time | No enforcement after copy |

**Winner for regulatory data sharing:** Delta Sharing — revocable, auditable, always up-to-date.

```python

```
### Delta Sharing Architecture Diagram

```
┌────────────────── PROVIDER ──────────────────┐
│                                               │
│  Unity Catalog                                │
│  ┌──────────────────────────────────────┐    │
│  │ SHARE: sales_insights_share            │    │
│  │  ├── TABLE: shared_sales_summary       │    │
│  │  └── TABLE: revenue_monthly (>=2025)  │    │
│  └──────────────────────────────────────┘    │
│                                               │
│  Recipients:                                  │
│   ├── acme_corp (CREATE CATALOG ... USING)    │
│   └── globex_inc                              │
└───────────────────┬───────────────────────────┘
                    │
              Delta Sharing Protocol
              (REST API / gRPC)
                    │
┌───────────────────┴───────────────────────────┐
│ RECIPIENT: acme_corp                           │
│                                                 │
│  Databricks Catalog: sales_from_partner         │
│   ├── shared_sales_summary ← LIVE QUERY        │
│   └── revenue_monthly      ← FILTERED (>=2025) │
│                                                 │
│  OR: Open-Source Client (pandas, Spark)         │
└─────────────────────────────────────────────────┘
```

```python

```
### What You'd Do in Production

1. **Identify shareable data** — clean, aggregated datasets (never raw PII, never unprocessed logs).
2. **Create shares by audience** — one share per partner, or one share per data product.
3. **Use partition restrictions** — share only agreed-upon date ranges or regions.
4. **Monitor recipient usage** — track query patterns via `system.access.audit`.
5. **Set up expiration** — recipient tokens have expiry; rotate regularly.
6. **Document the data contract** — what columns mean, refresh frequency, SLAs.
7. **Test with open-source Delta Sharing** — verify the share works from a non-Databricks client before handing to partners.
8. **Revoke promptly** — if a partnership ends, `DROP RECIPIENT` / `REVOKE SELECT ON SHARE` immediately.

| Step | Command |
|------|---------|
| Create share | `CREATE SHARE sales_insights_share;` |
| Add table | `ALTER SHARE ... ADD TABLE catalog.schema.table;` |
| Create recipient | `CREATE RECIPIENT partner USING ID 'partner@domain.com';` |
| Grant select | `GRANT SELECT ON SHARE sales_insights_share TO RECIPIENT partner;` |
| Recipient mounts | `CREATE CATALOG shared_data USING SHARE partner.sales_insights_share;` |
| Revoke | `REVOKE SELECT ON SHARE sales_insights_share FROM RECIPIENT partner;` |
| Remove table | `ALTER SHARE sales_insights_share REMOVE TABLE catalog.schema.table;` |
| Drop share | `DROP SHARE sales_insights_share;` |

```python

```
## Summary — Unity Catalog & Governance

### Concepts Covered (#61–#70)

| # | Concept | Key Takeaway |
|---|---------|-------------|
| 61 | **Three-Level Namespace** | `catalog.schema.table` organizes data; UC maps to org structure |
| 62 | **Permission Model** | Privileges cascade catalog→schema→table; use groups, not users |
| 63 | **Dynamic Views** | `IS_MEMBER()` and `CURRENT_USER()` embed security logic in views |
| 64 | **Data Lineage** | UC auto-tracks column-level lineage; critical for impact analysis |
| 65 | **Information Schema** | `system.information_schema.*` provides ANSI-standard metadata queries |
| 66 | **External Locations** | Governs cloud storage paths through UC; managed vs. external tables |
| 67 | **Service Principals** | Machine identities for production; NEVER use personal accounts for jobs |
| 68 | **Audit Logging** | `system.access.audit` records every operation for compliance |
| 69 | **Row Filters & Column Masks** | Table-level security policies; non-circumventable |
| 70 | **Delta Sharing** | Open protocol for live, cross-org data sharing without data copy |

```python

```
### Community Edition vs Production Unity Catalog — Comparison Matrix

| Feature | Community Edition (Hive Metastore) | Production (Unity Catalog) |
|---------|----------------------------------|----------------------------|
| **Namespace** | `database.table` (2-level) | `catalog.schema.table` (3-level) |
| **Permissions** | Limited Hive GRANT/REVOKE | Full RBAC with inheritance |
| **Data lineage** | Manual tracking only | Automatic column-level lineage |
| **Metadata** | `SHOW`/`DESCRIBE` commands | `system.information_schema.*` (ANSI SQL) |
| **External locations** | Direct `LOCATION '/path/'` | UC-governed storage credentials + locations |
| **Service principals** | Not available | Full support via Account API |
| **Audit logging** | Must build manually | `system.access.audit` (automatic) |
| **Row-level security** | Views only | Row filters + column masks on tables |
| **Cross-org sharing** | Not available | Delta Sharing (open protocol) |
| **Cost** | Free (limited resources) | Paid DBU + cloud costs |

```python

```
### Self-Assessment — Test Your Understanding

Answer these questions to validate your learning:

```python

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

```

```python

```
### Key Governance Principles — Cheat Sheet

```
1. LEAST PRIVILEGE
   Grant minimum access needed. Start from zero. Expand only with justification.

2. GROUPS, NOT USERS
   Never grant privileges to individuals. Use account groups. Manage membership centrally.

3. MACHINE IDENTITIES
   Production jobs → Service principals. Interactive work → User accounts. Never mix.

4. DEFENSE IN DEPTH
   Dynamic views for UX, row filters + column masks for enforcement. Layer them.

5. AUDIT EVERYTHING
   If it's not logged, it didn't happen. Use system.access.audit for compliance.

6. CATALOG TOPOLOGY MATTERS
   Design catalog/schema hierarchy to match your org. Plan before creating tables.

7. MANAGED vs EXTERNAL
   Managed tables: UC handles lifecycle. External tables: you keep the files.

8. SHARE WITHOUT COPYING
   Delta Sharing for live, revocable data sharing. No more CSV exports.

9. METADATA IS POWER
   information_schema tells you everything about your data estate. Query it regularly.

10. LINEAGE IS AUTOMATIC
    UC traces column-level lineage without instrumentation. Use it for impact analysis.
```

```python

```
### Cleanup — Drop Objects Created in This Notebook

```python

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

```

```python

```
## End of Notebook — 07 Unity Catalog & Governance

**What you learned:**
- The three-level catalog namespace and how it maps to organizational structure
- Unity Catalog's privilege model with cascade inheritance
- Building dynamic views for row-level and column-level security
- Automatic data lineage and manual lineage tracking techniques
- Querying information schema for metadata discovery and compliance
- External locations and the managed vs external table distinction
- Service principals as non-human identities for production workloads
- Audit logging for compliance and forensic investigation
- Row filters and column masks as table-level security enforcement
- Delta Sharing for live, cross-organization data sharing

**Next:** Move to the capstone project — apply everything you've learned in a comprehensive end-to-end pipeline!

```python
```