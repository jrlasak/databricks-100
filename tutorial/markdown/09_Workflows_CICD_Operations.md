# Databricks notebook source
# 09: Workflows, CI/CD & Operations
## Concepts 81â€“90 | Production Orchestration & DevOps on Databricks

### Overview
This notebook covers the operational backbone of a production Databricks environment:

| # | Concept | Difficulty |
|---|---------|-----------|
| 81 | Choosing Compute for Jobs | Easy |
| 82 | Secrets Management | Easy |
| 83 | Parameterization: Widgets, Job Parameters, Task Values | Easy |
| 84 | Notebook Development Patterns | Easy |
| 85 | Databricks Workflows: Multi-Task Jobs | Medium |
| 86 | Task Dependencies, Retries & Conditional Logic | Medium |
| 87 | Git Integration & Repos | Medium |
| 88 | Monitoring, Alerting & SQL Alerts | Medium |
| 89 | System Tables for Operations | Medium |
| 90 | Databricks Asset Bundles (DABs) | Hard |

### Community Edition Limitations
Community Edition **cannot** create multi-task jobs, use DBSQL alerts, integrate Repos, or deploy DABs. We explain the production feature, then show what's possible in CE with manual alternatives.

```
+=========================================================+
|             PRODUCTION DATABRICKS                        |
|  +---------+   +---------+   +---------+   +---------+  |
|  | Secrets |-->|  Jobs   |-->|  Alerts |-->|  DABs   |  |
|  | (AKV)   |   |(Workflow)|   | (Slack) |   | (CI/CD) |  |
|  +---------+   +---------+   +---------+   +---------+  |
+=========================================================+
                  | CE equivalent |
+=========================================================+
|            COMMUNITY EDITION                            |
|  +---------+   +---------+   +---------+   +---------+  |
|  | Env Vars|   | Widgets |   | Manual  |   | Manual  |  |
|  | dbutils |   | %run    |   | Print   |   | YAML    |  |
|  +---------+   +---------+   +---------+   +---------+  |
+=========================================================+
```

```python

```
## Concept 81: Choosing Compute for Jobs

### Production Behaviour
Databricks offers several compute types for jobs. Choosing the right one affects cost, start-up latency, and isolation:

| Compute Type | Use Case | Startup | Cost |
|-------------|----------|---------|------|
| **Serverless Compute** | Default for new jobs; zero management | ~1â€“5 s | Pay-per-second (higher unit cost but no idle) |
| **Job Cluster** | Ephemeral cluster per job run; single-task | ~2â€“5 min | Pay DBUs while active only |
| **All-Purpose Cluster** | Interactive development; shared across users/notebooks | Runs continuously | Pay for idle time too |
| **Instance Pool** | Pre-warmed VMs for fast job cluster creation | ~30â€“60 s | Pay pool idle + job DBUs |
| **SQL Warehouse** | DBSQL queries & dashboards | ~1â€“2 min | Pay DBUs while active |

### Why All-Purpose Is NOT for Production
- Runs 24/7 by default â€” you pay for idle DBUs
- Shared by multiple developers â€” resource contention
- Notebook output interleaving â€” confusing logs
- No automatic retry on failure
- Security: all notebooks on the cluster share the same execution context

### CE Workaround
Community Edition gives you **one all-purpose cluster**. We can still demonstrate cluster configuration principles and discuss the API patterns.

```python

```
#### 81.1 Job Cluster Configuration (API Representation)
This is what a well-configured job cluster looks like in the REST API / Jobs UI. You would create this via the "New Job" dialog or Terraform/DABs.

```python

import json

job_cluster_config = {
    "job_cluster_key": "etl_cluster",
    "new_cluster": {
        "cluster_name": "ETL Job Cluster",
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "autoscale": {
            "min_workers": 1,
            "max_workers": 10
        },
        "spark_conf": {
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
            "spark.sql.adaptive.enabled": "true"
        },
        "custom_tags": {
            "environment": "production",
            "team": "data-engineering",
            "cost_center": "de-2025"
        },
        "init_scripts": [
            {"dbfs": {"destination": "dbfs:/databricks/init/install_deps.sh"}}
        ],
        "spark_env_vars": {
            "ENV": "production",
            "LOG_LEVEL": "WARN"
        },
        "enable_elastic_disk": True,
        "policy_id": None,
        "driver_node_type_id": "i3.xlarge",
        "data_security_mode": "USER_ISOLATION"
    }
}

print("=== PRODUCTION JOB CLUSTER CONFIGURATION ===")
print(json.dumps(job_cluster_config, indent=2))

```

```python

```
#### 81.2 Compute Decision Matrix

```python

scenarios = [
    {"scenario": "Hourly sensor data ingestion", "answer": "Job Cluster", "reason": "Periodic, predictable, needs isolation"},
    {"scenario": "Ad-hoc data exploration by analysts", "answer": "All-Purpose (or SQL Warehouse)", "reason": "Interactive, unpredictable usage"},
    {"scenario": "100+ micro-jobs every 5 minutes", "answer": "Instance Pool", "reason": "Frequent restarts, pool amortises startup cost"},
    {"scenario": "Simple dashboard refresh on a schedule", "answer": "SQL Warehouse", "reason": "DBSQL is cheaper for SQL-only workloads"},
    {"scenario": "Pipeline with zero DevOps", "answer": "Serverless", "reason": "No cluster management; ideal for small teams"},
    {"scenario": "Development & unit testing", "answer": "All-Purpose", "reason": "Interactive, shared, low barrier"},
]

print(f"{'Scenario':<45} {'Best Compute':<25} {'Reason'}")
print("-" * 100)
for s in scenarios:
    print(f"{s['scenario']:<45} {s['answer']:<25} {s['reason']}")

```

```python

```
### Key Takeaway â€” Concept 81
- **Serverless** â€” zero-ops, faster startup, higher per-DBU cost
- **Job Clusters** â€” ephemeral, isolated, best bang-for-buck for periodic jobs
- **Instance Pools** â€” warm VMs, faster startup than job clusters, good for many short jobs
- **All-Purpose** â€” development ONLY; never prod
- Always tag clusters (environment, team, cost_center) for cost attribution

```python

```
## Concept 82: Secrets Management

### Production Behaviour
Databricks provides **Secret Scopes** to store credentials securely:

| Scope Type | Backed By | Use Case |
|-----------|-----------|----------|
| **Databricks-backed** | Encrypted DB table | Simple, quick |
| **Azure Key Vault** | AKV | Azure-native, audit log |
| **AWS Secrets Manager** | AWS SM | AWS-native, rotation |
| **GCP Secret Manager** | GCP SM | GCP-native |

```
  NEVER:  password = "hunter2"
  ALWAYS: password = dbutils.secrets.get(scope="my-scope", key="db-password")
```

### CE Workaround
Community Edition may not support full Secret Scopes. Use environment variables as an acceptable fallback.

```python

```
#### 82.1 Proper vs Improper Secret Handling

```python

# ============================================================
# NEVER DO THIS â€” HARDCODED SECRETS
# ============================================================

# BAD: secrets in plain text (would leak to Git, logs, etc.)
# db_password_bad = "super_secure_password_123!"
# api_key_bad = "dapi123456789abcdef"
# connection_string_bad = "Server=tcp:myserver.database.windows.net;User=admin;Password=pass123;"

print("NEVER hardcode secrets in notebooks or scripts.")
print("They would appear in Git history, job run logs, and notebook snapshots.")

# ============================================================
# USE THIS PATTERN â€” SECRETS VIA SCOPE / ENV VAR
# ============================================================

# Method 1: Databricks Secret Scope (production)
# Steps (via CLI or UI):
#   databricks secrets create-scope --scope my-app-scope
#   databricks secrets put --scope my-app-scope --key db-password
#   databricks secrets put --scope my-app-scope --key api-key
#
# Then in code:
# db_password = dbutils.secrets.get(scope="my-app-scope", key="db-password")

# CE Fallback: Environment variable (set via cluster spark_env_vars or init script)
import os

db_password = os.environ.get("DB_PASSWORD", "DEV_DEFAULT_DO_NOT_USE_IN_PROD")
api_key = os.environ.get("API_KEY", "DEV_DEFAULT_DO_NOT_USE_IN_PROD")

print("\nSecrets loaded from environment (CE fallback).")
print(f"  DB_PASSWORD is set: {db_password != 'DEV_DEFAULT_DO_NOT_USE_IN_PROD'}")
print(f"  API_KEY is set: {api_key != 'DEV_DEFAULT_DO_NOT_USE_IN_PROD'}")

```

```python

```
#### 82.2 Simulated Secrets Manager (CE-Compatible)

```python

class LocalSecretsManager:
    """
    Simulates a Secret Scope for Community Edition.
    In production, this is replaced by dbutils.secrets.get() or Key Vault SDK.
    """
    def __init__(self):
        self._secrets = {
            "database": {"password": "prod-db-pwd-2025", "host": "prod.db.internal", "port": "5432"},
            "api": {"api_key": "sk-prod-abc123def456", "endpoint": "https://api.example.com/v2"},
            "storage": {"account_key": "base64encodedkey==", "account_name": "prodstorageacct"},
            "monitoring": {"pagerduty_key": "pd-integration-key-xyz", "slack_webhook": "https://hooks.slack.com/..."}
        }

    def get(self, scope, key):
        """Same signature as dbutils.secrets.get(scope, key)"""
        if scope in self._secrets and key in self._secrets[scope]:
            return self._secrets[scope][key]
        raise KeyError(f"Secret not found: scope={scope}, key={key}")

    def list_scopes(self):
        return list(self._secrets.keys())

    def list_secrets(self, scope):
        return list(self._secrets.get(scope, {}).keys())

# Usage (identical API to production!)
secret_mgr = LocalSecretsManager()

print("=== AVAILABLE SCOPES ===")
for scope in secret_mgr.list_scopes():
    secrets = secret_mgr.list_secrets(scope)
    print(f"  {scope}: {secrets}")

print("\n=== RETRIEVING SECRETS ===")
db_pwd = secret_mgr.get("database", "password")
db_host = secret_mgr.get("database", "host")
api_key = secret_mgr.get("api", "api_key")

print(f"  Database host: {db_host}")
print(f"  Database password: {'*' * len(db_pwd)}")  # Never print actual secret!
print(f"  API key prefix: {api_key[:4]}...")

```

```python

```
#### 82.3 Secret Rotation Pattern

```python

import datetime

class SecretRotationManager:
    """Demonstrates the pattern for secret rotation."""

    def __init__(self, secret_manager):
        self.sm = secret_manager
        self.rotation_log = []

    def rotate_secret(self, scope, key, new_value):
        old_value = self.sm.get(scope, key)
        self.sm._secrets[scope][key] = new_value
        self.rotation_log.append({
            "timestamp": datetime.datetime.now().isoformat(),
            "scope": scope,
            "key": key,
            "rotated": True
        })
        print(f"  Rotated {scope}/{key}")

    def get_rotation_history(self):
        return self.rotation_log

rotator = SecretRotationManager(secret_mgr)
print("=== BEFORE ROTATION ===")
print(f"  API key: {secret_mgr.get('api', 'api_key')}")

print("\n=== ROTATING SECRET ===")
rotator.rotate_secret("api", "api_key", "sk-prod-NEW-rotated-key-999")

print("\n=== AFTER ROTATION ===")
print(f"  API key: {secret_mgr.get('api', 'api_key')}")

```

```python

```
### Key Takeaway â€” Concept 82
- Never hardcode secrets; use dbutils.secrets.get(scope, key)
- Use Key Vault-backed scopes for production (audit logs, auto-rotation)
- Environment variables are an acceptable fallback (CE / local dev)
- Principle of least privilege: one scope per application
- Rotate secrets regularly; automate via Key Vault policies

```python

```
## Concept 83: Parameterization: Widgets, Job Parameters, Task Values

### Production Behaviour
Databricks provides three layers of parameterisation:

| Mechanism | Scope | Persistence | Use Case |
|-----------|-------|-------------|----------|
| **Widgets** (dbutils.widgets) | Single notebook | Interactive only | Ad-hoc runs, debugging |
| **Job Parameters** | Single task | Per run | Scheduled runs from Workflows |
| **Task Values** | Cross-task | Per run | Pass data between tasks in a job |

### CE Availability
**ALL widget types work fully in Community Edition!** This is the most CE-friendly concept.

```python

```
#### 83.1 Creating Interactive Widgets

```python

dbutils.widgets.removeAll()  # Clean start

# Text widget â€” free-form input
dbutils.widgets.text("source_table", "sales_transactions", "Source Table Name")

# Dropdown widget â€” constrained choices
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "production"], "Deployment Environment")

# Dropdown for date (common pattern)
dbutils.widgets.dropdown("run_date", "2025-01-15",
    ["2025-01-01", "2025-01-08", "2025-01-15", "2025-01-22", "2025-01-29"],
    "Processing Date")

# Combobox â€” dropdown + custom value
dbutils.widgets.combobox("region", "US", ["US", "EU", "APAC", "LATAM"], "Region Filter")

# Multiselect â€” select multiple values
dbutils.widgets.multiselect("metrics", "revenue", ["revenue", "cost", "margin", "customers", "orders"], "Metrics to Calculate")

# Slider equivalent (no native slider; use dropdown for numeric thresholds)
dbutils.widgets.dropdown("threshold", "0.05", ["0.01", "0.05", "0.10", "0.15", "0.25", "0.50"], "Alert Threshold (%)")

print("Widgets created! See the widget panel at the top of the notebook.")
print("Change values and re-run cells to see the effect.")

```

```python

```
#### 83.2 Reading Widget Values in Code

```python

source_table = dbutils.widgets.get("source_table")
environment = dbutils.widgets.get("environment")
run_date = dbutils.widgets.get("run_date")
region = dbutils.widgets.get("region")
metrics_str = dbutils.widgets.get("metrics")
threshold_str = dbutils.widgets.get("threshold")

# Parse multi-select (comma-separated)
selected_metrics = [m.strip() for m in metrics_str.split(",")]
threshold = float(threshold_str)

print("=== CURRENT WIDGET VALUES ===")
print(f"  Source Table : {source_table}")
print(f"  Environment  : {environment}")
print(f"  Run Date     : {run_date}")
print(f"  Region       : {region}")
print(f"  Metrics      : {selected_metrics}")
print(f"  Threshold    : {threshold:.1%}")

```

```python

```
#### 83.3 Widget + Job Parameter Pattern
In production, job parameters override widget defaults. We simulate this pattern.

```python

def get_parameter(key, widget_key, default_value):
    """
    Production pattern: job parameters > widget values > hardcoded defaults.

    In Workflows, job parameters are passed as a JSON dict to the task.
    We simulate this with a local dict.
    """
    job_params = {
        "source_table": "transactions_prod",
        "environment": "production",
        "run_date": "2025-01-15",
    }

    if key in job_params:
        return job_params[key]

    try:
        val = dbutils.widgets.get(widget_key)
        return val
    except Exception:
        return default_value

source = get_parameter("source_table", "source_table", "sales_transactions")
env = get_parameter("environment", "environment", "dev")
dt = get_parameter("run_date", "run_date", "2025-01-01")

print("=== RESOLVED PARAMETERS (Job params > Widgets > Defaults) ===")
print(f"  source_table = {source!r}")
print(f"  environment  = {env!r}")
print(f"  run_date     = {dt!r}")

```

```python

```
#### 83.4 Simulated Task Values (Inter-Task Communication)

```python

import json
import os
import datetime

class TaskValues:
    """
    Simulates dbutils.jobs.taskValues for Community Edition.

    Production API:
      dbutils.jobs.taskValues.set(key="row_count", value=10000)
      dbutils.jobs.taskValues.get(taskKey="upstream_task", key="row_count")
    """
    _store = {}

    @classmethod
    def set(cls, key, value):
        cls._store[key] = value
        print(f"  taskValues.set(key={key!r}, value={value!r})")

    @classmethod
    def get(cls, key, task_key=None, default=None):
        val = cls._store.get(key, default)
        print(f"  taskValues.get(key={key!r}) = {val!r}")
        return val

# --- Simulate Task A: Ingest raw data ---
print("=== TASK A: DATA INGESTION ===")
rows_ingested = 157_832
TaskValues.set("row_count", rows_ingested)
TaskValues.set("source_file", "2025-01-15_sales.csv.gz")
TaskValues.set("status", "success")

# --- Simulate Task B: Transform (reads Task A's values) ---
print("\n=== TASK B: TRANSFORMATION (depends on Task A) ===")
row_count = TaskValues.get("row_count")
source_file = TaskValues.get("source_file")
status = TaskValues.get("status")

if status == "success":
    print(f"  Processing {row_count:,} rows from {source_file}")
    rows_after_dedup = int(row_count * 0.95)
    TaskValues.set("dedup_row_count", rows_after_dedup)
    TaskValues.set("duplicates_removed", row_count - rows_after_dedup)
else:
    print("  Upstream failed; skipping transformation.")

# --- Simulate Task C: Aggregate (reads both A and B) ---
print("\n=== TASK C: AGGREGATION (depends on Task B) ===")
dedup_count = TaskValues.get("dedup_row_count")
duplicates = TaskValues.get("duplicates_removed", default=0)
print(f"  Rows after dedup : {dedup_count:,}")
print(f"  Duplicates removed: {duplicates:,}")

final_aggregates = {"total_revenue": 4_523_910.50, "total_orders": 12_457, "avg_order_value": 363.14}
TaskValues.set("aggregates", json.dumps(final_aggregates))
print(f"  Aggregates: {final_aggregates}")

print("\n=== TASK VALUES STORE ===")
print(json.dumps(TaskValues._store, indent=2, default=str))

```

```python

```
### Key Takeaway â€” Concept 83
- **Widgets** â€” interactive parameterisation; all types work in CE
- **Job Parameters** â€” set once per run, override widgets; production scheduling
- **Task Values** â€” inter-task data passing in multi-task jobs
- Resolution order: Job Params > Widgets > Code defaults
- Widgets are CE's most powerful built-in feature â€” leverage them!

```python

```
## Concept 84: Notebook Development Patterns

### Production Behaviour
Databricks notebooks support powerful compositional patterns:

| Pattern | Command | Use Case |
|---------|---------|----------|
| **%run** | %run /path/to/notebook | Import shared functions/variables inline |
| **dbutils.notebook.run()** | dbutils.notebook.run(path, timeout, args) | Run a child notebook with parameters, capture exit value |
| **Magic Commands** | %sql, %python, %fs, %sh, %md | Multi-language in one notebook |
| **%pip** | %pip install <library> | Notebook-scoped library installation |

### CE Availability
**ALL patterns work fully in Community Edition** â€” %run, dbutils.notebook.run(), magic commands, and %pip.

```python

```
#### 84.1 Shared Utility Functions (simulates a %run target)

```python

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

# --- Shared Configuration ---
PROJECT_NAME = "09_Workflows_Demo"
DEFAULT_ENV = "dev"

ENV_CONFIGS = {
    "dev": {"db_prefix": "dev_", "log_level": "DEBUG", "sample_ratio": 1.0},
    "staging": {"db_prefix": "stg_", "log_level": "INFO", "sample_ratio": 0.5},
    "production": {"db_prefix": "", "log_level": "WARN", "sample_ratio": 0.01},
}

def get_env_config(env=DEFAULT_ENV):
    return ENV_CONFIGS.get(env, ENV_CONFIGS["dev"])

# --- Data Quality Utilities ---

def check_null_rates(df, threshold=0.05):
    """Check null rate for every column in a DataFrame."""
    total = df.count()
    if total == 0:
        return {}
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    return {c: (null_counts[c] / total if null_counts[c] is not None else 0) for c in df.columns}

def check_freshness(partition_col="load_date", max_age_days=1):
    """Check if the latest partition is within acceptable freshness window."""
    latest = datetime(2025, 1, 15)  # Simulated
    cutoff = datetime.now() - timedelta(days=max_age_days)
    is_fresh = latest >= cutoff
    return is_fresh, latest

def validate_row_count(df, expected_min=0, expected_max=None):
    """Validate row count is within expected bounds."""
    actual = df.count()
    result = {"actual": actual, "expected_min": expected_min, "expected_max": expected_max, "passed": actual >= expected_min}
    if expected_max is not None:
        result["passed"] = result["passed"] and (actual <= expected_max)
    return result

def log_quality_check(check_name, passed, details):
    """Centralised logging for data quality checks."""
    status = "PASS" if passed else "FAIL"
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {status}: {check_name}")
    if not passed:
        print(f"    Details: {details}")

print("Utility functions loaded.")
print(f"  Available: check_null_rates, check_freshness, validate_row_count, log_quality_check, get_env_config")

```

```python

```
#### 84.2 %run Pattern â€” Import Shared Utilities
In production: `%run /Shared/utils/dq_utils`

This imports all variables and functions into the current notebook's namespace.

```python

config = get_env_config("dev")
print(f"Environment config: {config}")

print("\nIn a real project you would use:")
print("  %run /Shared/utils/dq_utils")
print("")
print("%run imports ALL variables and functions into the caller's scope.")
print("Use for: shared configs, UDF libraries, DQ utilities.")
print("Avoid for: heavy work that should be isolated.")

```

```python

```
#### 84.3 dbutils.notebook.run() â€” Isolated Execution

```python

import time

def child_notebook_simulation(source_path, output_path, run_date):
    """
    Simulates a child notebook called via:
    dbutils.notebook.run("/Shared/etl/transform_sales", 600, {"source_path": "..."})
    """
    print(f"[Child Notebook] Starting transformation...")
    print(f"  Source: {source_path}, Output: {output_path}, Date: {run_date}")

    time.sleep(0.5)
    rows_processed = 125_000
    print(f"[Child Notebook] Complete: {rows_processed:,} rows processed.")

    return {
        "rows_processed": rows_processed,
        "status": "success",
        "duration_seconds": 45.2
    }

# --- Orchestrator Pattern ---
print("=" * 60)
print("ORCHESTRATOR NOTEBOOK (Parent)")
print("=" * 60)

result_1 = child_notebook_simulation(
    source_path="s3://datalake/raw/sales/2025/01/15/",
    output_path="s3://datalake/bronze/sales/",
    run_date="2025-01-15"
)

result_2 = child_notebook_simulation(
    source_path="s3://datalake/raw/inventory/2025/01/15/",
    output_path="s3://datalake/bronze/inventory/",
    run_date="2025-01-15"
)

print(f"\n{'=' * 60}")
print("ALL CHILDREN COMPLETE")
print(f"{'=' * 60}")
print(f"  Sales pipeline     : {result_1['status']} ({result_1['rows_processed']:,} rows)")
print(f"  Inventory pipeline : {result_2['status']} ({result_2['rows_processed']:,} rows)")

print(f"\n{'=' * 60}")
print("PATTERN SELECTION GUIDE")
print(f"{'=' * 60}")
print("""
  %run:
    + Shared configuration and helper functions
    + UDF libraries, quick prototyping
    - Cannot pass parameters dynamically
    - Namespace pollution risk

  dbutils.notebook.run():
    + Isolated execution (its own Spark job)
    + Parameter passing via arguments
    + Exit value for downstream decisions
    + Parallel execution possible
    - Overhead of separate notebook run
    - No shared variables
""")

```

```python

```
#### 84.4 Magic Commands Showcase

```python

```
##### %sql â€” Embedded SQL queries

```python

# MAGIC %sql
# MAGIC SELECT '2025-01-15' AS load_date, 157832 AS row_count
# MAGIC UNION ALL
# MAGIC SELECT '2025-01-14' AS load_date, 142109 AS row_count
# MAGIC UNION ALL
# MAGIC SELECT '2025-01-13' AS load_date, 138456 AS row_count
# MAGIC ORDER BY load_date DESC

```

```python

```
##### %fs â€” File system operations

```python

# MAGIC %fs ls /databricks-datasets/

```

```python

```
##### %sh â€” Shell commands on the driver node

```python

# MAGIC %sh echo "Driver node: $(hostname)"; echo "Python: $(python --version 2>&1)"

```

```python

```
##### %pip â€” Notebook-scoped library installation

```python

import sys
print(f"Python version: {sys.version}")

try:
    import requests
    print(f"requests: {requests.__version__}")
except ImportError:
    print("requests not installed (use: %pip install requests)")

try:
    import pandas as pd
    print(f"pandas: {pd.__version__}")
except ImportError:
    print("pandas not installed")

print("\n%pip install <package> â€” notebook-scoped, no cluster restart")
print("%pip list              â€” show installed packages")

```

```python

```
#### 84.5 Notebook Testing Pattern

```python

import unittest
from unittest.mock import MagicMock

class TestDataQualityUtils(unittest.TestCase):
    """Unit tests for utility functions (runs within notebook)."""

    def test_get_env_config_dev(self):
        cfg = get_env_config("dev")
        self.assertEqual(cfg["db_prefix"], "dev_")
        self.assertEqual(cfg["log_level"], "DEBUG")

    def test_get_env_config_production(self):
        cfg = get_env_config("production")
        self.assertEqual(cfg["db_prefix"], "")
        self.assertEqual(cfg["log_level"], "WARN")

    def test_get_env_config_unknown_fallback(self):
        cfg = get_env_config("nonexistent")
        self.assertEqual(cfg["db_prefix"], "dev_")

    def test_validate_row_count_pass(self):
        mock_df = MagicMock()
        mock_df.count.return_value = 500
        result = validate_row_count(mock_df, expected_min=100, expected_max=1000)
        self.assertTrue(result["passed"])
        self.assertEqual(result["actual"], 500)

    def test_validate_row_count_fail_min(self):
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        result = validate_row_count(mock_df, expected_min=100)
        self.assertFalse(result["passed"])

    def test_validate_row_count_fail_max(self):
        mock_df = MagicMock()
        mock_df.count.return_value = 2000
        result = validate_row_count(mock_df, expected_min=100, expected_max=1000)
        self.assertFalse(result["passed"])

suite = unittest.TestLoader().loadTestsFromTestCase(TestDataQualityUtils)
runner = unittest.TextTestRunner(verbosity=1)
result = runner.run(suite)

if result.wasSuccessful():
    print("All utility function tests passed!")
else:
    print(f"{len(result.failures)} failures, {len(result.errors)} errors")

```

```python

```
### Key Takeaway â€” Concept 84
- %run = inline import (shared namespace); dbutils.notebook.run() = isolated execution (exit value)
- Magic commands (%sql, %fs, %sh, %pip) make notebooks multi-language
- %pip installs libraries per-notebook, no cluster restart
- Test utility functions directly within notebooks (lightweight unit tests)
- All patterns work in Community Edition â€” great for learning!

```python

```
## Concept 85: Databricks Workflows: Multi-Task Jobs

### Production Behaviour
Databricks Workflows let you define multi-step pipelines with dependencies, retries, and conditional logic:

| Task Type | Description |
|-----------|-------------|
| **Notebook** | Run a Databricks notebook |
| **Python Script** | Run a .py file (Repo or DBFS) |
| **Python Wheel** | Run a packaged wheel |
| **SQL** | Run a DBSQL query or dashboard |
| **dbt** | Run dbt Core commands |
| **JAR** | Run a Spark JAR job |
| **Pipeline** | Trigger a Delta Live Tables pipeline |
| **For Each** | Dynamic fan-out over an array |

```
WORKFLOW: Daily ETL Pipeline

                +----------+
                |  Ingest  |
                |  Raw     |
                +----+-----+
                     |
          +----------+----------+
          v          v          v
    +----------+ +----------+ +----------+
    |Transform | |Transform | |Transform |
    |  Sales   | |Inventory | |Customers |  <- Fan-out
    +----+-----+ +----+-----+ +----+-----+
         |            |            |
         +------------+------------+
                      v
                +----------+
                |Aggregate |  <- Fan-in
                |  KPI     |
                +----+-----+
                     v
                +----------+
                |  DQ      |  <- Conditional: only if aggregate succeeds
                |  Check   |
                +----------+
```

### CE Limitation
Community Edition **cannot create multi-task jobs** (single-task only). We show the architecture conceptually and implement a Python-based orchestrator.

```python

```
#### 85.1 Job Definition â€” JSON Structure (What Production Looks Like)

```python

job_definition = {
    "name": "Daily ETL Pipeline",
    "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC", "pause_status": "UNPAUSED"},
    "email_notifications": {"on_failure": ["data-eng@company.com"]},
    "timeout_seconds": 7200,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "ingest_raw",
            "description": "Ingest raw data from S3 to Bronze",
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/prod/pipelines/ingest_raw",
                "base_parameters": {"source_bucket": "s3://datalake/raw/", "target_table": "bronze.raw_events"}
            },
            "max_retries": 3,
            "min_retry_interval_millis": 60000,
            "timeout_seconds": 1800
        },
        {
            "task_key": "transform_sales",
            "depends_on": [{"task_key": "ingest_raw"}],
            "notebook_task": {"notebook_path": "/Repos/prod/pipelines/transform_sales"},
            "run_if": "ALL_SUCCESS",
            "timeout_seconds": 3600
        },
        {
            "task_key": "transform_inventory",
            "depends_on": [{"task_key": "ingest_raw"}],
            "notebook_task": {"notebook_path": "/Repos/prod/pipelines/transform_inventory"},
            "run_if": "ALL_SUCCESS",
            "timeout_seconds": 3600
        },
        {
            "task_key": "aggregate_kpis",
            "depends_on": [{"task_key": "transform_sales"}, {"task_key": "transform_inventory"}],
            "notebook_task": {"notebook_path": "/Repos/prod/pipelines/aggregate_kpis"},
            "run_if": "ALL_SUCCESS"
        },
        {
            "task_key": "dq_check",
            "depends_on": [{"task_key": "aggregate_kpis"}],
            "notebook_task": {"notebook_path": "/Repos/prod/pipelines/dq_check"},
            "run_if": "ALL_SUCCESS"
        }
    ],
    "parameters": [
        {"name": "run_date", "default": "{{yesterday_dash_n}}"},
        {"name": "processing_mode", "default": "full"}
    ],
    "tags": {"team": "data-engineering", "project": "daily-etl", "environment": "production"},
    "format": "MULTI_TASK"
}

print("=== PRODUCTION JOB DEFINITION (JSON) ===")
print(json.dumps(job_definition, indent=2))

```

```python

```
#### 85.2 CE Workaround: Python Orchestrator with Dependencies

```python

import time
import random
from datetime import datetime
from enum import Enum
from typing import Dict, List, Callable, Optional
from dataclasses import dataclass, field

class TaskStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

@dataclass
class TaskResult:
    task_key: str
    status: TaskStatus = TaskStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    output: dict = field(default_factory=dict)
    error: Optional[str] = None

class CEWorkflowOrchestrator:
    """
    Simulates a Databricks Workflow orchestrator for Community Edition.
    Supports: dependencies, retries, conditional execution, fan-out/in.
    """
    def __init__(self, name):
        self.name = name
        self.tasks = {}
        self.results = {}
        self.run_id = datetime.now().strftime("%Y%m%d%H%M%S")

    def add_task(self, task_key, func, depends_on=None, max_retries=0, run_if="ALL_SUCCESS", timeout_seconds=3600):
        self.tasks[task_key] = {
            "key": task_key, "func": func,
            "depends_on": depends_on or [], "max_retries": max_retries,
            "run_if": run_if, "timeout_seconds": timeout_seconds,
        }
        self.results[task_key] = TaskResult(task_key=task_key)

    def _all_dependencies_succeeded(self, task_key):
        deps = self.tasks[task_key]["depends_on"]
        return all(self.results[dep].status == TaskStatus.SUCCESS for dep in deps) if deps else True

    def _should_run(self, task_key):
        run_if = self.tasks[task_key]["run_if"]
        if run_if == "ALL_SUCCESS":
            return self._all_dependencies_succeeded(task_key)
        return True

    def _execute_task(self, task_key):
        task = self.tasks[task_key]
        result = TaskResult(task_key=task_key, status=TaskStatus.RUNNING)
        result.start_time = datetime.now()

        for attempt in range(task["max_retries"] + 1):
            try:
                print(f"  [{task_key}] Attempt {attempt + 1}/{task['max_retries'] + 1}")
                output = task["func"](run_id=self.run_id)
                result.status = TaskStatus.SUCCESS
                result.output = output or {}
                break
            except Exception as e:
                if attempt < task["max_retries"]:
                    wait = 2 ** attempt
                    print(f"  [{task_key}] Failed, retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    result.status = TaskStatus.FAILED
                    result.error = str(e)

        result.end_time = datetime.now()
        result.duration_seconds = (result.end_time - result.start_time).total_seconds() if result.start_time else 0
        return result

    def run(self):
        print(f"\n{'=' * 60}")
        print(f"WORKFLOW: {self.name}  |  Run ID: {self.run_id}")
        print(f"{'=' * 60}")

        completed = set()
        failed = set()
        all_keys = set(self.tasks.keys())

        while len(completed) + len(failed) < len(all_keys):
            made_progress = False
            for task_key in all_keys:
                if task_key in completed or task_key in failed:
                    continue
                deps_done = all(dep in completed or dep in failed for dep in self.tasks[task_key]["depends_on"])
                if deps_done:
                    if self._should_run(task_key):
                        print(f"\n>> [{task_key}] Starting...")
                        self.results[task_key] = self._execute_task(task_key)
                        if self.results[task_key].status == TaskStatus.SUCCESS:
                            completed.add(task_key)
                            print(f"  [{task_key}] Completed in {self.results[task_key].duration_seconds:.1f}s")
                        else:
                            failed.add(task_key)
                            print(f"  [{task_key}] Failed: {self.results[task_key].error}")
                    else:
                        self.results[task_key].status = TaskStatus.SKIPPED
                        completed.add(task_key)
                        print(f"  [{task_key}] Skipped")
                    made_progress = True
            if not made_progress:
                break

        print(f"\n{'=' * 60}")
        print("WORKFLOW SUMMARY")
        print(f"{'=' * 60}")
        print(f"{'Task':<25} {'Status':<12} {'Duration':>10}")
        print("-" * 50)
        for key in self.tasks:
            r = self.results[key]
            dur_str = f"{r.duration_seconds:.1f}s" if r.duration_seconds else "--"
            print(f"{key:<25} {r.status.value:<12} {dur_str:>10}")
        return self.results

# --- Define Tasks ---

def task_ingest_raw(**kwargs):
    time.sleep(0.3)
    return {"rows_ingested": 157_832, "files_processed": 12}

def task_transform_sales(**kwargs):
    time.sleep(0.5)
    return {"sales_rows": 89_421, "revenue": 4_523_910.50}

def task_transform_inventory(**kwargs):
    time.sleep(0.4)
    return {"inventory_rows": 45_210, "skus": 3_450}

def task_aggregate_kpis(**kwargs):
    time.sleep(0.3)
    return {"total_revenue": 4_523_910.50, "total_orders": 12_457, "margin_pct": 34.2}

def task_dq_check(**kwargs):
    time.sleep(0.2)
    return {"all_checks_passed": True, "checks_run": 8}

def task_publish_dashboard(**kwargs):
    time.sleep(0.1)
    return {"dashboard_refreshed": True, "viewers": 42}

# --- Build and Run Workflow ---

orchestrator = CEWorkflowOrchestrator("Daily ETL Pipeline (CE Simulated)")

orchestrator.add_task("ingest_raw", task_ingest_raw)
orchestrator.add_task("transform_sales", task_transform_sales, depends_on=["ingest_raw"], max_retries=2)
orchestrator.add_task("transform_inventory", task_transform_inventory, depends_on=["ingest_raw"], max_retries=2)
orchestrator.add_task("aggregate_kpis", task_aggregate_kpis, depends_on=["transform_sales", "transform_inventory"])
orchestrator.add_task("dq_check", task_dq_check, depends_on=["aggregate_kpis"])
orchestrator.add_task("publish_dashboard", task_publish_dashboard, depends_on=["dq_check"])

results = orchestrator.run()

```

```python

```
#### 85.3 Scheduling Concepts (Trigger Types)

```python

print("=== JOB TRIGGER TYPES (Production) ===\n")

triggers = [
    {"type": "Scheduled", "example": "Cron: 0 0 6 * * ?", "ce": "No (single-task only)"},
    {"type": "File Arrival", "example": "Trigger on s3://.../*.csv", "ce": "No"},
    {"type": "Continuous", "example": "Streaming pipeline", "ce": "No"},
    {"type": "Manual (Run Now)", "example": "Click 'Run Now' in UI", "ce": "Yes"},
    {"type": "API Trigger", "example": "POST .../jobs/run-now", "ce": "Limited"},
    {"type": "Webhook", "example": "GitHub push, Slack command", "ce": "No"},
]

print(f"{'Trigger':<18} {'CE':<12} {'Example'}")
print("-" * 55)
for t in triggers:
    print(f"{t['type']:<18} {t['ce']:<12} {t['example']}")

```

```python

```
### Key Takeaway â€” Concept 85
- Multi-task Workflows are the core of production orchestration
- Task types: Notebook, Python Script, Python Wheel, SQL, dbt, JAR, Pipeline
- Fan-out (parallel tasks) and fan-in (aggregation after parallel)
- CE cannot create multi-task jobs â€” use our Python orchestrator for learning
- Job definitions can be JSON (API) or YAML (DABs) format
- Triggers: Scheduled, File Arrival, Continuous, Manual, API, Webhook
```python

```
## Concept 86: Task Dependencies, Retries & Conditional Logic

### Production Behaviour
Each task in a Workflow supports fine-grained retry and conditional execution:

| Feature | Options | Description |
|---------|---------|-------------|
| **max_retries** | 0â€“5 | Number of retry attempts on failure |
| **min_retry_interval_millis** | >= 0 | Minimum wait between retries |
| **retry_on_timeout** | true/false | Whether timeouts trigger retries |
| **timeout_seconds** | 0â€“259200 | Max task runtime before forced termination |
| **run_if** | ALL_SUCCESS, ALL_DONE, AT_LEAST_ONE_SUCCESS, NONE_FAILED | Conditional execution |
| **depends_on** | List of task keys | Upstream dependencies |

```
RETRY BEHAVIOUR WITH EXPONENTIAL BACKOFF:

Attempt 1 (t=0s)  --FAIL-->
  wait 60s
Attempt 2 (t=60s) --FAIL-->
  wait 120s
Attempt 3 (t=180s) --SUCCESS--> (done)

If max_retries exhausted -> task fails -> downstream runs/skips per run_if
```

```python

```
#### 86.1 Retry Decorator (CE Implementation)

```python

import functools
import time

def retry_with_backoff(max_retries=3, base_delay=1.0, backoff_factor=2,
                       retry_on_exceptions=(Exception,)):
    """
    Production-grade retry decorator for Community Edition.
    Emulates the retry behaviour of Databricks Workflow tasks.

    Args:
        max_retries: Maximum retry attempts (total = 1 + max_retries)
        base_delay: Initial delay in seconds between retries
        backoff_factor: Multiplier for each subsequent retry (2 = exponential)
        retry_on_exceptions: Tuple of exception types to retry on

    Usage:
        @retry_with_backoff(max_retries=3, base_delay=30)
        def my_etl_task():
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        delay = base_delay * (backoff_factor ** (attempt - 1))
                        print(f"    Retry {attempt}/{max_retries} in {delay:.1f}s...")
                        time.sleep(delay)
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        print(f"    Succeeded on attempt {attempt + 1}!")
                    return result
                except retry_on_exceptions as e:
                    last_exception = e
                    print(f"    Attempt {attempt + 1} failed: {e}")
            raise RuntimeError(
                f"Task '{func.__name__}' failed after {max_retries + 1} attempts. "
                f"Last error: {last_exception}"
            )
        return wrapper
    return decorator

# --- Demonstrate ---
call_count = {"value": 0}

@retry_with_backoff(max_retries=3, base_delay=0.1, backoff_factor=2,
                    retry_on_exceptions=(ValueError, ConnectionError))
def flaky_api_call(should_fail_times=5):
    call_count["value"] += 1
    if call_count["value"] <= should_fail_times:
        raise ConnectionError(f"Transient error (call #{call_count['value']})")
    return {"status": "success", "attempt": call_count["value"]}

print("=== RETRY DECORATOR DEMO ===\n")

print("1. Task that succeeds on 3rd attempt:")
try:
    result = flaky_api_call(should_fail_times=2)
    print(f"   Result: {result}")
except Exception as e:
    print(f"   {e}")

call_count["value"] = 0

print("\n2. Task that never succeeds:")
try:
    result = flaky_api_call(should_fail_times=10)
except RuntimeError as e:
    print(f"   Gracefully handled: {e}")

```

```python

```
#### 86.2 Conditional Execution Patterns (run_if)

```python

print("=== RUN_IF CONDITION DEMONSTRATION ===\n")

# Simulate upstream results
upstream = {
    "ingest_raw": True,
    "transform_customers": False,
    "transform_sales": True,
}

conditions = {
    "ALL_SUCCESS": all(upstream.values()),
    "ALL_DONE": True,
    "AT_LEAST_ONE_SUCCESS": any(upstream.values()),
    "NONE_FAILED": not any(not v for v in upstream.values()),
}

print(f"Upstream status: {upstream}\n")
print(f"{'Condition':<25} {'Result':<10} {'Runs?'}")
print("-" * 55)
for cond, result in conditions.items():
    print(f"{cond:<25} {str(result):<10} {'RUN' if result else 'SKIP'}")

print(f"\n{'=' * 60}")
print("PRODUCTION SCENARIOS")
print(f"{'=' * 60}")
print("""
  ALL_SUCCESS        -> DQ checks after ETL (all stages must pass)
  ALL_DONE           -> Cleanup after pipeline (run regardless of outcome)
  AT_LEAST_ONE_SUCCESS -> Fallback data source (primary or backup is enough)
  NONE_FAILED        -> Notify if smooth (alert only when everything works)
""")

```

```python

```
#### 86.3 Alerting Patterns (Email & Webhook Simulation)

```python

import json
import datetime

class AlertManager:
    """
    Simulates production alerting (PagerDuty, Slack, Email).

    Production:
      - Email notifications configured in Job definition
      - Webhook destinations for Slack, PagerDuty, Teams
      - SQL Alerts for threshold-based notifications
    """

    def __init__(self):
        self.alerts_sent = []

    def send_email(self, subject, body, recipients):
        self.alerts_sent.append({"type": "EMAIL", "timestamp": datetime.datetime.now().isoformat(), "subject": subject})
        print(f"EMAIL -> {recipients} | {subject}")

    def send_slack(self, channel, message, severity="info"):
        emoji = {"info": "(i)", "warning": "(!)", "critical": "(!!)"}.get(severity, "(i)")
        self.alerts_sent.append({"type": "SLACK", "channel": channel, "severity": severity, "message": message})
        print(f"{emoji} SLACK -> #{channel} | [{severity.upper()}] {message}")

    def send_pagerduty(self, service, summary, severity="error"):
        self.alerts_sent.append({"type": "PAGERDUTY", "service": service, "summary": summary})
        print(f"PAGERDUTY -> {service} | {summary}")

    def summary(self):
        print(f"\n{'=' * 50}")
        print(f"ALERT SUMMARY: {len(self.alerts_sent)} alerts sent")
        print(f"{'=' * 50}")
        for a in self.alerts_sent:
            print(f"  [{a['timestamp']}] {a['type']} {a.get('subject', a.get('message', ''))[:60]}")

alerts = AlertManager()

print("=== PRODUCTION ALERTING PATTERNS ===\n")

alerts.send_slack("data-alerts", "ETL job 'daily_sales' started (Run #4521)", "info")
alerts.send_slack("data-alerts", "DQ Check FAILED: null_rate > threshold (8.2% > 5%)", "critical")
alerts.send_pagerduty("Data Pipelines", "DQ failure in daily_sales: null_rate exceeded threshold")
alerts.send_email("ETL Job Failed: daily_sales", "DQ check failed. See logs.", ["data-eng@company.com"])
alerts.send_slack("data-alerts", "ETL job 'daily_sales' completed (after retry)", "info")

alerts.summary()

```

```python

```
### Key Takeaway â€” Concept 86
- **Retries** with exponential backoff handle transient failures gracefully
- **run_if** conditions enable sophisticated pipeline branching
- **Task Values** pass data between tasks (demonstrated in Section 83.4)
- **Alerting** integrates with Email, Slack, PagerDuty, Webhooks
- CE can simulate all of these patterns in Python

```python

```
## Concept 87: Git Integration & Repos

### Production Behaviour
Databricks Repos provides first-class Git integration for version-controlled notebook development:

| Feature | Description |
|---------|-------------|
| **Git Providers** | GitHub, GitLab, Bitbucket, Azure DevOps, AWS CodeCommit |
| **Repo Structure** | Notebooks, Python files, YAML configs all in one repo |
| **Branching** | Main, feature branches, PR-based workflow |
| **Sync** | Pull latest, create branch from repo, merge |
| **CI/CD** | Trigger jobs on PR, run tests on push |

```
REPOS DIRECTORY LAYOUT:

databricks-project/
|-- src/ingest/ingest_raw.py
|-- src/transform/transform_sales.py
|-- src/transform/transform_inventory.py
|-- src/aggregate/kpi_aggregates.py
|-- tests/test_ingest.py
|-- tests/test_transform.py
|-- config/dev_config.yaml
|-- config/prod_config.yaml
|-- databricks.yml          (DAB definition)
|-- .github/workflows/deploy.yml  (CI/CD)
|-- README.md
```

### CE Limitation
Community Edition has **limited Git support** (no Repos API). We demonstrate the patterns and best practices conceptually.

```python

```
#### 87.1 Branching Strategy for Notebook Development

```python

branching_strategy = """
GIT BRANCHING STRATEGY FOR DATABRICKS PROJECTS

  main ------*----------------------------*--- (v1.1.0) ---*-- (v1.2.0)
             |                            |                 |
             +-- develop -*--*--*------*------------------*--
                          |  |  |      |
                          |  |  |      +-- release/v1.1 --*-- merge back
                          |  |  |
                          |  |  +-- feature/add-dq   --*-- PR --> merge to develop
                          |  |
                          |  +-- feature/new-metrics --*-- PR --> merge to develop
                          |
                          +-- hotfix/critical-bug --*-- PR --> merge to main + develop

  WORKFLOW:
  1. Create feature branch from develop
  2. Develop in Databricks Repos (auto-syncs to feature branch)
  3. Open PR in GitHub -> trigger CI checks (lint, unit tests)
  4. Code review -> approve -> merge to develop
  5. Deploy from develop to staging workspace (DABs deploy)
  6. Create release branch -> deploy to production
  7. Merge release branch to main + back to develop

  PROTECTION RULES (main branch):
    - Require pull request reviews (>= 1 approval)
    - Require status checks to pass (CI)
    - Require branches to be up to date
    - No direct pushes to main
"""

print(branching_strategy)

```

```python

```
#### 87.2 CI/CD Pipeline Concept (GitHub Actions)

```python

github_actions_yaml = """
# .github/workflows/databricks-ci.yml
# PRODUCTION CI/CD PIPELINE FOR DATABRICKS NOTEBOOKS

name: Databricks CI/CD Pipeline

on:
  pull_request:
    branches: [main, develop]
  push:
    branches: [develop]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Lint with flake8
        run: |
          pip install flake8
          flake8 src/ tests/ --max-line-length=120
      - name: Run unit tests
        run: |
          pip install -r requirements.txt
          pytest tests/ -v --cov=src/ --cov-report=xml

  deploy-staging:
    needs: lint-and-test
    if: github.event_name == 'push' && github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        uses: databricks/setup-cli@main
      - name: Deploy to Staging
        run: |
          databricks bundle validate -t staging
          databricks bundle deploy -t staging
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_STAGING_TOKEN }}

  deploy-production:
    needs: lint-and-test
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        uses: databricks/setup-cli@main
      - name: Deploy to Production
        run: |
          databricks bundle validate -t production
          databricks bundle deploy -t production
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
"""

print("=== CI/CD PIPELINE (GitHub Actions) ===")
print(github_actions_yaml)

```

```python

```
#### 87.3 PR Review Checklist Simulation

```python

class PRReviewChecklist:
    """Simulates a pull request review process for notebook changes."""

    def __init__(self, pr_title, author, changed_files):
        self.pr_title = pr_title
        self.author = author
        self.changed_files = changed_files
        self.checks = self._run_automated_checks()

    def _run_automated_checks(self):
        checks = [
            ("No hardcoded secrets", self._check_no_secrets()),
            ("Widgets have defaults", (True, "OK")),
            ("Tests added for new logic", self._check_tests_exist()),
            ("Notebooks have Markdown descriptions", (True, "OK")),
            ("Cluster tags configured", (True, "OK")),
        ]
        return checks

    def _check_no_secrets(self):
        for f in self.changed_files:
            if "password" in f.get("content", "").lower() and "dbutils.secrets" not in f.get("content", ""):
                return False, f"Hardcoded password in {f['name']}"
        return True, "No hardcoded secrets found"

    def _check_tests_exist(self):
        has_test = any("test_" in f["name"] for f in self.changed_files)
        if not has_test:
            return False, "No test files added"
        return True, "Tests found"

    def print_report(self):
        print(f"\n{'=' * 60}")
        print(f"PR REVIEW: {self.pr_title}")
        print(f"Author: {self.author}  |  Files: {len(self.changed_files)}")
        print(f"{'=' * 60}")
        print(f"{'Check':<35} {'Status':<10} {'Details'}")
        print("-" * 60)
        all_pass = True
        for name, (passed, detail) in self.checks:
            icon = "PASS" if passed else "FAIL"
            if not passed:
                all_pass = False
            print(f"{name:<35} {icon:<10} {detail}")
        print("-" * 60)
        print(f"OVERALL: {'APPROVED' if all_pass else 'CHANGES REQUESTED'}")

pr = PRReviewChecklist(
    pr_title="feat: Add data quality checks to ETL pipeline",
    author="data-engineer-1",
    changed_files=[
        {"name": "src/transform/dq_checks.py", "content": "def check_nulls(df): return df.count()"},
        {"name": "tests/test_dq_checks.py", "content": "def test_check_nulls(): assert True"},
        {"name": "src/transform/legacy_script.py", "content": "password = 'hardcoded123'"},
        {"name": "config/dev_config.yaml", "content": "threshold: 0.05"},
    ]
)
pr.print_report()

```

```python

```
### Key Takeaway â€” Concept 87
- Use Databricks Repos for Git-backed notebook development (not /Users/ folders)
- Structured directory layout: src/, tests/, config/
- Branch strategy: Main (prod) <- Develop (integration) <- Feature branches
- PR workflow: automated lint + tests -> code review -> merge -> deploy
- CE has limited Git â€” practice the structure and workflows conceptually

```python

```
## Concept 88: Monitoring, Alerting & SQL Alerts

### Production Behaviour
Databricks provides several layers of monitoring:

| Mechanism | Scope | Use Case |
|-----------|-------|----------|
| **SQL Alerts** | DBSQL queries | Threshold-based: e.g., 0 rows ingested |
| **Job Run Notifications** | Per-job | Email on start/success/failure |
| **Webhook Destinations** | Account-wide | Slack, PagerDuty, Teams, custom HTTP |
| **System Tables** | Account-wide | Query job/cluster/audit history |
| **Cluster Event Logs** | Per-cluster | Spark events, GC logs, executor metrics |
| **Grafana Integration** | Workspace | Custom dashboards from system tables |

### CE Limitation
No DBSQL Alerts, no webhook destinations. We build manual monitoring and alerting in Python.

```python

```
#### 88.1 Data Freshness Monitor (CE Implementation)

```python

import datetime
import random
from dataclasses import dataclass

@dataclass
class FreshnessCheck:
    table_name: str
    passed: bool
    details: str

class DataPipelineMonitor:
    """
    Production-equivalent monitoring system.
    In production, these checks would be SQL Alerts in DBSQL.
    """

    def __init__(self):
        self.checks_run = []

    def check_freshness(self, table_name, expected_latest, actual_latest, max_delay_hours=4):
        delay_hours = (expected_latest - actual_latest).total_seconds() / 3600
        check = FreshnessCheck(
            table_name=table_name,
            passed=delay_hours <= max_delay_hours,
            details=f"Delay: {delay_hours:.1f}h (max: {max_delay_hours}h)"
        )
        self.checks_run.append(check)
        return check

    def check_null_rate(self, table_name, column, null_rate, threshold=0.05):
        check = FreshnessCheck(
            table_name=table_name,
            passed=null_rate <= threshold,
            details=f"Null rate {null_rate:.2%} for {column} (threshold: {threshold:.2%})"
        )
        self.checks_run.append(check)
        return check

    def check_volume(self, table_name, actual_rows, expected_min, expected_max=None):
        passed = actual_rows >= expected_min
        if expected_max is not None:
            passed = passed and (actual_rows <= expected_max)
        rng = f"{expected_min:,}-{expected_max:,}" if expected_max else f">={expected_min:,}"
        check = FreshnessCheck(
            table_name=table_name,
            passed=passed,
            details=f"Row count {actual_rows:,} (expected: {rng})"
        )
        self.checks_run.append(check)
        return check

    def run_all_checks(self):
        now = datetime.datetime(2025, 1, 15, 8, 0, 0)
        self.check_freshness("gold.daily_sales", now, now - datetime.timedelta(hours=2), 4)
        self.check_freshness("gold.daily_inventory", now, now - datetime.timedelta(hours=6), 4)
        self.check_freshness("gold.customer_360", now, now - datetime.timedelta(hours=1), 4)
        self.check_null_rate("silver.transactions", "customer_id", 0.02, 0.05)
        self.check_null_rate("silver.transactions", "product_sku", 0.08, 0.05)
        self.check_null_rate("silver.orders", "delivery_date", 0.01, 0.05)
        self.check_volume("bronze.raw_events", 157_832, 100_000, 200_000)
        self.check_volume("gold.daily_sales", 12_457, 1_000)

monitor = DataPipelineMonitor()
monitor.run_all_checks()

print("=" * 60)
print("DATA PIPELINE HEALTH REPORT")
print("=" * 60)

print(f"\n{'Check':<45} {'Status':<8} {'Details'}")
print("-" * 80)
passed_count = 0
for check in monitor.checks_run:
    icon = "PASS" if check.passed else "FAIL"
    if check.passed:
        passed_count += 1
    print(f"{check.table_name:<45} {icon:<8} {check.details}")

print("-" * 80)
total = len(monitor.checks_run)
print(f"\nHEALTH SCORE: {passed_count}/{total} passed ({(passed_count/total*100):.1f}%)")
if passed_count < total:
    print(f"{total - passed_count} alerts generated - review failed checks above.")
else:
    print("All checks passed - pipeline is healthy.")

```

```python

```
#### 88.2 Simulated SQL Alert (What Production Looks Like)

```python

sql_alert_definition = {
    "name": "Stale Data Alert: gold.daily_sales",
    "description": "Triggers if daily_sales hasn't been updated in 4+ hours",
    "query": """
        SELECT
            CASE WHEN MAX(load_timestamp) < DATEADD(HOUR, -4, CURRENT_TIMESTAMP())
                 THEN 'STALE' ELSE 'FRESH'
            END AS data_status,
            MAX(load_timestamp) AS last_load,
            DATEDIFF(HOUR, MAX(load_timestamp), CURRENT_TIMESTAMP()) AS hours_since_update
        FROM gold.daily_sales
        WHERE load_date = CURRENT_DATE()
    """,
    "condition": {"column": "data_status", "op": "==", "value": "STALE"},
    "schedule": {"quartz_cron_expression": "0 0/15 * * * ?", "timezone_id": "UTC"},
    "notification": {
        "destinations": [
            {"type": "EMAIL", "addresses": ["data-eng@company.com"]},
            {"type": "SLACK", "channel": "#data-alerts"},
            {"type": "PAGERDUTY", "integration_key": "secret://pd-key"}
        ]
    },
    "state": "ENABLED"
}

print("=== SIMULATED SQL ALERT DEFINITION (DBSQL) ===\n")
print(json.dumps(sql_alert_definition, indent=2))
print("\nIn Community Edition, this exact alert cannot be created.")
print("Use the Python monitor above + cron/scheduler instead.")

```

```python

```
#### 88.3 Alert Severity Classification

```python

class AlertRouter:
    """Routes alerts to the right team based on severity and domain."""

    ROUTING_TABLE = {
        "data_freshness": {
            "P1_CRITICAL": ["pagerduty://data-oncall", "slack://#data-critical"],
            "P2_HIGH": ["slack://#data-alerts", "email://data-eng@company.com"],
            "P3_MEDIUM": ["email://data-eng@company.com"],
            "P4_LOW": [],
        },
        "pipeline_failure": {
            "P1_CRITICAL": ["pagerduty://data-oncall"],
            "P2_HIGH": ["slack://#data-alerts"],
            "P3_MEDIUM": ["email://data-eng@company.com"],
            "P4_LOW": [],
        },
        "cost_anomaly": {
            "P1_CRITICAL": ["pagerduty://platform-oncall"],
            "P2_HIGH": ["slack://#platform-alerts"],
            "P3_MEDIUM": ["email://platform@company.com"],
            "P4_LOW": [],
        },
    }

    @staticmethod
    def route(domain, severity, message):
        channels = AlertRouter.ROUTING_TABLE.get(domain, {}).get(severity, [])
        if channels:
            print(f"[{severity[:2]}] {domain}: {message}")
            for ch in channels:
                print(f"    -> {ch}")
        else:
            print(f"[{severity[:2]}] {domain}: {message} (logged only)")
        return channels

print("=== ALERT SEVERITY CLASSIFICATION ===\n")

print(f"  P1 CRITICAL â€” Service Down (immediate response, 24/7 on-call)")
print(f"  P2 HIGH     â€” Degraded (respond within 30 min)")
print(f"  P3 MEDIUM   â€” Minor Issue (respond within 4 hours)")
print(f"  P4 LOW      â€” Informational (no SLA)")

print(f"\n=== ROUTING EXAMPLES ===\n")
AlertRouter.route("data_freshness", "P1_CRITICAL", "gold.daily_sales last updated 6 hours ago")
print()
AlertRouter.route("pipeline_failure", "P2_HIGH", "Task 'transform_sales' failed after 3 retries")
print()
AlertRouter.route("cost_anomaly", "P3_MEDIUM", "DBU usage 30% above 7-day average")

```

```python

```
### Key Takeaway â€” Concept 88
- SQL Alerts are the primary threshold-based notification mechanism in production
- Classify alerts: P1 (critical) to P4 (informational)
- Route alerts by domain and severity to appropriate channels
- CE: implement monitoring functions in Python; simulate alert routing
- Production: SQL Alerts -> Webhook Destinations -> PagerDuty/Slack/Email

```python

```
## Concept 89: System Tables for Operations

### Production Behaviour
System tables provide operational data for monitoring, cost tracking, and auditing:

| Schema | Key Tables | Purpose |
|--------|-----------|---------|
| **system.billing** | usage, list_prices | Cost tracking, DBU consumption |
| **system.compute** | clusters, node_timeline | Cluster lifecycle, auto-scaling events |
| **system.jobs** | job_runs, task_runs | Job execution history, durations, failures |
| **system.access** | audit | Access logs for compliance |
| **system.storage** | blob_usage | Storage consumption |

### CE Limitation
System tables are not available in Community Edition. We build our own operational tracking.

```python

```
#### 89.1 Custom Operational Tracking Tables

```python

import datetime
import random
import uuid
from collections import defaultdict

class OperationalDB:
    """Simulates system tables for Community Edition."""

    def __init__(self):
        self.job_runs = []
        self.cost_records = []

    def record_job_run(self, job_name, task_name, status, duration_seconds, run_date=None):
        self.job_runs.append({
            "run_id": str(uuid.uuid4())[:8],
            "job_name": job_name,
            "task_name": task_name,
            "status": status,
            "start_time": (run_date or datetime.datetime.now()) - datetime.timedelta(seconds=duration_seconds),
            "end_time": run_date or datetime.datetime.now(),
            "duration_seconds": duration_seconds,
            "run_date": (run_date or datetime.datetime.now()).strftime("%Y-%m-%d"),
        })

    def record_cost(self, sku, dbus, cost_usd, usage_date):
        self.cost_records.append({
            "usage_date": usage_date,
            "sku_name": sku,
            "dbus_consumed": dbus,
            "cost_usd": cost_usd,
        })

# --- Generate Synthetic Data ---
ops_db = OperationalDB()
base_date = datetime.datetime(2025, 1, 15)

for day_offset in range(30, 0, -1):
    run_date = base_date - datetime.timedelta(days=day_offset)
    run_date_str = run_date.strftime("%Y-%m-%d")

    ingest_status = random.choices(["SUCCESS", "FAILED"], weights=[0.92, 0.08])[0]
    ops_db.record_job_run("Daily ETL", "ingest_raw", ingest_status, random.uniform(120, 600), run_date)

    if ingest_status == "SUCCESS":
        for task in ["transform_sales", "transform_inventory"]:
            ops_db.record_job_run("Daily ETL", task,
                random.choices(["SUCCESS", "FAILED"], weights=[0.95, 0.05])[0],
                random.uniform(180, 900), run_date)
        ops_db.record_job_run("Daily ETL", "aggregate_kpis", "SUCCESS", random.uniform(60, 300), run_date)

    ops_db.record_cost("JOBS_COMPUTE", random.uniform(10, 80), random.uniform(4, 32), run_date_str)
    ops_db.record_cost("ALL_PURPOSE_COMPUTE", random.uniform(5, 40), random.uniform(2.75, 22), run_date_str)

print(f"Generated: {len(ops_db.job_runs)} job runs, {len(ops_db.cost_records)} cost records")

```

```python

```
#### 89.2 Operational Dashboard Queries

```python

from collections import Counter

print("=" * 70)
print("OPERATIONAL DASHBOARD")
print("=" * 70)

# --- Widget 1: Job Success Rate ---
print("\nJOB EXECUTION SUMMARY (last 30 days)")
print("-" * 70)

total_runs = len(ops_db.job_runs)
success_runs = sum(1 for r in ops_db.job_runs if r["status"] == "SUCCESS")
failed_runs = sum(1 for r in ops_db.job_runs if r["status"] == "FAILED")
avg_duration = sum(r["duration_seconds"] for r in ops_db.job_runs) / total_runs if total_runs else 0

print(f"  Total runs  : {total_runs}")
print(f"  Successes   : {success_runs} ({success_runs/total_runs*100:.1f}%)")
print(f"  Failures    : {failed_runs} ({failed_runs/total_runs*100:.1f}%)")
print(f"  Avg duration: {avg_duration/60:.1f} minutes")

# --- Widget 2: Per-Task Performance ---
print(f"\nTASK-LEVEL PERFORMANCE")
print("-" * 70)

task_stats = defaultdict(lambda: {"runs": 0, "success": 0, "total_dur": 0})
for r in ops_db.job_runs:
    t = r["task_name"]
    task_stats[t]["runs"] += 1
    if r["status"] == "SUCCESS":
        task_stats[t]["success"] += 1
    task_stats[t]["total_dur"] += r["duration_seconds"]

print(f"{'Task':<25} {'Runs':>6} {'Success %':>10} {'Avg Dur (s)':>12}")
print("-" * 56)
for task, stats in sorted(task_stats.items()):
    pct = stats["success"] / stats["runs"] * 100
    avg = stats["total_dur"] / stats["runs"]
    print(f"{task:<25} {stats['runs']:>6} {pct:>9.1f}% {avg:>11.0f}")

# --- Widget 3: Daily Cost Trend ---
print(f"\nDAILY COST TREND (last 7 days)")
print("-" * 70)

daily_costs = defaultdict(lambda: defaultdict(float))
for rec in ops_db.cost_records:
    daily_costs[rec["usage_date"]][rec["sku_name"]] += rec["cost_usd"]

dates = sorted(daily_costs.keys())[-7:]
print(f"{'Date':<12} {'Jobs':>10} {'All-Purpose':>12} {'Total':>10}")
print("-" * 46)
for d in dates:
    jobs = daily_costs[d].get("JOBS_COMPUTE", 0)
    ap = daily_costs[d].get("ALL_PURPOSE_COMPUTE", 0)
    print(f"{d:<12} ${jobs:>9.2f} ${ap:>11.2f} ${jobs+ap:>9.2f}")

# --- Widget 4: Threshold Check ---
print(f"\nAUTOMATED THRESHOLD CHECK")
print("-" * 70)

failure_rate = failed_runs / total_runs * 100 if total_runs else 0
if failure_rate > 10.0:
    print(f"  WARNING: Failure rate ({failure_rate:.1f}%) exceeds threshold (10%)!")
else:
    print(f"  Failure rate ({failure_rate:.1f}%) is within acceptable range (10%)")

```

```python

```
#### 89.3 Cost Attribution & Showback

```python

print("=" * 60)
print("COST ATTRIBUTION SUMMARY (Monthly Estimate)")
print("=" * 60)

monthly_costs = [
    ("Production ETL Jobs", 1420, 568.00, 45.2),
    ("Data Science (All-Purpose)", 680, 374.00, 29.8),
    ("DBSQL Dashboards", 320, 176.00, 14.0),
    ("Dev/Test Environments", 250, 137.50, 11.0),
]

print(f"\n{'Category':<30} {'DBUs':>8} {'Cost (USD)':>12} {'Share':>8}")
print("-" * 60)
total_cost = 0
for name, dbus, cost, pct in monthly_costs:
    print(f"{name:<30} {dbus:>8,.0f} ${cost:>11,.2f} {pct:>7.1f}%")
    total_cost += cost
print("-" * 60)
print(f"{'TOTAL':<30} {'':>8} ${total_cost:>11,.2f} {'100.0%':>8}")

print(f"\nCost management tips:")
print(f"  1. Tag all clusters with cost_center and team")
print(f"  2. Query system.billing.usage daily for anomalies")
print(f"  3. Set per-user DBU budgets with alert thresholds")
print(f"  4. Use Serverless for variable workloads, Job Clusters for predictable")
print(f"  5. Auto-terminate idle all-purpose clusters (< 60 min)")

```

```python

```
### Key Takeaway â€” Concept 89
- System tables are the operational backbone of a production Databricks workspace
- system.billing.usage -> track costs; system.compute.clusters -> cluster lifecycle; system.jobs.job_runs -> job history
- Build operational dashboards in DBSQL querying these tables
- CE: create your own operational tracking (even if in-memory or CSV)
- Tag everything for cost attribution (team, project, environment)

```python

```
## Concept 90: Databricks Asset Bundles (DABs)

### Production Behaviour
Databricks Asset Bundles (DABs) bring infrastructure-as-code to the Databricks platform:

| Concept | Description |
|---------|-------------|
| **databricks.yml** | Single file defining all resources (jobs, pipelines, models, schemas) |
| **Bundle** | A collection of resources deployed together to a target |
| **Targets** | Environment-specific overrides (dev, staging, production) |
| **Variables** | Parameterisation across environments |
| **bundle validate** | Validate YAML syntax and resource references |
| **bundle deploy** | Deploy resources to a target workspace |
| **bundle run** | Run a job or pipeline defined in the bundle |
| **bundle destroy** | Remove deployed resources |

```
DAB: INFRASTRUCTURE-AS-CODE

  databricks.yml                     Workspace
  +------------------+              +------------------+
  | resources:       |   deploy     | Jobs:            |
  |   jobs:          | ---------->  |   +- daily_etl   |
  |     daily_etl:   |              |   +- dq_monitor  |
  |       ...        |              | Pipelines:       |
  |   pipelines:     |              |   +- sales_dlt   |
  |     sales_dlt:   |              | Notifications:   |
  |       ...        |              |   +- webhooks    |
  | targets:         |              +------------------+
  |   dev/staging/prod  |            Same YAML, different targets
  +------------------+
```

### CE Limitation
The Databricks CLI bundle command does not work with Community Edition. We show the YAML structure and explain the IaC philosophy.

```python

```
#### 90.1 Complete databricks.yml Example

```python

databricks_yml = """
# ============================================================
# databricks.yml  --  Full Bundle Definition
# ============================================================
# Deploy with:
#   databricks bundle validate -t dev
#   databricks bundle deploy -t dev
#   databricks bundle run daily_etl -t dev
# ============================================================

bundle:
  name: "sales_data_pipeline"

variables:
  run_date:
    description: "Processing date (YYYY-MM-DD)"
    default: "2025-01-15"
  environment:
    description: "Deployment environment"
    default: "dev"

resources:
  jobs:
    daily_etl:
      name: "[${bundle.target}] Daily ETL Pipeline"
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
        pause_status: "UNPAUSED"

      email_notifications:
        on_failure:
          - "data-eng@company.com"
        no_alert_for_skipped_runs: false

      tasks:
        - task_key: ingest_raw
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ${workspace.root_path}/src/ingest/ingest_raw
            base_parameters:
              run_date: "{{run_date}}"
              environment: "${bundle.target}"
          max_retries: 3
          min_retry_interval_millis: 60000
          timeout_seconds: 1800

        - task_key: transform_sales
          depends_on:
            - task_key: ingest_raw
          notebook_task:
            notebook_path: ${workspace.root_path}/src/transform/transform_sales
          run_if: ALL_SUCCESS
          timeout_seconds: 3600

        - task_key: transform_inventory
          depends_on:
            - task_key: ingest_raw
          notebook_task:
            notebook_path: ${workspace.root_path}/src/transform/transform_inventory
          run_if: ALL_SUCCESS
          timeout_seconds: 3600

        - task_key: aggregate_kpis
          depends_on:
            - task_key: transform_sales
            - task_key: transform_inventory
          notebook_task:
            notebook_path: ${workspace.root_path}/src/aggregate/kpi_aggregates
          run_if: ALL_SUCCESS

        - task_key: dq_checks
          depends_on:
            - task_key: aggregate_kpis
          notebook_task:
            notebook_path: ${workspace.root_path}/src/quality/dq_checks
          run_if: ALL_SUCCESS

    # Job 2: Data Quality Monitoring
    dq_monitoring:
      name: "[${bundle.target}] Data Quality Monitor"
      schedule:
        quartz_cron_expression: "0 0/15 * * * ?"
        timezone_id: "UTC"
      tasks:
        - task_key: freshness_check
          sql_task:
            warehouse_id: "${var.sql_warehouse_id}"
            query:
              query_text: |
                SELECT
                  CASE WHEN MAX(load_timestamp) < DATEADD(HOUR, -4, CURRENT_TIMESTAMP())
                       THEN 'STALE' ELSE 'FRESH'
                  END AS data_status
                FROM gold.daily_sales
          timeout_seconds: 300

  # Shared job cluster definition
  job_clusters:
    - job_cluster_key: etl_cluster
      new_cluster:
        spark_version: "${var.spark_version}"
        node_type_id: "${var.node_type_id}"
        num_workers: "${var.num_workers}"
        autoscale:
          min_workers: "${var.min_workers}"
          max_workers: "${var.max_workers}"
        spark_conf:
          spark.databricks.delta.optimizeWrite.enabled: "true"
          spark.databricks.delta.autoCompact.enabled: "true"
        custom_tags:
          environment: "${bundle.target}"
          team: "data-engineering"
          project: "sales-data-pipeline"

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: "https://dev-workspace.cloud.databricks.com"
      root_path: "/Workspace/Users/.bundle/sales-pipeline/dev"
    variables:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "i3.xlarge"
      num_workers: 1
      min_workers: 1
      max_workers: 4
      sql_warehouse_id: "abc123dev"
      etl_schedule_paused: "PAUSED"

  staging:
    mode: production
    workspace:
      host: "https://staging-workspace.cloud.databricks.com"
      root_path: "/Workspace/Shared/.bundle/sales-pipeline/staging"
    variables:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "i3.2xlarge"
      num_workers: 2
      min_workers: 2
      max_workers: 8
      sql_warehouse_id: "def456staging"
      etl_schedule_paused: "UNPAUSED"

  production:
    mode: production
    workspace:
      host: "https://prod-workspace.cloud.databricks.com"
      root_path: "/Workspace/Shared/.bundle/sales-pipeline/production"
    run_as:
      service_principal_name: "sp-sales-pipeline-prod"
    variables:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "i3.2xlarge"
      num_workers: 4
      min_workers: 4
      max_workers: 20
      sql_warehouse_id: "ghi789prod"
      etl_schedule_paused: "UNPAUSED"
"""

print("=== COMPLETE databricks.yml (DABs Definition) ===\n")
print(databricks_yml)

```

```python

```
#### 90.2 DABs CLI Commands Cheat Sheet

```python

print("=== DABS CLI COMMAND REFERENCE ===\n")

commands = {
    "Project Initialization": [
        ("databricks bundle init", "Initialise a new bundle from a template"),
        ("databricks bundle init --template default-python", "Use the Python template"),
    ],
    "Validation & Preview": [
        ("databricks bundle validate", "Validate YAML syntax and resource references"),
        ("databricks bundle validate -t dev", "Validate for a specific target"),
    ],
    "Deployment": [
        ("databricks bundle deploy", "Deploy all resources to default target"),
        ("databricks bundle deploy -t staging", "Deploy to staging workspace"),
        ("databricks bundle deploy --force-lock", "Deploy even if bundle is locked"),
    ],
    "Execution": [
        ("databricks bundle run daily_etl", "Run a job defined in the bundle"),
        ("databricks bundle run daily_etl -t dev", "Run a job in a specific target"),
    ],
    "Lifecycle": [
        ("databricks bundle destroy", "Remove all deployed resources"),
        ("databricks bundle destroy -t staging", "Tear down staging resources"),
        ("databricks bundle summary", "Show deployed resources summary"),
    ],
    "Development": [
        ("databricks bundle sync", "Sync local files to workspace (dev mode)"),
    ],
}

for category, cmds in commands.items():
    print(f"--- {category} ---")
    for cmd, desc in cmds:
        print(f"  {cmd:<50}  {desc}")
    print()

```

```python

```
#### 90.3 Infrastructure-as-Code Philosophy

```python

iac_philosophy = """
INFRASTRUCTURE-AS-CODE PRINCIPLES WITH DABs
============================================

1. EVERYTHING AS CODE
   - Jobs, pipelines, clusters, queries all defined in YAML
   - Version-controlled alongside notebook code
   - No clicking in the UI to configure production resources

2. ENVIRONMENT PARITY
   - Same databricks.yml deploys to dev, staging, and production
   - Target-specific variables handle differences (cluster size, paths)
   - "Works on my machine" becomes "Works on every workspace"

3. IMMUTABLE DEPLOYMENTS
   - Each deploy is a complete, self-contained snapshot
   - Roll back by deploying a previous Git commit
   - No drift between Git and what's running

4. CI/CD INTEGRATION
   - GitHub Actions / Azure DevOps runs bundle validate on PR
   - Auto-deploys to staging on merge to develop
   - Manual approval gate for production deploys

5. SECURITY BY DEFAULT
   - Service principals run production jobs (not user accounts)
   - Secrets referenced via ${secrets/my-secret} never in YAML
   - Audit trail: every deploy is a Git commit

6. TESTABILITY
   - Bundle validate catches YAML errors before deploy
   - Dev target runs with smaller clusters, paused schedules
   - Integration tests can run via bundle run in CI

COMPARISON: DABs vs Manual Configuration
=========================================

| Aspect | Manual (UI) | DABs |
|--------|------------|------|
| Reproducibility | Click-by-click | Single command |
| Version Control | None | Full Git history |
| Rollback | Recreate from memory | git revert + deploy |
| Team Collaboration | Screenshots/Slack | PR review |
| Multi-workspace | Repeat N times | One command per target |
| Audit Trail | Manual notes | Git commits |
"""

print(iac_philosophy)

```

```python

```
### Key Takeaway â€” Concept 90
- DABs enable infrastructure-as-code for the entire Databricks platform
- Single databricks.yml defines jobs, pipelines, clusters, queries
- Targets (dev/staging/prod) manage environment-specific differences
- Bundle CLI: validate -> deploy -> run -> destroy
- CE cannot use bundles â€” understand the YAML structure and IaC philosophy
- Production: DABs + GitHub Actions = full GitOps workflow

```python

```
# COMPREHENSIVE SUMMARY

## Concepts 81â€“90: Workflows, CI/CD & Operations

### What You've Learned

| # | Concept | Level | Key Insight |
|---|---------|-------|-------------|
| 81 | Choosing Compute for Jobs | Easy | Job Clusters for prod, All-Purpose for dev only |
| 82 | Secrets Management | Easy | dbutils.secrets.get() > hardcoded; CE: env vars |
| 83 | Parameterization | Easy | Widgets + Job Params + Task Values; widgets work in CE |
| 84 | Notebook Patterns | Easy | %run (shared ns), dbutils.notebook.run() (isolated) |
| 85 | Multi-Task Workflows | Medium | DAG-based orchestration; CE: Python orchestrator |
| 86 | Retries & Conditional Logic | Medium | run_if conditions + exponential backoff retries |
| 87 | Git Integration & Repos | Medium | PR-based workflow for notebooks; main/develop/feature |
| 88 | Monitoring & Alerting | Medium | SQL Alerts in production; Python monitors in CE |
| 89 | System Tables | Medium | system.billing/compute/jobs; CE: custom tracking |
| 90 | Asset Bundles (DABs) | Hard | Infrastructure-as-code for Databricks |

### Architecture Overview

```
PRODUCTION OPERATIONAL ARCHITECTURE
====================================

  Developer                   CI/CD                     Databricks Workspace
  +-------+                  +-------+                  +-------------------+
  | Write |    git push      | GitHub|   bundle deploy  | Jobs + Pipelines  |
  | Code  | ---------------->|Actions| ---------------->|                   |
  | in    |                  |       |                  | +- daily_etl      |
  | Repos |                  | lint  |                  | +- dq_monitor     |
  +-------+                  | test  |                  | +- sales_dlt      |
                             | deploy|                  |                   |
                             +-------+                  +--------+----------+
                                                              |
                                                              | runs
                                                              v
                                                       +-------------------+
                                                       | Monitoring Stack  |
                                                       | +- SQL Alerts     |
                                                       | +- System Tables  |
                                                       | +- Webhooks       |
                                                       +--------+----------+
                                                                |
                                                                v
                                                         +-------------------+
                                                         | On-Call Team      |
                                                         | (PagerDuty/Slack) |
                                                         +-------------------+
```

### CE vs Production Feature Matrix

| Feature | Community Edition | Production |
|---------|------------------|-------------|
| Widgets | Fully supported | Fully supported |
| %run / dbutils.notebook.run() | Fully supported | Fully supported |
| %sql / %fs / %sh / %pip | Fully supported | Fully supported |
| Multi-task Jobs | Not available | Yes (core feature) |
| Job Clusters | Not available | Yes |
| Serverless Compute | Not available | Yes |
| Secret Scopes | Limited / Not available | Yes |
| Git / Repos | Limited | Yes (full integration) |
| SQL Alerts (DBSQL) | Not available | Yes |
| System Tables | Not available | Yes |
| Webhook Destinations | Not available | Yes |
| Asset Bundles (DABs) | Not available | Yes |

### Next Steps
1. **Practice** widget parameterisation â€” it's fully available in CE
2. **Build** the Python orchestrator for your own ETL tasks
3. **Study** the databricks.yml structure for when you have production access
4. **Apply** the retry decorator pattern to your existing code
5. **Set up** Git-based workflow using Repos (when available)
6. **Read** Databricks docs on Workflows, DABs, and System Tables

```python

```
## SELF-ASSESSMENT QUIZ

Test your understanding of Concepts 81â€“90:

### Easy (Concepts 81â€“84)

**Q1:** Which compute type should NEVER be used for production jobs?
- A) Job Cluster
- B) Serverless Compute
- C) All-Purpose Cluster
- D) Instance Pool

**Q2:** How do you retrieve a secret in a Databricks notebook?
- A) `os.environ.get('SECRET')`
- B) `dbutils.secrets.get(scope='x', key='y')`
- C) `open('/etc/secrets/.env').read()`
- D) `spark.conf.get('secret')`

**Q3:** What's the resolution order for parameters?
- A) Widgets > Code Defaults > Job Params
- B) Job Params > Widgets > Code Defaults
- C) Code Defaults > Job Params > Widgets
- D) All are equal priority

**Q4:** What's the difference between `%run` and `dbutils.notebook.run()`?
- A) No difference â€” they're aliases
- B) %run shares namespace; dbutils.notebook.run() is isolated
- C) %run is isolated; dbutils.notebook.run() shares namespace
- D) %run only works in production

### Medium (Concepts 85â€“89)

**Q5:** Which run_if condition means "run only if no upstream task failed"?
- A) ALL_SUCCESS
- B) ALL_DONE
- C) NONE_FAILED
- D) AT_LEAST_ONE_SUCCESS

**Q6:** What's the recommended Git branch strategy for Databricks projects?
- A) Everyone commits directly to main
- B) Main (prod) <- Develop (staging) <- Feature branches
- C) One branch per workspace
- D) No branches needed with notebooks

**Q7:** Which system table tracks DBU consumption?
- A) system.compute.clusters
- B) system.jobs.job_runs
- C) system.billing.usage
- D) system.access.audit

### Hard (Concept 90)

**Q8:** What command deploys a DAB to the staging target?
- A) `databricks bundle run -t staging`
- B) `databricks bundle deploy -t staging`
- C) `databricks bundle push staging`
- D) `databricks deploy --env staging`

**Q9:** Which file defines ALL resources in a Databricks Asset Bundle?
- A) `bundle.json`
- B) `deployment.yaml`
- C) `databricks.yml`
- D) `workspace.tf`

**Q10:** What's the primary benefit of DABs over manual UI configuration?
- A) Faster UI response time
- B) Infrastructure-as-code: versioned, reproducible, auditable
- C) Cheaper DBU pricing
- D) Automatic data quality checks

---
### Answers
1=C, 2=B, 3=B, 4=B, 5=C, 6=B, 7=C, 8=B, 9=C, 10=B

**Scoring:** 8-10 = Production Ready | 5-7 = On Track | <5 = Review Needed