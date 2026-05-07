# Databricks End-to-End Tutorial: Beginner to Advanced

A comprehensive, hands-on tutorial covering **100 essential Databricks concepts** organized into 10 notebooks — from absolute beginner to senior-level depth.

Inspired by [databricks-100](https://github.com/kaushikthakur97/databricks-100) and [dataengineer.wiki](https://dataengineer.wiki).

---

## What This Tutorial Covers

| # | Notebook | Concepts | Focus |
|---|----------|----------|-------|
| 1 | Delta Lake Fundamentals | #1-#10 | Storage layer fundamentals |
| 2 | Spark Execution | #11-#20 | How the engine works |
| 3 | SQL & DataFrames | #21-#30 | Daily transformation work |
| 4 | Data Ingestion | #31-#40 | Getting data in |
| 5 | Streaming | #41-#50 | Real-time processing |
| 6 | Performance & Cost | #51-#60 | Making it fast and cheap |
| 7 | Unity Catalog & Governance | #61-#70 | Security and access control |
| 8 | Lakeflow Declarative Pipelines | #71-#80 | Managed pipeline engine |
| 9 | Workflows, CI/CD & Operations | #81-#90 | Running in production |
| 10 | Architecture & Advanced | #91-#100 | Senior-level decisions |

---

## Difficulty Levels

- **Easy** — What you should know after month one
- **Medium** — Solid mid-level knowledge
- **Hard** — Senior-level depth

---

## Self-Assessment: Score Yourself

Go through every concept. Can you explain it without looking anything up?

| Score | Level | Focus |
|-------|-------|-------|
| 80-100 | Senior | Focus on the Hard concepts you missed |
| 50-79 | Mid-level | Fill gaps in Medium/Hard concepts |
| 30-49 | Foundation | Focus on Easy concepts in categories 1, 3, and 4 |
| Under 30 | Early stage | Start with all Easy concepts |

---

## How to Use This Tutorial

### Option A: Databricks Notebooks (Recommended)
1. Import the `.py` files in `notebooks/` into your Databricks workspace
2. Attach to a cluster (Community Edition works for most concepts)
3. Run cells sequentially — each builds on the previous

### Option B: Read Offline
Open the `.md` files in `markdown/` in any markdown viewer.

### Option C: 100-Day Challenge
One concept per day. Import a notebook, work through one concept's section, and check it off.

---

## Datasets Used

This tutorial uses **real Databricks public datasets** available in every workspace:

- **NYC Taxi Trips** — `/databricks-datasets/nyctaxi/tables/nyctaxi_yellowcab` (if available)
- **Flights / Airlines** — Sample time-series data
- **TPC-H / Retail** — Sales and customer data
- **Synthetic Datasets** — Generated in-notebook for concepts requiring specific patterns

All datasets are generated or use built-in samples — **no external uploads needed**.

---

## Prerequisites

- A Databricks account (Community Edition works for most concepts)
- Basic Python knowledge
- Basic SQL knowledge
- Understanding of basic data concepts (tables, rows, columns)

---

## Community Edition Notes

Some features require full Databricks platform (noted in each notebook):
- Unity Catalog → Concepts explained with Hive metastore equivalents
- Photon, Serverless, Predictive Optimization → Concepts explained, alternatives provided
- Lakeflow Pipelines → Architecture explained, manual equivalents shown
- Workflows → Pattern described, notebook-based alternatives shown

---

## Quick Start

```python
# In any Databricks notebook, create a cluster and run:
print("Let's learn Databricks!")
```

Start with **Notebook 1: Delta Lake Fundamentals** and work your way through.

---

Built for learning. Not affiliated with Databricks, Inc.
