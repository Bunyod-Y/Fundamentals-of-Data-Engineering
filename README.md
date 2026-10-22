# 🧊 Fundamentals of Data Engineering — Databricks Learning Path

> A hands-on, notebook-first curriculum for learning **Databricks** from the ground up —
> built for exam preparation (Databricks Certified Data Engineer Associate / Professional)
> and real-world fluency on the platform.

---

## 📌 Why This Repo Exists

Databricks is a powerful platform, but the learning curve is steep. The official
documentation is excellent for reference, but it doesn't always teach you *how to
think* about streaming, Delta Lake, or Unity Catalog in practice.

This repo solves that by providing **run-ready notebooks** that you can clone into
your own Databricks workspace, execute cell-by-cell, and learn by doing. Every
notebook follows the same proven structure:

```
Title & Setup  →  Concepts  →  Live Demos  →  Cheat Sheet  →  Check Understanding  →  Labs  →  Exam MCQs  →  Solutions  →  Cleanup
```

You read a concept, see it run on real data, try it yourself in the lab, then test
your knowledge with exam-style questions — all in one notebook.

---

## 📂 Repo Structure

```
Fundamentals-of-Data-Engineering/
├── Databricks - Learning/   ← every notebook you study (open these in Databricks)
├── _authoring/              ← only for making new lessons: CURRICULUM.md, PROGRESS.md, LNN.md specs, LNN.py sources, verified runs, build tools
├── CLAUDE.md                ← rules for writing lessons
└── README.md
```

---

## 📓 Notebooks (`Databricks - Learning/`)

| Notebook | Topics |
|---|---|
| L01 - Lakehouse Basics | Unity Catalog namespace, notebooks, compute, Delta tables, DML, MERGE, history, time travel, RESTORE, UNDROP |
| L01 - Practical Purposes | quick navigation queries (SHOW / USE) |
| L02 - Delta Lake Deep Dive | schema evolution, constraints, generated/identity columns, managed vs external, CLONE, maintenance |
| L03 - Spark SQL Transformations | joins incl. SEMI/ANTI/CROSS, NULL keys and `<=>`, the `NOT IN` + NULL trap, `QUALIFY` dedup, `LAG`/`LEAD`, window frames and ties, ranking functions, `ROLLUP`/`CUBE`/`GROUPING SETS`, `PIVOT`/`UNPIVOT`, higher-order functions, set operators, `FILTER` vs `CASE` |
| L04 - PySpark DataFrame API, Transforms and UDFs | the L03 queries in PySpark, lazy evaluation on serverless (Spark Connect), `explain()` and broadcast joins, DDL vs `StructType`, `DataFrame.transform`, caching on serverless, Python vs Pandas UDFs (5M-row timing), wrong `returnType` → silent NULLs, Unity Catalog SQL and Python functions |
| L05 - Semi-structured Data - JSON, Nested Types and VARIANT | FHIR-like JSON: `STRUCT`/`ARRAY` dot notation, `explode` vs `explode_outer` (lost rows), colon paths on JSON strings and their case rules, `from_json` / `schema_of_json` / `to_json`, `VARIANT` (`parse_json` vs `try_parse_json`, `variant_get` vs `try_variant_get`, `schema_of_variant(_agg)`, `variant_explode`, limits), malformed records across nine methods, flattening bronze to silver patients and observations |
| L06 - Ingestion with Auto Loader, read_files and COPY INTO | JSON claims, CSV providers and Parquet labs: `read_files` (inference, `schemaHints`), `COPY INTO` idempotency proved with `DESCRIBE HISTORY`, Auto Loader (`schemaLocation`, `inferColumnTypes`, `availableNow`, `cloud_files_state`), schema evolution (real stop/restart) and rescued data, two start errors (no `schemaLocation`, wrong format), `_metadata` lineage columns, `binaryFile` listing, `STREAM read_files` in a notebook vs streaming tables |
| L07 - Lakeflow Declarative Pipelines I - Medallion, Streaming Tables, Materialized Views and Expectations | a serverless pipeline created and run from the notebook (REST API through the Databricks SDK): bronze → silver → gold with `STREAM read_files`, streaming table vs materialized view vs temporary view, expectations (warn, `DROP ROW`, `FAIL UPDATE`) and a quarantine table, `event_log()` metrics, new files vs dimension changes, full refresh, dry run that finds a missing `STREAM`, the silver layer in Python (`from pyspark import pipelines as dp`) |
| L11 - Structured Streaming Fundamentals | readStream/writeStream, triggers, output modes, watermarks, foreachBatch, checkpoints, deduplication |

New lessons (L03 and later) are tested before they are published: the solved version runs twice on
serverless, and every number in the text comes from that run.

---

## 🚀 How to Use These Notebooks

### Option A — Clone into your own Databricks workspace (recommended)

1. **Clone this repo** to your Databricks workspace using Git:
   - Go to **Repos** → **Add Repo** → paste the Git URL
   - Or use the Databricks CLI: `databricks repos create <git-url>`

2. **Open a notebook** and run the **Setup cell first** — it creates the schema,
   tables, and volume you need. Everything is self-contained.

3. **Run cells top-to-bottom** for the full lesson, or jump to a specific demo/lab.

4. **For labs**: read the intro, write your code, then run the validation cell.
   Don't peek at the solutions until you've tried!

### Option B — Import as a single notebook

1. Download the notebook source from this repo
2. In your Databricks workspace: **Create** → **Import** → upload the file
3. Attach serverless compute (or any cluster running DBR 13.3+)
4. Run the setup cell, then proceed through the notebook

---

## ✅ Prerequisites

| Requirement | Details |
|---|---|
| **Databricks workspace** | Any tier (Community, Premium, or trial) |
| **Compute** | Serverless compute works out of the box. Classic clusters need DBR 13.3+ |
| **Unity Catalog** | The notebooks use `main.dbacademy_streaming` — you need `USE CATALOG` on `main` |
| **Python** | Basic familiarity |
| **SQL** | Basic SELECT / GROUP BY / MERGE knowledge |

> **No Unity Catalog?** Modify the `FULL_SCHEMA` variable at the top of the setup
> cell to use `default` or any schema you have access to.

---

## 🎯 Who This Is For

- **Aspiring data engineers** preparing for Databricks certification exams
- **Analysts and scientists** who want to understand streaming pipelines
- **Anyone** who learns better by running code than reading docs

---

## 🔧 Tech Stack

| Component | Version / Setting |
|---|---|
| Spark | 3.5+ (bundled with DBR 13.3+) |
| Compute | Serverless (auto-selected) or classic cluster |
| Storage | Delta Lake tables in Unity Catalog |
| Language | PySpark + SQL (both used throughout) |
| Trigger | `availableNow=True` (serverless-safe) |

---

## 📈 Learning Path

22 lessons are planned (`_authoring/CURRICULUM.md`, progress in `_authoring/PROGRESS.md`).
Numbers follow the plan, so the notebooks that exist today are L01, L02, L03, L04, L05, L06, L07 and L11.

```
L01 Lakehouse Basics → L02 Delta Lake Deep Dive → L03 Spark SQL → L04 PySpark → L05 JSON/VARIANT
→ L06 Ingestion → L07–L08 Lakeflow Declarative Pipelines → L09 Lakeflow Jobs → L10 Unity Catalog
→ L11–L12 Structured Streaming → L13 Delta optimization → L14 CDF → L15 Testing → L16 Monitoring
→ L17 Security → L18 Data modeling → L19 Performance → L20 Advanced ingestion & sharing
→ L21 Declarative Automation Bundles → L22 Mock exam
```

---

## 🤝 Contributing

Found a bug or have an idea for a new notebook? Open an issue or submit a pull
request. The notebooks are designed to be modular — each one is a standalone
lesson.

---

## 📜 License

This repository is for personal learning and exam preparation. The notebooks are
original work; the Databricks platform and documentation are products of
Databricks Inc.

---

*Built and maintained by [Bunyod Yokubov](https://github.com/bunyodyokubov)*