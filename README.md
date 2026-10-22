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
├── _authoring/              ← only for making new lessons: sources with solutions, specs, verified runs, build tools
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
| L03 - Ingestion with Auto Loader, read_files and COPY INTO | read_files, COPY INTO idempotency, Auto Loader, schema evolution, rescued data, `cloud_files_state` |
| L03 - Bronze Ingestion | helper notebook that L03 runs with `dbutils.notebook.run` (do not run it by hand) |
| L04 - Structured Streaming Fundamentals | readStream/writeStream, triggers, output modes, watermarks, foreachBatch, checkpoints, deduplication |

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

```
L01 – Lakehouse Basics
 ▼
L02 – Delta Lake Deep Dive
 ▼
L03 – Ingestion (Auto Loader, read_files, COPY INTO)
 ▼
L04 – Structured Streaming Fundamentals
 ▼
Next: Lakeflow Declarative Pipelines, PySpark transformations, Lakeflow Jobs, Unity Catalog governance
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