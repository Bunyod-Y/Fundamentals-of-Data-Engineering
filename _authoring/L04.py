# Databricks notebook source
# LESSON_TITLE: PySpark DataFrame API, Transforms and UDFs

# COMMAND ----------

# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L04 — PySpark DataFrame API, reusable transforms, UDFs
# MAGIC
# MAGIC **Exam objectives**
# MAGIC
# MAGIC * Professional (Oct 9, 2026), Section 1 *Developing Code for Data Processing using Python and SQL* (23% of the exam): "Develop User-Defined Functions (UDFs) using Pandas, Python, and SQL, including Unity Catalog functions for the given constraints" (exam guide); reusable, testable code.
# MAGIC * Professional Section 3 *Data Transformation, Cleansing, and Quality* (12%): the same transformations as L03, written with the DataFrame API.
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** the DataFrame API (`col`, `expr`, `selectExpr`, `when`, `withColumns`, `join`, `groupBy().agg()`, `Window`) to write the L03 SQL queries in PySpark.
# MAGIC 2. **Explain** lazy evaluation on serverless (Spark Connect): when an error appears, and what `explain()` shows.
# MAGIC 3. **Compare** built-in functions, Python UDFs, Pandas UDFs, SQL UDFs and Unity Catalog Python UDFs.
# MAGIC 4. **Implement** reusable transformations with `DataFrame.transform()` and functions stored in Unity Catalog.
# MAGIC 5. **Diagnose** silent NULLs from a wrong UDF `returnType`, and `when` without `otherwise`.
# MAGIC
# MAGIC > **Evidence-only rule:** every number and error class in this notebook comes from a real run of this notebook (the build fills them in), or from an official Databricks doc page named in the text. Things that cannot be tested on this workspace are marked **NOT VERIFIED — concept only (reason)**.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up (intro)
# MAGIC %md
# MAGIC ## 2 Warm-up — recall L03 Spark SQL Transformations (no hints)
# MAGIC
# MAGIC Write each task from memory, in SQL (`spark.sql(...)`). Run the validation cell after each one. Solutions are in section 11.
# MAGIC
# MAGIC > The warm-up uses its own small tables. Run the next cell first: it creates the lesson schema and a helper.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up prep (run first)
spark.sql("CREATE SCHEMA IF NOT EXISTS main.dbacademy_l04")   # the warm-up tables live here; setup recreates it later

import re

def error_class(e):
    """Return the Spark error class (condition) of an exception, the first [CLASS] in its message, or its Python type name."""
    for name in ("getCondition", "getErrorClass"):   # getCondition is the newer name of getErrorClass
        get = getattr(e, name, None)
        if get:
            try:
                value = get()
                if value:
                    return value
            except Exception:
                pass
    found = re.search(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", str(e))   # e.g. [PYTHON_EXCEPTION]
    return found.group(1) if found else type(e).__name__

spark.sql("CREATE OR REPLACE TABLE main.dbacademy_l04.wu_claims (claim_id INT, status STRING, updated_at TIMESTAMP)")
spark.sql("""INSERT INTO main.dbacademy_l04.wu_claims VALUES
    (1, 'SUBMITTED', TIMESTAMP'2025-03-01 08:00:00'), (1, 'PAID',   TIMESTAMP'2025-03-09 08:00:00'),
    (2, 'SUBMITTED', TIMESTAMP'2025-03-02 08:00:00'),
    (3, 'SUBMITTED', TIMESTAMP'2025-03-03 08:00:00'), (3, 'DENIED', TIMESTAMP'2025-03-05 08:00:00'),
    (3, 'PAID',      TIMESTAMP'2025-03-20 08:00:00')""")
spark.sql("CREATE OR REPLACE TABLE main.dbacademy_l04.wu_patients (patient_id INT, name STRING)")
spark.sql("INSERT INTO main.dbacademy_l04.wu_patients VALUES (1, 'Pat A'), (2, 'Pat B'), (3, 'Pat C'), (4, 'Pat D'), (5, 'Pat E')")
spark.sql("CREATE OR REPLACE TABLE main.dbacademy_l04.wu_visits (visit_id INT, patient_id INT, visit_date DATE)")
spark.sql("""INSERT INTO main.dbacademy_l04.wu_visits VALUES
    (10, 1, DATE'2025-01-05'), (11, 2, DATE'2025-01-07'), (12, NULL, DATE'2025-01-08'),
    (13, 2, DATE'2025-02-01'), (14, 4, DATE'2025-02-03'), (15, 1, DATE'2025-03-02'), (16, 2, DATE'2025-02-11')""")
print("Warm-up tables: wu_claims, wu_patients, wu_visits")

# COMMAND ----------

# DBTITLE 1,W1 Task
# MAGIC %md
# MAGIC ### W1 — Keep the newest version of each claim
# MAGIC `main.dbacademy_l04.wu_claims` has several versions of some claims (`claim_id`, `status`, `updated_at`).
# MAGIC Create the table `main.dbacademy_l04.wu1_latest` with **one row per `claim_id`**: the row with the newest `updated_at`. All three columns must stay.

# COMMAND ----------

# DBTITLE 1,W1 Code
# >>> BLANK W1
# Your code here
# >>> SOLUTION W1
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l04.wu1_latest AS
    SELECT * FROM main.dbacademy_l04.wu_claims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1
""")
# <<< END W1

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    rows = {r["claim_id"]: r["status"] for r in spark.table("main.dbacademy_l04.wu1_latest").collect()}
    cols = spark.table("main.dbacademy_l04.wu1_latest").columns
    ok = rows == {1: "PAID", 2: "SUBMITTED", 3: "PAID"} and cols == ["claim_id", "status", "updated_at"]
    WARMUP_PASSED["W1"] = ok
    print("✅ Passed" if ok else
          f"Hint: want claims 1 PAID, 2 SUBMITTED, 3 PAID and the columns claim_id, status, updated_at; got {rows}, {cols}.")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create main.dbacademy_l04.wu1_latest first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — Patients with no visit
# MAGIC Create `main.dbacademy_l04.wu2_no_visit` with the `patient_id` of every patient in `wu_patients` who has **no** row in `wu_visits`.
# MAGIC Careful: `wu_visits` has a visit with `patient_id = NULL`.

# COMMAND ----------

# DBTITLE 1,W2 Code
# >>> BLANK W2
# Your code here
# >>> SOLUTION W2
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l04.wu2_no_visit AS
    SELECT p.patient_id
    FROM main.dbacademy_l04.wu_patients p
    LEFT ANTI JOIN main.dbacademy_l04.wu_visits v ON p.patient_id = v.patient_id
""")
# <<< END W2

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    ids = sorted(r["patient_id"] for r in spark.table("main.dbacademy_l04.wu2_no_visit").collect())
    ok = ids == [3, 5]
    WARMUP_PASSED["W2"] = ok
    print("✅ Passed" if ok else f"Hint: want patients [3, 5], got {ids}.")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: create main.dbacademy_l04.wu2_no_visit first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — Days since the previous visit
# MAGIC Create `main.dbacademy_l04.wu3_gaps` from `wu_visits` (only rows with a `patient_id`) with the columns `visit_id, patient_id, visit_date, days_since_prev`.
# MAGIC `days_since_prev` is the number of days since the previous visit of the same patient (NULL on the first visit).

# COMMAND ----------

# DBTITLE 1,W3 Code
# >>> BLANK W3
# Your code here
# >>> SOLUTION W3
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l04.wu3_gaps AS
    SELECT visit_id, patient_id, visit_date,
           datediff(visit_date, LAG(visit_date) OVER (PARTITION BY patient_id ORDER BY visit_date)) AS days_since_prev
    FROM main.dbacademy_l04.wu_visits
    WHERE patient_id IS NOT NULL
""")
# <<< END W3

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    got = {r["visit_id"]: r["days_since_prev"] for r in spark.table("main.dbacademy_l04.wu3_gaps").collect()}
    want = {10: None, 15: 56, 11: None, 13: 25, 16: 10, 14: None}
    ok = got == want
    WARMUP_PASSED["W3"] = ok
    print("✅ Passed" if ok else f"Hint: want {want} (visit_id: days), got {got}. Check the argument order of datediff.")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create main.dbacademy_l04.wu3_gaps first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent (drop the schema, recreate it), fake healthcare data only (no PHI).
import random                                                   # isolated generator below, never random.seed()
from datetime import date, datetime, timedelta
from decimal import Decimal
from pyspark.sql import Window, functions as F, types as T      # used by the examples and the validation cells

CATALOG, SCHEMA = "main", "dbacademy_l04"
FQ = f"{CATALOG}.{SCHEMA}"                                      # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"                       # volume (used by the test build only)

spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")                # removes the warm-up tables and UC functions too
spark.sql(f"CREATE SCHEMA {FQ}")
spark.sql(f"CREATE VOLUME {FQ}.data")

rng = random.Random(42)                                         # one isolated, seeded generator
START = date(2025, 1, 1)

# patients: 200 rows (patients 191-200 never get an encounter); height/weight are added further down
patients = [[pid, rng.choice(["F", "M"]), rng.randint(1940, 2010), rng.choice(["CA", "NY", "TX", "FL", "WA"])]
            for pid in range(1, 201)]

# providers: 20 rows, 5 specialties
SPECIALTIES = ["Cardiology", "Endocrinology", "Family Medicine", "Emergency Medicine", "Oncology"]
providers = [(pid, f"Dr Provider {pid}", SPECIALTIES[pid % 5]) for pid in range(1, 21)]

# encounters: 2000 rows; NULL patient_id every 100th, NULL provider every 125th,
# every 50th encounter is a second visit of the previous patient on the same day (a tie on purpose)
TYPES = ["OUTPATIENT"] * 6 + ["ER"] * 2 + ["INPATIENT"] * 2
encounters = []
for eid in range(1, 2001):
    pid = None if eid % 100 == 0 else rng.randint(1, 190)
    day = START + timedelta(days=rng.randint(0, 364))
    if eid % 50 == 1 and eid > 1 and encounters[-1][1] is not None:
        pid, day = encounters[-1][1], encounters[-1][4]        # same patient, same date as the previous row
    etype = rng.choice(TYPES)
    prov = None if eid % 125 == 0 else rng.randint(1, 20)
    charge = Decimal(f"{rng.uniform(80, 3000):.2f}")
    encounters.append((eid, pid, prov, etype, day, charge))

# claims: 1350 claims + 150 later versions of existing claims (same claim_id, later updated_at) = 1500 rows
PAYERS = ["Medicare", "Medicaid", "Aetna", "BlueCross", None]   # None = self-pay
claims = []
for cid in range(1, 1351):
    enc = encounters[rng.randint(0, 1999)]
    updated = datetime.combine(enc[4], datetime.min.time()) + timedelta(days=rng.randint(1, 30), hours=8)
    claims.append((cid, enc[0], enc[1], rng.choice(PAYERS), rng.choice(["SUBMITTED", "PAID", "DENIED", "PENDING"]),
                   Decimal(f"{rng.uniform(50, 5000):.2f}"), updated))
for cid in rng.sample(range(1, 1351), 150):
    old = claims[cid - 1]
    claims.append(old[:4] + (rng.choice(["PAID", "DENIED"]), old[5], old[6] + timedelta(days=rng.randint(1, 20))))

# lab_results: 3000 rows (A1C, LDL, GLU), NULL patient_id every 150th
labs = []
for lid in range(1, 3001):
    pid = None if lid % 150 == 0 else rng.randint(1, 200)
    test = rng.choice(["A1C", "LDL", "GLU"])
    value = {"A1C": round(rng.uniform(4.5, 10.0), 1), "LDL": float(rng.randint(60, 200)), "GLU": float(rng.randint(70, 250))}[test]
    labs.append((lid, pid, test, value, datetime(2025, 1, 1) + timedelta(minutes=lid * 157)))

# height and weight for the BMI examples; weight is missing (NULL) for every 40th patient
for p in patients:
    p.append(round(rng.uniform(150, 195), 1))
    p.append(None if p[0] % 40 == 0 else round(rng.uniform(45, 130), 1))

def save(rows, ddl, name):
    """Write a list of tuples as a managed Delta table and return its row count."""
    spark.createDataFrame([tuple(r) for r in rows], ddl).write.saveAsTable(f"{FQ}.{name}")
    return spark.table(f"{FQ}.{name}").count()

n_patients = save(patients, "patient_id INT, gender STRING, birth_year INT, state STRING, height_cm DOUBLE, weight_kg DOUBLE", "patients")
n_providers = save(providers, "provider_id INT, provider_name STRING, specialty STRING", "providers")
n_enc = save(encounters, "encounter_id INT, patient_id INT, provider_id INT, encounter_type STRING, "
             "encounter_date DATE, charge DECIMAL(10,2)", "encounters")
n_claims = save(claims, "claim_id INT, encounter_id INT, patient_id INT, payer STRING, status STRING, "
                "amount DECIMAL(10,2), updated_at TIMESTAMP", "claims")
n_labs = save(labs, "lab_id INT, patient_id INT, test_code STRING, result_value DOUBLE, result_ts TIMESTAMP", "lab_results")

counts = spark.sql(f"""
    SELECT (SELECT count(DISTINCT claim_id) FROM {FQ}.claims)                          AS distinct_claims,
           (SELECT count(*) FROM {FQ}.encounters WHERE patient_id IS NULL)              AS enc_null_patient,
           (SELECT count(*) FROM {FQ}.encounters WHERE provider_id IS NULL)             AS enc_null_provider,
           (SELECT count(*) FROM {FQ}.patients WHERE weight_kg IS NULL)                 AS null_weight,
           (SELECT count(*) FROM {FQ}.lab_results WHERE test_code = 'A1C')              AS a1c_rows
""").first()
display(spark.sql(f"""
    SELECT 'patients' AS table_name, count(*) AS row_count FROM {FQ}.patients UNION ALL
    SELECT 'providers', count(*) FROM {FQ}.providers UNION ALL
    SELECT 'encounters', count(*) FROM {FQ}.encounters UNION ALL
    SELECT 'claims', count(*) FROM {FQ}.claims UNION ALL
    SELECT 'lab_results', count(*) FROM {FQ}.lab_results"""))
print(counts.asDict())
METRICS["setup.patients"], METRICS["setup.providers"], METRICS["setup.encounters"] = n_patients, n_providers, n_enc
METRICS["setup.claims"], METRICS["setup.labs"] = n_claims, n_labs
METRICS["setup.distinct_claims"] = counts["distinct_claims"]
METRICS["setup.enc_null_patient"] = counts["enc_null_patient"]
METRICS["setup.enc_null_provider"] = counts["enc_null_provider"]
METRICS["setup.null_weight"] = counts["null_weight"]
METRICS["setup.a1c_rows"] = counts["a1c_rows"]
assert (n_patients, n_providers, n_enc, n_claims, n_labs) == (200, 20, 2000, 1500, 3000)
assert counts["distinct_claims"] == 1350 and counts["enc_null_patient"] == 20 and counts["enc_null_provider"] == 16
assert counts["null_weight"] == 5

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):**
# MAGIC
# MAGIC | Table | Rows | Notes |
# MAGIC | --- | --- | --- |
# MAGIC | `patients` | {{setup.patients}} | `height_cm`, `weight_kg` for the BMI examples; `weight_kg` is NULL for {{setup.null_weight}} patients; patients 191–200 have no encounter |
# MAGIC | `providers` | {{setup.providers}} | 5 specialties |
# MAGIC | `encounters` | {{setup.encounters}} | `patient_id` NULL {{setup.enc_null_patient}}×, `provider_id` NULL {{setup.enc_null_provider}}×; every 50th encounter is a second visit on the same day |
# MAGIC | `claims` | {{setup.claims}} rows, {{setup.distinct_claims}} distinct `claim_id` | later versions of a claim have a newer `updated_at`; `payer` NULL = self-pay |
# MAGIC | `lab_results` | {{setup.labs}} ({{setup.a1c_rows}} A1C) | A1C, LDL and glucose results |
# MAGIC
# MAGIC The tables have the same shape as in L03, so you can compare the PySpark code with the SQL you already know. The data comes from one seeded generator (`random.Random(42)`), so every run creates the same rows.

# COMMAND ----------

# DBTITLE 1,4.1 Concept — DataFrame API and SQL
# MAGIC %md
# MAGIC ## 4 Concepts
# MAGIC
# MAGIC ### 4.1 DataFrame API and SQL: two front doors, one engine
# MAGIC
# MAGIC **What.** A DataFrame is a description of a query: a logical plan. `spark.sql("SELECT …")` and `spark.table(…).filter(…).groupBy(…)` both build such a plan. The Catalyst optimizer turns it into a physical plan, and on Databricks Photon runs most of it (the `explain()` output says "The query is fully supported by Photon" when it does — E3).
# MAGIC
# MAGIC **Why both?** SQL is short and easy to read. The DataFrame API is Python: you can put a step in a function, give it parameters, loop over columns, test it with `pytest` (L15) and reuse it in many pipelines.
# MAGIC
# MAGIC | Need | SQL | DataFrame API |
# MAGIC | --- | --- | --- |
# MAGIC | One-off report, analyst readers | ✅ best | possible |
# MAGIC | The same cleaning step in 10 pipelines | copy-paste | ✅ a Python function + `df.transform(fn)` |
# MAGIC | Column names that come from config or a loop | string building (risky) | ✅ `F.col(name)`, `withColumns({...})` |
# MAGIC | Unit tests | hard | ✅ (L15) |
# MAGIC
# MAGIC You can mix both: `df.selectExpr("datediff(a, b) AS d")`, `F.expr("…")`, or `spark.sql("SELECT … FROM {df}", df=df)`.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import functions as F, Window
# MAGIC enc = spark.table("main.dbacademy_l04.encounters")
# MAGIC enc.filter(F.col("encounter_type") == "ER").groupBy("patient_id").agg(F.count("*").alias("er_visits"))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.2 Concept — Lazy evaluation and Spark Connect
# MAGIC %md
# MAGIC ### 4.2 Lazy evaluation on serverless (Spark Connect)
# MAGIC
# MAGIC **What.** Transformations (`select`, `filter`, `withColumn`, `join`, `groupBy().agg()`) only add steps to the plan. **Actions** (`count`, `collect`, `first`, `show`, `display`, `write…saveAsTable`) send the plan to the engine and run it.
# MAGIC
# MAGIC **Serverless detail.** Serverless compute uses Spark Connect. The *Compare Spark Connect to Spark Classic* page says Spark Connect "defers analysis and name resolution to execution time", so a wrong column name does not fail when you write the transformation: it fails at the first action, or when you access `df.columns` / `df.schema` (these "trigger eager analysis"). Classic compute fails "eagerly". E2 shows it on this workspace.
# MAGIC
# MAGIC | Call | Runs the query? | Where a mistake shows up |
# MAGIC | --- | --- | --- |
# MAGIC | `df.withColumn(...)`, `df.filter(...)` | no | not yet (Spark Connect) |
# MAGIC | `df.columns`, `df.schema`, `df.explain()` | analysis only | wrong names, wrong types |
# MAGIC | `df.count()`, `df.collect()`, `display(df)` | yes | also data errors (for example a bad `CAST` under ANSI mode) |
# MAGIC
# MAGIC **UDFs on Spark Connect** (same page): a UDF's "serialization and registration are deferred until execution time", so a UDF that reads a Python variable sees the value the variable has **when the action runs**, not when you defined the UDF. E8 shows it.

# COMMAND ----------

# DBTITLE 1,4.3 Concept — Column expressions
# MAGIC %md
# MAGIC ### 4.3 Column expressions: `col`, `expr`, `selectExpr`, `when`, `withColumns`
# MAGIC
# MAGIC | Tool | Example | Use it when |
# MAGIC | --- | --- | --- |
# MAGIC | `F.col("x")` | `F.col("charge") * 1.1` | normal Python code; the column name may come from a variable |
# MAGIC | `F.lit(v)` | `F.lit("SELF-PAY")` | a constant as a column |
# MAGIC | `F.expr("sql")` | `F.expr("datediff(discharge, admit)")` | a SQL expression inside DataFrame code |
# MAGIC | `df.selectExpr(...)` | `df.selectExpr("patient_id", "2025 - birth_year AS age")` | a `select` written as SQL snippets |
# MAGIC | `F.when(c, v).when(...).otherwise(d)` | `F.when(F.col("age") < 18, "child").otherwise("adult")` | `CASE WHEN`; **without `otherwise`, no match gives NULL** (P2) |
# MAGIC | `df.withColumn(name, col)` | one new or replaced column | one column |
# MAGIC | `df.withColumns({name: col, …})` | many columns in one step | several columns: one call instead of a long chain |
# MAGIC | `df.withColumnsRenamed({old: new})`, `df.drop(...)` | rename, remove | |
# MAGIC
# MAGIC **Filter syntax.** `df.filter(F.col("a") > 1)`, `df.filter("a > 1")` and `df.where(...)` are the same. Combine conditions with `&`, `|`, `~` and **parentheses**: `(F.col("a") > 1) & (F.col("b") == "ER")`. Python's `and` / `or` do not work on columns.

# COMMAND ----------

# DBTITLE 1,4.4 Concept — Joins, aggregation, windows
# MAGIC %md
# MAGIC ### 4.4 Joins, aggregation and windows in the DataFrame API
# MAGIC
# MAGIC ```python
# MAGIC a.join(b, "patient_id")                         # inner join on a column with the same name (one key column in the result)
# MAGIC a.join(b, ["patient_id", "visit_date"], "left") # how: inner, left, right, full, left_semi, left_anti, cross
# MAGIC a.join(b, a.code.eqNullSafe(b.code))            # <=> (both key columns stay)
# MAGIC a.join(F.broadcast(small), "provider_id")        # broadcast hint: copy the small side to every task, no shuffle
# MAGIC
# MAGIC df.groupBy("payer").agg(F.count("*").alias("claims"),
# MAGIC                         F.count_if(F.col("status") == "DENIED").alias("denied"),
# MAGIC                         F.avg(F.when(F.col("status") == "PAID", F.col("amount"))).alias("avg_paid"))
# MAGIC
# MAGIC w = Window.partitionBy("patient_id").orderBy("encounter_date", "encounter_id")
# MAGIC df.withColumn("prev_date", F.lag("encounter_date").over(w))
# MAGIC df.withColumn("rn", F.row_number().over(w.orderBy(F.desc("updated_at"))))
# MAGIC w.rowsBetween(Window.unboundedPreceding, Window.currentRow)  # explicit ROWS frame
# MAGIC ```
# MAGIC
# MAGIC **Broadcast join.** The small table is sent to every task, so the big table is not shuffled. The SQL hint is `SELECT /*+ BROADCAST(p) */ …` (*Join hints* page). Databricks also broadcasts small tables on its own; E3 shows the plan with and without the hint.

# COMMAND ----------

# DBTITLE 1,4.5 Concept — Reusable transforms and schemas
# MAGIC %md
# MAGIC ### 4.5 Reusable transformations and schemas
# MAGIC
# MAGIC **`DataFrame.transform(func, *args)`** calls `func(df, *args)` and returns its result. A pipeline becomes a readable chain of small, named, testable functions:
# MAGIC
# MAGIC ```python
# MAGIC def drop_null_patients(df):
# MAGIC     return df.filter(F.col("patient_id").isNotNull())
# MAGIC
# MAGIC def add_charge_band(df, high=1000):
# MAGIC     return df.withColumn("charge_band", F.when(F.col("charge") >= high, "HIGH").otherwise("NORMAL"))
# MAGIC
# MAGIC clean = (spark.table("main.dbacademy_l04.encounters")
# MAGIC          .transform(drop_null_patients)
# MAGIC          .transform(add_charge_band, high=1500))    # extra arguments are passed to the function
# MAGIC ```
# MAGIC
# MAGIC **Schemas.** Two ways to write the same schema:
# MAGIC
# MAGIC | Way | Example | Good for |
# MAGIC | --- | --- | --- |
# MAGIC | DDL string | `"patient_id INT, weight_kg DOUBLE"` | short, the same text as SQL `CREATE TABLE` |
# MAGIC | `StructType` | `T.StructType([T.StructField("patient_id", T.IntegerType(), True), …])` | building schemas in code; setting `nullable=False` per field |
# MAGIC
# MAGIC Both work in `spark.createDataFrame(rows, schema)`, `spark.read.schema(schema)` and `from_json`. E4 compares them.

# COMMAND ----------

# DBTITLE 1,4.6 Concept — explain() and caching on serverless
# MAGIC %md
# MAGIC ### 4.6 `explain()` and caching on serverless
# MAGIC
# MAGIC **`df.explain()`** prints the physical plan; `df.explain("formatted")` prints a plan overview plus details per node; `"extended"` adds the logical plans. SQL: `EXPLAIN [EXTENDED | FORMATTED] SELECT …`. Read it from the bottom (the scans) to the top (the result). Look for the join type (`BroadcastHashJoin`, `SortMergeJoin`, `ShuffledHashJoin`), `Exchange` / shuffle nodes, and `BatchEvalPython` / `ArrowEvalPython` (a Python UDF is running).
# MAGIC
# MAGIC **Caching.** On classic compute, `df.cache()` / `df.persist()` keep a result in memory for re-use. The *Serverless compute limitations* page says: "Dataframe and SQL cache APIs are not supported on serverless compute. Using any of these APIs or SQL commands results in an exception." E6 tests what this workspace really does. On serverless, if you need a result many times, write it to a table (or a temporary view of a table).

# COMMAND ----------

# DBTITLE 1,4.7 Concept — The UDF family
# MAGIC %md
# MAGIC ### 4.7 User-defined functions (UDFs): five ways to add your own logic
# MAGIC
# MAGIC The *What are user-defined functions (UDFs)?* page: "Built-in Apache Spark functions are optimized for distributed processing and offer better performance at scale" and "Built-in functions and SQL UDFs are the most efficient options." **Rule: built-in functions first, then a SQL UDF, then a Pandas UDF, and a row-at-a-time Python UDF last.**
# MAGIC
# MAGIC | Kind | How you create it | Where it lives | Runs as |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Python UDF | `@F.udf(returnType)` / `F.udf(fn, returnType)`; `spark.udf.register("name", fn)` for SQL | this Spark session only | Python code, one row at a time |
# MAGIC | Pandas UDF (vectorized) | `@F.pandas_udf("double")` + type hints `pd.Series -> pd.Series` | this session only | Python code on batches of rows (Apache Arrow) |
# MAGIC | SQL UDF | `CREATE FUNCTION cat.sch.f(x INT) RETURNS STRING RETURN <SQL expression>` | Unity Catalog (or `TEMPORARY`) | a SQL expression in the query plan |
# MAGIC | Unity Catalog Python UDF | `CREATE FUNCTION … RETURNS DOUBLE LANGUAGE PYTHON AS $$ … $$` | Unity Catalog: governed, shared, `GRANT EXECUTE` | Python in "a secure, isolated environment" |
# MAGIC | Scala / Java UDF, UDAF, UDTF | — | — | NOT VERIFIED — concept only (out of scope for this lesson) |
# MAGIC
# MAGIC **Pandas UDF type hints** (*pandas user-defined functions* page): Series → Series (`pd.Series, … -> pd.Series`, the most common), Iterator of Series → Iterator of Series (load a model once per batch stream), Series → scalar (`pd.Series -> float`, an aggregate). The page says Pandas UDFs "can increase performance up to 100x compared to row-at-a-time" Python UDFs; E7 measures this workspace.
# MAGIC
# MAGIC **Python UDF `returnType`.** "The default return type is `StringType`" (*Python scalar UDFs* page). If the Python value does not fit the declared type, you can get NULL instead of an error (E8).
# MAGIC
# MAGIC **Unity Catalog functions** (*SQL and Python user-defined functions (UDFs) in Unity Catalog*; *CREATE FUNCTION* reference):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION main.dbacademy_l04.age_group(birth_year INT)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Age group in 2025'
# MAGIC RETURN CASE WHEN 2025 - birth_year < 18 THEN 'child' WHEN 2025 - birth_year < 65 THEN 'adult' ELSE 'senior' END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION main.dbacademy_l04.bmi(weight_kg DOUBLE, height_cm DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE PYTHON
# MAGIC DETERMINISTIC
# MAGIC AS $$
# MAGIC if weight_kg is None or height_cm is None:
# MAGIC     return None
# MAGIC return weight_kg / ((height_cm / 100.0) ** 2)
# MAGIC $$;
# MAGIC ```
# MAGIC
# MAGIC Python UDFs in Unity Catalog "must handle NULL values independently" and "run in a secure, isolated environment and do not have access to file systems or internal services". `CREATE TEMPORARY FUNCTION` makes a session-only function: "No persistent entry is made in the catalog." Call a UC function from PySpark with `F.expr("main.dbacademy_l04.age_group(birth_year)")` or `F.call_function("main.dbacademy_l04.age_group", F.col("birth_year"))`.

# COMMAND ----------

# DBTITLE 1,5 — Examples (intro)
# MAGIC %md
# MAGIC ## 5 Examples
# MAGIC
# MAGIC Each example: **Predict** (answer before you run) → **code** (every line commented) → **Explained** (real numbers from the run).
# MAGIC
# MAGIC E5 creates the table `encounters_enriched`; E9 and E10 create Unity Catalog functions in `main.dbacademy_l04`. Run the examples in order.

# COMMAND ----------

# DBTITLE 1,5 — Example helpers
import io, contextlib, time                                    # capture explain() text, measure time
import pandas as pd                                            # Pandas UDFs work on pandas Series

def plan_text(df, mode=None):
    """Return the text that df.explain(mode) prints."""
    buf = io.StringIO()                                        # a string buffer instead of the screen
    with contextlib.redirect_stdout(buf):                      # explain() prints; we catch the print
        df.explain(mode) if mode else df.explain()
    return buf.getvalue()

def physical(plan):
    """Only the physical plan part (the text before the Photon explanation)."""
    return plan.split("== Photon Explanation ==")[0]

def join_operator(plan):
    """The first join operator in a plan, for example PhotonBroadcastHashJoin."""
    m = re.search(r"\b\w*(?:BroadcastHashJoin|ShuffledHashJoin|SortMergeJoin|BroadcastNestedLoopJoin|CartesianProduct)\b", physical(plan))
    return m.group(0) if m else "none"

def diff_rows(a, b):
    """Rows in a but not in b plus rows in b but not in a (0 = same result)."""
    return a.exceptAll(b).count() + b.exceptAll(a).count()

def first_error(fn):
    """Run fn and return 'ok' or the error class."""
    try:
        fn()
        return "ok"
    except Exception as e:
        return error_class(e)

# COMMAND ----------

# DBTITLE 1,E1 Predict
# MAGIC %md
# MAGIC ### E1 — Three L03 queries in PySpark: dedup, `LAG`, report per payer
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. The SQL dedup with `QUALIFY` returned one row per claim in L03. Which DataFrame methods replace `QUALIFY ROW_NUMBER() OVER (…) = 1`?
# MAGIC 2. Will the PySpark and SQL versions return exactly the same rows?
# MAGIC 3. `F.col("x") + 1`, `F.expr("x + 1")` and `selectExpr("x + 1")`: different results or the same?

# COMMAND ----------

# DBTITLE 1,E1 Code
claims = spark.table(f"{FQ}.claims")                           # a DataFrame; nothing runs yet
enc = spark.table(f"{FQ}.encounters")
pat = spark.table(f"{FQ}.patients")

# (a) dedup: newest version of each claim
w_latest = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc())   # one window per claim, newest first
latest_py = (claims.withColumn("rn", F.row_number().over(w_latest))             # 1 = newest version
                   .filter(F.col("rn") == 1)                                    # QUALIFY ... = 1
                   .drop("rn"))                                                 # remove the helper column
dedup_sql = f"""SELECT * FROM {FQ}.claims
                QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1"""
latest_sql = spark.sql(dedup_sql)                                               # the L03 SQL version

# (b) days since the previous visit of the same patient
w_visit = Window.partitionBy("patient_id").orderBy("encounter_date", "encounter_id")   # encounter_id breaks ties
gaps_py = (enc.filter(F.col("patient_id").isNotNull())                          # WHERE before the window
              .withColumn("prev_date", F.lag("encounter_date").over(w_visit))   # previous visit date (NULL on the first)
              .withColumn("days_since_prev", F.datediff("encounter_date", "prev_date"))   # end date first
              .select("encounter_id", "patient_id", "encounter_date", "days_since_prev"))
gaps_sql = spark.sql(f"""SELECT encounter_id, patient_id, encounter_date,
                                datediff(encounter_date, LAG(encounter_date) OVER (PARTITION BY patient_id
                                         ORDER BY encounter_date, encounter_id)) AS days_since_prev
                         FROM {FQ}.encounters WHERE patient_id IS NOT NULL""")

# (c) report per payer on the deduplicated claims
report_py = (latest_py.groupBy(F.coalesce("payer", F.lit("SELF-PAY")).alias("payer_label"))   # NULL payer -> label
                      .agg(F.count("*").alias("claims"),                                        # all rows
                           F.count_if(F.col("status") == "DENIED").alias("denied"),            # conditional count
                           F.round(F.avg(F.when(F.col("status") == "PAID", F.col("amount"))), 2).alias("avg_paid")))   # no otherwise -> NULL -> skipped
report_sql = spark.sql(f"""SELECT coalesce(payer, 'SELF-PAY') AS payer_label, count(*) AS claims,
                                  count_if(status = 'DENIED') AS denied,
                                  round(avg(CASE WHEN status = 'PAID' THEN amount END), 2) AS avg_paid
                           FROM ({dedup_sql}) GROUP BY ALL""")
display(report_py.orderBy("payer_label"))                      # the first action: now Spark runs the plan

# three ways to write one column expression
ages = pat.select((F.lit(2025) - F.col("birth_year")).alias("age_col"),   # Column API
                  F.expr("2025 - birth_year").alias("age_expr"))          # SQL text inside the DataFrame API
ages_sel = pat.selectExpr("2025 - birth_year AS age_sel")                 # selectExpr: a select written as SQL
n_age_mismatch = ages.filter("age_col <> age_expr").count()               # rows where the two columns differ
n_sel_diff = diff_rows(ages.select("age_col"), ages_sel)                  # selectExpr vs Column API

n_latest_py, n_latest_sql, d_latest = latest_py.count(), latest_sql.count(), diff_rows(latest_py, latest_sql)
n_gaps, d_gaps, n_first = gaps_py.count(), diff_rows(gaps_py, gaps_sql), gaps_py.filter("days_since_prev IS NULL").count()
n_groups, d_report = report_py.count(), diff_rows(report_py, report_sql)
print(f"dedup {n_latest_py} vs {n_latest_sql} (diff {d_latest}); gaps {n_gaps} (diff {d_gaps}, first visits {n_first}); "
      f"report {n_groups} groups (diff {d_report}); age mismatch {n_age_mismatch}/{n_sel_diff}")
METRICS["E1.latest_py"], METRICS["E1.latest_sql"], METRICS["E1.latest_diff"] = n_latest_py, n_latest_sql, d_latest
METRICS["E1.gaps_rows"], METRICS["E1.gaps_diff"], METRICS["E1.first_visits"] = n_gaps, d_gaps, n_first
METRICS["E1.report_groups"], METRICS["E1.report_diff"] = n_groups, d_report
METRICS["E1.age_mismatch"], METRICS["E1.sel_diff"] = n_age_mismatch, n_sel_diff
assert n_latest_py == 1350 and d_latest == 0 and d_gaps == 0 and d_report == 0 and n_age_mismatch == 0 and n_sel_diff == 0

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * **Dedup.** The DataFrame API has no `qualify()` method; the pattern is `withColumn("rn", F.row_number().over(w))` → `filter(rn = 1)` → `drop("rn")`. It returned **{{E1.latest_py}}** rows, the SQL `QUALIFY` version {{E1.latest_sql}}; rows that differ between the two results: **{{E1.latest_diff}}**.
# MAGIC * **`LAG`.** {{E1.gaps_rows}} visits; {{E1.first_visits}} of them have no previous visit (`days_since_prev` is NULL). Rows that differ from the SQL version: {{E1.gaps_diff}}. `F.datediff(end, start)` has the same argument order as SQL.
# MAGIC * **Report.** {{E1.report_groups}} payer groups; rows that differ from the SQL version: {{E1.report_diff}}. `F.avg(F.when(paid, amount))` without `otherwise` is the PySpark form of `avg(CASE WHEN … THEN amount END)`: the other rows are NULL and `avg` skips them.
# MAGIC * **Three ways to write a column.** `F.col` vs `F.expr`: {{E1.age_mismatch}} different rows; `selectExpr` vs `F.col`: {{E1.sel_diff}}. They are three spellings of the same expression.
# MAGIC
# MAGIC > **Key takeaway:** every L03 SQL pattern has a DataFrame form: `Window` + `row_number` + `filter` for `QUALIFY`, `F.lag(...).over(w)` for `LAG`, `groupBy().agg(F.count_if, F.avg(F.when(...)))` for `FILTER` / `CASE` aggregates.

# COMMAND ----------

# DBTITLE 1,E2 Predict
# MAGIC %md
# MAGIC ### E2 — Lazy evaluation: when does an error appear?
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `df = spark.range(5).withColumn("bad", F.col("no_such_col") + 1)` — does this line fail?
# MAGIC 2. A column `CAST('abc' AS INT)` fails under ANSI mode. Does `df.count()` fail? Does `df.collect()` fail?
# MAGIC 3. Does `df.columns` run the query?

# COMMAND ----------

# DBTITLE 1,E2 Code
ansi = spark.conf.get("spark.sql.ansi.enabled")                 # ANSI mode makes a bad CAST an error
wrong_name = spark.range(5).withColumn("bad", F.col("no_such_col") + 1)   # a column that does not exist
defined = "ok"                                                  # we reached this line: defining did not fail
wrong_columns = first_error(lambda: wrong_name.columns)         # schema access -> analysis on the server
wrong_count = first_error(lambda: wrong_name.count())           # an action

bad_cast = spark.range(5).withColumn("x", F.expr("CAST('abc' AS INT)"))   # valid plan, bad data
cast_columns = first_error(lambda: bad_cast.columns)            # analysis only: the plan is valid
cast_count = first_error(lambda: bad_cast.count())              # count() does not need column x
cast_collect = first_error(lambda: bad_cast.collect())          # collect() must compute x
cast_count_x = first_error(lambda: bad_cast.filter("x IS NULL").count())   # a filter on x needs x ... or not?
cast_x_rows = bad_cast.filter("x IS NULL").count() if cast_count_x == "ok" else -1   # how many rows passed?

t0 = time.perf_counter()
big_plan = spark.range(50_000_000).withColumn("y", F.col("id") * 2).filter("y % 3 = 0")   # only a plan
define_s = round(time.perf_counter() - t0, 3)
t0 = time.perf_counter()
n_big = big_plan.count()                                        # now the 50M rows are generated and filtered
action_s = round(time.perf_counter() - t0, 3)

display(spark.createDataFrame([("wrong column", "define", defined), ("wrong column", "df.columns", wrong_columns),
                               ("wrong column", "count()", wrong_count), ("bad CAST", "df.columns", cast_columns),
                               ("bad CAST", "count()", cast_count), ("bad CAST", "collect()", cast_collect),
                               ("bad CAST", "filter(x IS NULL).count()", cast_count_x)],
                              "case STRING, step STRING, result STRING"))
print(f"ANSI {ansi}; define {define_s}s, count {action_s}s for {n_big} rows")
METRICS["E2.ansi"], METRICS["E2.defined"], METRICS["E2.wrong_columns"], METRICS["E2.wrong_count"] = ansi, defined, wrong_columns, wrong_count
METRICS["E2.cast_columns"], METRICS["E2.cast_count"], METRICS["E2.cast_collect"], METRICS["E2.cast_count_x"] = cast_columns, cast_count, cast_collect, cast_count_x
METRICS["E2.cast_x_rows"] = cast_x_rows
METRICS["E2.big_rows"], METRICS["nd.E2.define_s"], METRICS["nd.E2.action_s"] = n_big, define_s, action_s
assert defined == "ok" and wrong_count != "ok" and n_big == 16666667

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * **Wrong column name.** Defining the DataFrame: **{{E2.defined}}** — no error. The error came only with `df.columns` (`{{E2.wrong_columns}}`) and with `count()` (`{{E2.wrong_count}}`). This is the Spark Connect behaviour from 4.2: analysis is deferred until the schema is needed or an action runs.
# MAGIC * **Bad data (`CAST('abc' AS INT)`, ANSI mode = `{{E2.ansi}}`).** `df.columns` → {{E2.cast_columns}} (the plan is valid). `count()` → **{{E2.cast_count}}**: `count()` does not need column `x`, so the bad `CAST` was never computed. `collect()` must compute `x` and failed with **`{{E2.cast_collect}}`**.
# MAGIC * Surprise: `filter("x IS NULL").count()` → {{E2.cast_count_x}} too ({{E2.cast_x_rows}} rows). No error means the `CAST` was not evaluated on any row: the optimizer found the answer another way. So "the action failed" depends on which columns the action really needs.
# MAGIC * Timing (changes from run to run): defining a 50,000,000-row plan took {{nd.E2.define_s}} s; the `count()` that really generated and filtered the rows ({{E2.big_rows}} remained) took {{nd.E2.action_s}} s.
# MAGIC
# MAGIC > **Key takeaway:** on serverless, a typo in a column name is reported at the first action or schema access, not at the line with the typo. Data errors appear only when an action needs the bad column. Test transformations with an action (or `df.columns`) close to where you write them.

# COMMAND ----------

# DBTITLE 1,E3 Predict
# MAGIC %md
# MAGIC ### E3 — `explain()`: join strategy with and without a broadcast hint
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. 5,000,000 rows joined with 2,000,000 rows, no hint: which join operator do you expect?
# MAGIC 2. The same join with `F.broadcast(mid)`?
# MAGIC 3. `encounters` (2,000 rows) joined with `providers` (20 rows), no hint?

# COMMAND ----------

# DBTITLE 1,E3 Code
big = spark.range(5_000_000).withColumn("k", F.col("id") % 1000)         # 5M rows, generated
mid = spark.range(2_000_000).withColumn("k2", F.col("id") % 7)           # 2M rows
plan_nohint = plan_text(big.join(mid, "id"))                             # let Spark choose
plan_hint = plan_text(big.join(F.broadcast(mid), "id"))                  # ask for a broadcast of mid
big.createOrReplaceTempView("big_v")                                     # session temp views for the SQL hint
mid.createOrReplaceTempView("mid_v")
plan_sql_hint = plan_text(spark.sql("SELECT /*+ BROADCAST(m) */ * FROM big_v b JOIN mid_v m ON b.id = m.id"))
plan_small = plan_text(enc.join(spark.table(f"{FQ}.providers"), "provider_id"))   # small table, no hint
print(plan_hint)                                                         # read it bottom-up
n_nohint, n_hint = big.join(mid, "id").count(), big.join(F.broadcast(mid), "id").count()   # same rows either way
photon_full = "The query is fully supported by Photon" in plan_nohint
op_nohint, op_hint = join_operator(plan_nohint), join_operator(plan_hint)
op_sql_hint, op_small = join_operator(plan_sql_hint), join_operator(plan_small)
print(f"no hint: {op_nohint}; hint: {op_hint}; SQL hint: {op_sql_hint}; small table: {op_small}; "
      f"rows {n_nohint} / {n_hint}; Photon: {photon_full}")
METRICS["E3.op_nohint"], METRICS["E3.op_hint"], METRICS["E3.op_sql_hint"], METRICS["E3.op_small"] = op_nohint, op_hint, op_sql_hint, op_small
METRICS["E3.rows_nohint"], METRICS["E3.rows_hint"], METRICS["E3.photon_full"] = n_nohint, n_hint, photon_full
nohint_shuffles = physical(plan_nohint).count("hashpartitioning(")      # both sides shuffled by the join key?
hint_bcast = "EXECUTOR_BROADCAST" in physical(plan_hint)                 # the small side is broadcast
METRICS["E3.adaptive"], METRICS["E3.not_final"] = "AdaptiveSparkPlan" in plan_nohint, "isFinalPlan=false" in plan_nohint
hint_shuffles = physical(plan_hint).count("hashpartitioning(")          # shuffles left with the hint
METRICS["E3.nohint_shuffles"], METRICS["E3.hint_bcast"], METRICS["E3.hint_shuffles"] = nohint_shuffles, hint_bcast, hint_shuffles
assert n_nohint == n_hint == 2_000_000 and "BroadcastHashJoin" in op_hint

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * **No hint (5M × 2M rows):** `{{E3.op_nohint}}`. The plan has {{E3.nohint_shuffles}} `hashpartitioning(...)` exchanges: both sides are shuffled by the join key.
# MAGIC * **`F.broadcast(mid)`:** `{{E3.op_hint}}`; the 2M-row side is sent with an `EXECUTOR_BROADCAST` exchange ({{E3.hint_bcast}}), and the plan has {{E3.hint_shuffles}} `hashpartitioning(...)` exchanges: the 5M-row side is not shuffled. The SQL hint `/*+ BROADCAST(m) */` gave the same operator: `{{E3.op_sql_hint}}`.
# MAGIC * **Small table, no hint (`encounters` × `providers`):** `{{E3.op_small}}` — Databricks chose the broadcast itself.
# MAGIC * Both joins returned the same rows ({{E3.rows_nohint}} and {{E3.rows_hint}}): a hint changes the **strategy**, never the result.
# MAGIC * The plan starts with `AdaptiveSparkPlan` ({{E3.adaptive}}) and `isFinalPlan=false` ({{E3.not_final}}): `explain()` shows the **initial** plan; adaptive query execution can still change it at run time. "The query is fully supported by Photon": {{E3.photon_full}}; on Photon the operators have a `Photon` prefix.
# MAGIC
# MAGIC > **Key takeaway:** read `explain()` bottom-up and look for the join operator and the exchanges. Use a broadcast hint only for a side that really fits in memory; small tables are broadcast automatically.

# COMMAND ----------

# DBTITLE 1,E4 Predict
# MAGIC %md
# MAGIC ### E4 — Schemas: DDL string vs `StructType`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `"patient_id INT, weight_kg DOUBLE"` and the same fields as `StructType`: equal schemas?
# MAGIC 2. Are DDL-string fields nullable?
# MAGIC 3. A `StructField(..., nullable=False)` and a row with `None` in that field: what happens?

# COMMAND ----------

# DBTITLE 1,E4 Code
ddl = "patient_id INT, weight_kg DOUBLE"                                 # schema as a DDL string
st = T.StructType([T.StructField("patient_id", T.IntegerType(), True),   # the same schema as objects
                   T.StructField("weight_kg", T.DoubleType(), True)])
rows = [(1, 70.5), (2, None)]                                            # a missing weight is allowed
s_ddl = spark.createDataFrame(rows, ddl).schema                          # schema from the DDL string
s_st = spark.createDataFrame(rows, st).schema                            # schema from StructType
st_strict = T.StructType([T.StructField("patient_id", T.IntegerType(), False),   # NOT NULL patient_id
                          T.StructField("weight_kg", T.DoubleType(), True)])
try:
    spark.createDataFrame([(None, 70.5)], st_strict).collect()                 # None where NOT NULL
    strict_error, strict_msg = "ok", ""
except Exception as e:
    strict_error, strict_msg = error_class(e), str(e).strip().splitlines()[0][:120]   # class and first message line
table_schema = pat.schema                                                # read a schema from a table
ddl_nullable = all(f.nullable for f in s_ddl.fields)                     # are all DDL fields nullable?
print("equal:", s_ddl == s_st, "| simpleString:", s_ddl.simpleString())
print("all DDL fields nullable:", ddl_nullable, "| strict schema with None:", strict_error, strict_msg)
print(table_schema.simpleString())                                       # the patients schema as text
METRICS["E4.equal"], METRICS["E4.simple"] = s_ddl == s_st, s_ddl.simpleString()
METRICS["E4.ddl_nullable"], METRICS["E4.strict_error"], METRICS["E4.strict_msg"] = ddl_nullable, strict_error, strict_msg
METRICS["E4.patients_schema"] = table_schema.simpleString()
assert s_ddl == s_st and strict_error != "ok"

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * The DDL string and the `StructType` gave equal schemas: **{{E4.equal}}** (`{{E4.simple}}`).
# MAGIC * All fields from the DDL string are nullable: {{E4.ddl_nullable}}.
# MAGIC * A `StructField(…, nullable=False)` with a `None` value failed with `{{E4.strict_error}}`: "{{E4.strict_msg}}". This is a PySpark client error type, not a Spark error class: the Python client checked the rows before sending them.
# MAGIC * `spark.table(...).schema.simpleString()` prints a table schema in the same short form: `{{E4.patients_schema}}`.
# MAGIC
# MAGIC > **Key takeaway:** use DDL strings for short, readable schemas; use `StructType` when you build schemas in code or need `nullable=False` on a field.

# COMMAND ----------

# DBTITLE 1,E5 Predict
# MAGIC %md
# MAGIC ### E5 — A reusable transform chain with `DataFrame.transform`, `withColumns` and `when`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `df.transform(add_charge_band, high=1500)`: what does `transform` pass to the function?
# MAGIC 2. `F.when(charge >= 1500, "HIGH").when(charge >= 500, "MEDIUM")` without `otherwise`: what do charges under 500 get?
# MAGIC 3. Is the chained result equal to one SQL query that does the same?

# COMMAND ----------

# DBTITLE 1,E5 Code
def drop_null_patients(df):
    """Remove encounters without a patient."""
    return df.filter(F.col("patient_id").isNotNull())

def add_charge_band(df, high=1000, medium=500):
    """HIGH / MEDIUM / LOW charge band; the thresholds are parameters."""
    return df.withColumn("charge_band",
                         F.when(F.col("charge") >= high, "HIGH")
                          .when(F.col("charge") >= medium, "MEDIUM")
                          .otherwise("LOW"))                             # otherwise: no NULL band

def add_visit_features(df):
    """Several new columns in one step."""
    return df.withColumns({
        "visit_month": F.trunc("encounter_date", "month"),               # first day of the month (DATE)
        "is_er": F.col("encounter_type") == "ER",                        # boolean flag
        "charge_k": F.round(F.col("charge") / 1000, 2)})                 # charge in thousands

def with_specialty(providers):
    """Return a transform that adds the provider specialty (a function that makes a function)."""
    def inner(df):
        return (df.join(F.broadcast(providers.select("provider_id", "specialty")), "provider_id", "left")
                  .withColumn("specialty", F.coalesce("specialty", F.lit("UNKNOWN"))))   # NULL provider -> label
    return inner

enriched = (enc.transform(drop_null_patients)                            # = drop_null_patients(enc)
               .transform(add_charge_band, high=1500)                    # = add_charge_band(df, high=1500)
               .transform(add_visit_features)
               .transform(with_specialty(spark.table(f"{FQ}.providers"))))
enriched.write.mode("overwrite").saveAsTable(f"{FQ}.encounters_enriched")   # E6 re-uses this table
saved = spark.table(f"{FQ}.encounters_enriched")
display(saved.groupBy("charge_band").count().orderBy("charge_band"))

no_otherwise = enc.withColumn("band", F.when(F.col("charge") >= 1500, "HIGH")   # the same CASE without otherwise
                                       .when(F.col("charge") >= 500, "MEDIUM"))
expected_sql = spark.sql(f"""
    SELECT e.provider_id, e.encounter_id, e.patient_id, e.encounter_type, e.encounter_date, e.charge,
           CASE WHEN e.charge >= 1500 THEN 'HIGH' WHEN e.charge >= 500 THEN 'MEDIUM' ELSE 'LOW' END AS charge_band,
           trunc(e.encounter_date, 'MONTH') AS visit_month, e.encounter_type = 'ER' AS is_er,
           round(e.charge / 1000, 2) AS charge_k, coalesce(p.specialty, 'UNKNOWN') AS specialty
    FROM {FQ}.encounters e LEFT JOIN {FQ}.providers p ON e.provider_id = p.provider_id
    WHERE e.patient_id IS NOT NULL""")
n_enriched, n_cols, n_enc = saved.count(), len(saved.columns), enc.count()
n_null_band, n_low = no_otherwise.filter("band IS NULL").count(), saved.filter("charge_band = 'LOW'").count()
n_low_all = enc.filter("charge < 500").count()                            # charges under 500 in all encounters
n_unknown, d_enriched = saved.filter("specialty = 'UNKNOWN'").count(), diff_rows(saved, expected_sql)
n_no_provider = enc.filter("patient_id IS NOT NULL AND provider_id IS NULL").count()
print(f"rows {n_enriched}, columns {saved.columns}; LOW {n_low}; NULL band without otherwise {n_null_band} "
      f"(charges < 500 in all {n_enc} encounters: {n_low_all}); UNKNOWN specialty {n_unknown}; diff vs SQL {d_enriched}")
METRICS["E5.rows"], METRICS["E5.columns"], METRICS["E5.n_columns"] = n_enriched, ", ".join(saved.columns), n_cols
METRICS["E5.low"], METRICS["E5.null_band"], METRICS["E5.low_all"] = n_low, n_null_band, n_low_all
METRICS["E5.unknown"], METRICS["E5.diff"] = n_unknown, d_enriched
assert n_enriched == 1980 and d_enriched == 0 and n_unknown == n_no_provider and n_null_band == n_low_all

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `df.transform(add_charge_band, high=1500)` called `add_charge_band(df, high=1500)`: the DataFrame is the first argument, extra keyword arguments follow. `with_specialty(providers)` returns a function, so it also fits into `transform`.
# MAGIC * The chain produced **{{E5.rows}}** rows and {{E5.n_columns}} columns (`{{E5.columns}}`). Rows that differ from the one-query SQL version: **{{E5.diff}}**.
# MAGIC * With `otherwise("LOW")`: {{E5.low}} `LOW` rows. The same `when` chain **without** `otherwise`: **{{E5.null_band}}** rows got a NULL band — exactly the {{E5.low_all}} encounters with `charge < 500`. No error, just NULLs.
# MAGIC * {{E5.unknown}} encounters have a patient but no provider; the `left` join + `coalesce` labelled them `UNKNOWN` instead of dropping them.
# MAGIC
# MAGIC > **Key takeaway:** small named functions + `transform()` = a readable, testable pipeline (L15 tests them with `pytest`). Always end a `when` chain with `otherwise` unless NULL is what you want.

# COMMAND ----------

# DBTITLE 1,E6 Predict
# MAGIC %md
# MAGIC ### E6 — Caching on serverless
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Does `df.cache()` work on serverless?
# MAGIC 2. And `CACHE TABLE` / `spark.catalog.cacheTable`?
# MAGIC 3. What do you use instead when a result is needed many times?

# COMMAND ----------

# DBTITLE 1,E6 Code
reuse = spark.table(f"{FQ}.encounters_enriched").filter("is_er")         # a result we want to use several times

def cache_message(fn):
    """Run fn; return (error class, first line of the message without the class), or ('ok', '')."""
    try:
        fn()
        return "ok", ""
    except Exception as e:
        first_line = str(e).strip().split("\n")[0]
        return error_class(e), re.sub(r"^\[[A-Z0-9_.]+\]\s*", "", first_line)[:160]

df_cache, df_cache_msg = cache_message(lambda: reuse.cache())            # DataFrame cache
df_persist, _ = cache_message(lambda: reuse.persist())                   # the same API family
sql_cache, sql_cache_msg = cache_message(lambda: spark.sql(f"CACHE TABLE {FQ}.encounters_enriched"))   # SQL
cat_cache, _ = cache_message(lambda: spark.catalog.cacheTable(f"{FQ}.encounters_enriched"))           # catalog API

spark.sql(f"CREATE OR REPLACE TABLE {FQ}.er_encounters AS SELECT * FROM {FQ}.encounters_enriched WHERE is_er")   # the serverless way
er = spark.table(f"{FQ}.er_encounters")                                  # later queries read this small table
n_er = er.count()
import sys, pyspark                                                     # which client runs this notebook?
client_pyspark, client_python = pyspark.__version__, f"{sys.version_info.major}.{sys.version_info.minor}"
display(spark.createDataFrame([("df.cache()", df_cache), ("df.persist()", df_persist), ("CACHE TABLE", sql_cache),
                               ("spark.catalog.cacheTable", cat_cache)], "api STRING, result STRING"))
print(df_cache_msg, "|", sql_cache_msg, "| client pyspark", client_pyspark, "python", client_python)
METRICS["E6.df_cache"], METRICS["E6.df_persist"], METRICS["E6.sql_cache"], METRICS["E6.catalog_cache"] = df_cache, df_persist, sql_cache, cat_cache
METRICS["E6.df_cache_msg"], METRICS["E6.sql_cache_msg"], METRICS["E6.er_rows"] = df_cache_msg, sql_cache_msg, n_er
METRICS["E6.client_pyspark"], METRICS["E6.client_python"] = client_pyspark, client_python

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `df.cache()` → `{{E6.df_cache}}`, `df.persist()` → `{{E6.df_persist}}`. The message begins: "{{E6.df_cache_msg}}…". This notebook ran with the PySpark client {{E6.client_pyspark}} on Python {{E6.client_python}}.
# MAGIC * `CACHE TABLE` → `{{E6.sql_cache}}` ("{{E6.sql_cache_msg}}"); `spark.catalog.cacheTable` → `{{E6.catalog_cache}}`.
# MAGIC * This matches the *Serverless compute limitations* page: the cache APIs result in an exception. The DataFrame message asks for a newer client version, so a newer serverless environment may accept `df.cache()`: **NOT VERIFIED — concept only** (the job runs on the default environment; the lesson does not change it).
# MAGIC * The serverless pattern: write the reused result to a table. `er_encounters` has {{E6.er_rows}} rows and every later query reads only that small table.
# MAGIC
# MAGIC > **Key takeaway:** on serverless, do not plan on `cache()`. Materialize a reused intermediate result as a table (or recompute it; Delta + Photon reads are fast).

# COMMAND ----------

# DBTITLE 1,E7 Predict
# MAGIC %md
# MAGIC ### E7 — BMI on 5,000,000 rows: built-in vs Python UDF vs Pandas UDF
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which is fastest? Which is slowest?
# MAGIC 2. A patient has `weight_kg = NULL`. What does the Python UDF do? The Pandas UDF?
# MAGIC 3. What does `explain()` show for a Python UDF?

# COMMAND ----------

# DBTITLE 1,E7 Code
base = spark.range(5_000_000).select(                                    # 5M fake measurements, deterministic
    (F.lit(45.0) + (F.col("id") * 7919 % 850) / 10.0).alias("w"),       # weight 45.0 - 129.9 kg
    (F.lit(150.0) + (F.col("id") * 104729 % 450) / 10.0).alias("h"))    # height 150.0 - 194.9 cm

bmi_builtin = F.col("w") / ((F.col("h") / 100.0) * (F.col("h") / 100.0))   # built-in column arithmetic

@F.udf("double")                                                         # row-at-a-time Python UDF
def bmi_python(w, h):
    return w / ((h / 100.0) * (h / 100.0))                               # called once per row

@F.pandas_udf("double")                                                  # vectorized: pd.Series in, pd.Series out
def bmi_pandas(w: pd.Series, h: pd.Series) -> pd.Series:
    return w / ((h / 100.0) * (h / 100.0))                               # called once per batch of rows

def timed(column):
    """Count rows with BMI >= 30 and measure the seconds (the aggregate evaluates the column once per row)."""
    t0 = time.perf_counter()
    n = base.select(column.alias("bmi")).agg(F.count_if(F.col("bmi") >= 30).alias("n")).first()["n"]
    return n, round(time.perf_counter() - t0, 2)

results = {}
for rnd in (1, 2):                                                       # two rounds: the first includes warm-up
    for name, column in [("builtin", bmi_builtin), ("python", bmi_python("w", "h")), ("pandas", bmi_pandas("w", "h"))]:
        results[(name, rnd)] = timed(column)
best = {name: min(results[(name, 1)][1], results[(name, 2)][1]) for name in ("builtin", "python", "pandas")}   # best of two
ranking = " < ".join(sorted(best, key=best.get))                         # fastest first, in THIS run
display(spark.createDataFrame([(n, r, results[(n, r)][0], float(results[(n, r)][1])) for (n, r) in results],
                              "method STRING, round INT, bmi_30_plus BIGINT, seconds DOUBLE"))

plan_py_full = plan_text(base.select(bmi_python("w", "h").alias("bmi")).agg(F.count_if(F.col("bmi") >= 30)))
plan_py = physical(plan_py_full)                                         # physical part only
plan_pd = physical(plan_text(base.select(bmi_pandas("w", "h").alias("bmi")).agg(F.count_if(F.col("bmi") >= 30))))
plan_bi = plan_text(base.select(bmi_builtin.alias("bmi")).agg(F.count_if(F.col("bmi") >= 30)))
print(plan_py_full)                                                      # look for the Python node and the Photon note

null_py = first_error(lambda: pat.select(bmi_python("weight_kg", "height_cm")).collect())   # None / x in Python
try:
    pat.select(bmi_python("weight_kg", "height_cm")).collect()
    null_py_type = "no error"
except Exception as e:
    null_py_type = "TypeError" if "TypeError" in str(e) else "other"     # the Python error inside the worker
pd_counts = pat.select(bmi_pandas("weight_kg", "height_cm").alias("b")).agg(
    F.count("*").alias("all_rows"), F.count("b").alias("non_null")).first()   # NULL in -> NULL out?

@F.udf("double")
def bmi_python_safe(w, h):
    if w is None or h is None:                                           # handle NULL yourself
        return None
    return w / ((h / 100.0) * (h / 100.0))
spark.udf.register("bmi_session", bmi_python_safe)                       # also callable from SQL in this session
safe_sql = spark.sql(f"SELECT count(bmi_session(weight_kg, height_cm)) AS n FROM {FQ}.patients").first()["n"]

counts = {results[(n, r)][0] for (n, r) in results}                      # one value if all methods agree
py_node, pd_node = "BatchEvalPython" in plan_py, "ArrowEvalPython" in plan_pd
bi_photon = "The query is fully supported by Photon" in plan_bi
py_not_photon = "Unsupported node: BatchEvalPython" in plan_py_full       # Photon explanation of the Python UDF plan
all_times = {f"{n}_{r}": results[(n, r)][1] for (n, r) in results}
print(results, "| best:", best, "| ranking in this run:", ranking)
METRICS["E7.bmi30_rows"], METRICS["E7.same_counts"] = results[("builtin", 2)][0], len(counts) == 1
METRICS["nd.E7.builtin_s"], METRICS["nd.E7.python_s"], METRICS["nd.E7.pandas_s"] = best["builtin"], best["python"], best["pandas"]
METRICS["nd.E7.ranking"], METRICS["nd.E7.all"] = ranking, all_times
METRICS["E7.py_node"], METRICS["E7.pd_node"], METRICS["E7.builtin_photon"] = py_node, pd_node, bi_photon
METRICS["E7.py_not_photon"] = py_not_photon
METRICS["E7.null_py_error"], METRICS["E7.null_py_type"] = null_py, null_py_type
METRICS["E7.pd_all"], METRICS["E7.pd_non_null"], METRICS["E7.safe_sql"] = pd_counts["all_rows"], pd_counts["non_null"], safe_sql
assert len(counts) == 1 and safe_sql == 195 and null_py != "ok"

# COMMAND ----------

# DBTITLE 1,E7 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * All three methods found the same **{{E7.bmi30_rows}}** rows with BMI ≥ 30 (all counts equal: {{E7.same_counts}}).
# MAGIC * **Timing** (seconds, best of two rounds, changes from run to run): built-in **{{nd.E7.builtin_s}}**, Pandas UDF **{{nd.E7.pandas_s}}**, Python UDF **{{nd.E7.python_s}}**. Order in this run, fastest first: **{{nd.E7.ranking}}**; both verification runs of this notebook (the two runs of `--twice`) gave the same order. All six measurements: {{nd.E7.all}}. Treat the first round as warm-up; that is why the table keeps the best of two.
# MAGIC * **Plans.** Python UDF → a `BatchEvalPython` node ({{E7.py_node}}); Pandas UDF → an `ArrowEvalPython` node ({{E7.pd_node}}); the built-in version is "fully supported by Photon" ({{E7.builtin_photon}}). The Photon explanation of the Python UDF plan lists `BatchEvalPython` as an "Unsupported node" ({{E7.py_not_photon}}): that part runs outside Photon.
# MAGIC * **NULL input.** The plain Python UDF failed on the 5 patients without a weight: `{{E7.null_py_error}}`, with a Python `{{E7.null_py_type}}` inside the worker (`None / float`). The Pandas UDF returned {{E7.pd_non_null}} non-NULL values of {{E7.pd_all}}: the missing weights came back as NULL. The null-safe Python UDF, registered with `spark.udf.register` and called from SQL, returned {{E7.safe_sql}} values.
# MAGIC
# MAGIC > **Key takeaway:** built-in functions first. If you need Python, prefer a Pandas UDF (vectorized, Arrow) over a row-at-a-time Python UDF, and handle `None` in every Python UDF.

# COMMAND ----------

# DBTITLE 1,E8 Predict
# MAGIC %md
# MAGIC ### E8 — Wrong `returnType` → silent NULLs, and a Spark Connect UDF gotcha
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. A Python UDF returns a Python `float` (`weight_kg * 2.2046`) but is declared `IntegerType()`. Error, rounded numbers, or NULL?
# MAGIC 2. It returns a Python `int` but is declared `"double"`?
# MAGIC 3. A UDF reads a Python variable `x = 123`; you change it to `x = 456` before the action. Which value does the result show?

# COMMAND ----------

# DBTITLE 1,E8 Code
arrow_conf = spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled")   # are Python UDFs Arrow-optimized by default here?

@F.udf(T.IntegerType())                                                  # WRONG: the function returns a float
def lbs_wrong(kg):
    return None if kg is None else kg * 2.2046

@F.udf(T.DoubleType())                                                   # RIGHT type for a float
def lbs_right(kg):
    return None if kg is None else kg * 2.2046

@F.udf("double")                                                         # WRONG: the function returns an int
def age_wrong(birth_year):
    return None if birth_year is None else 2025 - birth_year

lbs_arrow = F.udf(lambda kg: None if kg is None else kg * 2.2046, T.IntegerType(), useArrow=True)   # same bug, Arrow on

checked = pat.select(
    "patient_id", "weight_kg", lbs_wrong("weight_kg").alias("lbs_wrong"), lbs_right("weight_kg").alias("lbs_right"),
    age_wrong("birth_year").alias("age_wrong"), lbs_arrow("weight_kg").alias("lbs_arrow"))
display(checked.orderBy("patient_id").limit(5))                          # look at the NULLs
c = checked.agg(F.count("*").alias("rows"), F.count("weight_kg").alias("weights"),
                F.count("lbs_wrong").alias("lbs_wrong"), F.count("lbs_right").alias("lbs_right"),
                F.count("age_wrong").alias("age_wrong"), F.count("lbs_arrow").alias("lbs_arrow"),
                F.count_if(F.col("lbs_arrow") == F.floor("lbs_right")).alias("arrow_truncated")).first()

x = 123                                                                  # a Python variable
@F.udf("int")
def read_x():
    return x                                                             # the UDF reads the global x
captured = spark.range(1).select(read_x().alias("v"))                    # define the DataFrame while x = 123
x = 456                                                                  # change x before the action
seen = captured.first()["v"]                                             # the action serializes the UDF now

def make_reader(value):                                                  # the fix: capture the value in a closure
    @F.udf("int")
    def reader():
        return value
    return reader
x = 123
fixed = spark.range(1).select(make_reader(x)().alias("v"))               # value = 123 is fixed inside the closure
x = 456
seen_fixed = fixed.first()["v"]
print(c.asDict(), "| arrow default:", arrow_conf, "| captured:", seen, "fixed:", seen_fixed)
METRICS["E8.arrow_conf"], METRICS["E8.rows"], METRICS["E8.weights"] = arrow_conf, c["rows"], c["weights"]
METRICS["E8.lbs_wrong"], METRICS["E8.lbs_right"], METRICS["E8.age_wrong"] = c["lbs_wrong"], c["lbs_right"], c["age_wrong"]
METRICS["E8.lbs_arrow"], METRICS["E8.arrow_truncated"] = c["lbs_arrow"], c["arrow_truncated"]
METRICS["E8.seen"], METRICS["E8.seen_fixed"] = seen, seen_fixed
assert c["lbs_right"] == c["weights"] == 195

# COMMAND ----------

# DBTITLE 1,E8 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * In this job, `spark.sql.execution.pythonUDF.arrow.enabled` = `{{E8.arrow_conf}}`: Python UDFs are not Arrow-optimized by default here.
# MAGIC * **`IntegerType()` + a Python float:** {{E8.lbs_wrong}} non-NULL values out of {{E8.weights}} weights — **every value became NULL, with no error**. The same function declared `DoubleType()` returned {{E8.lbs_right}}.
# MAGIC * **`"double"` + a Python int** (`2025 - birth_year`): {{E8.age_wrong}} non-NULL values of {{E8.rows}}. The returned Python type must match the declared type; Spark does not convert `int` to `double` here.
# MAGIC * **The same bug with `useArrow=True`:** {{E8.lbs_arrow}} non-NULL values, and {{E8.arrow_truncated}} of them were equal to `floor(lbs)`: the decimals were cut off silently. Arrow did not give NULLs, but it still gave wrong numbers without an error.
# MAGIC * **Spark Connect gotcha.** The UDF that reads the global `x` returned **{{E8.seen}}**: the value at the time of the action, not when the DataFrame was defined (*Compare Spark Connect to Spark Classic*). The closure fix (`make_reader(x)`) returned {{E8.seen_fixed}}.
# MAGIC
# MAGIC > **Key takeaway:** after writing a Python UDF, count the NULLs of its output (`F.count(col)` vs `F.count("*")`). A wrong `returnType` is a silent data-quality bug.

# COMMAND ----------

# DBTITLE 1,E9 Predict
# MAGIC %md
# MAGIC ### E9 — A SQL UDF in Unity Catalog: `age_group`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Where does `CREATE FUNCTION main.dbacademy_l04.age_group …` store the function?
# MAGIC 2. Can PySpark code call a SQL function?
# MAGIC 3. Will the physical plan show a call to `age_group`, or something else?

# COMMAND ----------

# DBTITLE 1,E9 Code
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {FQ}.age_group(birth_year INT)   -- three-level name: stored in Unity Catalog
    RETURNS STRING
    COMMENT 'Age group in 2025: child, adult or senior'
    RETURN CASE WHEN 2025 - birth_year < 18 THEN 'child'
                WHEN 2025 - birth_year < 65 THEN 'adult'
                ELSE 'senior' END
""")

def describe_function(name, keep=("Function", "Type", "Input", "Returns", "Comment", "Deterministic",
                                  "Data Access", "Language", "Parameter Style", "Body")):
    """DESCRIBE FUNCTION EXTENDED without the long Configs list: (label, value) pairs."""
    out, label = [], None
    for line in [r["function_desc"] for r in spark.sql(f"DESCRIBE FUNCTION EXTENDED {name}").collect()]:
        if line[:1] not in (" ", "\t") and ":" in line:                  # a new label, e.g. "Returns:  STRING"
            label, value = line.split(":", 1)
            label = label.strip()
            if label in keep:
                out.append((label, value.strip()))
        elif label in keep and out and line.strip():                     # a continuation line (2nd parameter, body)
            out[-1] = (out[-1][0], out[-1][1] + " ; " + line.strip())
    return out

desc_sql = describe_function(f"{FQ}.age_group")
display(spark.createDataFrame(desc_sql, "label STRING, value STRING"))
d_sql = dict(desc_sql)

groups_sql = spark.sql(f"""SELECT {FQ}.age_group(birth_year) AS age_group, count(*) AS patients
                           FROM {FQ}.patients GROUP BY ALL ORDER BY age_group""")
display(groups_sql)
g = {r["age_group"]: r["patients"] for r in groups_sql.collect()}

variants = {                                                             # three ways to call it from PySpark
    "expr": lambda: pat.select(F.expr(f"{FQ}.age_group(birth_year)").alias("age_group")),
    "selectExpr": lambda: pat.selectExpr(f"{FQ}.age_group(birth_year) AS age_group"),
    "call_function": lambda: pat.select(F.call_function(f"{FQ}.age_group", F.col("birth_year")).alias("age_group"))}
variant_result = {}
for name, make in variants.items():
    try:
        rows_v = make().collect()                                        # run the select
        got = {k: sum(1 for r in rows_v if r["age_group"] == k) for k in g}   # count per group in Python
        variant_result[name] = "same" if got == g and len(rows_v) == 200 else f"different {got}"
    except Exception as e:
        variant_result[name] = error_class(e)
by_age = variants["selectExpr"]()                                        # a DataFrame with the function result
groupby_str = first_error(lambda: by_age.groupBy("age_group").count().collect())          # group by name
groupby_col = first_error(lambda: by_age.groupBy(F.col("age_group")).count().collect())   # group by F.col
groupby_sql = first_error(lambda: spark.sql("SELECT age_group, count(*) AS n FROM {d} GROUP BY age_group", d=by_age).collect())
groupby_direct = first_error(lambda: pat.groupBy(F.expr(f"{FQ}.age_group(birth_year)").alias("age_group")).count().collect())

plan_age = plan_text(pat.selectExpr(f"{FQ}.age_group(birth_year) AS age_group"))
inlined = "CASE WHEN" in physical(plan_age) and "age_group(" not in physical(plan_age)   # body copied into the plan?
age_photon = "fully supported by Photon" in plan_age
show_in = first_error(lambda: spark.sql(f"SHOW USER FUNCTIONS IN {FQ}").collect())   # default catalog is bunyod, not main
routines = spark.sql("""SELECT routine_name, routine_body, data_type          -- Unity Catalog metadata of the functions
                        FROM main.information_schema.routines
                        WHERE routine_schema = 'dbacademy_l04' ORDER BY routine_name""")
display(routines)
routine_list = [(r["routine_name"], r["routine_body"]) for r in routines.collect()]
print(g, variant_result, "| groupBy after the function:", groupby_str, groupby_col, groupby_sql, groupby_direct, "| inlined:", inlined, "| Photon:", age_photon, "| SHOW USER FUNCTIONS IN:", show_in, routine_list)
METRICS["E9.type"], METRICS["E9.returns"], METRICS["E9.deterministic"] = d_sql.get("Type"), d_sql.get("Returns"), d_sql.get("Deterministic")
METRICS["E9.data_access"], METRICS["E9.language"] = d_sql.get("Data Access"), d_sql.get("Language", "(no Language line)")
METRICS["E9.child"], METRICS["E9.adult"], METRICS["E9.senior"] = g.get("child", 0), g.get("adult", 0), g.get("senior", 0)
METRICS["E9.variants"], METRICS["E9.inlined"] = variant_result, inlined
METRICS["E9.groupby_str"], METRICS["E9.groupby_col"] = groupby_str, groupby_col
METRICS["E9.groupby_sql"], METRICS["E9.groupby_direct"] = groupby_sql, groupby_direct
METRICS["E9.photon"], METRICS["E9.show_in"], METRICS["E9.routines"] = age_photon, show_in, ", ".join(f"{n} ({b})" for n, b in routine_list)
assert sum(g.values()) == 200

# COMMAND ----------

# DBTITLE 1,E9 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `CREATE FUNCTION main.dbacademy_l04.age_group …` stored the function in Unity Catalog: `main.information_schema.routines` lists `{{E9.routines}}`. `SHOW USER FUNCTIONS IN main.dbacademy_l04` failed with `{{E9.show_in}}`, because the default catalog of this workspace is `bunyod`; the `information_schema` query works from any default catalog.
# MAGIC * `DESCRIBE FUNCTION EXTENDED` (without the long `Configs` list): `Type: {{E9.type}}`, `Returns: {{E9.returns}}`, `Deterministic: {{E9.deterministic}}`, `Data Access: {{E9.data_access}}`, the SQL `Body`, and no `Language` line (E10 compares this with the Python function).
# MAGIC * Age groups from SQL: child {{E9.child}}, adult {{E9.adult}}, senior {{E9.senior}}. Calling the function from PySpark gave the same values with all three styles: {{E9.variants}}.
# MAGIC * The physical plan contains the function **body** as a `CASE WHEN` instead of a call ({{E9.inlined}}), and the query is "fully supported by Photon" ({{E9.photon}}). This is why SQL UDFs are among "the most efficient options" (*What are UDFs?*).
# MAGIC * Surprise: after `selectExpr("…age_group(birth_year) AS age_group")`, `groupBy("age_group")` (by name) failed with `{{E9.groupby_str}}` in this job. `groupBy(F.col("age_group"))` → {{E9.groupby_col}}; grouping directly on the function expression → {{E9.groupby_direct}}; the same `GROUP BY` through `spark.sql(..., d=df)` → {{E9.groupby_sql}}. The cause is NOT VERIFIED (it may depend on the notebook client, PySpark {{E6.client_pyspark}} here).
# MAGIC
# MAGIC > **Key takeaway:** a SQL UDF is a named, governed SQL expression: write it once in Unity Catalog, call it from SQL or PySpark, and Spark still optimizes the expression inside it.

# COMMAND ----------

# DBTITLE 1,E10 Predict
# MAGIC %md
# MAGIC ### E10 — A Unity Catalog Python UDF: `bmi`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. What does `DESCRIBE FUNCTION EXTENDED` show for a Python function that it does not show for a SQL function?
# MAGIC 2. 5 patients have no weight. How many BMI values are not NULL?
# MAGIC 3. Does the UC Python function give the same result as the Pandas UDF from E7?

# COMMAND ----------

# DBTITLE 1,E10 Code
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.bmi(weight_kg DOUBLE, height_cm DOUBLE)
RETURNS DOUBLE
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Body mass index from weight in kg and height in cm'
AS $$
if weight_kg is None or height_cm is None:   # Python UDFs in UC must handle NULL themselves
    return None
return weight_kg / ((height_cm / 100.0) * (height_cm / 100.0))
$$
""")
desc_py = describe_function(f"{FQ}.bmi")                                 # helper from E9
display(spark.createDataFrame(desc_py, "label STRING, value STRING"))
d_py = dict(desc_py)

uc = spark.sql(f"""SELECT count(*) AS patients,
                          count({FQ}.bmi(weight_kg, height_cm)) AS with_bmi,
                          count_if({FQ}.bmi(weight_kg, height_cm) >= 30) AS bmi_30_plus
                   FROM {FQ}.patients""").first()
both = pat.select("patient_id",
                  F.expr(f"{FQ}.bmi(weight_kg, height_cm)").alias("bmi_uc"),     # UC Python UDF from PySpark
                  bmi_pandas("weight_kg", "height_cm").alias("bmi_pd"))          # Pandas UDF from E7
n_differ = both.filter("NOT (bmi_uc <=> bmi_pd)").count()                        # null-safe compare
display(both.orderBy("patient_id").limit(5))
plan_uc = physical(plan_text(both.select("bmi_uc")))                    # plan of the UC Python UDF column
print(plan_uc)
uc_ops = list(dict.fromkeys(re.findall(r"[+:]- (\w+)", plan_uc)))       # operator names, top to bottom
py_ops = sorted(set(re.findall(r"\b\w*Python\w*\b", plan_uc)))          # Python nodes in the plan
print(uc.asDict(), "| differ:", n_differ, "| Python plan nodes:", py_ops)
METRICS["E10.language"], METRICS["E10.style"] = d_py.get("Language"), d_py.get("Parameter Style")
METRICS["E10.deterministic"], METRICS["E10.input"] = d_py.get("Deterministic"), d_py.get("Input")
METRICS["E10.has_body"], METRICS["E10.sql_has_language"] = "Body" in d_py, "Language" in d_sql
METRICS["E10.patients"], METRICS["E10.with_bmi"], METRICS["E10.bmi30"] = uc["patients"], uc["with_bmi"], uc["bmi_30_plus"]
METRICS["E10.differ"], METRICS["E10.plan_nodes"], METRICS["E10.plan_ops"] = n_differ, ", ".join(py_ops) or "none", ", ".join(uc_ops)
assert uc["with_bmi"] == 195

# COMMAND ----------

# DBTITLE 1,E10 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `DESCRIBE FUNCTION EXTENDED` of the Python function shows `Language: {{E10.language}}`, `Parameter Style: {{E10.style}}`, `Deterministic: {{E10.deterministic}}`, the two inputs (`{{E10.input}}`) and the Python `Body` ({{E10.has_body}}). The SQL function in E9 had a `Language` line: {{E10.sql_has_language}}; it showed `Type` and `Data Access` instead.
# MAGIC * {{E10.with_bmi}} of {{E10.patients}} patients got a BMI (the function returns `None` for a missing weight), and {{E10.bmi30}} have a BMI ≥ 30.
# MAGIC * The UC Python function and the Pandas UDF from E7 gave the same values: {{E10.differ}} rows differ (null-safe compare).
# MAGIC * Plan operators for the UC function column, top to bottom: `{{E10.plan_ops}}`. Nodes with "Python" in the name: {{E10.plan_nodes}}.
# MAGIC
# MAGIC > **Key takeaway:** a UC Python function is a Python UDF with a three-level name: it is stored and governed in Unity Catalog (`GRANT EXECUTE`), any notebook, job or SQL warehouse user with access can call it, and it must handle NULL itself.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "A typo in a column name fails on the line where I write it." | On serverless (Spark Connect) defining the DataFrame gave **{{E2.defined}}**; the error `{{E2.wrong_count}}` came at the action (E2, *Compare Spark Connect to Spark Classic*). |
# MAGIC | 2 | "If `count()` works, all columns are valid." | With a bad `CAST`, `count()` → {{E2.cast_count}}, `collect()` → `{{E2.cast_collect}}`: an action computes only the columns it needs (E2). |
# MAGIC | 3 | "A broadcast hint can change the join result." | Same rows with and without the hint ({{E3.rows_nohint}} / {{E3.rows_hint}}); only the operator changed: `{{E3.op_nohint}}` → `{{E3.op_hint}}` (E3). |
# MAGIC | 4 | "A small dimension table is broadcast only with a hint." | `encounters` × `providers` without a hint used `{{E3.op_small}}` (E3). |
# MAGIC | 5 | "`explain()` shows the plan that will run, final." | It showed `isFinalPlan=false` ({{E3.not_final}}): adaptive query execution can still change it (E3). |
# MAGIC | 6 | "`df.cache()` speeds up repeated reads on serverless." | `df.cache()` → `{{E6.df_cache}}`, `CACHE TABLE` → `{{E6.sql_cache}}` (E6, *Serverless compute limitations*). Write a table instead. |
# MAGIC | 7 | "`F.when(...)` without `otherwise` keeps the old value." | No branch matched → NULL: {{E5.null_band}} NULL bands in E5. |
# MAGIC | 8 | "A wrong UDF `returnType` raises an error." | `IntegerType()` + float result → {{E8.lbs_wrong}} non-NULL values of {{E8.weights}}: silent NULLs (E8). |
# MAGIC | 9 | "Spark converts a Python `int` to `double` when the UDF says `"double"`." | {{E8.age_wrong}} non-NULL values of {{E8.rows}} (E8). |
# MAGIC | 10 | "`useArrow=True` fixes a wrong `returnType`." | It returned numbers, but {{E8.arrow_truncated}} of {{E8.lbs_arrow}} were cut to whole numbers, silently (E8). |
# MAGIC | 11 | "Python UDFs handle NULL like built-in functions." | The plain UDF failed with `{{E7.null_py_error}}` (a Python `{{E7.null_py_type}}`) on 5 NULL weights (E7). UC Python UDFs "must handle NULL values independently" (*UDFs in Unity Catalog*). |
# MAGIC | 12 | "A Pandas UDF is as fast as a built-in function." | "Built-in functions and SQL UDFs are the most efficient options" (*What are UDFs?*). In this run: built-in {{nd.E7.builtin_s}} s vs Pandas {{nd.E7.pandas_s}} s vs Python {{nd.E7.python_s}} s (E7). |
# MAGIC | 13 | "A UDF uses the value a Python variable had when I defined the DataFrame." | On Spark Connect it used the value at the action: {{E8.seen}} instead of 123 (E8). |
# MAGIC | 14 | "A SQL UDF is a black box, like a Python UDF." | Its body was copied into the plan as a `CASE WHEN` ({{E9.inlined}}) and the query stayed fully on Photon ({{E9.photon}}) (E9). |
# MAGIC | 15 | "`@udf` / `spark.udf.register` makes the function available to other notebooks and users." | They are scoped to the notebook or Spark session; `CREATE FUNCTION catalog.schema.name` stores it in Unity Catalog (*UDFs in Unity Catalog*, E9, E10). |
# MAGIC | 16 | "`SHOW USER FUNCTIONS IN main.dbacademy_l04` works from any default catalog." | With default catalog `bunyod` it failed: `{{E9.show_in}}`. `main.information_schema.routines` listed the function (E9). |

# COMMAND ----------

# DBTITLE 1,7 — Syntax Cheat Sheet
# MAGIC %md
# MAGIC ## 7 Syntax Cheat Sheet
# MAGIC
# MAGIC | Goal | SQL | PySpark (`from pyspark.sql import Window, functions as F, types as T`) |
# MAGIC | --- | --- | --- |
# MAGIC | Read a table | `SELECT * FROM main.s.t` | `spark.table("main.s.t")` |
# MAGIC | Filter | `WHERE a > 1 AND b = 'ER'` | `df.filter((F.col("a") > 1) & (F.col("b") == "ER"))` or `df.filter("a > 1 AND b = 'ER'")` |
# MAGIC | New columns | `SELECT *, x * 2 AS y` | `df.withColumn("y", F.col("x") * 2)`; many: `df.withColumns({"y": ..., "z": ...})` |
# MAGIC | SQL snippet in Python | — | `F.expr("datediff(a, b)")`, `df.selectExpr("a", "b * 2 AS c")` |
# MAGIC | CASE WHEN | `CASE WHEN c THEN 'A' WHEN d THEN 'B' ELSE 'C' END` | `F.when(c, "A").when(d, "B").otherwise("C")` |
# MAGIC | NULL replace | `coalesce(payer, 'SELF-PAY')` | `F.coalesce("payer", F.lit("SELF-PAY"))` |
# MAGIC | Join | `a JOIN b USING (k)`, `LEFT ANTI JOIN` | `a.join(b, "k")`, `a.join(b, "k", "left_anti")` |
# MAGIC | Broadcast | `SELECT /*+ BROADCAST(b) */ …` | `a.join(F.broadcast(b), "k")` |
# MAGIC | Aggregate | `GROUP BY p` + `count(*)`, `count_if(c)`, `avg(CASE … END)` | `df.groupBy("p").agg(F.count("*"), F.count_if(c), F.avg(F.when(c, x)))` |
# MAGIC | Window | `ROW_NUMBER() OVER (PARTITION BY k ORDER BY t DESC)` | `F.row_number().over(Window.partitionBy("k").orderBy(F.desc("t")))` |
# MAGIC | Dedup | `QUALIFY ROW_NUMBER() OVER (…) = 1` | `.withColumn("rn", …).filter("rn = 1").drop("rn")` |
# MAGIC | Previous row | `LAG(d) OVER (…)` | `F.lag("d").over(w)` |
# MAGIC | Reusable step | a view / CTE | `def step(df, p): return df…` + `df.transform(step, p=1)` |
# MAGIC | Schema | `patient_id INT, weight_kg DOUBLE` | the same DDL string, or `T.StructType([T.StructField("patient_id", T.IntegerType()), …])` |
# MAGIC | Plan | `EXPLAIN [FORMATTED] SELECT …` | `df.explain()`, `df.explain("formatted")` |
# MAGIC | Python UDF | — | `@F.udf("double")` / `F.udf(fn, T.DoubleType())`; for SQL: `spark.udf.register("name", fn)` |
# MAGIC | Pandas UDF | — | `@F.pandas_udf("double")` + `def f(a: pd.Series, b: pd.Series) -> pd.Series:` |
# MAGIC | SQL UDF in UC | `CREATE OR REPLACE FUNCTION main.s.f(x INT) RETURNS STRING RETURN CASE … END` | call: `F.expr("main.s.f(x)")`, `F.call_function("main.s.f", F.col("x"))` |
# MAGIC | Python UDF in UC | `CREATE FUNCTION main.s.f(x DOUBLE) RETURNS DOUBLE LANGUAGE PYTHON AS $$ … $$` | the same calls as above |
# MAGIC | Session-only SQL function | `CREATE TEMPORARY FUNCTION f(x INT) RETURNS INT RETURN x + 1` | — |
# MAGIC | Inspect a function | `DESCRIBE FUNCTION EXTENDED main.s.f` | `spark.sql("DESCRIBE FUNCTION EXTENDED main.s.f")` |
# MAGIC | List functions | `SELECT * FROM main.information_schema.routines WHERE routine_schema = 's'` | the same through `spark.sql` |
# MAGIC | Re-use a result (serverless) | `CREATE OR REPLACE TABLE main.s.tmp AS SELECT …` | `df.write.mode("overwrite").saveAsTable("main.s.tmp")` (not `cache()`) |

# COMMAND ----------

# DBTITLE 1,8 — Check Your Understanding
# MAGIC %md
# MAGIC ## 8 Check Your Understanding
# MAGIC
# MAGIC Answer in your own words. Answers are in section 11.
# MAGIC
# MAGIC 1. On serverless, a 40-line PySpark cleaning chain fails at the final `display()` with `UNRESOLVED_COLUMN`, but the typo is on line 5. Why is the error reported so late, and how can you find such problems earlier?
# MAGIC 2. A lab team's Python UDF converts glucose from mg/dL to mmol/L and is declared with `IntegerType()`. The dashboard shows mostly empty values, but the job never failed. Explain what happened, how you would detect it, and what you would use instead of a Python UDF.
# MAGIC 3. Your organisation needs one BMI classification that analysts use in SQL dashboards and engineers use in PySpark jobs, with access control. Which kind of function do you create, and why not a `@udf` in a shared notebook?

# COMMAND ----------

# DBTITLE 1,9 — Practice (intro)
# MAGIC %md
# MAGIC ## 9 Practice
# MAGIC
# MAGIC Do these yourself, without AI help. Each task has a validation cell that prints ✅ Passed or a hint. Solutions are in section 11.
# MAGIC
# MAGIC > The practice uses the setup tables only. Run section 3 and the cell *5 — Example helpers* first (it imports `pandas as pd` and defines `diff_rows`).

# COMMAND ----------

# DBTITLE 1,P1 Task
# MAGIC %md
# MAGIC ### P1 — Fill in the blanks (3 blanks): `withColumns` and `when`
# MAGIC Create `main.dbacademy_l04.p1_patients` from `patients` with three new columns, added in **one** call:
# MAGIC * `age` = `2025 - birth_year`;
# MAGIC * `age_band` = `child` (age under 18), `adult` (under 65) or `senior` — never NULL;
# MAGIC * `has_weight` = `true` when `weight_kg` is not NULL.
# MAGIC
# MAGIC Replace the three `<FILL_IN>` markers.

# COMMAND ----------

# DBTITLE 1,P1 Code
# >>> BLANK P1
p1 = (spark.table(f"{FQ}.patients")
      .<FILL_IN>({                                                          # 1: several columns in one call
          "age": F.lit(2025) - F.col("birth_year"),
          "age_band": F.<FILL_IN>(F.lit(2025) - F.col("birth_year") < 18, "child")   # 2: start the CASE WHEN
                       .when(F.lit(2025) - F.col("birth_year") < 65, "adult")
                       .<FILL_IN>("senior"),                                # 3: the ELSE branch
          "has_weight": F.col("weight_kg").isNotNull()}))
p1.write.mode("overwrite").saveAsTable(f"{FQ}.p1_patients")
# >>> SOLUTION P1
p1 = (spark.table(f"{FQ}.patients")
      .withColumns({                                                        # 1: several columns in one call
          "age": F.lit(2025) - F.col("birth_year"),
          "age_band": F.when(F.lit(2025) - F.col("birth_year") < 18, "child")   # 2: start the CASE WHEN
                       .when(F.lit(2025) - F.col("birth_year") < 65, "adult")
                       .otherwise("senior"),                                # 3: the ELSE branch
          "has_weight": F.col("weight_kg").isNotNull()}))
p1.write.mode("overwrite").saveAsTable(f"{FQ}.p1_patients")
# <<< END P1

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    t = spark.table(f"{FQ}.p1_patients")
    expected = spark.sql(f"""SELECT patient_id, 2025 - birth_year AS age,
                                    CASE WHEN 2025 - birth_year < 18 THEN 'child'
                                         WHEN 2025 - birth_year < 65 THEN 'adult' ELSE 'senior' END AS age_band,
                                    weight_kg IS NOT NULL AS has_weight
                             FROM {FQ}.patients""")
    wrong = diff_rows(t.select("patient_id", "age", "age_band", "has_weight"), expected)
    null_bands, with_weight, n = t.filter("age_band IS NULL").count(), t.filter("has_weight").count(), t.count()
    ok = wrong == 0 and n == 200
    PRACTICE_PASSED["P1"] = ok
    print("✅ Passed" if ok else
          f"Hint: {wrong} rows differ from the expected result; {null_bands} rows have a NULL age_band. "
          "withColumns takes a dict; F.when(...).when(...) needs an otherwise(...) for the last band.")
    METRICS["P1.null_bands"], METRICS["P1.with_weight"] = null_bands, with_weight
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: the table main.dbacademy_l04.p1_patients does not exist yet. Check the three blanks.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs)
# MAGIC The nutrition team needs `main.dbacademy_l04.p2_patients` with:
# MAGIC * `weight_lbs` — the weight in pounds (`weight_kg * 2.2046`, one decimal); NULL only when `weight_kg` is NULL;
# MAGIC * `bmi_class` — `OBESE` (BMI ≥ 30), `OVERWEIGHT` (≥ 25), `NORMAL` (below 25), and `UNKNOWN` when there is no BMI. It must **never** be NULL.
# MAGIC
# MAGIC The cell runs without an error, but the result is wrong in two places. Fix them in place.

# COMMAND ----------

# DBTITLE 1,P2 Code
# >>> BLANK P2
@F.udf(T.IntegerType())
def kg_to_lbs(kg):
    return None if kg is None else round(kg * 2.2046, 1)

p2 = (spark.table(f"{FQ}.patients")
      .withColumn("weight_lbs", kg_to_lbs("weight_kg"))
      .withColumn("bmi", F.col("weight_kg") / ((F.col("height_cm") / 100) * (F.col("height_cm") / 100)))
      .withColumn("bmi_class", F.when(F.col("bmi") >= 30, "OBESE")
                                .when(F.col("bmi") >= 25, "OVERWEIGHT")))
p2.write.mode("overwrite").saveAsTable(f"{FQ}.p2_patients")
# >>> SOLUTION P2
@F.udf(T.DoubleType())                                                     # fix 1: the function returns a float
def kg_to_lbs(kg):
    return None if kg is None else round(kg * 2.2046, 1)

p2 = (spark.table(f"{FQ}.patients")
      .withColumn("weight_lbs", kg_to_lbs("weight_kg"))
      .withColumn("bmi", F.col("weight_kg") / ((F.col("height_cm") / 100) * (F.col("height_cm") / 100)))
      .withColumn("bmi_class", F.when(F.col("bmi") >= 30, "OBESE")
                                .when(F.col("bmi") >= 25, "OVERWEIGHT")
                                .when(F.col("bmi").isNull(), "UNKNOWN")      # fix 2: the missing branches
                                .otherwise("NORMAL")))
p2.write.mode("overwrite").saveAsTable(f"{FQ}.p2_patients")
# <<< END P2

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    t = spark.table(f"{FQ}.p2_patients")
    c = t.selectExpr("count(weight_lbs) AS lbs",
                     "count_if(abs(weight_lbs - weight_kg * 2.2046) > 0.051) AS lbs_wrong",
                     "count_if(bmi_class IS NULL) AS null_class",
                     "count_if(bmi_class = 'UNKNOWN') AS unknown",
                     "count_if(bmi_class = 'NORMAL') AS normal").first()
    ok_a = c["lbs"] == 195 and c["lbs_wrong"] == 0
    ok_b = c["null_class"] == 0 and c["unknown"] == 5
    ok = ok_a and ok_b
    PRACTICE_PASSED["P2"] = ok
    if ok:
        print("✅ Passed")
    if not ok_a:
        print(f"Hint (weight_lbs): {c['lbs']} non-NULL values, want 195. What does a Python UDF return when the "
              "value does not fit the declared returnType?")
    if not ok_b:
        print(f"Hint (bmi_class): {c['null_class']} NULL classes, {c['unknown']} UNKNOWN (want 0 and 5). "
              "What does F.when give when no branch matches?")
    METRICS["P2.lbs"], METRICS["P2.normal"], METRICS["P2.unknown"] = c["lbs"], c["normal"], c["unknown"]
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: run the P2 code cell first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): a claims cleaning module
# MAGIC Write **four functions** that each take a DataFrame and return a DataFrame, then chain them with `transform()` on `claims` and save the result as `main.dbacademy_l04.p3_claims_clean`:
# MAGIC 1. `dedup_latest(df)` — keep only the newest version (latest `updated_at`) of each `claim_id`;
# MAGIC 2. `label_payer(df)` — replace a NULL `payer` with `'SELF-PAY'`;
# MAGIC 3. `add_amount_band(df, high)` — new column `amount_band`: `'HIGH'` when `amount >= high`, else `'NORMAL'`; call it with `high=3000`;
# MAGIC 4. `add_is_final(df)` — new column `is_final`: `true` when `status` is `PAID` or `DENIED`, else `false`.
# MAGIC
# MAGIC Keep all original columns. **Hints:** `Window.partitionBy(...).orderBy(F.desc(...))` + `F.row_number()` · `F.coalesce` + `F.lit` · `F.when(...).otherwise(...)` · `F.col("status").isin(...)` · `df.transform(fn, high=3000)`.

# COMMAND ----------

# DBTITLE 1,P3 Code
# >>> BLANK P3
# Your code here
# >>> SOLUTION P3
def dedup_latest(df):
    w = Window.partitionBy("claim_id").orderBy(F.desc("updated_at"))
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

def label_payer(df):
    return df.withColumn("payer", F.coalesce("payer", F.lit("SELF-PAY")))

def add_amount_band(df, high):
    return df.withColumn("amount_band", F.when(F.col("amount") >= high, "HIGH").otherwise("NORMAL"))

def add_is_final(df):
    return df.withColumn("is_final", F.col("status").isin("PAID", "DENIED"))

(spark.table(f"{FQ}.claims")
    .transform(dedup_latest)
    .transform(label_payer)
    .transform(add_amount_band, high=3000)
    .transform(add_is_final)
    .write.mode("overwrite").saveAsTable(f"{FQ}.p3_claims_clean"))
# <<< END P3

# COMMAND ----------

# DBTITLE 1,P3 Validation
try:
    t = spark.table(f"{FQ}.p3_claims_clean")
    expected = spark.sql(f"""
        SELECT claim_id, encounter_id, patient_id, coalesce(payer, 'SELF-PAY') AS payer, status, amount, updated_at,
               CASE WHEN amount >= 3000 THEN 'HIGH' ELSE 'NORMAL' END AS amount_band,
               status IN ('PAID', 'DENIED') AS is_final
        FROM {FQ}.claims
        QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1""")
    cols = ["claim_id", "encounter_id", "patient_id", "payer", "status", "amount", "updated_at", "amount_band", "is_final"]
    missing_cols = [c for c in cols if c not in t.columns]
    wrong = diff_rows(t.select(*cols), expected) if not missing_cols else -1
    missing_fns = [n for n in ("dedup_latest", "label_payer", "add_amount_band", "add_is_final") if not callable(globals().get(n))]
    n_rows, n_high = t.count(), t.filter("amount_band = 'HIGH'").count()
    ok = not missing_cols and wrong == 0 and not missing_fns
    PRACTICE_PASSED["P3"] = ok
    print("✅ Passed" if ok else
          f"Hint: missing columns {missing_cols}, {wrong} rows differ, missing functions {missing_fns}. "
          "Did you dedup first (1350 rows)? Is amount_band 'HIGH' from 3000 up?")
    METRICS["P3.rows"], METRICS["P3.high"] = n_rows, n_high
except Exception as e:
    PRACTICE_PASSED["P3"] = False
    print("Hint: the table main.dbacademy_l04.p3_claims_clean does not exist yet.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints)
# MAGIC Create a **Unity Catalog SQL function** `main.dbacademy_l04.bmi_class(bmi DOUBLE)` that returns `'UNKNOWN'` for NULL, `'OBESE'` for ≥ 30, `'OVERWEIGHT'` for ≥ 25, otherwise `'NORMAL'`.
# MAGIC Then, in **one** DataFrame query, use a **Pandas UDF** to compute the BMI (`weight_kg / (height_cm / 100)²`) and the UC function to classify it, and save `main.dbacademy_l04.p4_patient_bmi` with the columns `patient_id`, `bmi` (rounded to 1 decimal) and `bmi_class` (the class of the rounded `bmi`).

# COMMAND ----------

# DBTITLE 1,P4 Code
# >>> BLANK P4
# Your code here
# >>> SOLUTION P4
spark.sql(f"""
    CREATE OR REPLACE FUNCTION {FQ}.bmi_class(bmi DOUBLE)
    RETURNS STRING
    RETURN CASE WHEN bmi IS NULL THEN 'UNKNOWN'
                WHEN bmi >= 30 THEN 'OBESE'
                WHEN bmi >= 25 THEN 'OVERWEIGHT'
                ELSE 'NORMAL' END
""")

@F.pandas_udf("double")
def bmi_pd(weight_kg: pd.Series, height_cm: pd.Series) -> pd.Series:
    return weight_kg / ((height_cm / 100) ** 2)

(spark.table(f"{FQ}.patients")
    .withColumn("bmi", F.round(bmi_pd("weight_kg", "height_cm"), 1))
    .withColumn("bmi_class", F.expr(f"{FQ}.bmi_class(bmi)"))
    .select("patient_id", "bmi", "bmi_class")
    .write.mode("overwrite").saveAsTable(f"{FQ}.p4_patient_bmi"))
# <<< END P4

# COMMAND ----------

# DBTITLE 1,P4 Validation
try:
    fn = spark.sql(f"""SELECT {FQ}.bmi_class(NULL) AS a, {FQ}.bmi_class(31.0) AS b,
                              {FQ}.bmi_class(25.0) AS c, {FQ}.bmi_class(24.9) AS d""").first()
    ok_fn = (fn["a"], fn["b"], fn["c"], fn["d"]) == ("UNKNOWN", "OBESE", "OVERWEIGHT", "NORMAL")
    t = spark.table(f"{FQ}.p4_patient_bmi")
    check = t.join(spark.table(f"{FQ}.patients"), "patient_id").selectExpr(
        "count(*) AS n",
        "count_if(NOT (bmi <=> round(weight_kg / ((height_cm / 100) * (height_cm / 100)), 1)) "
        "AND NOT abs(bmi - weight_kg / ((height_cm / 100) * (height_cm / 100))) <= 0.051) AS bad_bmi",
        f"count_if(NOT (bmi_class <=> {FQ}.bmi_class(bmi))) AS bad_class",
        "count_if(bmi_class = 'OBESE') AS obese").first()
    ok = ok_fn and check["n"] == 200 and check["bad_bmi"] == 0 and check["bad_class"] == 0 and t.columns == ["patient_id", "bmi", "bmi_class"]
    PRACTICE_PASSED["P4"] = ok
    print("✅ Passed" if ok else f"Not yet: function checks {tuple(fn)}, table {check.asDict()}, columns {t.columns}.")
    METRICS["P4.obese"] = check["obese"]
except Exception as e:
    PRACTICE_PASSED["P4"] = False
    print("The function main.dbacademy_l04.bmi_class or the table p4_patient_bmi does not exist yet.", error_class(e))

# COMMAND ----------

# DBTITLE 1,10 — Exam-Style Questions
# MAGIC %md
# MAGIC ## 10 Exam-Style Questions
# MAGIC
# MAGIC Choose the **one best answer**. Answers and explanations are in section 11.
# MAGIC
# MAGIC **Q1.** A nutrition pipeline defines `@F.udf(T.IntegerType())` for `lambda kg: kg * 2.2046` and writes `weight_lbs` to a silver table. Serverless, default settings. What does the column contain?
# MAGIC A. Pounds rounded to the nearest integer · B. The job fails with a type error · C. NULL in every row · D. Pounds with the decimals cut off
# MAGIC
# MAGIC **Q2.** A hospital wants one BMI classification that analysts call from SQL dashboards and engineers call from PySpark jobs, governed with Unity Catalog privileges. What should the engineer create?
# MAGIC A. A `@F.udf` in a shared notebook that every job runs with `%run` · B. `spark.udf.register("bmi_class", fn)` in a setup notebook · C. `CREATE FUNCTION main.clinical.bmi_class(bmi DOUBLE) RETURNS STRING RETURN CASE … END` · D. A Pandas UDF in a Python wheel
# MAGIC
# MAGIC **Q3.** On serverless compute, an engineer calls `claims_df.cache()` before using an expensive deduplicated claims result in five reports. The call raises an exception. What is the best fix?
# MAGIC A. `CACHE TABLE claims_dedup` · B. `spark.catalog.cacheTable("claims_dedup")` · C. Write the result to a Delta table and let the five reports read it · D. `claims_df.persist(StorageLevel.MEMORY_ONLY)`
# MAGIC
# MAGIC **Q4.** A data engineer must compute BMI for 50 million vital-sign rows. Which approach is the most efficient?
# MAGIC A. Column arithmetic with built-in functions · B. A Pandas UDF · C. A row-at-a-time Python UDF · D. A Unity Catalog Python UDF
# MAGIC
# MAGIC **Q5.** In a serverless notebook, `df = encounters.withColumn("los", F.datediff("discharge_dt", "admit_dt"))` is written, but the column is called `admission_date`, not `admit_dt`. When is the error raised?
# MAGIC A. On the `withColumn` line · B. At the first action, or when `df.columns` / `df.schema` is accessed · C. Never: the column is NULL · D. Only when the result is written to a table
# MAGIC
# MAGIC **Q6.** A 500-million-row claims fact table is joined to a 60-row payer table. The engineer wants to avoid shuffling the fact table. What should they write?
# MAGIC A. `claims.join(F.broadcast(payers), "payer_id")` · B. `claims.repartition(200).join(payers, "payer_id")` · C. `payers.cache()` before the join · D. `claims.join(payers.hint("merge"), "payer_id")`
# MAGIC
# MAGIC **Q7.** Which definition is a valid Series-to-Series Pandas UDF?
# MAGIC A. `@F.pandas_udf("double")` + `def f(a: pd.Series, b: pd.Series) -> pd.Series` · B. `@F.udf("double")` + `def f(a: pd.Series) -> pd.Series` · C. `@F.pandas_udf("double")` + `def f(a: pd.DataFrame) -> float` · D. `@F.pandas_udf` + `def f(a: list) -> list`
# MAGIC
# MAGIC **Q8.** A sepsis risk band is written as `F.when(F.col("score") >= 8, "HIGH").when(F.col("score") >= 5, "MEDIUM")`. What do patients with a score of 3 get?
# MAGIC A. `LOW` · B. An error · C. NULL · D. `MEDIUM`

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — keep the newest version (`QUALIFY`)**
# MAGIC {{solution:W1}}
# MAGIC
# MAGIC **W2 — no match (`LEFT ANTI JOIN`, safe with the NULL `patient_id`)**
# MAGIC {{solution:W2}}
# MAGIC
# MAGIC **W3 — days since the previous visit (`LAG` + `datediff(end, start)`)**
# MAGIC {{solution:W3}}
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. Serverless uses Spark Connect, which "defers analysis and name resolution to execution time": the chain is only a local plan until an action or a schema access (E2: defining gave {{E2.defined}}, the action gave `{{E2.wrong_count}}`). Check earlier: call `df.columns` or a small action (`df.limit(1).collect()`) after each step while developing, keep steps in small functions and test them one by one (`transform`), and read the suggestion list in the error ("Did you mean …").
# MAGIC 2. The function returns a Python float but the UDF is declared `IntegerType()`; a value that does not fit becomes NULL without an error (E8: {{E8.lbs_wrong}} of {{E8.weights}} values). Detect it by comparing `F.count(col)` with `F.count("*")` (or an expectation / data-quality check on NULLs) after the step. Better: no UDF at all — the conversion is built-in arithmetic (`F.col("glucose") / 18.0`); if Python is really needed, declare `DoubleType()` and prefer a Pandas UDF.
# MAGIC 3. A Unity Catalog function: `CREATE FUNCTION main.<schema>.bmi_class(...)`. A SQL function if the logic is a SQL expression (it is inlined into the plan, E9), a UC Python function if it needs Python (E10). It is stored once in Unity Catalog, callable from SQL warehouses, notebooks and jobs, and governed with `GRANT EXECUTE`. A `@udf` lives only in one notebook's session: every job needs the code again, and there is no access control or single definition.
# MAGIC
# MAGIC ### P1
# MAGIC {{solution:P1}}
# MAGIC
# MAGIC ### P2 (fixes: `DoubleType()` for a float result; `otherwise` plus an `UNKNOWN` branch)
# MAGIC {{solution:P2}}
# MAGIC
# MAGIC ### P3
# MAGIC {{solution:P3}}
# MAGIC
# MAGIC ### P4
# MAGIC {{solution:P4}}
# MAGIC
# MAGIC ### Exam answers
# MAGIC
# MAGIC **Q1 → C.** E8: `IntegerType()` + a float result gave {{E8.lbs_wrong}} non-NULL values of {{E8.weights}}, with no error. A wrong: Spark does not round. B wrong: no error was raised. D wrong: cutting off the decimals happened only with `useArrow=True` ({{E8.arrow_truncated}} rows), not with the default settings of this run (`arrow.enabled` = `{{E8.arrow_conf}}`).
# MAGIC
# MAGIC **Q2 → C.** A UC function has one definition in Unity Catalog, is callable from SQL and PySpark (E9: `F.expr`, `selectExpr`, `F.call_function`), and is governed by privileges. A and B wrong: notebook / session scope, no governance (*UDFs in Unity Catalog*: PySpark UDFs are "scoped to a notebook or SparkSession"). D wrong: a Pandas UDF is session-scoped too and cannot be called from a SQL warehouse.
# MAGIC
# MAGIC **Q3 → C.** E6: `df.cache()` → `{{E6.df_cache}}`; the serverless pattern is a table ({{E6.er_rows}}-row `er_encounters`). A and B wrong: `CACHE TABLE` and `cacheTable` failed with `{{E6.sql_cache}}`. D wrong: `persist()` failed like `cache()` (`{{E6.df_persist}}`).
# MAGIC
# MAGIC **Q4 → A.** "Built-in functions and SQL UDFs are the most efficient options" (*What are UDFs?*); the built-in plan was fully on Photon (E7: {{E7.builtin_photon}}). B wrong: faster than a Python UDF, but still Python with Arrow serialization. C wrong: row-at-a-time, outside Photon. D wrong: also Python code, run in an isolated environment.
# MAGIC
# MAGIC **Q5 → B.** Spark Connect defers analysis (*Compare Spark Connect to Spark Classic*; E2: `{{E2.wrong_columns}}` at `df.columns`, `{{E2.wrong_count}}` at `count()`). A wrong: that is Spark Classic behaviour. C wrong: a missing column is an analysis error, not NULL. D wrong: any action or schema access raises it.
# MAGIC
# MAGIC **Q6 → A.** A broadcast join sends the small table to every task, so the fact table is not shuffled (E3: `{{E3.op_hint}}` with {{E3.hint_shuffles}} `hashpartitioning` exchanges, vs {{E3.nohint_shuffles}} without the hint). For a 60-row table Databricks would probably broadcast on its own (E3: `{{E3.op_small}}` without a hint), but the hint makes the intent explicit. B wrong: `repartition` adds a shuffle. C wrong: caching is not supported on serverless and does not choose the join strategy. D wrong: a merge hint asks for a sort-merge join, which shuffles both sides.
# MAGIC
# MAGIC **Q7 → A.** *pandas user-defined functions*: Series to Series uses `pd.Series` type hints in and out. B wrong: `@F.udf` is a row-at-a-time UDF and receives single values. C wrong: `pd.Series -> scalar` is the aggregate form, and the input is not a DataFrame here. D wrong: no Series type hints.
# MAGIC
# MAGIC **Q8 → C.** No branch matches and there is no `otherwise`, so the value is NULL (E5: {{E5.null_band}} NULL bands). A wrong: Spark does not invent a default. B wrong: no error. D wrong: `when` branches are not carried over.

# COMMAND ----------

# DBTITLE 1,12 — Cleanup
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")        # drops all tables, the volume and the UC functions
spark.catalog.dropTempView("big_v")                      # session temp views from E3
spark.catalog.dropTempView("mid_v")
print(f"Dropped {FQ} and the temp views.")

# COMMAND ----------

# DBTITLE 1,13 — Final Summary
# MAGIC %md
# MAGIC ## 13 Final Summary (numbers from the real run)
# MAGIC
# MAGIC | Example | Key result |
# MAGIC | --- | --- |
# MAGIC | Setup | {{setup.patients}} patients ({{setup.null_weight}} without weight), {{setup.providers}} providers, {{setup.encounters}} encounters, {{setup.claims}} claim rows ({{setup.distinct_claims}} claims), {{setup.labs}} labs |
# MAGIC | E1 L03 in PySpark | dedup {{E1.latest_py}} rows · `LAG` {{E1.gaps_rows}} rows · report {{E1.report_groups}} groups · differences to SQL: {{E1.latest_diff}} / {{E1.gaps_diff}} / {{E1.report_diff}} |
# MAGIC | E2 lazy | define {{E2.defined}} · action `{{E2.wrong_count}}` · bad `CAST`: `count()` {{E2.cast_count}}, `collect()` `{{E2.cast_collect}}` |
# MAGIC | E3 explain | no hint `{{E3.op_nohint}}` · hint `{{E3.op_hint}}` · small table `{{E3.op_small}}` |
# MAGIC | E4 schemas | DDL = `StructType`: {{E4.equal}} · `nullable=False` + None → `{{E4.strict_error}}` |
# MAGIC | E5 transform | {{E5.rows}} rows, {{E5.diff}} differences to SQL · no `otherwise` → {{E5.null_band}} NULLs |
# MAGIC | E6 caching | `df.cache()` `{{E6.df_cache}}` · `CACHE TABLE` `{{E6.sql_cache}}` |
# MAGIC | E7 UDF speed | same {{E7.bmi30_rows}} rows; this run: built-in {{nd.E7.builtin_s}} s, Pandas {{nd.E7.pandas_s}} s, Python {{nd.E7.python_s}} s · NULL input → `{{E7.null_py_error}}` |
# MAGIC | E8 returnType | wrong type: {{E8.lbs_wrong}}/{{E8.weights}} non-NULL · Arrow: {{E8.arrow_truncated}} truncated · Connect variable: {{E8.seen}} |
# MAGIC | E9 SQL UDF | {{E9.child}} child / {{E9.adult}} adult / {{E9.senior}} senior · inlined {{E9.inlined}} · `groupBy("age_group")` after it → `{{E9.groupby_str}}`, `groupBy(F.col(...))` → {{E9.groupby_col}} |
# MAGIC | E10 UC Python UDF | `Language: {{E10.language}}` · {{E10.with_bmi}} BMIs, {{E10.bmi30}} ≥ 30 · {{E10.differ}} differences to the Pandas UDF |
# MAGIC | Practice | P1 {{P1.with_weight}} patients with weight · P2 {{P2.unknown}} UNKNOWN · P3 {{P3.rows}} claims ({{P3.high}} HIGH) · P4 {{P4.obese}} OBESE |
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **`df.cache()` on a newer serverless environment version** — the E6 error message asks for a newer client version (this run: PySpark {{E6.client_pyspark}}); this lesson runs on the default job environment and does not change it.
# MAGIC * **Spark Classic behaviour** (eager analysis, `cache()` on classic compute) — this workspace runs serverless only; described from *Compare Spark Connect to Spark Classic*.
# MAGIC * **Scala / Java UDFs, UDAFs, UDTFs, batch UC Python UDFs, Iterator and Series-to-scalar Pandas UDFs, `mapInPandas` / `applyInPandas`** — from *What are UDFs?* and *pandas user-defined functions*; no example runs them.
# MAGIC * **"Up to 100x" faster Pandas UDFs** — a docs statement; E7 measured only this data and this run.
# MAGIC * **`GRANT EXECUTE ON FUNCTION`** — needs a second principal (L10).
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. `df.cache()` did not fail with `NOT_SUPPORTED_WITH_SERVERLESS` like `CACHE TABLE`, but with `{{E6.df_cache}}` (a client-version message) (E6).
# MAGIC 2. A wrong `returnType` gave NULLs not only for float → int but also for int → `"double"` ({{E8.age_wrong}} non-NULL values); with `useArrow=True` the float → int case truncated instead (E8).
# MAGIC 3. With a bad `CAST`, `count()` and even `filter("x IS NULL").count()` succeeded; only `collect()` failed (E2).
# MAGIC 4. `SHOW USER FUNCTIONS IN main.dbacademy_l04` failed with `{{E9.show_in}}` because the default catalog is `bunyod` (E9).
# MAGIC 5. After a UC SQL function, `groupBy("age_group")` by name failed with `{{E9.groupby_str}}` (client PySpark {{E6.client_pyspark}}), while `groupBy(F.col("age_group"))` worked ({{E9.groupby_col}}) (E9).
# MAGIC 6. The UC Python function ran as a Photon operator (`{{E10.plan_ops}}`), not as a `BatchEvalPython` node like the session Python UDF (E10 vs E7).
