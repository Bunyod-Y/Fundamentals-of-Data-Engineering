# Databricks notebook source
# LESSON_TITLE: Spark SQL Transformations

# COMMAND ----------

# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L03 — Spark SQL Transformations: joins, aggregations, window functions
# MAGIC
# MAGIC **Exam objectives**
# MAGIC
# MAGIC * Professional (Oct 9, 2026), Section 3 *Data Transformation, Cleansing, and Quality* (12% of the exam): write efficient Spark SQL to clean, join, aggregate and deduplicate data.
# MAGIC * The same SQL is the base of every later lesson: Lakeflow Declarative Pipelines (formerly Delta Live Tables), MERGE with deduplicated sources, and data modeling.
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** all join types (`INNER`, `LEFT`, `FULL`, `LEFT SEMI`, `LEFT ANTI`, `CROSS`) and the null-safe operator `<=>`.
# MAGIC 2. **Explain** the logical order of a query (`FROM` → `WHERE` → `GROUP BY` → `HAVING` → window → `QUALIFY` → `SELECT` → `ORDER BY` → `LIMIT`) and why `NOT IN` breaks when the subquery has a `NULL`.
# MAGIC 3. **Compare** `ROW_NUMBER`, `RANK` and `DENSE_RANK` on ties, and the default window frame with an explicit `ROWS` frame.
# MAGIC 4. **Implement** deduplication with `QUALIFY`, gaps between visits with `LAG`, subtotals with `ROLLUP`, and reshaping with `PIVOT` / `UNPIVOT` and higher-order functions.
# MAGIC 5. **Diagnose** wrong results caused by `NULL` keys, ties, the default frame and `AVG(CASE … ELSE 0 END)`.
# MAGIC
# MAGIC > **Evidence-only rule:** every number and error class in this notebook comes from a real run of this notebook (the build fills them in), or from an official Databricks doc page named in the text. Things that cannot be tested on this workspace are marked **NOT VERIFIED — concept only (reason)**.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up (intro)
# MAGIC %md
# MAGIC ## 2 Warm-up — recall L02 Delta Lake Deep Dive (no hints)
# MAGIC
# MAGIC Write each task from memory. Run the validation cell after each one. Solutions are in section 11.
# MAGIC
# MAGIC > The warm-up uses its own small tables. Run the next cell first: it creates the lesson schema and a helper.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up prep (run first)
spark.sql("CREATE SCHEMA IF NOT EXISTS main.dbacademy_l03")   # the warm-up tables live here; setup recreates it later

def error_class(e):
    """Return the Spark error class (condition) of an exception, or its Python type name."""
    for name in ("getCondition", "getErrorClass"):   # getCondition is the newer name of getErrorClass
        get = getattr(e, name, None)
        if get:
            try:
                value = get()
                if value:
                    return value
            except Exception:
                pass
    return type(e).__name__

# COMMAND ----------

# DBTITLE 1,W1 Task
# MAGIC %md
# MAGIC ### W1 — Append a DataFrame with a new column
# MAGIC Create the table `main.dbacademy_l03.wu1_encounters (encounter_id INT, encounter_type STRING)` with the rows `(1, 'ER')` and `(2, 'INPATIENT')`.
# MAGIC Then append a DataFrame with the columns `encounter_id, encounter_type, provider_id` and the rows `(3, 'OUTPATIENT', 7)` and `(4, 'ER', 9)`, so that the table **gets the new column** `provider_id` (type `INT`).

# COMMAND ----------

# DBTITLE 1,W1 Code
# >>> BLANK W1
# Your code here
# >>> SOLUTION W1
spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu1_encounters")
spark.sql("CREATE TABLE main.dbacademy_l03.wu1_encounters (encounter_id INT, encounter_type STRING)")
spark.sql("INSERT INTO main.dbacademy_l03.wu1_encounters VALUES (1, 'ER'), (2, 'INPATIENT')")
new_rows = spark.createDataFrame([(3, "OUTPATIENT", 7), (4, "ER", 9)], "encounter_id INT, encounter_type STRING, provider_id INT")
new_rows.write.mode("append").option("mergeSchema", "true").saveAsTable("main.dbacademy_l03.wu1_encounters")
# <<< END W1

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    t = spark.table("main.dbacademy_l03.wu1_encounters")
    cols, rows = t.columns, t.count()
    null_providers = t.filter("provider_id IS NULL").count() if "provider_id" in cols else None
    ok = cols == ["encounter_id", "encounter_type", "provider_id"] and rows == 4 and null_providers == 2
    WARMUP_PASSED["W1"] = ok
    print("✅ Passed" if ok else
          f"Hint: want columns [encounter_id, encounter_type, provider_id], 4 rows, 2 NULL provider_id; "
          f"got {cols}, {rows} rows, {null_providers} NULL. An append with a new column needs one writer option.")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create main.dbacademy_l03.wu1_encounters first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — CHECK constraint on a table that already has bad rows
# MAGIC Create `main.dbacademy_l03.wu2_labs (lab_id INT, result_value DOUBLE)` with the rows `(1, 5.4)`, `(2, -1.0)`, `(3, 7.2)`.
# MAGIC Add a CHECK constraint named `non_negative` so that `result_value >= 0`. The first try fails because of the existing bad row: catch that error and print its class. Then remove the bad row and add the constraint.

# COMMAND ----------

# DBTITLE 1,W2 Code
# >>> BLANK W2
# Your code here
# >>> SOLUTION W2
spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu2_labs")
spark.sql("CREATE TABLE main.dbacademy_l03.wu2_labs (lab_id INT, result_value DOUBLE)")
spark.sql("INSERT INTO main.dbacademy_l03.wu2_labs VALUES (1, 5.4), (2, -1.0), (3, 7.2)")
try:
    spark.sql("ALTER TABLE main.dbacademy_l03.wu2_labs ADD CONSTRAINT non_negative CHECK (result_value >= 0)")
except Exception as e:
    print("first try failed:", error_class(e))
spark.sql("DELETE FROM main.dbacademy_l03.wu2_labs WHERE result_value < 0")
spark.sql("ALTER TABLE main.dbacademy_l03.wu2_labs ADD CONSTRAINT non_negative CHECK (result_value >= 0)")
# <<< END W2

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    rows = spark.table("main.dbacademy_l03.wu2_labs").count()
    props = {r["key"]: r["value"] for r in spark.sql("SHOW TBLPROPERTIES main.dbacademy_l03.wu2_labs").collect()}
    has_check = "delta.constraints.non_negative" in props
    ok = rows == 2 and has_check
    WARMUP_PASSED["W2"] = ok
    print("✅ Passed" if ok else
          f"Hint: want 2 rows and a constraint named non_negative; got {rows} rows, constraint present = {has_check}.")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: create main.dbacademy_l03.wu2_labs first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — SHALLOW CLONE for a dev copy
# MAGIC Create `main.dbacademy_l03.wu3_src` with the ids 1 to 5 (one column `id`). Make a **shallow clone** called `main.dbacademy_l03.wu3_dev`.
# MAGIC Delete the id 5 from the **clone** only. The source must still have 5 rows.

# COMMAND ----------

# DBTITLE 1,W3 Code
# >>> BLANK W3
# Your code here
# >>> SOLUTION W3
spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu3_dev")
spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu3_src")
spark.sql("CREATE TABLE main.dbacademy_l03.wu3_src AS SELECT CAST(id AS INT) AS id FROM range(1, 6)")
spark.sql("CREATE TABLE main.dbacademy_l03.wu3_dev SHALLOW CLONE main.dbacademy_l03.wu3_src")
spark.sql("DELETE FROM main.dbacademy_l03.wu3_dev WHERE id = 5")
# <<< END W3

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    src = spark.table("main.dbacademy_l03.wu3_src").count()
    dev = spark.table("main.dbacademy_l03.wu3_dev").count()
    first = spark.sql("DESCRIBE HISTORY main.dbacademy_l03.wu3_dev").orderBy("version").first()
    is_shallow = first["operation"] == "CLONE" and first["operationParameters"].get("isShallow") == "true"
    ok = src == 5 and dev == 4 and is_shallow
    WARMUP_PASSED["W3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want source 5 rows, clone 4 rows, first clone operation a SHALLOW CLONE; got {src}, {dev}, "
          f"{first['operation']} {first['operationParameters']}.")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create wu3_src and wu3_dev first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent (drop the schema, recreate it), fake healthcare data only (no PHI).
import random                                                   # isolated generator below, never random.seed()
from datetime import date, datetime, timedelta
from decimal import Decimal
from pyspark.sql import Window, functions as F                  # used by the examples and the validation cells

CATALOG, SCHEMA = "main", "dbacademy_l03"
FQ = f"{CATALOG}.{SCHEMA}"                                      # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"                       # volume (used by the test build only)

spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")                # removes the warm-up tables too
spark.sql(f"CREATE SCHEMA {FQ}")
spark.sql(f"CREATE VOLUME {FQ}.data")

rng = random.Random(42)                                         # one isolated, seeded generator
START = date(2025, 1, 1)

# patients: 200 rows (patients 191-200 never get an encounter, patients 1-10 never get a lab)
patients = [(pid, rng.choice(["F", "M"]), rng.randint(1940, 2010), rng.choice(["CA", "NY", "TX", "FL", "WA"]))
            for pid in range(1, 201)]

# providers: 20 rows, 5 specialties
SPECIALTIES = ["Cardiology", "Endocrinology", "Family Medicine", "Emergency Medicine", "Oncology"]
providers = [(pid, f"Dr Provider {pid}", SPECIALTIES[pid % 5]) for pid in range(1, 21)]

# diagnosis_codes: 7 real ICD-10 style codes + one row with a NULL code ("not coded")
diagnosis_codes = [("E11.9", "Type 2 diabetes without complications", "Endocrine"),
                   ("I10", "Essential hypertension", "Circulatory"),
                   ("I50.9", "Heart failure, unspecified", "Circulatory"),
                   ("J18.9", "Pneumonia, unspecified", "Respiratory"),
                   ("N18.3", "Chronic kidney disease, stage 3", "Genitourinary"),
                   ("R07.9", "Chest pain, unspecified", "Symptoms"),
                   ("Z00.00", "General adult exam", "Factors"),
                   (None, "Not coded", "Unknown")]
CODES = [c[0] for c in diagnosis_codes if c[0]]

# encounters: 2000 rows; NULL patient_id every 100th, NULL provider every 125th, NULL code every 40th,
# and every 50th encounter is a second visit of the previous patient on the same day (a tie on purpose)
TYPES = ["OUTPATIENT"] * 6 + ["ER"] * 2 + ["INPATIENT"] * 2
encounters = []
for eid in range(1, 2001):
    pid = None if eid % 100 == 0 else rng.randint(1, 190)
    day = START + timedelta(days=rng.randint(0, 364))
    if eid % 50 == 1 and eid > 1 and encounters[-1][1] is not None:
        pid, day = encounters[-1][1], encounters[-1][4]        # same patient, same date as the previous row
    etype = rng.choice(TYPES)
    discharge = day + timedelta(days=rng.randint(1, 7)) if etype == "INPATIENT" else None
    prov = None if eid % 125 == 0 else rng.randint(1, 20)
    code = None if eid % 40 == 0 else rng.choice(CODES)
    charge = Decimal(f"{rng.uniform(80, 3000):.2f}")
    encounters.append((eid, pid, prov, etype, day, discharge, code, charge))

# claims: 1350 claims + 150 later versions of existing claims (same claim_id, later updated_at) = 1500 rows
PAYERS = ["Medicare", "Medicaid", "Aetna", "BlueCross", None]   # None = self-pay
claims = []
for cid in range(1, 1351):
    enc = encounters[rng.randint(0, 1999)]
    updated = datetime.combine(enc[4], datetime.min.time()) + timedelta(days=rng.randint(1, 30), hours=8)
    claims.append((cid, enc[0], enc[1], rng.choice(PAYERS), rng.choice(["SUBMITTED", "PAID", "DENIED", "PENDING"]),
                   Decimal(f"{rng.uniform(50, 5000):.2f}"), enc[4], updated))
for cid in rng.sample(range(1, 1351), 150):
    old = claims[cid - 1]
    claims.append(old[:4] + (rng.choice(["PAID", "DENIED"]), old[5], old[6], old[7] + timedelta(days=rng.randint(1, 20))))

# lab_results: 3000 rows, patients 11-200 only, NULL patient_id every 150th, TSH only for patients with id % 5 = 0,
# every result_ts is unique; A1C has one decimal, so equal values (ties) are common
minutes = rng.sample(range(365 * 24 * 60), 3000)
labs = []
for lid in range(1, 3001):
    pid = None if lid % 150 == 0 else rng.randint(11, 200)
    test = rng.choice(["A1C", "LDL", "GLU", "TSH"] if pid is not None and pid % 5 == 0 else ["A1C", "LDL", "GLU"])
    value = {"A1C": round(rng.uniform(4.5, 10.0), 1), "LDL": float(rng.randint(60, 200)),
             "GLU": float(rng.randint(70, 250)), "TSH": round(rng.uniform(0.4, 5.0), 1)}[test]
    labs.append((lid, pid, test, value, datetime(2025, 1, 1) + timedelta(minutes=minutes[lid - 1])))

def save(rows, ddl, name):
    """Write a list of tuples as a managed Delta table and return its row count."""
    spark.createDataFrame(rows, ddl).write.saveAsTable(f"{FQ}.{name}")
    return spark.table(f"{FQ}.{name}").count()

n_patients = save(patients, "patient_id INT, gender STRING, birth_year INT, state STRING", "patients")
n_providers = save(providers, "provider_id INT, provider_name STRING, specialty STRING", "providers")
n_codes = save(diagnosis_codes, "code STRING, description STRING, category STRING", "diagnosis_codes")
n_enc = save(encounters, "encounter_id INT, patient_id INT, provider_id INT, encounter_type STRING, "
             "encounter_date DATE, discharge_date DATE, diagnosis_code STRING, charge DECIMAL(10,2)", "encounters")
n_claims = save(claims, "claim_id INT, encounter_id INT, patient_id INT, payer STRING, status STRING, "
                "amount DECIMAL(10,2), service_date DATE, updated_at TIMESTAMP", "claims")
n_labs = save(labs, "lab_id INT, patient_id INT, test_code STRING, result_value DOUBLE, result_ts TIMESTAMP", "lab_results")

counts = spark.sql(f"""
    SELECT (SELECT count(DISTINCT claim_id) FROM {FQ}.claims)                          AS distinct_claims,
           (SELECT count(*) FROM {FQ}.encounters WHERE patient_id IS NULL)              AS enc_null_patient,
           (SELECT count(*) FROM {FQ}.encounters WHERE provider_id IS NULL)             AS enc_null_provider,
           (SELECT count(*) FROM {FQ}.encounters WHERE diagnosis_code IS NULL)          AS enc_null_code,
           (SELECT count(*) FROM {FQ}.lab_results WHERE patient_id IS NULL)             AS lab_null_patient,
           (SELECT count(*) FROM {FQ}.encounters WHERE encounter_type = 'INPATIENT')    AS inpatient,
           (SELECT count(*) FROM {FQ}.claims WHERE payer IS NULL)                       AS claims_self_pay
""").first()
display(spark.sql(f"""
    SELECT 'patients' AS table_name, count(*) AS row_count FROM {FQ}.patients UNION ALL
    SELECT 'providers', count(*) FROM {FQ}.providers UNION ALL
    SELECT 'diagnosis_codes', count(*) FROM {FQ}.diagnosis_codes UNION ALL
    SELECT 'encounters', count(*) FROM {FQ}.encounters UNION ALL
    SELECT 'claims', count(*) FROM {FQ}.claims UNION ALL
    SELECT 'lab_results', count(*) FROM {FQ}.lab_results"""))
print(counts.asDict())
METRICS["setup.patients"], METRICS["setup.providers"], METRICS["setup.codes"] = n_patients, n_providers, n_codes
METRICS["setup.encounters"], METRICS["setup.claims"], METRICS["setup.labs"] = n_enc, n_claims, n_labs
METRICS["setup.distinct_claims"] = counts["distinct_claims"]
METRICS["setup.enc_null_patient"] = counts["enc_null_patient"]
METRICS["setup.enc_null_provider"] = counts["enc_null_provider"]
METRICS["setup.enc_null_code"] = counts["enc_null_code"]
METRICS["setup.lab_null_patient"] = counts["lab_null_patient"]
METRICS["setup.inpatient"] = counts["inpatient"]
METRICS["setup.claims_self_pay"] = counts["claims_self_pay"]
assert (n_patients, n_providers, n_codes, n_enc, n_claims, n_labs) == (200, 20, 8, 2000, 1500, 3000)
assert counts["distinct_claims"] == 1350 and counts["enc_null_patient"] == 20 and counts["lab_null_patient"] == 20
assert counts["enc_null_provider"] == 16 and counts["enc_null_code"] == 50

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):**
# MAGIC
# MAGIC | Table | Rows | Traps built in on purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `patients` | {{setup.patients}} | patients 191–200 have no encounter, patients 1–10 have no lab result |
# MAGIC | `providers` | {{setup.providers}} | 5 specialties |
# MAGIC | `diagnosis_codes` | {{setup.codes}} | one reference row has `code = NULL` ("Not coded") |
# MAGIC | `encounters` | {{setup.encounters}} ({{setup.inpatient}} `INPATIENT`) | `patient_id` NULL {{setup.enc_null_patient}}×, `provider_id` NULL {{setup.enc_null_provider}}×, `diagnosis_code` NULL {{setup.enc_null_code}}×; every 50th encounter is a second visit of the same patient on the same day (a tie) |
# MAGIC | `claims` | {{setup.claims}} rows, {{setup.distinct_claims}} distinct `claim_id` | later versions of the same claim with a newer `updated_at`; `payer` NULL (self-pay) in {{setup.claims_self_pay}} rows |
# MAGIC | `lab_results` | {{setup.labs}} | `patient_id` NULL {{setup.lab_null_patient}}×; TSH only for patients with `patient_id % 5 = 0`; A1C has one decimal, so equal values are common |
# MAGIC
# MAGIC The data comes from one seeded generator (`random.Random(42)`), so every run creates the same rows.

# COMMAND ----------

# DBTITLE 1,4.1 Concept — Logical order of a query
# MAGIC %md
# MAGIC ## 4 Concepts
# MAGIC
# MAGIC ### 4.1 The logical order of a query
# MAGIC
# MAGIC **What.** You write `SELECT … FROM … WHERE …`, but the engine thinks about the clauses in a different order. This is the standard SQL logical order. The Databricks *SELECT* reference lists the clauses but does not print this order; E4 shows its effects with real errors.
# MAGIC
# MAGIC | Step | Clause | What it can see |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | `FROM` + `JOIN` | the tables |
# MAGIC | 2 | `WHERE` | single rows ("Filters the result of the `FROM` clause", *SELECT* reference) — **no** aggregates, **no** window functions |
# MAGIC | 3 | `GROUP BY` | groups |
# MAGIC | 4 | `HAVING` | groups ("filter rows after the grouping is performed") — aggregates are allowed |
# MAGIC | 5 | window functions (`OVER`) | the rows that survived steps 1–4 |
# MAGIC | 6 | `QUALIFY` | the result of window functions ("Filters the results of window functions", *QUALIFY clause*) |
# MAGIC | 7 | `SELECT` (+ `DISTINCT`) | the final columns |
# MAGIC | 8 | `ORDER BY`, `LIMIT` | the final result |
# MAGIC
# MAGIC One practical exception: Databricks lets `QUALIFY` use an alias from the `SELECT` list (the *QUALIFY clause* example ends with `QUALIFY rank = 1`).
# MAGIC
# MAGIC **Why it matters.** It explains three classic errors: a window function in `WHERE`, an aggregate in `WHERE`, and a window function that "does not see" rows that `WHERE` removed.
# MAGIC
# MAGIC **CTE (common table expression).** `WITH name AS (subquery)` gives a subquery a name: "one or more named queries which can be reused multiple times within the main query block" (*Query* reference). Use CTEs instead of deep nesting; dbt models are full of them.
# MAGIC
# MAGIC ```sql
# MAGIC WITH er AS (SELECT * FROM main.dbacademy_l03.encounters WHERE encounter_type = 'ER')
# MAGIC SELECT patient_id, count(*) AS er_visits FROM er GROUP BY patient_id HAVING count(*) >= 3;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.2 Concept — Joins
# MAGIC %md
# MAGIC ### 4.2 Joins
# MAGIC
# MAGIC Source: *JOIN* (SQL reference). `join_type: { [ INNER ] | LEFT [ OUTER ] | [ LEFT ] SEMI | RIGHT [ OUTER ] | FULL [ OUTER ] | [ LEFT ] ANTI | CROSS }`
# MAGIC
# MAGIC | Join | Returns | Typical healthcare question |
# MAGIC | --- | --- | --- |
# MAGIC | `INNER` (default) | "rows that have matching values in both table references" | encounters with their provider |
# MAGIC | `LEFT [OUTER]` | all left rows, NULLs where the right has no match | all patients, with their claims if any |
# MAGIC | `RIGHT`, `FULL [OUTER]` | all right rows / all rows of both sides | reconcile two claim feeds |
# MAGIC | `LEFT SEMI` | left rows **that have** a match; only left columns, **no duplicates** | patients who had an ER visit |
# MAGIC | `LEFT ANTI` | left rows **with no** match | patients who never had a visit |
# MAGIC | `CROSS` | "the Cartesian product of two relations" | every provider × every month (a calendar grid) |
# MAGIC
# MAGIC **SEMI / ANTI vs alternatives**
# MAGIC
# MAGIC | Need | Best | Alternative | Risk of the alternative |
# MAGIC | --- | --- | --- | --- |
# MAGIC | "has at least one match" | `LEFT SEMI JOIN` or `EXISTS` | `INNER JOIN` + `DISTINCT` | duplicates if you forget `DISTINCT` (E2) |
# MAGIC | "has no match" | `LEFT ANTI JOIN` or `NOT EXISTS` | `NOT IN (subquery)` | returns **nothing** if the subquery has a NULL (E1) |
# MAGIC
# MAGIC ```sql
# MAGIC SELECT p.* FROM main.dbacademy_l03.patients p
# MAGIC LEFT ANTI JOIN main.dbacademy_l03.encounters e ON p.patient_id = e.patient_id;
# MAGIC ```
# MAGIC ```python
# MAGIC patients.join(encounters, "patient_id", "left_anti")        # how: inner, left, right, full, left_semi, left_anti, cross
# MAGIC patients.crossJoin(months)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.3 Concept — NULL semantics
# MAGIC %md
# MAGIC ### 4.3 NULL semantics
# MAGIC
# MAGIC Source: *NULL semantics* (SQL reference).
# MAGIC
# MAGIC | Rule | Quote from the page |
# MAGIC | --- | --- |
# MAGIC | `=` with a NULL is UNKNOWN, so a join on `=` never matches NULL keys | — (E3 shows it) |
# MAGIC | `<=>` is the null-safe equal | it "returns `False` when one of the operand is `NULL`" and "`True` when both the operands are `NULL`" |
# MAGIC | `NOT IN` with a NULL in the list | "`NOT IN` always returns `UNKNOWN` when the list contains `NULL` and does not contain the input value" |
# MAGIC | aggregates skip NULLs | "`NULL` values are ignored from processing by all the aggregate functions", except `count(*)` |
# MAGIC | `GROUP BY`, `DISTINCT` | NULLs are "grouped together into the same bucket" |
# MAGIC | set operators | "`NULL` values are compared in a null-safe manner for equality in the context of set operations" |
# MAGIC
# MAGIC **NULL functions:** `coalesce(a, b, …)` (first non-NULL), `nvl(a, b)` / `ifnull(a, b)`, `nullif(a, b)` (NULL if equal), `a IS [NOT] NULL`, `a IS [NOT] DISTINCT FROM b` (same as `<=>`).
# MAGIC
# MAGIC ```python
# MAGIC encounters.join(codes, encounters.diagnosis_code.eqNullSafe(codes.code))   # PySpark version of <=>
# MAGIC F.coalesce("payer", F.lit("SELF-PAY"))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.4 Concept — Aggregation
# MAGIC %md
# MAGIC ### 4.4 Aggregation: GROUP BY, HAVING, FILTER, COUNT variants, subtotals
# MAGIC
# MAGIC Source: *GROUP BY clause* (SQL reference).
# MAGIC
# MAGIC | Tool | Meaning |
# MAGIC | --- | --- |
# MAGIC | `GROUP BY ALL` | "A shorthand notation to add all `SELECT`-list expressions not containing aggregate functions as `group_expression`s" |
# MAGIC | `HAVING` | filter groups (after `GROUP BY`) |
# MAGIC | `agg(...) FILTER (WHERE cond)` | "only the matching rows are passed to that function" |
# MAGIC | `count(*)` / `count(col)` / `count(DISTINCT col)` | all rows / non-NULL values / distinct non-NULL values |
# MAGIC | `count_if(cond)` | rows where `cond` is true |
# MAGIC | `approx_count_distinct(col [, relativeSD])` | fast estimate, "accurate within a default value of 5%" (*approx_count_distinct* page) |
# MAGIC | `GROUPING SETS ((a, b), (a), ())` | several groupings in one query, like a `UNION ALL` of the aggregations |
# MAGIC | `ROLLUP(a, b)` | = `GROUPING SETS((a, b), (a), ())`: "N elements … result in N+1 `GROUPING SETS`" |
# MAGIC | `CUBE(a, b)` | = `GROUPING SETS((a, b), (a), (b), ())`: "2^N `GROUPING SETS`" |
# MAGIC | `grouping(col)` | 1 if `col` is aggregated away in this row (a subtotal row), else 0 |
# MAGIC
# MAGIC **FILTER vs CASE.** `count(*) FILTER (WHERE status = 'DENIED')` = `count_if(status = 'DENIED')` = `sum(CASE WHEN status = 'DENIED' THEN 1 ELSE 0 END)`. For averages, `ELSE 0` is a bug: the zeros are counted (E12).
# MAGIC
# MAGIC ```sql
# MAGIC SELECT payer, status, count(*) AS claims
# MAGIC FROM main.dbacademy_l03.claims_clean
# MAGIC GROUP BY ROLLUP (payer, status);
# MAGIC ```
# MAGIC ```python
# MAGIC claims.groupBy("payer").agg(F.count("*"), F.count_if(F.col("status") == "DENIED"), F.approx_count_distinct("patient_id"))
# MAGIC claims.rollup("payer", "status").count()        # also: claims.cube("payer", "status")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.5 Concept — Window functions and QUALIFY
# MAGIC %md
# MAGIC ### 4.5 Window functions and QUALIFY
# MAGIC
# MAGIC **What.** A window function computes a value for **each row** from a group of related rows (the *window*), without collapsing the rows like `GROUP BY` does. Source: *Window functions* (SQL reference).
# MAGIC
# MAGIC `function(...) OVER (PARTITION BY … ORDER BY … [frame])`
# MAGIC
# MAGIC | Family | Functions | Notes |
# MAGIC | --- | --- | --- |
# MAGIC | Ranking | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK`, `CUME_DIST` | need `ORDER BY`, no frame ("must include an ORDER BY clause, but not a window_frame clause") |
# MAGIC | Analytic | `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` | `lag(expr [, offset [, default]]) [IGNORE NULLS]`; offset default 1, default value NULL (*lag function*) |
# MAGIC | Aggregate | `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` … `OVER (...)` | running totals, moving averages |
# MAGIC
# MAGIC **Frames.** Source: *Window frame clause*. `{ RANGE | ROWS } BETWEEN frame_start AND frame_end`.
# MAGIC * `ROWS` counts physical rows before/after the current row.
# MAGIC * `RANGE` works on the `ORDER BY` value: "Unlike ROWS mode, RANGE groups rows with equal `ORDER BY` values together."
# MAGIC * The docs page does not state the frame you get when you write **no** frame. E6 measures it on this workspace.
# MAGIC
# MAGIC **QUALIFY** filters on window results, like `HAVING` filters on aggregates. Syntax: `QUALIFY boolean_expression` (*QUALIFY clause*).
# MAGIC
# MAGIC | Need | Use | Not |
# MAGIC | --- | --- | --- |
# MAGIC | one row per key (dedup) | `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC) = 1` | `RANK()` (keeps all ties), `GROUP BY` (loses other columns) |
# MAGIC | "top 3 values, with ties" | `DENSE_RANK() <= 3` | `ROW_NUMBER() <= 3` |
# MAGIC | running total | `SUM(x) OVER (… ORDER BY d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | no frame + ties in `d` |
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM main.dbacademy_l03.claims
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1;
# MAGIC ```
# MAGIC ```python
# MAGIC from pyspark.sql import Window, functions as F
# MAGIC w = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc())
# MAGIC claims.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
# MAGIC running = Window.partitionBy("patient_id").orderBy("encounter_date", "encounter_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.6 Concept — Set operators, PIVOT and UNPIVOT
# MAGIC %md
# MAGIC ### 4.6 Set operators, PIVOT and UNPIVOT
# MAGIC
# MAGIC **Set operators** (*Set operators* reference): `UNION`, `INTERSECT`, `EXCEPT` (synonym `MINUS`). **`DISTINCT` is the default** for all three; add `ALL` to keep duplicates. Columns are matched **by position**: "Both subqueries must have the same number of columns and share a least common type for each respective column". `INTERSECT` binds more tightly than `UNION` and `EXCEPT`.
# MAGIC
# MAGIC | Need | Use |
# MAGIC | --- | --- |
# MAGIC | stack two feeds, keep every row | `UNION ALL` (no shuffle for dedup, so cheaper) |
# MAGIC | list of distinct ids from two tables | `UNION` |
# MAGIC | in both / in the first only | `INTERSECT` / `EXCEPT` |
# MAGIC
# MAGIC **PIVOT** (rows → columns, *PIVOT clause*): `PIVOT ( aggregate_expression [AS alias] FOR column IN (value [AS alias], …) )`. Every column of the input that is not in the aggregate or the `FOR` column becomes a **grouping column**, so select only the columns you need first. The `IN` values must be literals.
# MAGIC
# MAGIC **UNPIVOT** (columns → rows, *UNPIVOT clause*): `UNPIVOT [ INCLUDE NULLS | EXCLUDE NULLS ] (value_col FOR name_col IN (col [AS alias], …))`. "The default is `EXCLUDE NULLS`."
# MAGIC
# MAGIC ```python
# MAGIC labs.groupBy("patient_id").pivot("test_code", ["A1C", "LDL"]).agg(F.avg("result_value"))     # PIVOT
# MAGIC wide.unpivot("patient_id", ["a1c", "ldl"], "test_code", "avg_value")                            # UNPIVOT (drops nothing)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.7 Concept — Higher-order functions, dates, CASE
# MAGIC %md
# MAGIC ### 4.7 Arrays and higher-order functions, dates, CASE
# MAGIC
# MAGIC **Arrays.** `collect_list(x)` (keeps duplicates) and `collect_set(x)` (distinct) build an array per group. The order inside the array is not guaranteed: wrap it in `array_sort(...)` when order matters.
# MAGIC
# MAGIC **Higher-order functions** take a lambda `x -> expression`:
# MAGIC
# MAGIC | Function | Result |
# MAGIC | --- | --- |
# MAGIC | `transform(arr, x -> f(x))` | a new array with `f` applied to each element |
# MAGIC | `filter(arr, x -> cond)` | only the elements where `cond` is true |
# MAGIC | `exists(arr, x -> cond)` / `forall(...)` | true if any / all elements match |
# MAGIC | `aggregate(arr, start, (acc, x) -> merge [, acc -> finish])` | fold the array into one value; "a synonym for reduce function" (*aggregate function* page) |
# MAGIC
# MAGIC **Why.** You work on the array inside one row: no `explode` and no second `GROUP BY`.
# MAGIC
# MAGIC **Dates.** `datediff(endDate, startDate)` — **end first** (*datediff function*: `datediff('2009-07-31', '2009-07-30')` is 1). Others: `date_add(d, n)`, `add_months(d, n)`, `trunc(d, 'MONTH')` (a `DATE`), `date_trunc('MONTH', ts)` (a `TIMESTAMP`), `year(d)`, `current_date()`.
# MAGIC
# MAGIC **CASE.** `CASE WHEN cond THEN a [WHEN …] [ELSE b] END`. Without `ELSE`, no match gives NULL — which is what you want inside `avg(...)` and `count(...)`.
# MAGIC
# MAGIC ```python
# MAGIC F.transform("a1c_values", lambda v: F.round(v, 0))         # PySpark higher-order functions take Python lambdas
# MAGIC F.filter("a1c_values", lambda v: v >= 6.5)
# MAGIC F.aggregate("a1c_values", F.lit(0.0), lambda acc, v: acc + v)
# MAGIC F.when(F.col("status") == "PAID", F.col("amount"))          # CASE WHEN … THEN … END
# MAGIC F.datediff("encounter_date", "prev_date")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,5 — Examples (intro)
# MAGIC %md
# MAGIC ## 5 Examples
# MAGIC
# MAGIC Each example: **Predict** (answer before you run) → **code** (every line commented) → **Explained** (real numbers from the run).
# MAGIC
# MAGIC E4 creates the table `claims_clean` that E8 and E12 use, and E9 creates `lab_wide`. Run the examples in order.

# COMMAND ----------

# DBTITLE 1,E1 Predict
# MAGIC %md
# MAGIC ### E1 — Patients with no encounter: `NOT IN` vs `NOT EXISTS` vs `LEFT ANTI JOIN`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. In the setup, patients 191–200 never got an encounter, and 20 encounters have `patient_id = NULL`. How many rows does `WHERE patient_id NOT IN (SELECT patient_id FROM encounters)` return?
# MAGIC 2. How many rows do `LEFT ANTI JOIN` and `NOT EXISTS` return?
# MAGIC 3. What is `5 NOT IN (1, 2, NULL)`: true, false or NULL?

# COMMAND ----------

# DBTITLE 1,E1 Code
def show_value(v):
    """Text for a value from a query result: NULL for None."""
    return "NULL" if v is None else str(v)

truth = spark.sql("SELECT 5 NOT IN (1, 2, NULL) AS with_null, 5 NOT IN (1, 2) AS without_null")   # the core rule
display(truth)                                                   # one row, two columns
t_row = truth.first()                                            # keep the values for the explanation

not_in = spark.sql(f"""
    SELECT patient_id                                            -- patients ...
    FROM {FQ}.patients
    WHERE patient_id NOT IN (SELECT patient_id                   -- ... whose id is not in this list
                             FROM {FQ}.encounters)               -- the list contains 20 NULLs
""")
display(not_in)                                                  # how many rows?

anti = spark.sql(f"""
    SELECT p.patient_id                                          -- only left-side columns are allowed
    FROM {FQ}.patients p
    LEFT ANTI JOIN {FQ}.encounters e                             -- keep left rows that have NO match
      ON p.patient_id = e.patient_id                             -- NULL keys never match, so they do no harm
    ORDER BY p.patient_id
""")
display(anti)                                                    # the patients without an encounter

not_exists = spark.sql(f"""
    SELECT p.patient_id
    FROM {FQ}.patients p
    WHERE NOT EXISTS (SELECT 1 FROM {FQ}.encounters e            -- correlated subquery
                      WHERE e.patient_id = p.patient_id)         -- same logic as the anti join
""")
not_in_fixed = spark.sql(f"""
    SELECT patient_id FROM {FQ}.patients
    WHERE patient_id NOT IN (SELECT patient_id FROM {FQ}.encounters
                             WHERE patient_id IS NOT NULL)       -- remove the NULLs, then NOT IN works
""")
anti_py = (spark.table(f"{FQ}.patients")                         # the same anti join in PySpark
           .join(spark.table(f"{FQ}.encounters"), "patient_id", "left_anti"))

n_not_in, n_anti, n_not_exists = not_in.count(), anti.count(), not_exists.count()   # count each result
n_fixed, n_anti_py = not_in_fixed.count(), anti_py.count()
anti_ids = [r["patient_id"] for r in anti.collect()]             # which patients
print(f"NOT IN {n_not_in}, LEFT ANTI {n_anti}, NOT EXISTS {n_not_exists}, NOT IN without NULLs {n_fixed}, PySpark {n_anti_py}")
METRICS["E1.truth_with_null"] = show_value(t_row["with_null"])
METRICS["E1.truth_without_null"] = show_value(t_row["without_null"])
METRICS["E1.not_in_rows"], METRICS["E1.anti_rows"], METRICS["E1.not_exists_rows"] = n_not_in, n_anti, n_not_exists
METRICS["E1.not_in_fixed_rows"], METRICS["E1.anti_py_rows"] = n_fixed, n_anti_py
METRICS["E1.anti_ids"] = f"{anti_ids[0]}–{anti_ids[-1]}"
assert n_anti == 10 and n_not_exists == 10 and n_fixed == 10 and n_anti_py == 10
assert anti_ids == list(range(191, 201))

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `5 NOT IN (1, 2, NULL)` = **{{E1.truth_with_null}}**, but `5 NOT IN (1, 2)` = **{{E1.truth_without_null}}**. The *NULL semantics* page: "`NOT IN` always returns `UNKNOWN` when the list contains `NULL` and does not contain the input value". `WHERE` keeps only rows where the condition is true, so UNKNOWN rows are dropped.
# MAGIC * That is why `NOT IN (SELECT patient_id FROM encounters)` returned **{{E1.not_in_rows}}** rows, although 10 patients have no encounter: the subquery contains NULLs.
# MAGIC * `LEFT ANTI JOIN` returned **{{E1.anti_rows}}** rows (patients {{E1.anti_ids}}), `NOT EXISTS` {{E1.not_exists_rows}}, `NOT IN` with `WHERE patient_id IS NOT NULL` in the subquery {{E1.not_in_fixed_rows}}, and the PySpark `left_anti` join {{E1.anti_py_rows}}.
# MAGIC
# MAGIC > **Key takeaway:** for "has no match", write `LEFT ANTI JOIN` or `NOT EXISTS`. Use `NOT IN` only with a list that can never contain NULL.

# COMMAND ----------

# DBTITLE 1,E2 Predict
# MAGIC %md
# MAGIC ### E2 — `LEFT SEMI JOIN` vs `INNER JOIN`, and a `CROSS JOIN` calendar grid
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. "Patients who had at least one ER visit": does `INNER JOIN` return one row per patient?
# MAGIC 2. Does `LEFT SEMI JOIN` return the columns of `encounters`?
# MAGIC 3. 20 providers `CROSS JOIN` 12 months: how many rows?

# COMMAND ----------

# DBTITLE 1,E2 Code
inner = spark.sql(f"""
    SELECT p.patient_id, p.state                                 -- patient columns
    FROM {FQ}.patients p
    JOIN {FQ}.encounters e                                       -- INNER JOIN: one row per matching encounter
      ON p.patient_id = e.patient_id AND e.encounter_type = 'ER'
""")
semi = spark.sql(f"""
    SELECT *                                                     -- only patient columns come back
    FROM {FQ}.patients p
    LEFT SEMI JOIN {FQ}.encounters e                             -- keep a patient once if ANY match exists
      ON p.patient_id = e.patient_id AND e.encounter_type = 'ER'
""")
display(semi.orderBy("patient_id").limit(5))                     # first rows of the semi join
n_inner, n_semi, n_inner_distinct = inner.count(), semi.count(), inner.distinct().count()
semi_cols = semi.columns                                         # which columns did SEMI return?
print(f"INNER {n_inner} rows, INNER + DISTINCT {n_inner_distinct}, LEFT SEMI {n_semi}; semi columns {semi_cols}")

grid = spark.sql(f"""
    WITH months AS (                                             -- 12 month start dates
        SELECT explode(sequence(DATE'2025-01-01', DATE'2025-12-01', INTERVAL 1 MONTH)) AS month),
    visits AS (                                                  -- real encounter counts per provider and month
        SELECT provider_id, trunc(encounter_date, 'MONTH') AS month, count(*) AS n
        FROM {FQ}.encounters
        WHERE provider_id IS NOT NULL
        GROUP BY ALL)                                            -- GROUP BY provider_id, month
    SELECT pr.provider_id, m.month, coalesce(v.n, 0) AS encounters   -- 0 instead of NULL for empty months
    FROM {FQ}.providers pr
    CROSS JOIN months m                                          -- every provider x every month
    LEFT JOIN visits v ON v.provider_id = pr.provider_id AND v.month = m.month
    ORDER BY pr.provider_id, m.month
""")
display(grid)                                                    # the full grid
g = grid.selectExpr("count(*) AS cells", "count_if(encounters = 0) AS empty_cells",
                    "min(encounters) AS min_n", "max(encounters) AS max_n").first()
print(g.asDict())
METRICS["E2.inner_rows"], METRICS["E2.semi_rows"], METRICS["E2.inner_distinct_rows"] = n_inner, n_semi, n_inner_distinct
METRICS["E2.semi_columns"] = ", ".join(semi_cols)
METRICS["E2.grid_cells"], METRICS["E2.grid_empty"] = g["cells"], g["empty_cells"]
METRICS["E2.grid_min"], METRICS["E2.grid_max"] = g["min_n"], g["max_n"]
assert n_semi == n_inner_distinct and g["cells"] == 240

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `INNER JOIN` returned **{{E2.inner_rows}}** rows: one row per ER encounter, so a patient with three ER visits appears three times.
# MAGIC * `INNER JOIN` + `DISTINCT` gave {{E2.inner_distinct_rows}} rows, and `LEFT SEMI JOIN` gave **{{E2.semi_rows}}** rows — the same answer without `DISTINCT`.
# MAGIC * The semi join returned only the columns `{{E2.semi_columns}}`, although the query says `SELECT *`. A semi join is a filter on the left table.
# MAGIC * `CROSS JOIN` of 20 providers and 12 months gave **{{E2.grid_cells}}** cells. The encounter counts per cell go from {{E2.grid_min}} to {{E2.grid_max}}; {{E2.grid_empty}} cells are empty in this data. With `LEFT JOIN` + `coalesce(v.n, 0)`, an empty month would show `0` instead of being missing — the reason to build the grid first.
# MAGIC
# MAGIC > **Key takeaway:** "rows that have a match" → `LEFT SEMI JOIN` / `EXISTS`. Use `INNER JOIN` only when you need columns from both sides.

# COMMAND ----------

# DBTITLE 1,E3 Predict
# MAGIC %md
# MAGIC ### E3 — NULL keys in joins: `=` vs `<=>`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. What is `NULL = NULL`? And `NULL <=> NULL`?
# MAGIC 2. 50 encounters have `diagnosis_code = NULL`, and `diagnosis_codes` has one row with `code = NULL` ("Not coded"). Does an `INNER JOIN … ON e.diagnosis_code = d.code` return those 50 encounters?
# MAGIC 3. And with `<=>`?

# COMMAND ----------

# DBTITLE 1,E3 Code
ops = spark.sql("SELECT NULL = NULL AS equals, NULL <=> NULL AS null_safe, 'I10' <=> NULL AS one_side_null")
display(ops)                                                     # three comparisons
o = ops.first()                                                  # keep the values

eq = spark.sql(f"""
    SELECT e.encounter_id, e.diagnosis_code, d.description
    FROM {FQ}.encounters e
    JOIN {FQ}.diagnosis_codes d ON e.diagnosis_code = d.code     -- normal equality
""")
ns = spark.sql(f"""
    SELECT e.encounter_id, e.diagnosis_code, d.description
    FROM {FQ}.encounters e
    JOIN {FQ}.diagnosis_codes d ON e.diagnosis_code <=> d.code   -- null-safe equality
""")
left = spark.sql(f"""
    SELECT e.encounter_id, e.diagnosis_code,
           d.description,                                        -- NULL when there is no match
           coalesce(d.description, 'Not coded') AS label         -- a common fix instead of <=>
    FROM {FQ}.encounters e
    LEFT JOIN {FQ}.diagnosis_codes d ON e.diagnosis_code = d.code
""")
display(ns.filter("diagnosis_code IS NULL").limit(5))            # NULL-coded encounters found by <=>
display(left.filter("diagnosis_code IS NULL").limit(5))          # the same encounters in a LEFT JOIN with =
e_df, d_df = spark.table(f"{FQ}.encounters"), spark.table(f"{FQ}.diagnosis_codes")
ns_py = e_df.join(d_df, e_df.diagnosis_code.eqNullSafe(d_df.code))   # PySpark <=>
n_eq, n_ns, n_ns_py = eq.count(), ns.count(), ns_py.count()
n_left_no_desc = left.filter("description IS NULL").count()      # LEFT JOIN rows with no description
print(f"= : {n_eq} rows, <=> : {n_ns} rows (PySpark {n_ns_py}), LEFT JOIN rows without description: {n_left_no_desc}")
METRICS["E3.null_eq_null"], METRICS["E3.null_safe"] = show_value(o["equals"]), show_value(o["null_safe"])
METRICS["E3.one_side_null"] = show_value(o["one_side_null"])
METRICS["E3.eq_rows"], METRICS["E3.nullsafe_rows"], METRICS["E3.nullsafe_py_rows"] = n_eq, n_ns, n_ns_py
METRICS["E3.left_no_desc"] = n_left_no_desc
assert n_ns == n_eq + 50 and n_left_no_desc == 50 and n_ns_py == n_ns

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `NULL = NULL` → **{{E3.null_eq_null}}**, `NULL <=> NULL` → **{{E3.null_safe}}**, `'I10' <=> NULL` → **{{E3.one_side_null}}** (as the *NULL semantics* page says).
# MAGIC * The join with `=` returned **{{E3.eq_rows}}** rows: the {{setup.enc_null_code}} encounters with a NULL code are gone. The join with `<=>` returned **{{E3.nullsafe_rows}}** rows (PySpark `eqNullSafe`: {{E3.nullsafe_py_rows}}): they matched the "Not coded" row.
# MAGIC * The `LEFT JOIN` with `=` keeps every encounter, but {{E3.left_no_desc}} rows have no description; `coalesce(d.description, 'Not coded')` labels them without `<=>`.
# MAGIC
# MAGIC > **Key takeaway:** NULL keys normally should **not** match (an unknown patient is not the same as another unknown patient). Use `<=>` only when NULL is a real value on both sides, like a "Not coded" reference row.

# COMMAND ----------

# DBTITLE 1,E4 Predict
# MAGIC %md
# MAGIC ### E4 — Deduplicate claims with `QUALIFY`, and the logical order of a query
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `claims` has 1500 rows and 1350 distinct `claim_id`s. How many rows does the dedup return?
# MAGIC 2. Can you write the window function in `WHERE` instead of `QUALIFY`?
# MAGIC 3. Can you write `count(*) > 300` in `WHERE`?

# COMMAND ----------

# DBTITLE 1,E4 Code
dedup_sql = f"""
    SELECT *
    FROM {FQ}.claims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id             -- one group per claim
                               ORDER BY updated_at DESC) = 1     -- newest version first, keep only it
"""
spark.sql(f"CREATE OR REPLACE TABLE {FQ}.claims_clean AS {dedup_sql}")   # E8 and E12 use this table
clean = spark.table(f"{FQ}.claims_clean")
display(clean.orderBy("claim_id").limit(5))

sub = spark.sql(f"""
    SELECT * EXCEPT (rn)                                         -- drop the helper column
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) AS rn
          FROM {FQ}.claims)                                      -- the same without QUALIFY: a subquery
    WHERE rn = 1
""")
not_latest = spark.sql(f"""
    SELECT count(*) AS n                                         -- kept rows that are NOT the newest version
    FROM {FQ}.claims_clean c
    JOIN (SELECT claim_id, max(updated_at) AS newest FROM {FQ}.claims GROUP BY claim_id) m
      ON c.claim_id = m.claim_id
    WHERE c.updated_at <> m.newest
""").first()["n"]

from pyspark.sql import Window, functions as F                  # the PySpark version
w = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc())
dedup_py = spark.table(f"{FQ}.claims").withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

try:                                                             # a window function in WHERE
    spark.sql(f"SELECT * FROM {FQ}.claims WHERE ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1").collect()
    where_window = "no error"
except Exception as e:
    where_window = error_class(e)
try:                                                             # an aggregate in WHERE
    spark.sql(f"SELECT payer, count(*) AS n FROM {FQ}.claims WHERE count(*) > 300 GROUP BY payer").collect()
    where_agg = "no error"
except Exception as e:
    where_agg = error_class(e)
having = spark.sql(f"""
    SELECT payer, count(*) AS n
    FROM {FQ}.claims_clean
    GROUP BY payer
    HAVING count(*) > 250                                        -- HAVING runs after GROUP BY: aggregates are fine
    ORDER BY payer
""")
display(having)
n_clean, n_sub, n_py, n_having = clean.count(), sub.count(), dedup_py.count(), having.count()
print(f"QUALIFY {n_clean}, subquery {n_sub}, PySpark {n_py}; not newest: {not_latest}; "
      f"window in WHERE: {where_window}; aggregate in WHERE: {where_agg}; HAVING groups: {n_having}")
METRICS["E4.clean_rows"], METRICS["E4.sub_rows"], METRICS["E4.py_rows"] = n_clean, n_sub, n_py
METRICS["E4.not_latest"] = not_latest
METRICS["E4.where_window_error"], METRICS["E4.where_agg_error"] = where_window, where_agg
METRICS["E4.having_groups"] = n_having
assert n_clean == 1350 and n_sub == 1350 and n_py == 1350 and not_latest == 0

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `QUALIFY ROW_NUMBER() … = 1` kept **{{E4.clean_rows}}** of {{setup.claims}} rows — one per distinct `claim_id` ({{setup.distinct_claims}}). The subquery version gave {{E4.sub_rows}} rows and the PySpark version {{E4.py_rows}}. Kept rows that are **not** the newest version: {{E4.not_latest}}.
# MAGIC * A window function in `WHERE` failed with `{{E4.where_window_error}}`, and an aggregate in `WHERE` failed with `{{E4.where_agg_error}}`. Both show the logical order from 4.1: `WHERE` runs before `GROUP BY` and before window functions, so it cannot see their results.
# MAGIC * The same filter in `HAVING` worked: {{E4.having_groups}} payer groups have more than 250 claims.
# MAGIC
# MAGIC > **Key takeaway:** dedup = `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY version_column DESC) = 1`. This is the standard step before a `MERGE` (a later lesson): a `MERGE` fails when several source rows match the same target row (*MERGE INTO* reference).

# COMMAND ----------

# DBTITLE 1,E5 Predict
# MAGIC %md
# MAGIC ### E5 — Days between visits with `LAG` and `LEAD`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. What does `LAG(encounter_date)` return on the first visit of each patient?
# MAGIC 2. Some patients have two visits on the same day. What is the gap for the second one?
# MAGIC 3. `datediff(prev_date, encounter_date)` — is that right?

# COMMAND ----------

# DBTITLE 1,E5 Code
gaps = spark.sql(f"""
    SELECT patient_id, encounter_id, encounter_date,
           LAG(encounter_date) OVER w                AS prev_date,        -- previous visit of the same patient
           datediff(encounter_date, LAG(encounter_date) OVER w) AS days_since_prev,   -- end date first
           datediff(LAG(encounter_date) OVER w, encounter_date) AS swapped,           -- wrong order on purpose
           LEAD(encounter_date) OVER w               AS next_date         -- next visit
    FROM {FQ}.encounters
    WHERE patient_id IS NOT NULL                                          -- WHERE runs before the window
    WINDOW w AS (PARTITION BY patient_id ORDER BY encounter_date, encounter_id)   -- named window; encounter_id breaks ties
""")
s = gaps.selectExpr("count(*) AS n_rows",
                    "count_if(prev_date IS NULL) AS first_visits",              # one per patient
                    "count(DISTINCT patient_id) AS patients",
                    "count_if(days_since_prev = 0) AS same_day",                # ties on encounter_date
                    "round(avg(days_since_prev), 2) AS avg_gap",
                    "max(days_since_prev) AS max_gap",
                    "count_if(swapped < 0) AS swapped_negative",
                    "count_if(days_since_prev > 0) AS positive_gaps").first()
tie_patient = gaps.filter("days_since_prev = 0").agg(F.min("patient_id")).first()[0]   # a patient with a same-day visit
display(gaps.filter(F.col("patient_id") == tie_patient).orderBy("encounter_date", "encounter_id"))
print(s.asDict(), "tie patient:", tie_patient)
METRICS["E5.rows"], METRICS["E5.first_visits"], METRICS["E5.patients"] = s["n_rows"], s["first_visits"], s["patients"]
METRICS["E5.same_day"], METRICS["E5.avg_gap"], METRICS["E5.max_gap"] = s["same_day"], str(s["avg_gap"]), s["max_gap"]
METRICS["E5.swapped_negative"], METRICS["E5.positive_gaps"] = s["swapped_negative"], s["positive_gaps"]
METRICS["E5.tie_patient"] = tie_patient
assert s["n_rows"] == 1980 and s["first_visits"] == s["patients"] and s["swapped_negative"] == s["positive_gaps"]

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * {{E5.rows}} visits of {{E5.patients}} patients. `WHERE patient_id IS NOT NULL` removed the 20 visits without a patient **before** the window was computed.
# MAGIC * `prev_date` is NULL on {{E5.first_visits}} rows — exactly one per patient, the first visit (the default value of `LAG` is NULL, *lag function*).
# MAGIC * {{E5.same_day}} rows have `days_since_prev = 0`: same-day visits (patient {{E5.tie_patient}} is shown above). `encounter_id` in the `ORDER BY` decides which of the two comes first, so the result is the same every run.
# MAGIC * Average gap {{E5.avg_gap}} days, longest gap {{E5.max_gap}} days.
# MAGIC * With the arguments swapped, `datediff` was negative on {{E5.swapped_negative}} rows — exactly the {{E5.positive_gaps}} rows with a positive gap. The order is `datediff(endDate, startDate)`.
# MAGIC
# MAGIC > **Key takeaway:** `LAG`/`LEAD` + `datediff` answer "days since the last visit" and "days until the next visit" without a self-join. The `WINDOW w AS (…)` clause lets you write the window once.

# COMMAND ----------

# DBTITLE 1,E6 Predict
# MAGIC %md
# MAGIC ### E6 — Running total: default frame vs `ROWS` frame, with ties
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `SUM(charge) OVER (PARTITION BY patient_id ORDER BY encounter_date)` — no frame written. Two visits on the same day: do they get different running totals?
# MAGIC 2. Is the default frame `ROWS` or `RANGE`?
# MAGIC 3. `SUM(charge) OVER (PARTITION BY patient_id)` without `ORDER BY`: what does it return?

# COMMAND ----------

# DBTITLE 1,E6 Code
rt = spark.sql(f"""
    SELECT patient_id, encounter_id, encounter_date, charge,
           SUM(charge) OVER (PARTITION BY patient_id ORDER BY encounter_date) AS rt_default,      -- no frame written
           SUM(charge) OVER (PARTITION BY patient_id ORDER BY encounter_date
                             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rt_range,    -- explicit RANGE
           SUM(charge) OVER (PARTITION BY patient_id ORDER BY encounter_date, encounter_id
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rt_rows,      -- explicit ROWS + tie-breaker
           SUM(charge) OVER (PARTITION BY patient_id) AS patient_total,                         -- no ORDER BY at all
           count(*) OVER (PARTITION BY patient_id, encounter_date) AS same_day_visits           -- > 1 means a tie
    FROM {FQ}.encounters
    WHERE patient_id IS NOT NULL
""")
display(rt.filter(F.col("patient_id") == tie_patient).orderBy("encounter_date", "encounter_id"))   # the patient from E5
r = rt.selectExpr("count(*) AS n_rows",
                  "count_if(same_day_visits > 1) AS tied_rows",
                  "count_if(rt_default <> rt_rows) AS default_ne_rows",
                  "count_if(rt_default = rt_range) AS default_eq_range",
                  "count_if(patient_total = rt_default) AS total_eq_default",
                  "count(DISTINCT patient_id) AS patients").first()
w_rows = (Window.partitionBy("patient_id").orderBy("encounter_date", "encounter_id")      # PySpark ROWS frame
          .rowsBetween(Window.unboundedPreceding, Window.currentRow))
rt_py = rt.withColumn("rt_py", F.sum("charge").over(w_rows))
py_diff = rt_py.filter("rt_py <> rt_rows").count()               # PySpark vs SQL ROWS frame
print(r.asDict(), "PySpark differences:", py_diff)
METRICS["E6.rows"], METRICS["E6.tied_rows"] = r["n_rows"], r["tied_rows"]
METRICS["E6.default_ne_rows"], METRICS["E6.default_eq_range"] = r["default_ne_rows"], r["default_eq_range"]
METRICS["E6.total_eq_default"], METRICS["E6.patients"], METRICS["E6.py_diff"] = r["total_eq_default"], r["patients"], py_diff
assert r["n_rows"] == 1980 and py_diff == 0

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * {{E6.tied_rows}} of {{E6.rows}} rows share the patient **and** the date with another row.
# MAGIC * The running total with **no frame written** (`rt_default`) was different from the `ROWS` running total on **{{E6.default_ne_rows}}** rows: on a same-day pair, the first row already includes the charge of the second.
# MAGIC * `rt_default` was equal to the explicit `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` on **{{E6.default_eq_range}} of {{E6.rows}}** rows. So, measured on this workspace: with `ORDER BY` and no frame, the frame behaves like `RANGE … CURRENT ROW` (all rows with the same `ORDER BY` value are included). The *Window frame clause* page does not state this default.
# MAGIC * `SUM(charge) OVER (PARTITION BY patient_id)` (no `ORDER BY`) is the patient total on every row. It is equal to `rt_default` on {{E6.total_eq_default}} rows: the last visit of each of the {{E6.patients}} patients, plus the rows that tie with a last visit.
# MAGIC * The PySpark window with `rowsBetween(Window.unboundedPreceding, Window.currentRow)` matched the SQL `ROWS` result ({{E6.py_diff}} differences).
# MAGIC
# MAGIC > **Key takeaway:** for a row-by-row running total, write the frame (`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`) **and** make the `ORDER BY` unique with a tie-breaker.

# COMMAND ----------

# DBTITLE 1,E7 Predict
# MAGIC %md
# MAGIC ### E7 — `ROW_NUMBER` vs `RANK` vs `DENSE_RANK` on ties
# MAGIC
# MAGIC **Predict:** A1C results have one decimal, so many patients share the same value.
# MAGIC 1. How many rows have `ROW_NUMBER() <= 3`? `RANK() <= 3`? `DENSE_RANK() <= 3`?
# MAGIC 2. After a tie on rank 1, what is the next `RANK`? The next `DENSE_RANK`?

# COMMAND ----------

# DBTITLE 1,E7 Code
ranked = spark.sql(f"""
    SELECT lab_id, patient_id, result_value,
           ROW_NUMBER() OVER w_tie AS row_number,                -- 1, 2, 3, ... always unique
           RANK()       OVER w     AS rank,                      -- ties share a rank, then a gap
           DENSE_RANK() OVER w     AS dense_rank                 -- ties share a rank, no gap
    FROM {FQ}.lab_results
    WHERE test_code = 'A1C'
    WINDOW w     AS (PARTITION BY test_code ORDER BY result_value DESC),
           w_tie AS (PARTITION BY test_code ORDER BY result_value DESC, lab_id)   -- lab_id makes ROW_NUMBER repeatable
""")
display(ranked.orderBy("row_number").limit(15))                  # the top of the list
k = ranked.selectExpr("count_if(row_number <= 3) AS rn3", "count_if(rank <= 3) AS rk3",
                      "count_if(dense_rank <= 3) AS dr3", "max(result_value) AS top_value",
                      "count_if(rank = 1) AS top_ties",
                      "min_by(rank, CASE WHEN dense_rank = 2 THEN 0 ELSE 1 END) AS rank_of_second_value").first()
print(k.asDict())
METRICS["E7.rn3"], METRICS["E7.rk3"], METRICS["E7.dr3"] = k["rn3"], k["rk3"], k["dr3"]
METRICS["E7.top_value"], METRICS["E7.top_ties"] = k["top_value"], k["top_ties"]
METRICS["E7.rank_of_second_value"] = k["rank_of_second_value"]
assert k["rn3"] == 3 and k["rk3"] >= 3 and k["dr3"] >= k["rk3"]

# COMMAND ----------

# DBTITLE 1,E7 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * The highest A1C value is {{E7.top_value}}, and **{{E7.top_ties}}** results share it.
# MAGIC * `ROW_NUMBER() <= 3` → **{{E7.rn3}}** rows: three of the tied rows; which three depends only on the tie-breaker (`lab_id` here).
# MAGIC * `RANK() <= 3` → **{{E7.rk3}}** rows: all tied rows get rank 1, and the next value gets rank **{{E7.rank_of_second_value}}** (a gap). So "rank ≤ 3" contains only the tied top rows.
# MAGIC * `DENSE_RANK() <= 3` → **{{E7.dr3}}** rows: the three highest **distinct** values, with all their ties (no gaps).
# MAGIC
# MAGIC > **Key takeaway:** dedup → `ROW_NUMBER`; "top N values including ties" → `DENSE_RANK`; "competition ranking" (1, 1, 3) → `RANK`.

# COMMAND ----------

# DBTITLE 1,E8 Predict
# MAGIC %md
# MAGIC ### E8 — Subtotals with `ROLLUP`, `CUBE` and `GROUPING SETS`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `GROUP BY ROLLUP (payer, status)`: which extra rows appear?
# MAGIC 2. Some claims have `payer = NULL` (self-pay). How can you tell a self-pay row from a subtotal row, where `payer` is also NULL?
# MAGIC 3. Which returns more rows: `ROLLUP` or `CUBE`?

# COMMAND ----------

# DBTITLE 1,E8 Code
rollup = spark.sql(f"""
    SELECT payer, status,
           grouping(payer)  AS g_payer,                          -- 1 = payer is "all payers" in this row
           grouping(status) AS g_status,                         -- 1 = status is "all statuses"
           CASE WHEN grouping(payer) = 1 THEN 'ALL PAYERS'
                ELSE coalesce(payer, 'SELF-PAY') END AS payer_label,   -- a readable label
           count(*) AS claims,
           sum(amount) AS total_amount
    FROM {FQ}.claims_clean
    GROUP BY ROLLUP (payer, status)                              -- (payer, status), (payer), ()
    ORDER BY g_payer, payer_label, g_status, status
""")
display(rollup)
cube = spark.sql(f"SELECT payer, status, count(*) AS claims FROM {FQ}.claims_clean GROUP BY CUBE (payer, status)")
sets = spark.sql(f"SELECT payer, status, count(*) AS claims FROM {FQ}.claims_clean GROUP BY GROUPING SETS ((payer), (status))")
rollup_py = spark.table(f"{FQ}.claims_clean").rollup("payer", "status").count()   # PySpark rollup
q = rollup.selectExpr("count(*) AS n_rows",
                      "count_if(payer IS NULL) AS payer_null_rows",
                      "count_if(g_payer = 1) AS grand_total_rows",
                      "count_if(g_payer = 0 AND g_status = 1) AS payer_subtotals",
                      "count_if(payer IS NULL AND g_payer = 0) AS self_pay_rows",
                      "max_by(claims, g_payer) AS grand_total_claims").first()
n_cube, n_sets, n_rollup_py = cube.count(), sets.count(), rollup_py.count()
print(q.asDict(), f"CUBE {n_cube}, GROUPING SETS {n_sets}, PySpark rollup {n_rollup_py}")
METRICS["E8.rollup_rows"], METRICS["E8.payer_null_rows"] = q["n_rows"], q["payer_null_rows"]
METRICS["E8.grand_total_rows"], METRICS["E8.payer_subtotals"] = q["grand_total_rows"], q["payer_subtotals"]
METRICS["E8.self_pay_rows"], METRICS["E8.grand_total_claims"] = q["self_pay_rows"], q["grand_total_claims"]
METRICS["E8.cube_rows"], METRICS["E8.sets_rows"], METRICS["E8.rollup_py_rows"] = n_cube, n_sets, n_rollup_py
assert q["grand_total_rows"] == 1 and q["grand_total_claims"] == 1350 and n_rollup_py == q["n_rows"]

# COMMAND ----------

# DBTITLE 1,E8 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `ROLLUP (payer, status)` returned **{{E8.rollup_rows}}** rows: the detail rows, **{{E8.payer_subtotals}}** payer subtotals (`g_status = 1`) and **{{E8.grand_total_rows}}** grand total (`g_payer = 1`) with {{E8.grand_total_claims}} claims.
# MAGIC * `payer IS NULL` is true on **{{E8.payer_null_rows}}** rows: {{E8.self_pay_rows}} of them are real self-pay rows (`g_payer = 0`) and the rest is the grand total. Only `grouping(payer)` tells them apart; the `payer_label` column uses it.
# MAGIC * `CUBE (payer, status)` returned {{E8.cube_rows}} rows (it adds subtotals per status over all payers). `GROUPING SETS ((payer), (status))` returned {{E8.sets_rows}} rows: only the two subtotal levels, no detail and no grand total. The PySpark `rollup` returned {{E8.rollup_py_rows}} rows, like the SQL `ROLLUP`.
# MAGIC
# MAGIC > **Key takeaway:** `ROLLUP` = hierarchy subtotals, `CUBE` = all combinations, `GROUPING SETS` = exactly the groups you list. Always label subtotal rows with `grouping()`, never with `IS NULL`.

# COMMAND ----------

# DBTITLE 1,E9 Predict
# MAGIC %md
# MAGIC ### E9 — `PIVOT` and `UNPIVOT`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. We pivot the average lab value per patient into the columns `a1c, ldl, glu, tsh`. 190 patients have labs. How many rows? What happens if we pivot the **whole** `lab_results` table (with `lab_id` and `result_ts`)?
# MAGIC 2. Only patients with `patient_id % 5 = 0` have TSH tests. What is in `tsh` for the others?
# MAGIC 3. `UNPIVOT` back to rows: are the NULL cells kept?

# COMMAND ----------

# DBTITLE 1,E9 Code
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.lab_wide AS
    SELECT *
    FROM (SELECT patient_id, test_code, result_value             -- ONLY the columns the pivot needs
          FROM {FQ}.lab_results
          WHERE patient_id IS NOT NULL)
    PIVOT (round(avg(result_value), 2)                           -- the value in each new column
           FOR test_code IN ('A1C' AS a1c, 'LDL' AS ldl, 'GLU' AS glu, 'TSH' AS tsh))   -- literal values -> columns
""")
wide = spark.table(f"{FQ}.lab_wide")
display(wide.orderBy("patient_id").limit(10))

trap = spark.sql(f"""
    SELECT *
    FROM {FQ}.lab_results                                        -- lab_id and result_ts become grouping columns
    PIVOT (avg(result_value) FOR test_code IN ('A1C' AS a1c, 'LDL' AS ldl, 'GLU' AS glu, 'TSH' AS tsh))
""")
long_default = spark.sql(f"""
    SELECT * FROM {FQ}.lab_wide
    UNPIVOT (avg_value FOR test_code IN (a1c AS `A1C`, ldl AS `LDL`, glu AS `GLU`, tsh AS `TSH`))   -- EXCLUDE NULLS is the default
""")
long_all = spark.sql(f"""
    SELECT * FROM {FQ}.lab_wide
    UNPIVOT INCLUDE NULLS (avg_value FOR test_code IN (a1c AS `A1C`, ldl AS `LDL`, glu AS `GLU`, tsh AS `TSH`))
""")
display(long_default.orderBy("patient_id", "test_code").limit(8))
wide_py = (spark.table(f"{FQ}.lab_results").filter("patient_id IS NOT NULL")          # PySpark pivot
           .groupBy("patient_id").pivot("test_code", ["A1C", "LDL", "GLU", "TSH"]).agg(F.round(F.avg("result_value"), 2)))
long_py = wide.unpivot("patient_id", ["a1c", "ldl", "glu", "tsh"], "test_code", "avg_value")   # PySpark unpivot
n_wide, n_trap, n_long, n_long_all = wide.count(), trap.count(), long_default.count(), long_all.count()
nulls = wide.selectExpr("count_if(a1c IS NULL) + count_if(ldl IS NULL) + count_if(glu IS NULL) + count_if(tsh IS NULL) AS cells",
                       "count_if(tsh IS NULL) AS tsh").first()                   # empty cells in the wide table
n_null_cells, n_tsh_null = nulls["cells"], nulls["tsh"]
n_wide_py, n_long_py = wide_py.count(), long_py.count()
print(f"wide {n_wide} rows {wide.columns}; trap {n_trap} rows; NULL cells {n_null_cells} (TSH {n_tsh_null}); "
      f"UNPIVOT {n_long}, INCLUDE NULLS {n_long_all}; PySpark pivot {n_wide_py}, unpivot {n_long_py}")
METRICS["E9.wide_rows"], METRICS["E9.wide_columns"] = n_wide, ", ".join(wide.columns)
METRICS["E9.trap_rows"], METRICS["E9.trap_columns"] = n_trap, ", ".join(trap.columns)
METRICS["E9.tsh_null"], METRICS["E9.long_rows"], METRICS["E9.long_all_rows"] = n_tsh_null, n_long, n_long_all
METRICS["E9.wide_py_rows"], METRICS["E9.long_py_rows"], METRICS["E9.null_cells"] = n_wide_py, n_long_py, n_null_cells
assert n_wide == 190 and n_long_all == 4 * n_wide and n_long == n_long_all - n_null_cells

# COMMAND ----------

# DBTITLE 1,E9 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `lab_wide` has **{{E9.wide_rows}}** rows (one per patient with labs) and the columns `{{E9.wide_columns}}`. The PySpark `pivot` also returned {{E9.wide_py_rows}} rows.
# MAGIC * The trap: pivoting the **whole** table returned **{{E9.trap_rows}}** rows with the columns `{{E9.trap_columns}}`. `lab_id` and `result_ts` became grouping columns, so every lab is its own group.
# MAGIC * `lab_wide` has {{E9.null_cells}} empty (NULL) cells: {{E9.tsh_null}} in `tsh` (patients without TSH tests) and the rest because a few patients have no A1C or no LDL result.
# MAGIC * `UNPIVOT` (default `EXCLUDE NULLS`) returned **{{E9.long_rows}}** rows; `UNPIVOT INCLUDE NULLS` returned **{{E9.long_all_rows}}** rows (4 × {{E9.wide_rows}}). The difference is exactly the {{E9.null_cells}} NULL cells.
# MAGIC * The PySpark `unpivot` returned {{E9.long_py_rows}} rows: unlike the SQL default, it kept the NULL cells.
# MAGIC
# MAGIC > **Key takeaway:** before `PIVOT`, select only the grouping column(s), the pivot column and the value column. After `UNPIVOT`, check whether you need `INCLUDE NULLS`.

# COMMAND ----------

# DBTITLE 1,E10 Predict
# MAGIC %md
# MAGIC ### E10 — `collect_list` + `filter`, `transform`, `exists`, `aggregate`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Can you compute the average A1C per patient from the array alone, without a second `GROUP BY`?
# MAGIC 2. `aggregate(a1c_values, 0, (acc, v) -> acc + v)` with `DOUBLE` values: does it work?

# COMMAND ----------

# DBTITLE 1,E10 Code
hof = spark.sql(f"""
    WITH a1c AS (
        SELECT patient_id,
               array_sort(collect_list(result_value)) AS a1c_values,   -- one array per patient, sorted
               avg(result_value) AS avg_check                           -- normal average, to compare
        FROM {FQ}.lab_results
        WHERE test_code = 'A1C' AND patient_id IS NOT NULL
        GROUP BY patient_id)
    SELECT patient_id, a1c_values,
           filter(a1c_values, v -> v >= 6.5)                             AS diabetic_range,   -- keep some elements
           transform(a1c_values, v -> CASE WHEN v >= 6.5 THEN 'HIGH' ELSE 'OK' END) AS flags, -- map each element
           exists(a1c_values, v -> v >= 9.0)                             AS any_very_high,    -- any element matches?
           aggregate(a1c_values, 0D, (acc, v) -> acc + v,
                     acc -> acc / size(a1c_values))                      AS avg_from_array,   -- fold + finish
           avg_check
    FROM a1c
""")
display(hof.orderBy("patient_id").limit(5))
h = hof.selectExpr("count(*) AS patients",
                   "count_if(size(diabetic_range) > 0) AS with_diabetic",
                   "count_if(any_very_high) AS with_very_high",
                   "sum(size(a1c_values)) AS values_total",
                   "count_if(abs(avg_from_array - avg_check) > 1e-9) AS avg_mismatch").first()
try:                                                             # INT start value with DOUBLE elements
    spark.sql(f"SELECT aggregate(array(5.4D, 7.2D), 0, (acc, v) -> acc + v) AS total").collect()
    int_start = "no error"
except Exception as e:
    int_start = error_class(e)
print(h.asDict(), "INT start:", int_start)
METRICS["E10.patients"], METRICS["E10.with_diabetic"] = h["patients"], h["with_diabetic"]
METRICS["E10.with_very_high"], METRICS["E10.values_total"] = h["with_very_high"], h["values_total"]
METRICS["E10.avg_mismatch"], METRICS["E10.int_start_error"] = h["avg_mismatch"], int_start
assert h["avg_mismatch"] == 0

# COMMAND ----------

# DBTITLE 1,E10 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * {{E10.patients}} patients have A1C results ({{E10.values_total}} values in the arrays).
# MAGIC * `filter(a1c_values, v -> v >= 6.5)`: {{E10.with_diabetic}} patients have at least one value in the diabetic range. `exists(…, v -> v >= 9.0)`: {{E10.with_very_high}} patients have a value of 9.0 or more.
# MAGIC * `aggregate(a1c_values, 0D, (acc, v) -> acc + v, acc -> acc / size(a1c_values))` gave the same average as `avg()` for every patient ({{E10.avg_mismatch}} differences larger than 1e-9).
# MAGIC * With an `INT` start value (`0`) and `DOUBLE` elements, `aggregate` failed with `{{E10.int_start_error}}`: the start value fixes the type of the accumulator. Use `0D` or `CAST(0 AS DOUBLE)`.
# MAGIC
# MAGIC > **Key takeaway:** `collect_list` + higher-order functions work on many values per row without `explode` and a second `GROUP BY`. Sort the array (`array_sort`) if the order matters.

# COMMAND ----------

# DBTITLE 1,E11 Predict
# MAGIC %md
# MAGIC ### E11 — `UNION` vs `UNION ALL`, `INTERSECT`, `EXCEPT`
# MAGIC
# MAGIC **Predict:** We stack the `patient_id` column of `encounters` (2000 rows) and `lab_results` (3000 rows).
# MAGIC 1. How many rows does `UNION ALL` return? And `UNION`?
# MAGIC 2. Both columns contain NULLs. How many NULL rows are in the `UNION` result?
# MAGIC 3. Which patients are in `encounters EXCEPT lab_results`?

# COMMAND ----------

# DBTITLE 1,E11 Code
def ids(op):
    """patient_id from encounters <op> patient_id from lab_results."""
    return spark.sql(f"SELECT patient_id FROM {FQ}.encounters {op} SELECT patient_id FROM {FQ}.lab_results")

union_all, union = ids("UNION ALL"), ids("UNION")                # keep duplicates / remove duplicates
intersect, except_ = ids("INTERSECT"), ids("EXCEPT")             # in both / only in encounters
display(except_.orderBy("patient_id"))                           # patients with visits but no labs
n_all, n_union = union_all.count(), union.count()
n_union_null = union.filter("patient_id IS NULL").count()        # NULLs are "equal" in set operations
n_intersect, n_except = intersect.count(), except_.count()
n_intersect_null = intersect.filter("patient_id IS NULL").count()   # NULL is in both inputs
except_ids = sorted(r["patient_id"] for r in except_.collect() if r["patient_id"] is not None)
n_union_py = (spark.table(f"{FQ}.encounters").select("patient_id")            # PySpark: union() is UNION ALL
              .union(spark.table(f"{FQ}.lab_results").select("patient_id")).distinct().count())
print(f"UNION ALL {n_all}, UNION {n_union} (NULL rows {n_union_null}), INTERSECT {n_intersect}, EXCEPT {n_except} {except_ids}, PySpark {n_union_py}")
METRICS["E11.union_all_rows"], METRICS["E11.union_rows"], METRICS["E11.union_null_rows"] = n_all, n_union, n_union_null
METRICS["E11.intersect_rows"], METRICS["E11.except_rows"], METRICS["E11.intersect_null"] = n_intersect, n_except, n_intersect_null
METRICS["E11.except_ids"] = ", ".join(map(str, except_ids))
METRICS["E11.union_py_rows"] = n_union_py
assert n_all == 5000 and n_union == n_union_py and n_union_null == 1

# COMMAND ----------

# DBTITLE 1,E11 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `UNION ALL` returned **{{E11.union_all_rows}}** rows (2000 + 3000). `UNION` returned **{{E11.union_rows}}** rows: it removes duplicates, because `DISTINCT` is the default (*Set operators*). The PySpark `union()` + `distinct()` gave {{E11.union_py_rows}}.
# MAGIC * `UNION` kept **{{E11.union_null_rows}}** NULL row: in set operations, NULLs are compared "in a null-safe manner" (*NULL semantics*), so all NULLs collapse into one.
# MAGIC * `INTERSECT` returned {{E11.intersect_rows}} rows; NULL rows among them: {{E11.intersect_null}} (NULL is in both inputs, and NULLs are equal here).
# MAGIC * `EXCEPT` returned {{E11.except_rows}} rows: patients {{E11.except_ids}} — the patients with visits but no labs from the setup.
# MAGIC
# MAGIC > **Key takeaway:** use `UNION ALL` unless you really need duplicates removed. Removing duplicates costs work, and PySpark `union()` never removes them.

# COMMAND ----------

# DBTITLE 1,E12 Predict
# MAGIC %md
# MAGIC ### E12 — `FILTER` vs `CASE`, COUNT variants, and NULL functions
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Are `count(*) FILTER (WHERE status = 'DENIED')`, `count_if(status = 'DENIED')` and `sum(CASE WHEN status = 'DENIED' THEN 1 ELSE 0 END)` equal?
# MAGIC 2. "Average paid amount": is `avg(CASE WHEN status = 'PAID' THEN amount ELSE 0 END)` correct?
# MAGIC 3. Is `count(payer)` equal to `count(*)`?

# COMMAND ----------

# DBTITLE 1,E12 Code
agg = spark.sql(f"""
    SELECT count(*)                                              AS all_rows,          -- every row
           count(payer)                                          AS payer_not_null,    -- skips NULL payer
           count(DISTINCT payer)                                 AS distinct_payers,   -- NULL is not counted
           count(DISTINCT patient_id)                            AS distinct_patients,
           approx_count_distinct(patient_id)                     AS approx_patients,   -- estimate (HyperLogLog)
           count(*) FILTER (WHERE status = 'DENIED')             AS denied_filter,
           count_if(status = 'DENIED')                           AS denied_count_if,
           sum(CASE WHEN status = 'DENIED' THEN 1 ELSE 0 END)    AS denied_case,
           round(avg(amount) FILTER (WHERE status = 'PAID'), 2)  AS avg_paid_filter,   -- right
           round(avg(CASE WHEN status = 'PAID' THEN amount END), 2)        AS avg_paid_case_null,  -- right: no ELSE
           round(avg(CASE WHEN status = 'PAID' THEN amount ELSE 0 END), 2) AS avg_paid_case_zero   -- WRONG: zeros count
    FROM {FQ}.claims_clean
""")
display(agg)
a = agg.first()
by_payer = spark.sql(f"""
    SELECT coalesce(payer, 'SELF-PAY') AS payer_label,           -- replace NULL with a label
           count(*) AS claims,
           count_if(status = 'DENIED') AS denied,
           round(100 * count_if(status = 'DENIED') / count(*), 1) AS denied_pct
    FROM {FQ}.claims_clean
    GROUP BY ALL                                                 -- = GROUP BY coalesce(payer, 'SELF-PAY')
    ORDER BY payer_label
""")
display(by_payer)
self_pay = by_payer.filter("payer_label = 'SELF-PAY'").first()["claims"]
print(a.asDict(), "self-pay claims:", self_pay)
METRICS["E12.all_rows"], METRICS["E12.payer_not_null"], METRICS["E12.distinct_payers"] = a["all_rows"], a["payer_not_null"], a["distinct_payers"]
METRICS["E12.distinct_patients"], METRICS["E12.approx_patients"] = a["distinct_patients"], a["approx_patients"]
METRICS["E12.denied_filter"], METRICS["E12.denied_count_if"], METRICS["E12.denied_case"] = a["denied_filter"], a["denied_count_if"], a["denied_case"]
METRICS["E12.avg_paid_filter"], METRICS["E12.avg_paid_case_null"] = str(a["avg_paid_filter"]), str(a["avg_paid_case_null"])
METRICS["E12.avg_paid_case_zero"], METRICS["E12.self_pay"] = str(a["avg_paid_case_zero"]), self_pay
assert a["denied_filter"] == a["denied_count_if"] == a["denied_case"]
assert a["avg_paid_filter"] == a["avg_paid_case_null"] and a["all_rows"] - a["payer_not_null"] == self_pay

# COMMAND ----------

# DBTITLE 1,E12 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `count(*)` = {{E12.all_rows}}, `count(payer)` = {{E12.payer_not_null}}: the difference is the {{E12.self_pay}} self-pay claims with a NULL payer. `count(DISTINCT payer)` = {{E12.distinct_payers}}: NULL is not counted as a value.
# MAGIC * `count(DISTINCT patient_id)` = {{E12.distinct_patients}}, `approx_count_distinct(patient_id)` = {{E12.approx_patients}}. The estimate is not exact even on this small table; the docs promise accuracy "within a default value of 5%".
# MAGIC * Denied claims: `FILTER` {{E12.denied_filter}}, `count_if` {{E12.denied_count_if}}, `sum(CASE … ELSE 0 END)` {{E12.denied_case}} — all equal.
# MAGIC * Average paid amount: `avg(amount) FILTER (WHERE status = 'PAID')` = {{E12.avg_paid_filter}} and `avg(CASE WHEN … THEN amount END)` = {{E12.avg_paid_case_null}} — equal. With `ELSE 0` the result is {{E12.avg_paid_case_zero}}: every claim that is not paid adds a 0 to the average.
# MAGIC
# MAGIC > **Key takeaway:** for counts, `FILTER`, `count_if` and `CASE … ELSE 0` are equivalent. For averages, leave out `ELSE` (or use `FILTER`), so that the other rows are NULL and skipped.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "`NOT IN (subquery)` and `NOT EXISTS` always give the same result." | One NULL in the subquery and `NOT IN` returns **{{E1.not_in_rows}}** rows; `NOT EXISTS` / `LEFT ANTI` returned {{E1.anti_rows}} (E1, *NULL semantics*). |
# MAGIC | 2 | "`INNER JOIN` answers 'patients who had an ER visit'." | It returns one row per visit ({{E2.inner_rows}} rows); `LEFT SEMI JOIN` returns each patient once ({{E2.semi_rows}}) (E2). |
# MAGIC | 3 | "`LEFT SEMI JOIN` gives columns from both tables." | Only left-side columns: `{{E2.semi_columns}}` (E2). |
# MAGIC | 4 | "A join on `a.code = b.code` matches rows where both codes are NULL." | `NULL = NULL` is `{{E3.null_eq_null}}`; use `<=>` (`{{E3.null_safe}}`) or `eqNullSafe` (E3). |
# MAGIC | 5 | "You can filter a window function in `WHERE`." | Error `{{E4.where_window_error}}`. Use `QUALIFY` or a subquery (E4). |
# MAGIC | 6 | "`WHERE count(*) > 300` filters groups." | Error `{{E4.where_agg_error}}`. Groups are filtered with `HAVING` (E4). |
# MAGIC | 7 | "`RANK() = 1` keeps one row per key." | Ties share rank 1 ({{E7.top_ties}} A1C rows had rank 1 in E7). Use `ROW_NUMBER()` for dedup. |
# MAGIC | 8 | "`SUM(x) OVER (PARTITION BY p ORDER BY d)` is a row-by-row running total." | With ties in `d`, {{E6.default_ne_rows}} rows differed from the `ROWS` version; the default behaved like `RANGE` on all {{E6.default_eq_range}} rows (E6). |
# MAGIC | 9 | "`SUM(x) OVER (PARTITION BY p)` without `ORDER BY` is a running total." | It is the partition total on every row (E6). |
# MAGIC | 10 | "In a `ROLLUP` result, `payer IS NULL` marks the subtotal rows." | Real NULL payers also exist ({{E8.self_pay_rows}} self-pay rows). Use `grouping(payer)` (E8). |
# MAGIC | 11 | "`PIVOT` groups only by the columns I care about." | Every other input column is a grouping column: pivoting the whole lab table gave {{E9.trap_rows}} rows instead of {{E9.wide_rows}} (E9). |
# MAGIC | 12 | "`UNPIVOT` keeps NULL cells." | Default is `EXCLUDE NULLS`: {{E9.long_rows}} rows vs {{E9.long_all_rows}} with `INCLUDE NULLS` (E9, *UNPIVOT clause*). |
# MAGIC | 13 | "`UNION` keeps all rows." | `UNION` is `UNION DISTINCT`: {{E11.union_rows}} rows vs {{E11.union_all_rows}} for `UNION ALL` (E11, *Set operators*). PySpark `union()` is `UNION ALL`. |
# MAGIC | 14 | "`avg(CASE WHEN status = 'PAID' THEN amount ELSE 0 END)` is the average paid amount." | The zeros are averaged too: {{E12.avg_paid_case_zero}} vs the correct {{E12.avg_paid_filter}} (E12). |
# MAGIC | 15 | "`count(col)` counts all rows." | It skips NULLs: `count(payer)` = {{E12.payer_not_null}}, `count(*)` = {{E12.all_rows}} (E12). |
# MAGIC | 16 | "`datediff(start, end)` gives a positive number of days." | The order is `datediff(endDate, startDate)`; the swapped call was negative on {{E5.swapped_negative}} rows (E5, *datediff function*). |

# COMMAND ----------

# DBTITLE 1,7 — Syntax Cheat Sheet
# MAGIC %md
# MAGIC ## 7 Syntax Cheat Sheet
# MAGIC
# MAGIC | Goal | SQL | PySpark (`from pyspark.sql import Window, functions as F`) |
# MAGIC | --- | --- | --- |
# MAGIC | Rows with a match (no duplicates) | `a LEFT SEMI JOIN b ON a.k = b.k` | `a.join(b, "k", "left_semi")` |
# MAGIC | Rows without a match | `a LEFT ANTI JOIN b ON a.k = b.k` / `NOT EXISTS` | `a.join(b, "k", "left_anti")` |
# MAGIC | Every combination | `a CROSS JOIN b` | `a.crossJoin(b)` |
# MAGIC | NULL-safe join | `ON a.k <=> b.k` | `a.join(b, a.k.eqNullSafe(b.k))` |
# MAGIC | Dedup, newest wins | `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1` | `df.withColumn("rn", F.row_number().over(Window.partitionBy("id").orderBy(F.desc("updated_at")))).filter("rn = 1")` |
# MAGIC | Previous / next value | `LAG(d) OVER (PARTITION BY p ORDER BY d)`, `LEAD(...)` | `F.lag("d").over(w)`, `F.lead("d").over(w)` |
# MAGIC | Running total | `SUM(x) OVER (PARTITION BY p ORDER BY d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | `w.rowsBetween(Window.unboundedPreceding, Window.currentRow)` |
# MAGIC | Rank with ties | `RANK()`, `DENSE_RANK()` | `F.rank()`, `F.dense_rank()` |
# MAGIC | Conditional count | `count(*) FILTER (WHERE c)`, `count_if(c)` | `F.count_if(cond)`, `F.count(F.when(cond, 1))` |
# MAGIC | Distinct estimate | `approx_count_distinct(x)` | `F.approx_count_distinct("x")` |
# MAGIC | Subtotals | `GROUP BY ROLLUP (a, b)`, `CUBE (a, b)`, `GROUPING SETS ((a), (b))`; `grouping(a)` | `df.rollup("a", "b")`, `df.cube("a", "b")`; `F.grouping("a")` |
# MAGIC | Group by all non-aggregates | `GROUP BY ALL` | — (list the columns) |
# MAGIC | Rows → columns | `FROM (SELECT k, cat, v FROM t) PIVOT (avg(v) FOR cat IN ('A' AS a, 'B' AS b))` | `df.groupBy("k").pivot("cat", ["A", "B"]).agg(F.avg("v"))` |
# MAGIC | Columns → rows | `UNPIVOT [INCLUDE NULLS] (v FOR cat IN (a AS A, b AS B))` | `df.unpivot("k", ["a", "b"], "cat", "v")` |
# MAGIC | Array per group | `collect_list(x)`, `collect_set(x)`, `array_sort(...)` | `F.collect_list("x")`, `F.array_sort(...)` |
# MAGIC | Array lambdas | `filter(arr, v -> v > 1)`, `transform(...)`, `exists(...)`, `aggregate(arr, 0D, (acc, v) -> acc + v)` | `F.filter("arr", lambda v: v > 1)`, `F.transform`, `F.exists`, `F.aggregate("arr", F.lit(0.0), lambda a, v: a + v)` |
# MAGIC | Stack results | `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT` | `union()` (= ALL), `.distinct()`, `intersect()`, `exceptAll()` / `subtract()` |
# MAGIC | NULL handling | `coalesce(a, 'X')`, `nvl(a, b)`, `nullif(a, b)` | `F.coalesce("a", F.lit("X"))` |
# MAGIC | Days between dates | `datediff(end, start)` | `F.datediff("end", "start")` |
# MAGIC | First day of month | `trunc(d, 'MONTH')` (DATE), `date_trunc('MONTH', ts)` (TIMESTAMP) | `F.trunc("d", "month")`, `F.date_trunc("month", "ts")` |
# MAGIC | Named query part | `WITH x AS (...) SELECT ...` | assign a DataFrame to a variable |

# COMMAND ----------

# DBTITLE 1,8 — Check Your Understanding
# MAGIC %md
# MAGIC ## 8 Check Your Understanding
# MAGIC
# MAGIC Answer in your own words. Answers are in section 11.
# MAGIC
# MAGIC 1. A report "patients without any lab result" returned zero rows last night, although new patients were registered. The query uses `NOT IN (SELECT patient_id FROM lab_results)`. What probably changed in the data, and how do you make the query robust?
# MAGIC 2. You deduplicate a CDC feed of claims before a `MERGE`. Why `ROW_NUMBER()` and not `RANK()`? What must the `ORDER BY` of the window contain so that the result is the same every run?
# MAGIC 3. A finance analyst says the "running total of charges per patient" sometimes stays the same for two rows and then jumps. Explain why, and give the fix.

# COMMAND ----------

# DBTITLE 1,9 — Practice (intro)
# MAGIC %md
# MAGIC ## 9 Practice
# MAGIC
# MAGIC Do these yourself, without AI help. Each task has a validation cell that prints ✅ Passed or a hint. Solutions are in section 11.
# MAGIC
# MAGIC > The practice uses the setup tables only. Run section 3 first.

# COMMAND ----------

# DBTITLE 1,P1 Task
# MAGIC %md
# MAGIC ### P1 — Fill in the blanks (3 blanks): visit number and running charge
# MAGIC Create `main.dbacademy_l03.p1_visits` with one row per encounter (only rows with a `patient_id`): `visit_no` (1, 2, 3, … per patient in date order, ties broken by `encounter_id`) and `running_charge` (the sum of `charge` of this and all earlier visits of the patient). Replace the three `<FILL_IN>` markers.

# COMMAND ----------

# DBTITLE 1,P1 Code
# >>> BLANK P1
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p1_visits AS
    SELECT patient_id, encounter_id, encounter_date, charge,
           <FILL_IN>() OVER (PARTITION BY patient_id ORDER BY encounter_date, encounter_id) AS visit_no,   -- 1: unique numbers
           SUM(charge) OVER (<FILL_IN> patient_id                                                          -- 2: one window per patient
                             ORDER BY encounter_date, encounter_id
                             <FILL_IN> BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_charge      -- 3: count physical rows
    FROM {FQ}.encounters
    WHERE patient_id IS NOT NULL
""")
# >>> SOLUTION P1
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p1_visits AS
    SELECT patient_id, encounter_id, encounter_date, charge,
           ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY encounter_date, encounter_id) AS visit_no,   -- 1: unique numbers
           SUM(charge) OVER (PARTITION BY patient_id                                                         -- 2: one window per patient
                             ORDER BY encounter_date, encounter_id
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_charge            -- 3: count physical rows
    FROM {FQ}.encounters
    WHERE patient_id IS NOT NULL
""")
# <<< END P1

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    t = spark.table(f"{FQ}.p1_visits")
    w_check = Window.partitionBy("patient_id").orderBy("encounter_date", "encounter_id")
    c = (t.withColumn("prev_rc", F.lag("running_charge").over(w_check))
          .withColumn("total", F.sum("charge").over(Window.partitionBy("patient_id")))
          .withColumn("n", F.count("*").over(Window.partitionBy("patient_id")))
          .selectExpr("count(*) AS n_rows",
                      "count(DISTINCT patient_id, visit_no) AS distinct_visits",
                      "count_if(visit_no = n AND running_charge <> total) AS bad_last",
                      "count_if(running_charge <= prev_rc) AS not_increasing").first())
    ok = c["n_rows"] == 1980 and c["distinct_visits"] == 1980 and c["bad_last"] == 0 and c["not_increasing"] == 0
    PRACTICE_PASSED["P1"] = ok
    print("✅ Passed" if ok else
          f"Hint: want 1980 rows, 1980 distinct (patient_id, visit_no), last running_charge = patient total, and a running "
          f"charge that grows on every row; got {c.asDict()}. Ties on the same day need a unique number and a ROWS frame.")
    METRICS["P1.rows"] = c["n_rows"]
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: the table main.dbacademy_l03.p1_visits does not exist yet. Check the three blanks.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs)
# MAGIC The cell builds two tables for the care-gap team:
# MAGIC * `p2_no_labs` — patients who never had a lab result (the setup made exactly **10** of them);
# MAGIC * `p2_provider_running` — per provider, the running total of `charge` over the year, one **different** value per encounter (ties on the same day broken by `encounter_id`).
# MAGIC
# MAGIC Each query has one bug. Fix them in place.

# COMMAND ----------

# DBTITLE 1,P2 Code
# >>> BLANK P2
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p2_no_labs AS
    SELECT patient_id
    FROM {FQ}.patients
    WHERE patient_id NOT IN (SELECT patient_id FROM {FQ}.lab_results)
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p2_provider_running AS
    SELECT provider_id, encounter_id, encounter_date, charge,
           SUM(charge) OVER (PARTITION BY provider_id ORDER BY encounter_date) AS running_total
    FROM {FQ}.encounters
    WHERE provider_id IS NOT NULL
""")
# >>> SOLUTION P2
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p2_no_labs AS
    SELECT p.patient_id
    FROM {FQ}.patients p
    LEFT ANTI JOIN {FQ}.lab_results l ON p.patient_id = l.patient_id     -- fix 1: NULL-safe "no match"
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p2_provider_running AS
    SELECT provider_id, encounter_id, encounter_date, charge,
           SUM(charge) OVER (PARTITION BY provider_id ORDER BY encounter_date, encounter_id
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total   -- fix 2: ROWS + tie-breaker
    FROM {FQ}.encounters
    WHERE provider_id IS NOT NULL
""")
# <<< END P2

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    no_labs = sorted(r["patient_id"] for r in spark.table(f"{FQ}.p2_no_labs").collect())
    w_check = Window.partitionBy("provider_id").orderBy("encounter_date", "encounter_id")
    dup_totals = (spark.table(f"{FQ}.p2_provider_running")
                  .withColumn("prev_total", F.lag("running_total").over(w_check))
                  .filter("running_total = prev_total").count())          # equal totals on two rows = tie problem
    ok_a, ok_b = no_labs == list(range(1, 11)), dup_totals == 0
    ok = ok_a and ok_b
    PRACTICE_PASSED["P2"] = ok
    if ok:
        print("✅ Passed")
    if not ok_a:
        print(f"Hint (p2_no_labs): want patients 1-10, got {len(no_labs)} rows. Does lab_results.patient_id contain NULLs?")
    if not ok_b:
        print(f"Hint (p2_provider_running): {dup_totals} rows repeat the running total of the row before. "
              "Which frame do you get when you write none, and what happens on ties?")
    METRICS["P2.no_labs"], METRICS["P2.dup_totals"] = len(no_labs), dup_totals
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: run the P2 code cell first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): 30-day readmission flag
# MAGIC Create `main.dbacademy_l03.p3_readmissions` with **one row per `INPATIENT` encounter** that has a `patient_id`, and the columns
# MAGIC `encounter_id, patient_id, encounter_date, discharge_date, next_admit_date, days_to_next, readmit_30d`:
# MAGIC * `next_admit_date` — the `encounter_date` of the **next INPATIENT** encounter of the same patient (order by `encounter_date`, then `encounter_id`), or NULL;
# MAGIC * `days_to_next` — days from `discharge_date` to `next_admit_date`;
# MAGIC * `readmit_30d` — `true` if `days_to_next <= 30`, otherwise `false` (also `false` when there is no next admission).
# MAGIC
# MAGIC **Hints:** filter first, then the window (`WHERE` runs before window functions) · `LEAD` · `datediff(end, start)` · `coalesce(…, false)`.

# COMMAND ----------

# DBTITLE 1,P3 Code
# >>> BLANK P3
# Your code here
# >>> SOLUTION P3
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p3_readmissions AS
    WITH stays AS (
        SELECT encounter_id, patient_id, encounter_date, discharge_date,
               LEAD(encounter_date) OVER (PARTITION BY patient_id
                                          ORDER BY encounter_date, encounter_id) AS next_admit_date
        FROM {FQ}.encounters
        WHERE encounter_type = 'INPATIENT' AND patient_id IS NOT NULL)
    SELECT *,
           datediff(next_admit_date, discharge_date) AS days_to_next,
           coalesce(datediff(next_admit_date, discharge_date) <= 30, false) AS readmit_30d
    FROM stays
""")
# <<< END P3

# COMMAND ----------

# DBTITLE 1,P3 Validation
try:
    t = spark.table(f"{FQ}.p3_readmissions")
    expected = spark.sql(f"""
        WITH ip AS (SELECT * FROM {FQ}.encounters WHERE encounter_type = 'INPATIENT' AND patient_id IS NOT NULL),
        nxt AS (SELECT a.encounter_id, a.discharge_date,
                       min(struct(b.encounter_date, b.encounter_id)).encounter_date AS next_date
                FROM ip a LEFT JOIN ip b
                  ON a.patient_id = b.patient_id
                 AND struct(b.encounter_date, b.encounter_id) > struct(a.encounter_date, a.encounter_id)
                GROUP BY a.encounter_id, a.discharge_date)
        SELECT encounter_id, coalesce(datediff(next_date, discharge_date) <= 30, false) AS readmit_30d FROM nxt""")
    got = t.select("encounter_id", "readmit_30d")
    missing, extra = expected.exceptAll(got).count(), got.exceptAll(expected).count()
    rows, readmits = t.count(), t.filter("readmit_30d").count()
    ok = missing == 0 and extra == 0 and set(t.columns) >= {"next_admit_date", "days_to_next"}
    PRACTICE_PASSED["P3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {expected.count()} rows; {missing} expected (encounter_id, readmit_30d) pairs are missing and {extra} are wrong. "
          "Did you filter INPATIENT before LEAD? Is the flag false (not NULL) when there is no next stay?")
    METRICS["P3.rows"], METRICS["P3.readmits"] = rows, readmits
except Exception as e:
    PRACTICE_PASSED["P3"] = False
    print("Hint: the table main.dbacademy_l03.p3_readmissions does not exist yet.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints)
# MAGIC Create `main.dbacademy_l03.p4_latest_labs`: for every patient and test (ignore rows without `patient_id`), the **latest** lab result, with the columns
# MAGIC `patient_id, test_code, result_ts, result_value, prev_value, change_from_prev`. `prev_value` is the value of the test before the latest one (NULL if there is none) and `change_from_prev = result_value - prev_value`.

# COMMAND ----------

# DBTITLE 1,P4 Code
# >>> BLANK P4
# Your code here
# >>> SOLUTION P4
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p4_latest_labs AS
    SELECT patient_id, test_code, result_ts, result_value,
           LAG(result_value) OVER w AS prev_value,
           result_value - LAG(result_value) OVER w AS change_from_prev
    FROM {FQ}.lab_results
    WHERE patient_id IS NOT NULL
    WINDOW w AS (PARTITION BY patient_id, test_code ORDER BY result_ts)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY patient_id, test_code ORDER BY result_ts DESC) = 1
""")
# <<< END P4

# COMMAND ----------

# DBTITLE 1,P4 Validation
try:
    t = spark.table(f"{FQ}.p4_latest_labs")
    expected = spark.sql(f"""
        SELECT patient_id, test_code, h[0].result_ts AS result_ts, h[0].result_value AS result_value,
               round(h[0].result_value - get(h, 1).result_value, 4) AS change   -- get() is NULL when there is no element 2
        FROM (SELECT patient_id, test_code,
                     sort_array(collect_list(struct(result_ts, result_value)), false) AS h   -- newest first
              FROM {FQ}.lab_results WHERE patient_id IS NOT NULL GROUP BY patient_id, test_code)""")
    got = t.selectExpr("patient_id", "test_code", "result_ts", "result_value", "round(change_from_prev, 4) AS change")
    missing, extra = expected.exceptAll(got).count(), got.exceptAll(expected).count()
    rows, no_prev = t.count(), t.filter("prev_value IS NULL").count()
    ok = missing == 0 and extra == 0
    PRACTICE_PASSED["P4"] = ok
    print("✅ Passed" if ok else f"Not yet: {missing} expected rows are missing and {extra} rows are wrong (want {expected.count()} rows).")
    METRICS["P4.rows"], METRICS["P4.no_prev"] = rows, no_prev
except Exception as e:
    PRACTICE_PASSED["P4"] = False
    print("The table main.dbacademy_l03.p4_latest_labs does not exist yet.", error_class(e))

# COMMAND ----------

# DBTITLE 1,10 — Exam-Style Questions
# MAGIC %md
# MAGIC ## 10 Exam-Style Questions
# MAGIC
# MAGIC Choose the **one best answer**. Answers and explanations are in section 11.
# MAGIC
# MAGIC **Q1.** A care-gap query lists patients with no visit: `SELECT patient_id FROM patients WHERE patient_id NOT IN (SELECT patient_id FROM encounters)`. Some walk-in encounters have `patient_id = NULL`. What does the query return?
# MAGIC A. All patients without a visit · B. No rows · C. An error · D. All patients
# MAGIC
# MAGIC **Q2.** A claims CDC feed has several versions of each `claim_id` with different `updated_at`. Before a `MERGE`, the engineer needs exactly one row per claim, the newest. Which query is correct?
# MAGIC A. `… QUALIFY RANK() OVER (PARTITION BY claim_id ORDER BY updated_at) = 1` · B. `… QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1` · C. `… WHERE ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY updated_at DESC) = 1` · D. `SELECT DISTINCT claim_id, max(updated_at) FROM claims`
# MAGIC
# MAGIC **Q3.** `SUM(charge) OVER (PARTITION BY patient_id ORDER BY encounter_date)` shows the same running total on two visits of one patient on the same day. Why?
# MAGIC A. A bug in Delta · B. Without a frame clause, rows with the same `encounter_date` are treated as one group (like `RANGE`), so both get the total up to the end of that day · C. `SUM` skips the second row · D. The window has no `PARTITION BY`
# MAGIC
# MAGIC **Q4.** A revenue report uses `GROUP BY ROLLUP (payer, status)`. Self-pay claims have `payer = NULL`. How does the engineer label only the real subtotal rows as "ALL PAYERS"?
# MAGIC A. `CASE WHEN payer IS NULL THEN 'ALL PAYERS' END` · B. `coalesce(payer, 'ALL PAYERS')` · C. `CASE WHEN grouping(payer) = 1 THEN 'ALL PAYERS' ELSE coalesce(payer, 'SELF-PAY') END` · D. `nullif(payer, 'ALL PAYERS')`
# MAGIC
# MAGIC **Q5.** A population-health team needs the patients (all patient columns, one row each) who had at least one ER encounter. Which is best?
# MAGIC A. `patients INNER JOIN encounters … WHERE encounter_type = 'ER'` · B. `patients LEFT SEMI JOIN encounters ON … AND encounter_type = 'ER'` · C. `patients LEFT JOIN encounters …` · D. `patients CROSS JOIN encounters`
# MAGIC
# MAGIC **Q6.** An engineer runs `SELECT * FROM lab_results PIVOT (avg(result_value) FOR test_code IN ('A1C' AS a1c, 'LDL' AS ldl))` and expects one row per patient. The result has about as many rows as the table. Why?
# MAGIC A. PIVOT does not aggregate · B. `lab_id` and `result_ts` are also grouping columns, because every input column not used in the PIVOT groups the result · C. The IN list must be columns, not literals · D. PIVOT needs `GROUP BY ALL`
# MAGIC
# MAGIC **Q7.** A dashboard shows "average paid claim amount" as `avg(CASE WHEN status = 'PAID' THEN amount ELSE 0 END)`. Finance says it is too low. Which expression is correct?
# MAGIC A. `sum(amount) / count(*)` · B. `avg(amount) FILTER (WHERE status = 'PAID')` · C. `avg(coalesce(amount, 0))` · D. `count_if(status = 'PAID')`
# MAGIC
# MAGIC **Q8.** Encounters with a missing diagnosis must join to the reference row `code = NULL` ("Not coded") in `diagnosis_codes`. Which join condition does this?
# MAGIC A. `ON e.diagnosis_code = d.code` · B. `ON e.diagnosis_code <=> d.code` · C. `ON e.diagnosis_code IN (d.code)` · D. `ON coalesce(e.diagnosis_code, '') = d.code`

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — append with a new column (`mergeSchema`)**
# MAGIC {{solution:W1}}
# MAGIC
# MAGIC **W2 — CHECK constraint after cleanup**
# MAGIC {{solution:W2}}
# MAGIC
# MAGIC **W3 — SHALLOW CLONE**
# MAGIC {{solution:W3}}
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. A row with `patient_id = NULL` probably arrived in `lab_results` (for example a lab result that is not matched to a patient yet). One NULL in the subquery makes `NOT IN` UNKNOWN for every patient, so no row qualifies (E1: {{E1.not_in_rows}} rows instead of {{E1.anti_rows}}). Use `LEFT ANTI JOIN` or `NOT EXISTS`, which ignore NULL keys; or add `WHERE patient_id IS NOT NULL` inside the subquery.
# MAGIC 2. `RANK()` gives the same rank to ties, so two versions with the same `updated_at` would both survive and the `MERGE` would fail or update twice. `ROW_NUMBER()` always returns exactly one row with 1 per key (E4: {{E4.clean_rows}} rows for {{setup.distinct_claims}} claim ids). The `ORDER BY` must be unique inside the partition — add a tie-breaker such as a sequence number or file offset after `updated_at DESC` — otherwise the winner can change from run to run.
# MAGIC 3. With `ORDER BY encounter_date` and no frame, rows with the same date are peers: each gets the total up to the end of that day (E6: {{E6.default_ne_rows}} rows differed from the `ROWS` version). Add a unique tie-breaker to `ORDER BY` and write `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
# MAGIC
# MAGIC ### P1
# MAGIC {{solution:P1}}
# MAGIC
# MAGIC ### P2 (fixes: `LEFT ANTI JOIN` instead of `NOT IN`; tie-breaker + `ROWS` frame)
# MAGIC {{solution:P2}}
# MAGIC
# MAGIC ### P3
# MAGIC {{solution:P3}}
# MAGIC
# MAGIC ### P4
# MAGIC {{solution:P4}}
# MAGIC
# MAGIC `LAG` is computed before `QUALIFY` filters the rows (logical order, section 4.1), so the latest row still sees the value before it.
# MAGIC
# MAGIC ### Exam answers
# MAGIC
# MAGIC **Q1 → B.** One NULL in the subquery makes `NOT IN` UNKNOWN (*NULL semantics*); E1 returned {{E1.not_in_rows}} rows. A wrong: that is what `NOT EXISTS` returns ({{E1.not_exists_rows}} rows). C wrong: no error. D wrong: `NOT IN` never returns more rows because of a NULL.
# MAGIC
# MAGIC **Q2 → B.** `ROW_NUMBER` + `DESC` keeps exactly the newest row (E4: {{E4.clean_rows}} rows, {{E4.not_latest}} not newest). A wrong: ascending order keeps the oldest, and `RANK` keeps ties. C wrong: error `{{E4.where_window_error}}`. D wrong: even with a `GROUP BY claim_id` it returns only the key and a timestamp, not the claim rows that `MERGE` needs.
# MAGIC
# MAGIC **Q3 → B.** E6: the default result was equal to the explicit `RANGE` frame on {{E6.default_eq_range}} of {{E6.rows}} rows and differed from the `ROWS` frame on {{E6.default_ne_rows}} rows. A wrong: expected behaviour. C wrong: both rows are summed. D wrong: the window has `PARTITION BY`.
# MAGIC
# MAGIC **Q4 → C.** `grouping(payer)` is 1 only on rows where `payer` was aggregated away (E8: {{E8.grand_total_rows}} grand-total row, {{E8.self_pay_rows}} real self-pay rows). A and B wrong: they also relabel the real self-pay rows. D wrong: `nullif` makes NULLs, it does not label them.
# MAGIC
# MAGIC **Q5 → B.** `LEFT SEMI JOIN` returns each patient once with only patient columns (E2: {{E2.semi_rows}} rows). A wrong: one row per ER visit ({{E2.inner_rows}} rows). C wrong: keeps every patient. D wrong: Cartesian product.
# MAGIC
# MAGIC **Q6 → B.** Every input column that is not in the aggregate or the `FOR` column groups the result (*PIVOT clause*; E9: {{E9.trap_rows}} rows instead of {{E9.wide_rows}}). Select only `patient_id, test_code, result_value` first. A wrong: PIVOT aggregates. C wrong: the IN list must be literals. D wrong: PIVOT groups implicitly.
# MAGIC
# MAGIC **Q7 → B.** E12: `FILTER` gave {{E12.avg_paid_filter}}, the `ELSE 0` version {{E12.avg_paid_case_zero}}. A wrong: divides by all claims. C wrong: averages all claims. D wrong: a count, not an average.
# MAGIC
# MAGIC **Q8 → B.** `<=>` is true when both sides are NULL (*NULL semantics*; E3: {{E3.nullsafe_rows}} rows vs {{E3.eq_rows}} with `=`). A wrong: `NULL = NULL` is `{{E3.null_eq_null}}`. C wrong: `IN` uses `=`. D wrong: `''` does not equal the NULL code either.

# COMMAND ----------

# DBTITLE 1,12 — Cleanup
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")        # drops all tables and the volume
print(f"Dropped {FQ}.")

# COMMAND ----------

# DBTITLE 1,13 — Final Summary
# MAGIC %md
# MAGIC ## 13 Final Summary (numbers from the real run)
# MAGIC
# MAGIC | Example | Key result |
# MAGIC | --- | --- |
# MAGIC | Setup | {{setup.patients}} patients, {{setup.providers}} providers, {{setup.encounters}} encounters, {{setup.claims}} claim rows ({{setup.distinct_claims}} claims), {{setup.labs}} labs, {{setup.codes}} codes |
# MAGIC | E1 no-match | `NOT IN` + NULL {{E1.not_in_rows}} rows · `LEFT ANTI` {{E1.anti_rows}} · `NOT EXISTS` {{E1.not_exists_rows}} |
# MAGIC | E2 semi / cross | `INNER` {{E2.inner_rows}} rows · `LEFT SEMI` {{E2.semi_rows}} · grid {{E2.grid_cells}} cells |
# MAGIC | E3 NULL keys | `=` {{E3.eq_rows}} rows · `<=>` {{E3.nullsafe_rows}} rows · `NULL = NULL` → {{E3.null_eq_null}} |
# MAGIC | E4 dedup | `QUALIFY` {{E4.clean_rows}} rows · window in `WHERE` `{{E4.where_window_error}}` · aggregate in `WHERE` `{{E4.where_agg_error}}` |
# MAGIC | E5 `LAG` | {{E5.first_visits}} first visits · {{E5.same_day}} same-day gaps · average gap {{E5.avg_gap}} days |
# MAGIC | E6 frames | default ≠ `ROWS` on {{E6.default_ne_rows}} rows · default = `RANGE` on {{E6.default_eq_range}}/{{E6.rows}} rows |
# MAGIC | E7 ranking | ≤ 3: `ROW_NUMBER` {{E7.rn3}} · `RANK` {{E7.rk3}} · `DENSE_RANK` {{E7.dr3}} |
# MAGIC | E8 subtotals | `ROLLUP` {{E8.rollup_rows}} · `CUBE` {{E8.cube_rows}} · `GROUPING SETS` {{E8.sets_rows}} rows; `payer IS NULL` on {{E8.payer_null_rows}} rows, only {{E8.grand_total_rows}} is a grand total |
# MAGIC | E9 pivot | wide {{E9.wide_rows}} rows (trap: {{E9.trap_rows}}) · `UNPIVOT` {{E9.long_rows}} / `INCLUDE NULLS` {{E9.long_all_rows}} |
# MAGIC | E10 arrays | {{E10.with_diabetic}} of {{E10.patients}} patients with an A1C ≥ 6.5 · `INT` start → `{{E10.int_start_error}}` |
# MAGIC | E11 set ops | `UNION ALL` {{E11.union_all_rows}} · `UNION` {{E11.union_rows}} · `EXCEPT` {{E11.except_rows}} |
# MAGIC | E12 FILTER / CASE | avg paid {{E12.avg_paid_filter}} vs `ELSE 0` {{E12.avg_paid_case_zero}} · exact distinct {{E12.distinct_patients}} vs approx {{E12.approx_patients}} |
# MAGIC | Practice | P1 {{P1.rows}} rows · P2 {{P2.no_labs}} patients without labs · P3 {{P3.readmits}} readmissions in {{P3.rows}} stays · P4 {{P4.rows}} latest labs ({{P4.no_prev}} without a previous value) |
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **The logical order of query clauses (4.1)** — standard SQL; the Databricks *SELECT* and *Query* reference pages list the clauses but do not state the order. This notebook only shows its effects (E4 errors, E5 `WHERE` before the window, P4 `LAG` before `QUALIFY`).
# MAGIC * **`RIGHT`, `FULL OUTER`, `NATURAL` and `LATERAL` joins, `NTILE`, `PERCENT_RANK`, `CUME_DIST`, `FIRST_VALUE`, `LAST_VALUE`, `collect_set`, `forall`** — described from the *JOIN* and *Window functions* reference pages; no example runs them.
# MAGIC * **The 5% accuracy of `approx_count_distinct`** — from the docs; E12 shows one estimate, not the error bound.
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. `approx_count_distinct` was not exact even for {{E12.distinct_patients}} distinct patients: it returned {{E12.approx_patients}} (E12).
# MAGIC 2. The *Window frame clause* page does not state the default frame. The run showed that `ORDER BY` without a frame behaved like `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` on all {{E6.rows}} rows (E6).
# MAGIC 3. The first version of E9 expected NULLs only in `tsh`. The real data also had patients without an A1C or LDL result, so `UNPIVOT` dropped {{E9.null_cells}} cells, not {{E9.tsh_null}}.
# MAGIC 4. `aggregate` does not widen an `INT` start value to `DOUBLE`; it fails with `{{E10.int_start_error}}` (E10).
# MAGIC 5. The PySpark `unpivot` kept the NULL cells ({{E9.long_py_rows}} rows), while SQL `UNPIVOT` drops them by default (E9).
