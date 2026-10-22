# Databricks notebook source
# LESSON_TITLE: Lakeflow Declarative Pipelines I - Medallion, Streaming Tables, Materialized Views and Expectations

# COMMAND ----------

# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L07 — Lakeflow Declarative Pipelines I: medallion, streaming tables, materialized views, expectations
# MAGIC
# MAGIC **Exam objectives**
# MAGIC
# MAGIC * Professional (Oct 9, 2026), Section 1 *Developing code* (23%): "Create production-ready data pipelines for streaming data using Lakeflow Pipelines and Auto Loader." L06 covered Auto Loader; this lesson covers the pipeline part.
# MAGIC * Professional Section 3 *Data Transformation, Cleansing, and Quality* (12%): data quality rules with expectations (warn, drop, fail) and a quarantine table for bad rows.
# MAGIC * Professional Section 4 *Monitoring & alerting* (10%): read pipeline updates, row counts and data quality results from the pipeline **event log**.
# MAGIC
# MAGIC **Names.** The product is called **Lakeflow Declarative Pipelines** (formerly **Delta Live Tables, DLT**). The current docs also say "Lakeflow pipelines" or "Lakeflow Spark Declarative Pipelines"; the open-source part in Apache Spark 4.1 is "Spark Declarative Pipelines" (*Lakeflow pipelines Python language reference*). All names mean the same thing in this lesson.
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** pipeline SQL (`CREATE OR REFRESH STREAMING TABLE`, `CREATE OR REFRESH MATERIALIZED VIEW`, `CREATE TEMPORARY VIEW`, `STREAM`, `CONSTRAINT … EXPECT`) and the Python API (`from pyspark import pipelines as dp`) to build a bronze → silver → gold pipeline.
# MAGIC 2. **Explain** what a pipeline update does with a streaming table (only new input) and with a materialized view (the current result of its query), and what a full refresh changes.
# MAGIC 3. **Compare** streaming table vs materialized view vs temporary view, the three expectation actions (warn, `DROP ROW`, `FAIL UPDATE`), and Structured Streaming in a notebook vs a declarative pipeline.
# MAGIC 4. **Implement** a serverless pipeline from a notebook with the Databricks SDK: write the source files, create the pipeline, start updates (normal, full refresh, dry run) and wait for them.
# MAGIC 5. **Diagnose** pipeline results with the `event_log()` function: update states, rows per flow, passed/failed/dropped records per expectation, and the error of a failed update.
# MAGIC
# MAGIC > **Evidence-only rule:** every number, state and error class in this notebook comes from a real run of this notebook (the build fills them in), or from an official Databricks doc page named in the text. Things that cannot be tested on this workspace are marked **NOT VERIFIED — concept only (reason)**.
# MAGIC
# MAGIC > **Run time:** this lesson starts real pipeline updates. Each update takes one to a few minutes (the build records the real durations in the final summary). Run the cells in order and wait for each one.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up (intro)
# MAGIC %md
# MAGIC ## 2 Warm-up — recall L06 Ingestion with Auto Loader, read_files and COPY INTO (no hints)
# MAGIC
# MAGIC Write each task from memory. Run the validation cell after each one. Solutions are in section 11.
# MAGIC
# MAGIC > Run the next cell first: it creates the lesson schema, the volume and a few small warm-up files.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up prep (run first)
spark.sql("CREATE SCHEMA IF NOT EXISTS main.dbacademy_l07")             # setup recreates the schema later
spark.sql("CREATE VOLUME IF NOT EXISTS main.dbacademy_l07.data")
WU = "/Volumes/main/dbacademy_l07/data/warmup"                           # warm-up files live here
WU_PROVIDERS = f"{WU}/providers_csv"                                     # 2 CSV files with a header row
WU_LABS = f"{WU}/labs_json"                                              # 2 JSON files with lab results
WU_CHECKPOINTS = f"{WU}/_checkpoints"                                    # checkpoints of the warm-up streams

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
    found = re.search(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", str(e))
    return found.group(1) if found else type(e).__name__

dbutils.fs.rm(WU, True)                                                  # start the warm-up from zero
for t in ("wu1_providers", "wu2_labs", "wu3_rescued"):
    spark.sql(f"DROP TABLE IF EXISTS main.dbacademy_l07.{t}")
dbutils.fs.put(f"{WU_PROVIDERS}/providers_1.csv", "provider_id,provider_name,specialty\n" +
               "\n".join(f"{i},Dr Fake {i},General" for i in range(1, 6)) + "\n", overwrite=True)
dbutils.fs.put(f"{WU_PROVIDERS}/providers_2.csv", "provider_id,provider_name,specialty\n" +
               "\n".join(f"{i},Dr Fake {i},Cardiology" for i in range(6, 11)) + "\n", overwrite=True)
dbutils.fs.put(f"{WU_LABS}/labs_1.json", "\n".join(
    f'{{"lab_id": {i}, "patient_id": {100 + i}, "test_code": "4548-4", "value": {5 + i / 10}}}' for i in range(1, 6)) + "\n",
    overwrite=True)
dbutils.fs.put(f"{WU_LABS}/labs_2.json", "\n".join(
    f'{{"lab_id": {i}, "patient_id": {100 + i}, "test_code": "2345-7", "value": ' +
    ('"N/A"' if i in (7, 9) else str(90 + i)) + "}" for i in range(6, 11)) + "\n", overwrite=True)   # 2 bad values
print("Warm-up files:", [f.name for f in dbutils.fs.ls(WU_PROVIDERS)], [f.name for f in dbutils.fs.ls(WU_LABS)])

# COMMAND ----------

# DBTITLE 1,W1 Task
# MAGIC %md
# MAGIC ### W1 — `COPY INTO`, twice
# MAGIC The folder `WU_PROVIDERS` has two CSV files with a header row (`provider_id,provider_name,specialty`).
# MAGIC Create the table `main.dbacademy_l07.wu1_providers` and load **both** files into it with `COPY INTO`, so that `provider_id` is a number, not a string.
# MAGIC Then run exactly the same `COPY INTO` a second time and store its `num_inserted_rows` in the variable `w1_second_inserted`.

# COMMAND ----------

# DBTITLE 1,W1 Code
# >>> BLANK W1
# Your code here
# >>> SOLUTION W1
spark.sql("CREATE TABLE IF NOT EXISTS main.dbacademy_l07.wu1_providers")          # schemaless target
w1_sql = f"""
    COPY INTO main.dbacademy_l07.wu1_providers
    FROM '{WU_PROVIDERS}'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
"""
spark.sql(w1_sql)                                                                  # first load: both files
w1_second_inserted = spark.sql(w1_sql).first()["num_inserted_rows"]                # second load: files are skipped
# <<< END W1

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    t = spark.table("main.dbacademy_l07.wu1_providers")
    rows, types = t.count(), dict(t.dtypes)
    ok = rows == 10 and types.get("provider_id") in ("int", "bigint") and w1_second_inserted == 0
    WARMUP_PASSED["W1"] = ok
    print("✅ Passed" if ok else
          f"Hint: want 10 rows, provider_id int, and 0 rows from the second run; got {rows} rows, "
          f"provider_id {types.get('provider_id')}, w1_second_inserted = {w1_second_inserted}.")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create main.dbacademy_l07.wu1_providers and the variable w1_second_inserted first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — Auto Loader with a typed column
# MAGIC The folder `WU_LABS` has two JSON files with lab results (`lab_id`, `patient_id`, `test_code`, `value`). Some `value` entries are text.
# MAGIC Load **all** files into `main.dbacademy_l07.wu2_labs` with **Auto Loader**, in one run that stops by itself. Use `f"{WU_CHECKPOINTS}/wu2"` for the checkpoint and the schema location.
# MAGIC `lab_id` must be a number and `value` must be `DOUBLE`. The load must not fail, and no input value may be lost.

# COMMAND ----------

# DBTITLE 1,W2 Code
# >>> BLANK W2
# Your code here
# >>> SOLUTION W2
w2_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")                  # numbers instead of strings
    .option("cloudFiles.schemaHints", "value DOUBLE")               # force DOUBLE; text values go to _rescued_data
    .option("cloudFiles.schemaLocation", f"{WU_CHECKPOINTS}/wu2")
    .load(WU_LABS)
    .writeStream
    .option("checkpointLocation", f"{WU_CHECKPOINTS}/wu2")
    .trigger(availableNow=True)
    .toTable("main.dbacademy_l07.wu2_labs"))
w2_query.awaitTermination()
# <<< END W2

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    t = spark.table("main.dbacademy_l07.wu2_labs")
    rows, types = t.count(), dict(t.dtypes)
    ok = rows == 10 and types.get("lab_id") in ("int", "bigint") and types.get("value") == "double"
    WARMUP_PASSED["W2"] = ok
    print("✅ Passed" if ok else
          f"Hint: want 10 rows, lab_id int/bigint and value double; got {rows} rows, lab_id {types.get('lab_id')}, "
          f"value {types.get('value')}.")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: create main.dbacademy_l07.wu2_labs with Auto Loader first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — Find the values Auto Loader could not read
# MAGIC From `main.dbacademy_l07.wu2_labs`, create the table `main.dbacademy_l07.wu3_rescued` with two columns: `lab_id`, and `bad_value` — the **original text** of `value` for every row where it did not fit the `DOUBLE` type (a `STRING`).

# COMMAND ----------

# DBTITLE 1,W3 Code
# >>> BLANK W3
# Your code here
# >>> SOLUTION W3
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l07.wu3_rescued AS
    SELECT lab_id, _rescued_data:value::string AS bad_value     -- the original value is in the rescued JSON
    FROM main.dbacademy_l07.wu2_labs
    WHERE _rescued_data IS NOT NULL
""")
# <<< END W3

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    t = spark.table("main.dbacademy_l07.wu3_rescued")
    got = sorted((int(r["lab_id"]), r["bad_value"]) for r in t.collect())
    ok = got == [(7, "N/A"), (9, "N/A")] and t.columns == ["lab_id", "bad_value"]
    WARMUP_PASSED["W3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want [(7, 'N/A'), (9, 'N/A')] and the columns lab_id, bad_value; got {got}, {t.columns}.")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create main.dbacademy_l07.wu3_rescued first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent. It deletes the pipelines and files of an earlier run, recreates the schema,
# and lands fake claim files (no PHI).
import io, json, random, time
from databricks.sdk import WorkspaceClient                  # the Databricks SDK for Python (preinstalled)
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

CATALOG, SCHEMA = "main", "dbacademy_l07"
FQ = f"{CATALOG}.{SCHEMA}"                                   # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"                    # Unity Catalog volume for all files
LANDING = f"{VOL}/landing/claims"                            # the clearinghouse drops claim files here
PREFIX = "dbacademy_l07"                                     # every pipeline of this lesson starts with this name

w = WorkspaceClient()                                        # inside a notebook it signs in as you
ME = w.current_user.me().user_name
RUNTIME = f"/Users/{ME}/_lesson_runtime/L07"                 # workspace folder for the pipeline source files

def api(method, path, body=None, query=None):
    """Call the Databricks REST API as the notebook user; the SDK client adds the host and the token (4.7)."""
    return w.api_client.do(method, path, query=query, body=body) or {}

def lesson_pipelines():
    """{name: id} of every pipeline whose name starts with PREFIX."""
    found = api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{PREFIX}%'", "max_results": 100})
    return {p["name"]: p["pipeline_id"] for p in found.get("statuses", [])}

def delete_pipeline(pipeline_id):
    """Delete a pipeline. Its tables are kept by default (E6)."""
    api("DELETE", f"/api/2.0/pipelines/{pipeline_id}")

# 1. Delete the pipelines of an earlier run (their tables are kept; DROP SCHEMA below removes them)
old = lesson_pipelines()
for pipeline_id in old.values():
    delete_pipeline(pipeline_id)
# 2. Delete the runtime folder of an earlier run and create it again
try:
    w.workspace.delete(RUNTIME, recursive=True)
except NotFound:
    pass                                                     # nothing to delete
w.workspace.mkdirs(RUNTIME)
# 3. Recreate the schema and the volume
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")             # removes warm-up tables, volume and files
spark.sql(f"CREATE SCHEMA {FQ}")
spark.sql(f"CREATE VOLUME {FQ}.data")

# 4. Fake claim files
STATUSES = ["SUBMITTED", "APPROVED", "DENIED", "PENDING"]

def claim_row(claim_id):
    """One fake claim. The generator is seeded per claim, so a claim always gets the same values."""
    rng = random.Random(f"claim-{claim_id}")                 # isolated generator (string seeds are deterministic)
    return {"claim_id": claim_id,
            "patient_id": rng.randint(1, 50),
            "provider_id": rng.randint(1, 10),
            "claim_amount": round(rng.uniform(50, 5000), 2),
            "claim_status": rng.choice(STATUSES),
            "service_date": f"2026-{rng.randint(1, 9):02d}-{rng.randint(1, 28):02d}"}

def land_claims(file_name, first_id, n, negative_ids=(), null_id_ids=(), odd_status_ids=()):
    """Write one newline-delimited JSON file with n claims into LANDING, with some bad rows on purpose."""
    lines = []
    for cid in range(first_id, first_id + n):
        row = claim_row(cid)
        if cid in negative_ids:
            row["claim_amount"] = -row["claim_amount"]       # breaks the rule "amount > 0"
        if cid in null_id_ids:
            row["claim_id"] = None                           # breaks the rule "claim_id IS NOT NULL"
        if cid in odd_status_ids:
            row["claim_status"] = "IN_REVIEW"                # a status that is not in the list of known statuses
        lines.append(json.dumps(row))
    dbutils.fs.put(f"{LANDING}/{file_name}", "\n".join(lines) + "\n", overwrite=True)
    return n

land_claims("claims_001.json", 1, 100, negative_ids=(7, 57), null_id_ids=(13,), odd_status_ids=(21,))
land_claims("claims_002.json", 101, 100, negative_ids=(107, 157), null_id_ids=(113,), odd_status_ids=(121, 171))

# 5. A provider reference table (a normal Delta table, outside every pipeline)
SPECIALTIES = ["General", "Cardiology", "Neurology", "Pediatrics", "Oncology"]
spark.sql(f"""
    CREATE TABLE {FQ}.providers AS
    SELECT CAST(id AS INT) AS provider_id,
           concat('Dr Fake ', id) AS provider_name,
           element_at(array({", ".join(repr(s) for s in SPECIALTIES)}), CAST(id % 5 + 1 AS INT)) AS specialty
    FROM range(1, 11)""")

# Check what we created and record the real counts
claim_files = [f.name for f in dbutils.fs.ls(LANDING)]
claim_rows = spark.read.text(LANDING).count()                # one JSON object per line
provider_rows = spark.table(f"{FQ}.providers").count()
METRICS["nd.setup.old_pipelines_deleted"] = len(old)
METRICS["setup.claim_files"] = len(claim_files)
METRICS["setup.claim_rows"] = claim_rows
METRICS["setup.provider_rows"] = provider_rows
METRICS["nd.setup.sdk_version"] = __import__("databricks.sdk.version", fromlist=["__version__"]).__version__
assert claim_files == ["claims_001.json", "claims_002.json"] and claim_rows == 200 and provider_rows == 10
print(f"claim files: {claim_files}, claim rows: {claim_rows}, providers: {provider_rows}, runtime folder: {RUNTIME}")
display(spark.table(f"{FQ}.providers").orderBy("provider_id"))

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):** {{setup.claim_files}} JSON files with {{setup.claim_rows}} claims, and the Delta table `providers` with {{setup.provider_rows}} rows. The setup also deletes every pipeline whose name starts with `dbacademy_l07` (left by an earlier run) and the runtime folder, so you can run it again at any time.
# MAGIC
# MAGIC Some claims are bad **on purpose**, so the expectations have something to find:
# MAGIC
# MAGIC | Problem | Claims (by id) |
# MAGIC | --- | --- |
# MAGIC | negative `claim_amount` | 7, 57, 107, 157 |
# MAGIC | `claim_id` is NULL | the rows that would have ids 13 and 113 |
# MAGIC | unknown `claim_status` `IN_REVIEW` | 21, 121, 171 |
# MAGIC
# MAGIC The helper `land_claims(file_name, first_id, n, negative_ids, null_id_ids, odd_status_ids)` writes one more file into the landing folder; E3 uses it for a new delivery.
# MAGIC
# MAGIC **Two places, two kinds of storage:**
# MAGIC * **Data** (claim files, tables) lives in Unity Catalog: `main.dbacademy_l07` and the volume `/Volumes/main/dbacademy_l07/data`.
# MAGIC * **Pipeline source code** (`.sql` and `.py` files) lives in the **workspace**, in the folder `RUNTIME` (`/Users/<you>/_lesson_runtime/L07`). A pipeline reads its source files from the workspace; this notebook writes them there (section 4.8).

# COMMAND ----------

# DBTITLE 1,4.1 Concept — Declarative vs imperative
# MAGIC %md
# MAGIC ## 4 Concepts
# MAGIC
# MAGIC ### 4.1 Declarative vs imperative pipelines
# MAGIC
# MAGIC **What.** In L06 you wrote **imperative** code: *how* to load (`readStream`, checkpoint folder, `trigger`, `toTable`, `awaitTermination`, and a restart after a schema change). In a **declarative** pipeline you only write *what* each table should contain — a `SELECT` per table. The pipeline finds the order of the tables (the dependency graph), creates and updates them, keeps the checkpoints and schema locations, retries, and records everything in an event log.
# MAGIC
# MAGIC **Why.** Less code to maintain, data quality rules (expectations) built in, lineage and monitoring for free.
# MAGIC
# MAGIC **When to use which**
# MAGIC
# MAGIC | | Structured Streaming in a notebook or job (L06, L11) | Lakeflow Declarative Pipeline (this lesson) |
# MAGIC | --- | --- | --- |
# MAGIC | You write | the full read → transform → write code | one `CREATE … AS SELECT` (or one Python function) per dataset |
# MAGIC | Order of steps | you call them in order | the pipeline builds a dependency graph from the queries |
# MAGIC | Checkpoints, schema location | you choose and keep the folders | managed by the pipeline |
# MAGIC | Data quality | your own code (filters, asserts) | expectations: warn, drop row, fail update, with metrics |
# MAGIC | Monitoring | `query.lastProgress`, your own logs | pipeline UI and the **event log** |
# MAGIC | Full control (custom sinks, `foreachBatch` logic, any library) | yes | limited to what pipelines support |
# MAGIC | Run it | a notebook/job task | a pipeline **update** (UI, job task, API, SDK, CLI) |
# MAGIC
# MAGIC **Rule of thumb:** new ingestion and medallion tables → a declarative pipeline; very custom streaming logic or sinks → Structured Streaming.

# COMMAND ----------

# DBTITLE 1,4.2 Concept — Datasets and flows
# MAGIC %md
# MAGIC ### 4.2 Datasets and flows: streaming table, materialized view, temporary view
# MAGIC
# MAGIC A pipeline defines **datasets** (tables and views) and **flows** (the queries that fill them). With `CREATE … AS SELECT`, the flow is defined together with its table.
# MAGIC
# MAGIC | | Streaming table | Materialized view | Temporary view |
# MAGIC | --- | --- | --- | --- |
# MAGIC | SQL | `CREATE OR REFRESH STREAMING TABLE t AS SELECT … FROM STREAM src` | `CREATE OR REFRESH MATERIALIZED VIEW mv AS SELECT …` | `CREATE TEMPORARY VIEW v AS SELECT …` |
# MAGIC | Python | `@dp.table` (returns a streaming DataFrame) | `@dp.materialized_view` | `@dp.temporary_view` |
# MAGIC | What an update does (*Run a pipeline update*) | "Processes new records through the logic defined in streaming tables and flows" | "Updates results to reflect the current results of the defining query"; incremental when that is cheaper | nothing is stored |
# MAGIC | Published to Unity Catalog? | yes | yes | no — "They are private to the defining pipeline" (*CREATE TEMPORARY VIEW (pipelines)*) |
# MAGIC | Typical layer | bronze (files), silver (append-only cleaning) | gold (aggregates, joins that must always be correct) | a helper step inside the pipeline |
# MAGIC | Source must be | append-only ("If the read encounters a change or deletion to an existing record, an error is thrown") | any table or view | any |
# MAGIC
# MAGIC Quotes from *CREATE STREAMING TABLE (pipelines)* and *Run a pipeline update*. A `PRIVATE` streaming table or materialized view is also not published to the catalog (formerly the `TEMPORARY` keyword).
# MAGIC
# MAGIC **`STREAM`.** "This query must be a streaming query. Use the `STREAM` keyword to use streaming semantics to read from the source" (*CREATE STREAMING TABLE (pipelines)*). Examples: `FROM STREAM read_files('/Volumes/…')`, `FROM STREAM(claims_bronze)`.
# MAGIC
# MAGIC **Names inside a pipeline.** An unqualified name such as `claims_bronze` means a dataset in the pipeline's default catalog and schema (here `main.dbacademy_l07`). Tables outside the pipeline are read with their full name, e.g. `main.dbacademy_l07.providers`. The old `LIVE.` prefix (`LIVE.claims_bronze`) is legacy syntax.

# COMMAND ----------

# DBTITLE 1,4.3 Concept — SQL and Python syntax
# MAGIC %md
# MAGIC ### 4.3 SQL and Python syntax
# MAGIC
# MAGIC **SQL** (*Pipeline SQL language reference*): a source file is a list of `CREATE` statements separated by `;`.
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE claims_bronze
# MAGIC AS SELECT *, _metadata.file_name AS source_file
# MAGIC FROM STREAM read_files('/Volumes/main/dbacademy_l07/data/landing/claims', format => 'json');
# MAGIC
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW claims_by_status
# MAGIC AS SELECT claim_status, count(*) AS claims FROM claims_bronze GROUP BY claim_status;
# MAGIC ```
# MAGIC
# MAGIC **Python** (*Lakeflow pipelines Python language reference*): import the module, and write one function per dataset that **returns a DataFrame**.
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp            # current API (Spark 4.1+ / Databricks)
# MAGIC
# MAGIC @dp.table(comment="Raw claims")                # streaming table: the function returns a streaming DataFrame
# MAGIC def claims_bronze():
# MAGIC     return (spark.readStream.format("cloudFiles")
# MAGIC             .option("cloudFiles.format", "json")
# MAGIC             .load("/Volumes/main/dbacademy_l07/data/landing/claims"))
# MAGIC
# MAGIC @dp.materialized_view                           # materialized view: the function returns a batch DataFrame
# MAGIC def claims_by_status():
# MAGIC     return spark.read.table("claims_bronze").groupBy("claim_status").count()
# MAGIC ```
# MAGIC
# MAGIC **Legacy `dlt` module.** Older code uses `import dlt`, `@dlt.table` (for both streaming tables and materialized views) and `@dlt.view`. "The `dlt` module has been replaced by the `pyspark.pipelines` module. You may still use `dlt`, but Databricks recommends using pipelines" (*Lakeflow pipelines Python language reference*).
# MAGIC
# MAGIC **Rules for Python pipeline code** (same page): the function must return a DataFrame, and must **not** call `collect()`, `count()`, `toPandas()`, `save()`, `saveAsTable()`, `start()` or `toTable()`. The pipeline runs your file several times while it plans the graph.
# MAGIC
# MAGIC > Pipeline code **cannot run in a normal notebook cell** as a pipeline: `CREATE OR REFRESH STREAMING TABLE` in this serverless notebook failed in L06 (E11, `STREAMING_TABLE_OPERATION_NOT_ALLOWED.ST_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE`). The code goes into source files, and a pipeline **update** runs them (4.8).

# COMMAND ----------

# DBTITLE 1,4.4 Concept — Expectations and quarantine
# MAGIC %md
# MAGIC ### 4.4 Expectations (data quality rules) and quarantine
# MAGIC
# MAGIC **What.** An expectation is a named SQL boolean rule on a streaming table, materialized view or temporary view. Source: *Manage data quality with pipeline expectations*.
# MAGIC
# MAGIC | Action | SQL | Python | Invalid record (docs) |
# MAGIC | --- | --- | --- | --- |
# MAGIC | warn (default) | `CONSTRAINT name EXPECT (rule)` | `@dp.expect("name", "rule")` | "written to the target" |
# MAGIC | drop | `… EXPECT (rule) ON VIOLATION DROP ROW` | `@dp.expect_or_drop(…)` | "dropped before data is written to the target" |
# MAGIC | fail | `… EXPECT (rule) ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail(…)` | "prevent the update from succeeding"; a table update is rolled back |
# MAGIC
# MAGIC * Several rules: SQL lists several `CONSTRAINT` clauses, separated by commas. Python can also group rules in a dict: `@dp.expect_all({...})`, `@dp.expect_all_or_drop({...})`, `@dp.expect_all_or_fail({...})` — "only Python allows you to group multiple expectations and specify collective actions".
# MAGIC * A rule must not use Python UDFs, external calls or subqueries on other tables.
# MAGIC * Metrics (passed / failed / dropped records) exist for warn and drop. For fail "metrics are not recorded" because the update fails.
# MAGIC * "In a triggered pipeline, the failure of a single flow does not cause other parallel flows to fail" (E6 tests this).
# MAGIC
# MAGIC **Quarantine.** `DROP ROW` removes bad rows from silver, so they are gone. To keep them for review, add a second table with the **opposite** rule (*Expectation recommendations and advanced patterns*, "Quarantine invalid records"):
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE claims_quarantine
# MAGIC AS SELECT * FROM STREAM(claims_bronze)
# MAGIC WHERE NOT (claim_id IS NOT NULL AND claim_amount IS NOT NULL AND claim_amount > 0);   -- NOT (silver rules)
# MAGIC ```
# MAGIC The docs page shows a variant: one table with an `is_quarantined` flag column (`NOT(rule1 AND rule2)`) and two views on it.
# MAGIC
# MAGIC **Write NULL-safe rules.** `claim_amount > 0` is NULL (not false) when `claim_amount` is NULL, and `NOT (NULL)` is still NULL, so a quarantine filter would miss that row. Write `claim_amount IS NOT NULL AND claim_amount > 0`.

# COMMAND ----------

# DBTITLE 1,4.5 Concept — Running a pipeline
# MAGIC %md
# MAGIC ### 4.5 Running a pipeline: updates, refresh, full refresh, modes
# MAGIC
# MAGIC **Update.** "A pipeline update starts a cluster, validates your source code, and refreshes the tables and views defined in the pipeline" (*Run a pipeline update*). Update states from the API (`UpdateInfoState` in the Databricks SDK): `QUEUED`, `CREATED`, `WAITING_FOR_RESOURCES`, `INITIALIZING`, `SETTING_UP_TABLES`, `RUNNING`, `STOPPING`, and the final states `COMPLETED`, `FAILED`, `CANCELED`.
# MAGIC
# MAGIC | Update type (*Run a pipeline update*) | Materialized view | Streaming table |
# MAGIC | --- | --- | --- |
# MAGIC | Refresh (default) | current result of the query; incremental when cheaper | processes **new** records only |
# MAGIC | Full refresh | recomputes the result | "Clears data from streaming tables, clears checkpoints from flows, and reprocesses all records from the data source" |
# MAGIC | Reset streaming flow checkpoints | not applicable | clears checkpoints, **keeps** the data, reprocesses all records |
# MAGIC
# MAGIC Full refresh warning from the same page: if the source no longer has the old data (Kafka retention, deleted files), those records are **lost** from the streaming table. To block full refresh on a table: `TBLPROPERTIES ('pipelines.reset.allowed' = 'false')`.
# MAGIC
# MAGIC **Dry run** (`validate_only`): "resolves the definitions of datasets and flows defined in the pipeline but does not materialize or publish any datasets" — a fast check for wrong table or column names (Public Preview).
# MAGIC
# MAGIC **Triggered vs continuous** (*Triggered vs. continuous pipeline mode*): triggered "refreshes available data and stops"; continuous "keeps tables fresh as new data arrives" and needs always-running compute. Databricks now recommends a **continuous job** around a triggered pipeline instead of the pipeline's own continuous setting. This lesson uses triggered updates only.
# MAGIC
# MAGIC **Development vs production.** The pipeline setting `development` (true/false) is the old "development mode" switch; Declarative Automation Bundles set it to `true` for `mode: development` targets and check it is `false` for `mode: production` (*Declarative Automation Bundles deployment modes*). The *Run a pipeline update* page now describes the **run behavior**: UI runs of triggered pipelines reuse compute and disable retries (fast debugging); updates started from Jobs or the API "use automatic retry and restart behavior". This lesson creates its pipelines with `development=True`.
# MAGIC
# MAGIC **Incremental refresh of materialized views.** On serverless pipelines, "Updates for materialized views are refreshed incrementally whenever possible. Incremental refresh has the same results as full recomputation" (*Configure a serverless pipeline*). The event log tells you which technique was chosen (`planning_information`, E3).

# COMMAND ----------

# DBTITLE 1,4.6 Concept — The event log
# MAGIC %md
# MAGIC ### 4.6 The pipeline event log
# MAGIC
# MAGIC **What.** Every pipeline writes an event log: "audit logs, data quality checks, pipeline progress, and data lineage" (*Pipeline event log*). Read it with the table-valued function:
# MAGIC ```sql
# MAGIC SELECT * FROM event_log('<pipeline-id>');
# MAGIC ```
# MAGIC "By default, only the pipeline's run-as user can query the event log table." You can also **publish** it as a normal table with the pipeline setting `event_log` (`catalog`, `schema`, `name`). Databricks also recommends the `system.lakeflow` pipeline system table (Beta) for all pipelines of a region — lesson L16.
# MAGIC
# MAGIC **Columns** (*Pipeline event log schema*): `id`, `sequence`, `origin` (pipeline, update and flow names), `timestamp`, `message`, `level` (`INFO`, `WARN`, `ERROR`, `METRICS`), `maturity_level`, `error`, `details` (a JSON string), `event_type`.
# MAGIC
# MAGIC | `event_type` | What `details` holds |
# MAGIC | --- | --- |
# MAGIC | `create_update` | a new update started (and why) |
# MAGIC | `update_progress` | the update state, e.g. `{"update_progress":{"state":"COMPLETED"}}` |
# MAGIC | `flow_progress` | per flow: status, `metrics.num_output_rows`, `data_quality.dropped_records`, `data_quality.expectations[]` with `name`, `dataset`, `passed_records`, `failed_records` |
# MAGIC | `flow_definition` | lineage: output and input datasets |
# MAGIC | `planning_information` | how a materialized view will be refreshed (incremental or full recompute) |
# MAGIC
# MAGIC Read JSON fields with the colon syntax from L05: `details:flow_progress.metrics.num_output_rows::bigint`. The expectation list needs `from_json(…, 'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')` and `explode` (the docs query in *Pipeline event log* uses this pattern).

# COMMAND ----------

# DBTITLE 1,4.7 Concept — How this notebook runs pipelines
# MAGIC %md
# MAGIC ### 4.7 How this notebook runs pipelines (REST API through the Databricks SDK)
# MAGIC
# MAGIC Usually you create a pipeline in the UI (**Jobs & Pipelines → Create → ETL pipeline**) and edit the code in the Lakeflow Pipelines Editor. This lesson does the same steps from Python, so the whole lesson stays in one notebook and every result is checked by code. It calls the **Pipelines REST API** (`/api/2.0/pipelines`) through the Databricks SDK client `w.api_client.do(...)`, which signs each request as you. The request bodies are the same JSON that `databricks pipelines create --json …` and Declarative Automation Bundles use.
# MAGIC
# MAGIC | Step | REST call (used here) | Typed SDK call | CLI |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 Save a source file | — | `w.workspace.upload(path, data, format=ImportFormat.AUTO, overwrite=True)` | `databricks workspace import` |
# MAGIC | 2 Create a pipeline | `POST /api/2.0/pipelines` | `w.pipelines.create(...)` | `databricks pipelines create --json …` |
# MAGIC | 3 Change its settings | `PUT /api/2.0/pipelines/{id}` (all settings) | `w.pipelines.update(id, ...)` | `databricks pipelines update <id> --json …` |
# MAGIC | 4 Start an update | `POST /api/2.0/pipelines/{id}/updates` with `full_refresh`, `validate_only` | `w.pipelines.start_update(id, ...)` | `databricks pipelines start-update <id> [--full-refresh] [--validate-only]` |
# MAGIC | 5 Wait for it | `GET /api/2.0/pipelines/{id}/updates/{update_id}` → `update.state` | `w.pipelines.get_update(id, update_id)` | `databricks pipelines get-update <id> <update-id>` |
# MAGIC | 6 Read errors | `GET /api/2.0/pipelines/{id}/events?filter=level='ERROR'` | `w.pipelines.list_pipeline_events(id, filter=...)` | `databricks pipelines list-pipeline-events <id>` |
# MAGIC | 7 Read results | `spark.table(…)`, `event_log('<id>')` | — | — |
# MAGIC | 8 Delete | `DELETE /api/2.0/pipelines/{id}` | `w.pipelines.delete(id)` | `databricks pipelines delete <id>` |
# MAGIC
# MAGIC **Why REST and not the typed SDK calls?** The SDK version that is preinstalled on serverless job compute (this run: `{{nd.setup.sdk_version}}`) can be older than the API: in a test run of this lesson, `w.pipelines.create(..., root_path=...)` failed with `TypeError: PipelinesAPI.create() got an unexpected keyword argument 'root_path'`. The REST call does not depend on the SDK version.
# MAGIC
# MAGIC Pipeline settings used here (JSON keys): `name`, `catalog` + `schema` (where tables are published), `serverless: true`, `continuous: false` (triggered), `development: true`, `root_path` (the folder of the source files) and `libraries` (one `{"file": {"path": …}}` per source file).
# MAGIC
# MAGIC The next cell defines small helper functions for these steps. Read them once: you will use `run_update`, `flow_metrics` and `expectation_metrics` in every example.

# COMMAND ----------

# DBTITLE 1,4.7 Pipeline helpers (run first)
def write_source(file_name, text):
    """Save one pipeline source file (.sql or .py) in the RUNTIME workspace folder; return its full path."""
    w.workspace.upload(f"{RUNTIME}/{file_name}", io.BytesIO(text.encode("utf-8")),
                       format=ImportFormat.AUTO, overwrite=True)     # a plain workspace file, not a notebook
    return f"/Workspace{RUNTIME}/{file_name}"

def pipeline_settings(name, file_names):
    """All settings of a lesson pipeline (JSON): serverless, triggered, publishing to main.dbacademy_l07."""
    return {"name": f"{PREFIX}_{name}", "catalog": CATALOG, "schema": SCHEMA,
            "serverless": True, "continuous": False, "development": True,
            "root_path": f"/Workspace{RUNTIME}",
            "libraries": [{"file": {"path": f"/Workspace{RUNTIME}/{f}"}} for f in file_names]}

def create_pipeline(name, file_names):
    """Create the pipeline and return its id."""
    return api("POST", "/api/2.0/pipelines", body=pipeline_settings(name, file_names))["pipeline_id"]

def edit_pipeline(pipeline_id, name, file_names):
    """Replace all settings of an existing pipeline (for example, a new list of source files)."""
    api("PUT", f"/api/2.0/pipelines/{pipeline_id}", body={"id": pipeline_id, **pipeline_settings(name, file_names)})

def update_errors(pipeline_id, update_id):
    """The ERROR events of one update (events API): a list of (event_type, message, exceptions as text)."""
    found, token = [], None
    while True:
        query = {"filter": "level='ERROR'", "max_results": 100, **({"page_token": token} if token else {})}
        page = api("GET", f"/api/2.0/pipelines/{pipeline_id}/events", query=query)
        for e in page.get("events", []):
            if e.get("origin", {}).get("update_id") == update_id:
                exceptions = e.get("error", {}).get("exceptions", [])
                found.append((e.get("event_type"), e.get("message", ""),
                              " | ".join(f"{x.get('class_name')}: {x.get('message')}" for x in exceptions)))
        token = page.get("next_page_token")
        if not token:
            return found

def run_update(pipeline_id, full_refresh=False, validate_only=False, timeout_s=1800, tries=3):
    """Start one update, wait until it ends, and return (update_id, final state, seconds)."""
    started = time.time()
    for attempt in range(1, tries + 1):
        update_id = api("POST", f"/api/2.0/pipelines/{pipeline_id}/updates",
                        body={"full_refresh": full_refresh, "validate_only": validate_only})["update_id"]
        while True:
            state = api("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")["update"]["state"]
            if state in ("COMPLETED", "FAILED", "CANCELED"):
                break
            if time.time() - started > timeout_s:
                api("POST", f"/api/2.0/pipelines/{pipeline_id}/stop")   # do not leave compute running
                raise TimeoutError(f"update {update_id} still {state} after {timeout_s} s")
            time.sleep(10)                                    # check again in 10 seconds
        no_capacity = state == "FAILED" and any("RESOURCE_EXHAUSTED" in x[2] for x in update_errors(pipeline_id, update_id))
        if no_capacity and attempt < tries:                   # free-usage limit: no serverless capacity yet
            print(f"update {update_id}: no serverless capacity (RESOURCE_EXHAUSTED), trying again in 60 s")
            time.sleep(60)
            continue
        seconds = round(time.time() - started)
        print(f"update {update_id}: {state} after {seconds} s")
        return update_id, state, seconds

def flow_metrics(pipeline_id, update_id):
    """One row per flow of one update: rows written, rows dropped and rows warned by expectations (event log)."""
    return spark.sql(f"""
        SELECT replace(origin.flow_name, '{FQ}.', '')                              AS flow,
               max_by(details:flow_progress.status::string, timestamp)
                 FILTER (WHERE details:flow_progress.status IS NOT NULL)            AS final_status,
               coalesce(sum(details:flow_progress.metrics.num_output_rows::bigint), 0)      AS output_rows,
               coalesce(sum(details:flow_progress.data_quality.dropped_records::bigint), 0) AS dropped_rows,
               coalesce(sum(details:flow_progress.data_quality.warned_records::bigint), 0)  AS warned_rows
        FROM event_log('{pipeline_id}')
        WHERE event_type = 'flow_progress'
          AND origin.update_id = '{update_id}'
          AND origin.flow_name LIKE '{FQ}.%'                                       -- skip internal metric rows
        GROUP BY ALL
        ORDER BY flow""")

def expectation_metrics(pipeline_id, update_id):
    """One row per expectation of one update: passed and failed records (event log)."""
    return spark.sql(f"""
        SELECT replace(e.dataset, '{FQ}.', '') AS dataset, e.name AS expectation,
               sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
        FROM (SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                     'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) AS e
              FROM event_log('{pipeline_id}')
              WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}')
        GROUP BY ALL
        ORDER BY dataset, expectation""")

def error_classes(text):
    """All [ERROR_CLASS] names in a text, in order, without duplicates."""
    return list(dict.fromkeys(re.findall(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", text or "")))

def mv_technique(pipeline_id, update_id, mv_name):
    """How an update refreshed a materialized view (planning_information in the event log), e.g. COMPLETE_RECOMPUTE."""
    row = spark.sql(f"""
        SELECT regexp_extract(message, 'executed as ([A-Z_]+)', 1) AS technique
        FROM event_log('{pipeline_id}')
        WHERE event_type = 'planning_information' AND origin.update_id = '{update_id}'
          AND origin.flow_name = '{FQ}.{mv_name}'""").first()
    return row["technique"] if row else "no planning event"

def as_dict(df, key, value):
    """{key: value} from two columns of a small DataFrame."""
    return {r[key]: r[value] for r in df.collect()}

print("Helpers ready: api, write_source, create_pipeline, edit_pipeline, delete_pipeline, run_update, "
      "flow_metrics, expectation_metrics, update_errors, mv_technique")

# COMMAND ----------

# DBTITLE 1,5 — Examples (intro)
# MAGIC %md
# MAGIC ## 5 Examples
# MAGIC
# MAGIC Each example: **Predict** (answer before you run) → **code** (every line commented) → **Explained** (real numbers from the run).
# MAGIC
# MAGIC All examples use **one** pipeline, `dbacademy_l07_claims` (bronze → silver → gold), and change it step by step: E1 creates it, E3/E4/E7/E8 run more updates. E6 uses a second, short-lived pipeline. Run the examples in order.
# MAGIC
# MAGIC > **Free Edition limit (seen on this workspace in a test while this lesson was built):** while two lesson pipelines existed, a third pipeline could not start (`RESOURCE_EXHAUSTED: You've hit the limit for severless compute for free usage`). The *Databricks Free Edition limitations* page lists "One active pipeline per pipeline type". The lesson therefore never keeps more than two pipelines: E6 deletes its pipeline at the end, and all practice tasks share one practice pipeline.

# COMMAND ----------

# DBTITLE 1,E1 Predict
# MAGIC %md
# MAGIC ### E1 — Bronze → silver → gold in one pipeline
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. The landing folder has 200 claims; 6 of them break a `DROP ROW` rule and 3 have an unknown status (warn only). How many rows will `claims_bronze`, `claims_silver` and `claims_quarantine` have after the first update?
# MAGIC 2. The three source files are written in the order bronze, silver, gold — does the order matter?
# MAGIC 3. Which of the datasets will you find in the catalog, and with which table type?

# COMMAND ----------

# DBTITLE 1,E1 Code — the pipeline source (bronze, silver, gold)
BRONZE_SQL = f"""
-- Bronze: load every claim file once (Auto Loader behind STREAM read_files)
CREATE OR REFRESH STREAMING TABLE claims_bronze
COMMENT 'Raw claims: one row per JSON line, plus the source file name'
AS SELECT *,
          _metadata.file_name AS source_file                  -- lineage column (L06)
FROM STREAM read_files('{LANDING}', format => 'json');        -- STREAM: only files that were not loaded before
"""

SILVER_SQL = """
-- Silver: valid claims. Two rules drop bad rows, one rule only warns.
CREATE OR REFRESH STREAMING TABLE claims_silver (
  CONSTRAINT valid_claim_id  EXPECT (claim_id IS NOT NULL)                          ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (claim_amount IS NOT NULL AND claim_amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT known_status    EXPECT (claim_status IN ('SUBMITTED', 'APPROVED', 'DENIED', 'PENDING'))
)                                                             -- known_status has no ON VIOLATION: warn, keep the row
COMMENT 'Valid claims with typed columns and the provider specialty at load time'
AS SELECT c.claim_id, c.patient_id, c.provider_id,
          p.specialty,                                        -- looked up when the row is processed
          CAST(c.claim_amount AS DECIMAL(10, 2)) AS claim_amount,
          c.claim_status,
          CAST(c.service_date AS DATE)           AS service_date,
          c.source_file
FROM STREAM(claims_bronze) AS c                               -- only new bronze rows (a dataset of this pipeline)
LEFT JOIN main.dbacademy_l07.providers AS p                   -- a normal Delta table outside the pipeline
  ON c.provider_id = p.provider_id;

-- Quarantine: the rows that break a DROP ROW rule above, kept for review
CREATE OR REFRESH STREAMING TABLE claims_quarantine
COMMENT 'Claims that break a silver rule'
AS SELECT *,
          CASE WHEN claim_id IS NULL THEN 'missing claim_id'
               ELSE 'amount not positive' END AS quarantine_reason
FROM STREAM(claims_bronze)
WHERE NOT (claim_id IS NOT NULL AND claim_amount IS NOT NULL AND claim_amount > 0);   -- NOT (the silver rules)
"""

GOLD_SQL = """
-- A temporary view: a helper inside the pipeline, not published to the catalog
CREATE TEMPORARY VIEW provider_lookup AS
SELECT provider_id, specialty FROM main.dbacademy_l07.providers;

-- Gold: claims and amount per specialty, always computed with the CURRENT provider table
CREATE OR REFRESH MATERIALIZED VIEW claims_by_specialty
COMMENT 'Claims per provider specialty'
AS SELECT p.specialty, count(*) AS claims, sum(s.claim_amount) AS total_amount
FROM claims_silver AS s                                       -- no STREAM: a batch read of all silver rows
JOIN provider_lookup AS p ON s.provider_id = p.provider_id
GROUP BY p.specialty;
"""
print("Source files defined: BRONZE_SQL, SILVER_SQL, GOLD_SQL")

# COMMAND ----------

# DBTITLE 1,E1 Code — create the pipeline and run the first update
for file_name, text in [("bronze.sql", BRONZE_SQL), ("silver.sql", SILVER_SQL), ("gold.sql", GOLD_SQL)]:
    print("wrote", write_source(file_name, text))            # save each source file in the workspace
MAIN = create_pipeline("claims", ["gold.sql", "silver.sql", "bronze.sql"])   # files in "wrong" order on purpose
print("pipeline id:", MAIN)
u1, u1_state, u1_secs = run_update(MAIN)                      # update 1: creates and fills every dataset

published = spark.sql(f"""
    SELECT table_name, table_type
    FROM {CATALOG}.information_schema.tables                   -- what the pipeline published to Unity Catalog
    WHERE table_schema = '{SCHEMA}' AND table_name NOT LIKE '\\_\\_%' AND table_name NOT LIKE 'event_log%'
    ORDER BY table_name""")
display(published)
counts = {t: spark.table(f"{FQ}.{t}").count()                 # rows in each published dataset
          for t in ["claims_bronze", "claims_silver", "claims_quarantine", "claims_by_specialty"]}
print(counts)
display(spark.table(f"{FQ}.claims_by_specialty").orderBy("specialty"))
METRICS["E1.state"] = u1_state
METRICS["nd.E1.secs"] = u1_secs
METRICS["E1.published"] = ", ".join(f"{r['table_name']} ({r['table_type']})" for r in published.collect())
METRICS["E1.temp_view_published"] = spark.catalog.tableExists(f"{FQ}.provider_lookup")
METRICS["E1.bronze"], METRICS["E1.silver"] = counts["claims_bronze"], counts["claims_silver"]
METRICS["E1.quarantine"], METRICS["E1.gold_rows"] = counts["claims_quarantine"], counts["claims_by_specialty"]
METRICS["E1.gold_claims"] = spark.table(f"{FQ}.claims_by_specialty").agg({"claims": "sum"}).first()[0]
assert u1_state == "COMPLETED" and counts["claims_bronze"] == 200 and counts["claims_silver"] == 194

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Update 1 ended in state **`{{E1.state}}`**.
# MAGIC * Tables in the schema now: `{{E1.published}}` — the pipeline published the four `claims_…` datasets as streaming tables and a materialized view (`providers` is the normal table from the setup). The temporary view `provider_lookup` is not in the catalog (`tableExists` = {{E1.temp_view_published}}): it lives only inside the pipeline.
# MAGIC * Rows: bronze **{{E1.bronze}}**, silver **{{E1.silver}}**, quarantine **{{E1.quarantine}}**, gold {{E1.gold_rows}} specialties with {{E1.gold_claims}} claims in total.
# MAGIC * We registered the files as `gold.sql, silver.sql, bronze.sql` — gold first, although it reads silver, which reads bronze. The update still worked: the pipeline reads all files first and builds the dependency graph from the `FROM` clauses. You never call the steps in order yourself.
# MAGIC
# MAGIC > **Key takeaway:** a pipeline is a set of declarations. `STREAM(...)` marks an incremental (streaming) read; a plain `FROM` is a batch read. Streaming table for append-only layers, materialized view for gold aggregates.

# COMMAND ----------

# DBTITLE 1,E2 Predict
# MAGIC %md
# MAGIC ### E2 — Data quality numbers from the event log
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Silver has three rules. For `positive_amount`, is a row with a NULL `claim_id` counted as passed, failed, or not checked at all (because `valid_claim_id` already drops it)?
# MAGIC 2. How many silver rows have the unknown status `IN_REVIEW`?
# MAGIC 3. Where in the event log are these numbers stored?

# COMMAND ----------

# DBTITLE 1,E2 Code
events = spark.sql(f"""
    SELECT timestamp, event_type, replace(origin.flow_name, '{FQ}.', '') AS flow, level, message
    FROM event_log('{MAIN}')                                   -- the table-valued function, by pipeline id
    WHERE origin.update_id = '{u1}'                            -- only update 1
    ORDER BY timestamp""")
display(events)                                                # every event of the update, in order
e2_flows = flow_metrics(MAIN, u1)                              # rows written / dropped / warned per flow
display(e2_flows)
e2_exp = expectation_metrics(MAIN, u1)                         # passed / failed per expectation
display(e2_exp)
status = spark.sql(f"""SELECT claim_status, count(*) AS claims FROM {FQ}.claims_silver
                       GROUP BY claim_status ORDER BY claim_status""")   # warn rules keep the rows
display(status)
raw = spark.sql(f"""
    SELECT details:flow_progress.data_quality AS data_quality  -- the raw JSON, as stored in the event log
    FROM event_log('{MAIN}')
    WHERE origin.update_id = '{u1}' AND event_type = 'flow_progress'
      AND origin.flow_name = '{FQ}.claims_silver' AND details:flow_progress.data_quality.expectations IS NOT NULL""")
display(raw)

flows = {r["flow"]: r for r in e2_flows.collect()}
exp = {(r["dataset"], r["expectation"]): (r["passed"], r["failed"]) for r in e2_exp.collect()}
METRICS["E2.event_types"] = ", ".join(sorted({r["event_type"] for r in events.collect()}))
METRICS["E2.bronze_out"] = flows["claims_bronze"]["output_rows"]
METRICS["E2.silver_out"] = flows["claims_silver"]["output_rows"]
METRICS["E2.silver_dropped"] = flows["claims_silver"]["dropped_rows"]
METRICS["E2.silver_warned"] = flows["claims_silver"]["warned_rows"]
METRICS["E2.quarantine_out"] = flows["claims_quarantine"]["output_rows"]
METRICS["E2.gold_out"] = flows["claims_by_specialty"]["output_rows"]
METRICS["E2.valid_claim_id.passed"], METRICS["E2.valid_claim_id.failed"] = exp[("claims_silver", "valid_claim_id")]
METRICS["E2.positive_amount.passed"], METRICS["E2.positive_amount.failed"] = exp[("claims_silver", "positive_amount")]
METRICS["E2.known_status.passed"], METRICS["E2.known_status.failed"] = exp[("claims_silver", "known_status")]
METRICS["E2.in_review_rows"] = as_dict(status, "claim_status", "claims").get("IN_REVIEW", 0)
METRICS["E2.raw_example"] = raw.first()["data_quality"]
assert flows["claims_silver"]["dropped_rows"] == 6 and as_dict(status, "claim_status", "claims").get("IN_REVIEW") == 3

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Event types of update 1: `{{E2.event_types}}`.
# MAGIC * Rows per flow (`flow_progress` → `metrics.num_output_rows`): bronze {{E2.bronze_out}}, silver {{E2.silver_out}}, quarantine {{E2.quarantine_out}}, gold {{E2.gold_out}}.
# MAGIC * Silver dropped **{{E2.silver_dropped}}** rows and warned {{E2.silver_warned}} rows (`data_quality.dropped_records`, `data_quality.warned_records`).
# MAGIC
# MAGIC | Expectation (action) | passed | failed |
# MAGIC | --- | --- | --- |
# MAGIC | `valid_claim_id` (DROP ROW) | {{E2.valid_claim_id.passed}} | {{E2.valid_claim_id.failed}} |
# MAGIC | `positive_amount` (DROP ROW) | {{E2.positive_amount.passed}} | {{E2.positive_amount.failed}} |
# MAGIC | `known_status` (warn) | {{E2.known_status.passed}} | {{E2.known_status.failed}} |
# MAGIC
# MAGIC * Every rule was checked on **every** input row: for each rule, passed + failed = the {{E1.bronze}} bronze rows. A row that fails `valid_claim_id` is still counted by `positive_amount`. The dropped count ({{E2.silver_dropped}}) is the number of rows that failed **at least one** drop rule.
# MAGIC * The warn rule `known_status` failed for {{E2.known_status.failed}} rows, and these rows are **still in silver**: {{E2.in_review_rows}} rows with status `IN_REVIEW`.
# MAGIC * The raw JSON from the event log: `{{E2.raw_example}}`. It has a `warned_records` field that the *Pipeline event log schema* page does not list (it lists `dropped_records` and `expectations`).
# MAGIC
# MAGIC > **Key takeaway:** the event log is the place for data quality numbers: `event_type = 'flow_progress'`, `details:flow_progress.data_quality`. Group by `origin.update_id` to see one run.

# COMMAND ----------

# DBTITLE 1,E3 Predict
# MAGIC %md
# MAGIC ### E3 — A new file arrives: what does the next update process?
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. A new file with 50 claims arrives (2 with a negative amount). How many rows does bronze write in update 2: 50 or 250?
# MAGIC 2. How many rows does silver write, and how many does it drop?
# MAGIC 3. Does the gold materialized view recompute everything, or only what changed?

# COMMAND ----------

# DBTITLE 1,E3 Code
land_claims("claims_003.json", 201, 50, negative_ids=(210, 240))   # a new delivery: 50 claims, 2 bad amounts
u2, u2_state, u2_secs = run_update(MAIN)                       # update 2: a normal refresh
e3_flows = flow_metrics(MAIN, u2)                              # what each flow did in update 2
display(e3_flows)
display(expectation_metrics(MAIN, u2))
per_file = spark.sql(f"""SELECT source_file, count(*) AS row_count FROM {FQ}.claims_bronze
                         GROUP BY source_file ORDER BY source_file""")     # bronze rows per landed file
display(per_file)
e3_technique = mv_technique(MAIN, u2, "claims_by_specialty")   # how the materialized view was refreshed
print("gold refresh technique:", e3_technique)

flows = {r["flow"]: r for r in e3_flows.collect()}
METRICS["E3.state"] = u2_state
METRICS["nd.E3.secs"] = u2_secs
METRICS["E3.bronze_out"] = flows["claims_bronze"]["output_rows"]
METRICS["E3.silver_out"] = flows["claims_silver"]["output_rows"]
METRICS["E3.silver_dropped"] = flows["claims_silver"]["dropped_rows"]
METRICS["E3.quarantine_out"] = flows["claims_quarantine"]["output_rows"]
METRICS["E3.bronze_total"] = spark.table(f"{FQ}.claims_bronze").count()
METRICS["E3.silver_total"] = spark.table(f"{FQ}.claims_silver").count()
METRICS["E3.rows_per_file"] = ", ".join(f"{r['source_file']}: {r['row_count']}" for r in per_file.collect())
METRICS["E3.gold_technique"] = e3_technique
METRICS["E1.gold_technique"] = mv_technique(MAIN, u1, "claims_by_specialty")
assert flows["claims_bronze"]["output_rows"] == 50 and spark.table(f"{FQ}.claims_bronze").count() == 250

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Update 2: `{{E3.state}}`. Bronze wrote **{{E3.bronze_out}}** rows — only the new file. Silver wrote {{E3.silver_out}} and dropped {{E3.silver_dropped}}; quarantine got {{E3.quarantine_out}}.
# MAGIC * Totals now: bronze {{E3.bronze_total}}, silver {{E3.silver_total}}. Rows per file: {{E3.rows_per_file}}.
# MAGIC * Gold refresh technique from `planning_information`: update 1 `{{E1.gold_technique}}` (the first computation), update 2 **`{{E3.gold_technique}}`**. *Incremental refresh for materialized views* lists `GROUP_AGGREGATE`, `ROW_BASED` and `PARTITION_OVERWRITE` as incremental techniques and the full recompute as not incremental (the page calls it `FULL_RECOMPUTE`; the event message here said `COMPLETE_RECOMPUTE`). So update 2 refreshed gold **incrementally**.
# MAGIC
# MAGIC > **Key takeaway:** a streaming table remembers what it has read (checkpoint), exactly like Auto Loader in L06 — but you never touched a checkpoint folder. A materialized view is always correct for the current input; how it gets there (full recompute or incremental) is the pipeline's choice.

# COMMAND ----------

# DBTITLE 1,E4 Predict
# MAGIC %md
# MAGIC ### E4 — A source table changes: streaming table vs materialized view
# MAGIC
# MAGIC Provider 3 changes its specialty from Pediatrics to Oncology (an `UPDATE` on `main.dbacademy_l07.providers`, outside the pipeline). No new claim files arrive.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. After the next update, which specialty do the **old** silver rows of provider 3 show?
# MAGIC 2. Which specialty does the gold materialized view count them under?
# MAGIC 3. How many rows do bronze and silver write in this update?

# COMMAND ----------

# DBTITLE 1,E4 Code
spark.sql(f"UPDATE {FQ}.providers SET specialty = 'Oncology' WHERE provider_id = 3")   # change outside the pipeline
u3, u3_state, u3_secs = run_update(MAIN)                       # update 3: a normal refresh
e4_flows = flow_metrics(MAIN, u3)                              # streaming flows had no new input
display(e4_flows)
silver_p3 = spark.sql(f"""SELECT specialty, count(*) AS claims FROM {FQ}.claims_silver
                          WHERE provider_id = 3 GROUP BY specialty""")    # provider 3 in silver
display(silver_p3)
compare = spark.sql(f"""
    SELECT g.specialty, s.claims AS claims_by_silver_specialty, g.claims AS claims_in_gold_mv
    FROM {FQ}.claims_by_specialty AS g
    FULL JOIN (SELECT specialty, count(*) AS claims FROM {FQ}.claims_silver GROUP BY specialty) AS s
      ON g.specialty = s.specialty
    ORDER BY g.specialty""")                                   # silver's frozen value vs the recomputed gold
display(compare)

flows = {r["flow"]: r for r in e4_flows.collect()}
cmp = {r["specialty"]: (r["claims_by_silver_specialty"], r["claims_in_gold_mv"]) for r in compare.collect()}
METRICS["E4.state"] = u3_state
METRICS["nd.E4.secs"] = u3_secs
METRICS["E4.bronze_out"] = flows.get("claims_bronze", {"output_rows": 0})["output_rows"]
METRICS["E4.silver_out"] = flows.get("claims_silver", {"output_rows": 0})["output_rows"]
METRICS["E4.silver_p3"] = ", ".join(f"{r['specialty']}: {r['claims']}" for r in silver_p3.collect())
METRICS["E4.pediatrics_silver"], METRICS["E4.pediatrics_gold"] = cmp["Pediatrics"]
METRICS["E4.oncology_silver"], METRICS["E4.oncology_gold"] = cmp["Oncology"]
METRICS["E4.gold_technique"] = mv_technique(MAIN, u3, "claims_by_specialty")
assert cmp["Pediatrics"][0] != cmp["Pediatrics"][1]                # silver column and gold view disagree

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Update 3: `{{E4.state}}`. Bronze wrote {{E4.bronze_out}} rows and silver {{E4.silver_out}}: no new input, so the streaming tables had nothing to do.
# MAGIC * Silver rows of provider 3 still show **{{E4.silver_p3}}**. A streaming table processes each input row **once**; the join with `providers` happened when the row was processed, and the change to `providers` does not reach old rows.
# MAGIC * The gold materialized view joins `providers` again: Pediatrics {{E4.pediatrics_silver}} claims by silver's column vs **{{E4.pediatrics_gold}}** in gold; Oncology {{E4.oncology_silver}} vs **{{E4.oncology_gold}}**. Gold follows the current provider table (technique: `{{E4.gold_technique}}`).
# MAGIC
# MAGIC > **Key takeaway:** a stream–static join in a streaming table uses the dimension **as it was** when each row arrived. If old rows must follow dimension changes, compute that column in a materialized view, or run a full refresh (E7).

# COMMAND ----------

# DBTITLE 1,E5 Predict
# MAGIC %md
# MAGIC ### E5 — The quarantine table
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Silver dropped rows in updates 1 and 2. Is every dropped row in `claims_quarantine`?
# MAGIC 2. Is bronze = silver + quarantine?

# COMMAND ----------

# DBTITLE 1,E5 Code
reasons = spark.sql(f"""SELECT quarantine_reason, count(*) AS claims FROM {FQ}.claims_quarantine
                        GROUP BY quarantine_reason ORDER BY quarantine_reason""")   # why rows were quarantined
display(reasons)
display(spark.table(f"{FQ}.claims_quarantine").select(
    "claim_id", "claim_amount", "claim_status", "source_file", "quarantine_reason").orderBy("source_file"))
dropped_total = sum(r["dropped_rows"] for u in (u1, u2, u3)    # all silver drops in the event log so far
                    for r in flow_metrics(MAIN, u).collect() if r["flow"] == "claims_silver")
bronze_n = spark.table(f"{FQ}.claims_bronze").count()
silver_n = spark.table(f"{FQ}.claims_silver").count()
quarantine_n = spark.table(f"{FQ}.claims_quarantine").count()
print(f"bronze {bronze_n} = silver {silver_n} + quarantine {quarantine_n}? dropped in the event log: {dropped_total}")
METRICS["E5.reasons"] = ", ".join(f"{r['quarantine_reason']}: {r['claims']}" for r in reasons.collect())
METRICS["E5.dropped_total"] = dropped_total
METRICS["E5.bronze"], METRICS["E5.silver"], METRICS["E5.quarantine"] = bronze_n, silver_n, quarantine_n
METRICS["E5.balanced"] = bronze_n == silver_n + quarantine_n
assert quarantine_n == dropped_total and bronze_n == silver_n + quarantine_n

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Quarantine reasons: {{E5.reasons}}.
# MAGIC * Silver dropped {{E5.dropped_total}} rows in all updates (event log), and quarantine holds **{{E5.quarantine}}** rows: every dropped row is kept for review.
# MAGIC * Bronze {{E5.bronze}} = silver {{E5.silver}} + quarantine {{E5.quarantine}}: **{{E5.balanced}}**. Nothing was lost between the layers.
# MAGIC * This works only because the quarantine filter is the exact **negation** of the drop rules, written NULL-safe (4.4). If you change a rule in silver, change the quarantine filter too — or use the docs variant with one `is_quarantined` flag column.
# MAGIC
# MAGIC > **Key takeaway:** `DROP ROW` + a quarantine table = clean silver **and** no lost data. Exam wording: "invalid records must not block the pipeline and must be available for later review".

# COMMAND ----------

# DBTITLE 1,E6 Predict
# MAGIC %md
# MAGIC ### E6 — When an update fails: `FAIL UPDATE`, and a dry run that finds a missing `STREAM`
# MAGIC
# MAGIC A second pipeline, `dbacademy_l07_checks`, has two datasets: `claims_strict` (a streaming table with `positive_amount … ON VIOLATION FAIL UPDATE`) and `claims_status_counts` (an independent materialized view).
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. The landing files contain negative amounts. What is the final state of the update? How many rows does `claims_strict` keep?
# MAGIC 2. Does the independent materialized view still get its data?
# MAGIC 3. Where do you find the error class and the bad record?
# MAGIC 4. A streaming table reads `read_files(...)` **without** `STREAM`. Does a dry run (`validate_only`) find this problem?

# COMMAND ----------

# DBTITLE 1,E6 Code — FAIL UPDATE
STRICT_SQL = f"""
-- A strict copy of the claims: a single bad amount must stop the update
CREATE OR REFRESH STREAMING TABLE claims_strict (
  CONSTRAINT positive_amount EXPECT (claim_amount > 0) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM STREAM read_files('{LANDING}', format => 'json');

-- An independent materialized view in the same pipeline (it does not read claims_strict)
CREATE OR REFRESH MATERIALIZED VIEW claims_status_counts
AS SELECT claim_status, count(*) AS claims
FROM read_files('{LANDING}', format => 'json')               -- batch read of all files
GROUP BY claim_status;
"""
write_source("strict.sql", STRICT_SQL)                         # save the source file
CHECKS = create_pipeline("checks", ["strict.sql"])             # a second pipeline (two in total now)
u6, u6_state, u6_secs = run_update(CHECKS)                     # the update meets negative amounts
display(flow_metrics(CHECKS, u6))                              # final status of each flow
u6_errors = update_errors(CHECKS, u6)                          # ERROR events with their exceptions
for event_type, message, exceptions in u6_errors:
    print(f"{event_type}: {message}\n    {exceptions[:600]}\n")
strict_rows = spark.table(f"{FQ}.claims_strict").count()      # what the failed flow left behind
status_rows = spark.table(f"{FQ}.claims_status_counts").count()

all_text = " ".join(m + " " + x for _, m, x in u6_errors)
violated = re.findall(r"Violated expectations: '([^']+)'", all_text)
bad_record = re.findall(r'Output record: \'(\{[^\']*\})\'', all_text)
f6 = {r["flow"]: r["final_status"] for r in flow_metrics(CHECKS, u6).collect()}
METRICS["E6.state"] = u6_state
METRICS["nd.E6.secs"] = u6_secs
METRICS["E6.flow_status"] = ", ".join(f"{k}: {v}" for k, v in sorted(f6.items()))
METRICS["E6.error_classes"] = ", ".join(error_classes(all_text))
METRICS["E6.error_event_types"] = ", ".join(sorted({t for t, _, _ in u6_errors}))
METRICS["E6.violated"] = violated[0] if violated else "none"
METRICS["nd.E6.bad_record"] = bad_record[0] if bad_record else "none"
METRICS["E6.restart_message"] = next((m for _, m, _ in u6_errors if "restarted" in m), "none")
METRICS["E6.strict_rows"], METRICS["E6.status_rows"] = strict_rows, status_rows
assert u6_state == "FAILED"

# COMMAND ----------

# DBTITLE 1,E6 Code — dry run of a streaming table without STREAM, then delete the pipeline
NOSTREAM_SQL = f"""
-- Bug on purpose: a streaming table that reads the files WITHOUT the STREAM keyword
CREATE OR REFRESH STREAMING TABLE claims_nostream
AS SELECT * FROM read_files('{LANDING}', format => 'json');
"""
write_source("nostream.sql", NOSTREAM_SQL)
edit_pipeline(CHECKS, "checks", ["nostream.sql"])   # same pipeline, new list of files
u6b, u6b_state, u6b_secs = run_update(CHECKS, validate_only=True)            # dry run: check only, publish nothing
u6b_errors = update_errors(CHECKS, u6b)
for event_type, message, exceptions in u6b_errors:
    print(f"{event_type}: {message}\n    {exceptions[:600]}\n")
nostream_published = spark.catalog.tableExists(f"{FQ}.claims_nostream")      # did the dry run create a table?

delete_pipeline(CHECKS)                                        # delete the pipeline (keep at most two)
time.sleep(15)                                                 # give the catalog a moment
strict_kept = spark.catalog.tableExists(f"{FQ}.claims_strict") # are its tables still there?
METRICS["E6.dry_state"] = u6b_state
METRICS["nd.E6.dry_secs"] = u6b_secs
METRICS["E6.dry_error_classes"] = ", ".join(error_classes(" ".join(m + " " + x for _, m, x in u6b_errors)))
METRICS["E6.dry_published"] = nostream_published
METRICS["E6.tables_kept_after_delete"] = strict_kept
assert u6b_state == "FAILED"

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC **Part A — `FAIL UPDATE`.**
# MAGIC * The update ended in state **`{{E6.state}}`**. Final flow status: {{E6.flow_status}}.
# MAGIC * Error class: **`{{E6.error_classes}}`** (in events of type `{{E6.error_event_types}}`). The message names the violated expectation (`{{E6.violated}}`) and shows one bad output record, e.g. `{{nd.E6.bad_record}}` — exactly the format on the *Manage data quality with pipeline expectations* page.
# MAGIC * `claims_strict` has **{{E6.strict_rows}}** rows: the failed flow wrote nothing (the docs: the transaction is rolled back). A flow message said: "{{E6.restart_message}}".
# MAGIC * The independent materialized view `claims_status_counts` still has {{E6.status_rows}} rows: "In a triggered pipeline, the failure of a single flow does not cause other parallel flows to fail."
# MAGIC
# MAGIC **Part B — dry run.** With `validate_only=True`, the pipeline with the missing `STREAM` ended in **`{{E6.dry_state}}`** with **`{{E6.dry_error_classes}}`**, and no table was published (`claims_nostream` exists: {{E6.dry_published}}). The message says: use `STREAM()` or `readStream` "to declare the source as a streaming input". A dry run finds such analysis errors without processing data.
# MAGIC
# MAGIC **Delete.** After `delete_pipeline(CHECKS)` (`DELETE /api/2.0/pipelines/{id}`), `claims_strict` still exists: **{{E6.tables_kept_after_delete}}**. *Use Unity Catalog with pipelines*: "the associated materialized views, streaming tables, and views are retained by default" — they can be queried but not updated; the REST API deletes them too with `cascade=true`.
# MAGIC
# MAGIC > **Key takeaway:** `FAIL UPDATE` is for rules that must never be broken (the update fails, nothing is written, you fix the data or the rule). Read the error in the event log (`level = 'ERROR'`) or through the API. Use a dry run to catch missing `STREAM`, wrong names and wrong columns early.

# COMMAND ----------

# DBTITLE 1,E7 Predict
# MAGIC %md
# MAGIC ### E7 — Full refresh
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. A full refresh of the main pipeline: how many rows does bronze write — 0, 50 or 250?
# MAGIC 2. After it, which specialty do the silver rows of provider 3 show?
# MAGIC 3. What do you see in `DESCRIBE HISTORY` of `claims_bronze`?

# COMMAND ----------

# DBTITLE 1,E7 Code
u4, u4_state, u4_secs = run_update(MAIN, full_refresh=True)    # update 4: clear the streaming tables, reprocess all
e7_flows = flow_metrics(MAIN, u4)
display(e7_flows)
silver_p3_after = spark.sql(f"""SELECT specialty, count(*) AS claims FROM {FQ}.claims_silver
                                WHERE provider_id = 3 GROUP BY specialty""")
display(silver_p3_after)
history = spark.sql(f"DESCRIBE HISTORY {FQ}.claims_bronze").select("version", "operation", "operationMetrics")
display(history.orderBy("version"))                           # one row per table version

flows = {r["flow"]: r for r in e7_flows.collect()}
ops = [r["operation"] for r in history.orderBy("version").collect()]
METRICS["E7.state"] = u4_state
METRICS["nd.E7.secs"] = u4_secs
METRICS["E7.bronze_out"] = flows["claims_bronze"]["output_rows"]
METRICS["E7.silver_out"] = flows["claims_silver"]["output_rows"]
METRICS["E7.silver_dropped"] = flows["claims_silver"]["dropped_rows"]
METRICS["E7.quarantine_out"] = flows["claims_quarantine"]["output_rows"]
METRICS["E7.bronze_total"] = spark.table(f"{FQ}.claims_bronze").count()
METRICS["E7.silver_p3"] = ", ".join(f"{r['specialty']}: {r['claims']}" for r in silver_p3_after.collect())
METRICS["E7.gold_technique"] = mv_technique(MAIN, u4, "claims_by_specialty")
METRICS["E7.has_refresh_op"] = "DLT REFRESH" in ops
METRICS["nd.E7.history_ops"] = ", ".join(ops)
assert flows["claims_bronze"]["output_rows"] == 250 and spark.table(f"{FQ}.claims_bronze").count() == 250

# COMMAND ----------

# DBTITLE 1,E7 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Update 4 (full refresh): `{{E7.state}}`. Bronze wrote **{{E7.bronze_out}}** rows — every file again, not only new ones — and the table has {{E7.bronze_total}} rows, **not** duplicates: the full refresh first cleared it. Silver wrote {{E7.silver_out}} and dropped {{E7.silver_dropped}}; quarantine wrote {{E7.quarantine_out}}.
# MAGIC * Silver rows of provider 3 now show **{{E7.silver_p3}}**: all rows were joined again with today's provider table.
# MAGIC * Gold technique: `{{E7.gold_technique}}`.
# MAGIC * `DESCRIBE HISTORY claims_bronze` has a `DLT REFRESH` operation: {{E7.has_refresh_op}}. In this run the operations were: {{nd.E7.history_ops}}.
# MAGIC
# MAGIC > **Key takeaway:** full refresh = "start from zero" for streaming tables. It is safe only when the source still has all the data (here the files are still in the volume). For Kafka with short retention or deleted files, data would be lost — protect such tables with `pipelines.reset.allowed = false`.

# COMMAND ----------

# DBTITLE 1,E8 Predict
# MAGIC %md
# MAGIC ### E8 — The silver layer in Python
# MAGIC
# MAGIC We add a Python source file to the **same** pipeline. It defines `claims_silver_py` with the same rules as the SQL silver table.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Can one pipeline mix SQL and Python files?
# MAGIC 2. In a normal update, how many rows does the new table read from bronze: 0 (bronze wrote nothing new) or all of them?
# MAGIC 3. Will `claims_silver` and `claims_silver_py` have exactly the same rows?

# COMMAND ----------

# DBTITLE 1,E8 Code
SILVER_PY = '''
from pyspark import pipelines as dp                  # current API (legacy: import dlt)
from pyspark.sql import functions as F

@dp.table(name="claims_silver_py",                   # a streaming table, because the function returns a stream
          comment="Python version of claims_silver")
@dp.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")                             # DROP ROW
@dp.expect_or_drop("positive_amount", "claim_amount IS NOT NULL AND claim_amount > 0")   # DROP ROW
@dp.expect("known_status", "claim_status IN ('SUBMITTED', 'APPROVED', 'DENIED', 'PENDING')")   # warn
def claims_silver_py():
    providers = spark.read.table("main.dbacademy_l07.providers").select("provider_id", "specialty")   # batch
    return (spark.readStream.table("claims_bronze")  # STREAM(claims_bronze) in SQL
            .join(providers, "provider_id", "left")  # stream-static join, like the SQL version
            .select("claim_id", "patient_id", "provider_id", "specialty",
                    F.col("claim_amount").cast("decimal(10,2)").alias("claim_amount"),
                    "claim_status",
                    F.col("service_date").cast("date").alias("service_date"),
                    "source_file"))
'''
write_source("silver_py.py", SILVER_PY)                        # a .py file is a Python source file
edit_pipeline(MAIN, "claims", ["bronze.sql", "silver.sql", "gold.sql", "silver_py.py"])   # add the Python file
u5, u5_state, u5_secs = run_update(MAIN)                       # update 5: a normal refresh
e8_flows = flow_metrics(MAIN, u5)
display(e8_flows)
e8_exp = expectation_metrics(MAIN, u5)
display(e8_exp)
only_sql = spark.sql(f"SELECT * FROM {FQ}.claims_silver EXCEPT SELECT * FROM {FQ}.claims_silver_py").count()
only_py = spark.sql(f"SELECT * FROM {FQ}.claims_silver_py EXCEPT SELECT * FROM {FQ}.claims_silver").count()
print(f"rows only in SQL silver: {only_sql}, rows only in Python silver: {only_py}")

flows = {r["flow"]: r for r in e8_flows.collect()}
exp = {(r["dataset"], r["expectation"]): (r["passed"], r["failed"]) for r in e8_exp.collect()}
METRICS["E8.state"] = u5_state
METRICS["nd.E8.secs"] = u5_secs
METRICS["E8.py_out"] = flows["claims_silver_py"]["output_rows"]
METRICS["E8.py_dropped"] = flows["claims_silver_py"]["dropped_rows"]
METRICS["E8.sql_silver_out"] = flows.get("claims_silver", {"output_rows": 0})["output_rows"]
METRICS["E8.py_expectations"] = ", ".join(f"{n}: {p} passed / {f} failed" for (d, n), (p, f) in sorted(exp.items()) if d == "claims_silver_py")
METRICS["E8.only_sql"], METRICS["E8.only_py"] = only_sql, only_py
METRICS["E8.same_types"] = spark.table(f"{FQ}.claims_silver").dtypes == spark.table(f"{FQ}.claims_silver_py").dtypes
assert only_sql == 0 and only_py == 0

# COMMAND ----------

# DBTITLE 1,E8 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Update 5: `{{E8.state}}`. One pipeline ran SQL and Python files together.
# MAGIC * The new Python table read **all** bronze rows: it wrote {{E8.py_out}} rows and dropped {{E8.py_dropped}}, while the SQL silver table wrote {{E8.sql_silver_out}} (nothing new for it). A new streaming flow starts with an empty checkpoint, so it reads the whole source once.
# MAGIC * Expectations of `claims_silver_py`: {{E8.py_expectations}}.
# MAGIC * Rows only in the SQL table: {{E8.only_sql}}; only in the Python table: {{E8.only_py}}; same column types: {{E8.same_types}}. Both versions are the same silver layer.
# MAGIC
# MAGIC | SQL | Python (`from pyspark import pipelines as dp`) |
# MAGIC | --- | --- |
# MAGIC | `CREATE OR REFRESH STREAMING TABLE t` | `@dp.table(name="t")` + a function that returns `spark.readStream…` |
# MAGIC | `CREATE OR REFRESH MATERIALIZED VIEW mv` | `@dp.materialized_view` + a function that returns `spark.read…` |
# MAGIC | `CREATE TEMPORARY VIEW v` | `@dp.temporary_view` |
# MAGIC | `CONSTRAINT n EXPECT (rule)` | `@dp.expect("n", "rule")` |
# MAGIC | `… ON VIOLATION DROP ROW` | `@dp.expect_or_drop("n", "rule")` |
# MAGIC | `… ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail("n", "rule")` |
# MAGIC | `FROM STREAM(claims_bronze)` | `spark.readStream.table("claims_bronze")` |
# MAGIC | `FROM STREAM read_files(path, format => 'json')` | `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(path)` |
# MAGIC
# MAGIC > **Key takeaway:** SQL and Python are two syntaxes for the same datasets. Python adds grouped rules (`expect_all…`) and reusable code; the rules themselves are SQL strings in both.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "Pipeline SQL runs in a normal notebook cell." | Not as a pipeline: `CREATE OR REFRESH STREAMING TABLE` failed on serverless notebook compute in L06 (E11). The code goes into source files and runs in a pipeline update (4.3). |
# MAGIC | 2 | "The source files must be listed in dependency order." | We listed gold first; the update built the graph itself (E1). |
# MAGIC | 3 | "A temporary view in a pipeline is published to Unity Catalog." | `provider_lookup` was not in the catalog (E1: {{E1.temp_view_published}}). |
# MAGIC | 4 | "A streaming table re-reads all files on every update." | Update 2 wrote {{E3.bronze_out}} bronze rows — only the new file (E3). |
# MAGIC | 5 | "A materialized view is always fully recomputed." | On serverless it used `{{E3.gold_technique}}` after new data (E3, *Configure a serverless pipeline*). |
# MAGIC | 6 | "A dimension change updates old rows of a streaming table." | Old silver rows kept {{E4.silver_p3}}; only the materialized view followed the change (E4). |
# MAGIC | 7 | "A warn expectation removes bad rows." | `known_status` failed {{E2.known_status.failed}} rows and all of them stayed in silver (E2). |
# MAGIC | 8 | "Each expectation only checks the rows that passed the rule before it." | Every rule counted all input rows; dropped = rows failing at least one drop rule (E2). |
# MAGIC | 9 | "`DROP ROW` loses data without a trace." | The event log counts them ({{E2.silver_dropped}} in update 1), and a quarantine table can keep them (E5). |
# MAGIC | 10 | "`FAIL UPDATE` writes the good rows and stops at the first bad one." | `claims_strict` kept {{E6.strict_rows}} rows; the update was `{{E6.state}}` (E6). |
# MAGIC | 11 | "One failed flow fails every flow of a triggered update." | The independent materialized view still completed ({{E6.flow_status}}) (E6). |
# MAGIC | 12 | "Expectation metrics show how many rows broke a `FAIL UPDATE` rule." | "Because fail causes the update to fail … metrics are not recorded" (*Manage data quality with pipeline expectations*); read the error event instead (E6). |
# MAGIC | 13 | "A missing `STREAM` only shows up as wrong results later." | It is an analysis error, found by a dry run: `{{E6.dry_error_classes}}` (E6). |
# MAGIC | 14 | "A full refresh appends the data again (duplicates)." | It clears the streaming tables first: bronze wrote {{E7.bronze_out}} rows and has {{E7.bronze_total}} (E7). |
# MAGIC | 15 | "A full refresh is always safe." | Data that is no longer in the source (Kafka retention, deleted files) is lost; block it with `pipelines.reset.allowed = false` (*Run a pipeline update*). |
# MAGIC | 16 | "Deleting a pipeline deletes its tables." | The tables were kept ({{E6.tables_kept_after_delete}}); `cascade=true` in the REST API deletes them too (E6, *Use Unity Catalog with pipelines*). |
# MAGIC | 17 | "`import dlt` is the current Python API." | `from pyspark import pipelines as dp`; `dlt` still works (*Lakeflow pipelines Python language reference*). |
# MAGIC | 18 | "The event log is a normal table in Catalog Explorer." | It is a hidden table; query it with `event_log('<pipeline-id>')`, or publish it with the `event_log` setting (*Pipeline event log*). |
# MAGIC | 19 | "Continuous mode is the recommended way to run a pipeline all the time." | Databricks recommends a continuous **job** around a triggered pipeline (*Triggered vs. continuous pipeline mode*). |

# COMMAND ----------

# DBTITLE 1,7 — Syntax Cheat Sheet
# MAGIC %md
# MAGIC ## 7 Syntax Cheat Sheet
# MAGIC
# MAGIC | Goal | Pipeline SQL | Pipeline Python (`from pyspark import pipelines as dp`) |
# MAGIC | --- | --- | --- |
# MAGIC | Bronze from files | `CREATE OR REFRESH STREAMING TABLE b AS SELECT *, _metadata.file_name AS source_file FROM STREAM read_files('/Volumes/…', format => 'json')` | `@dp.table` → `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(path)` |
# MAGIC | Silver from bronze | `CREATE OR REFRESH STREAMING TABLE s AS SELECT … FROM STREAM(b)` | `@dp.table` → `spark.readStream.table("b")` |
# MAGIC | Gold aggregate | `CREATE OR REFRESH MATERIALIZED VIEW g AS SELECT k, count(*) FROM s GROUP BY k` | `@dp.materialized_view` → `spark.read.table("s").groupBy("k").count()` |
# MAGIC | Helper view | `CREATE TEMPORARY VIEW v AS SELECT …` | `@dp.temporary_view` |
# MAGIC | Warn | `CONSTRAINT n EXPECT (rule)` | `@dp.expect("n", "rule")` |
# MAGIC | Drop row | `CONSTRAINT n EXPECT (rule) ON VIOLATION DROP ROW` | `@dp.expect_or_drop("n", "rule")` |
# MAGIC | Fail update | `CONSTRAINT n EXPECT (rule) ON VIOLATION FAIL UPDATE` | `@dp.expect_or_fail("n", "rule")` |
# MAGIC | Several rules, one action | several `CONSTRAINT` clauses, separated by commas | `@dp.expect_all({...})`, `@dp.expect_all_or_drop({...})`, `@dp.expect_all_or_fail({...})` |
# MAGIC | Quarantine | `CREATE OR REFRESH STREAMING TABLE q AS SELECT * FROM STREAM(b) WHERE NOT (rules)` | `@dp.table` → `spark.readStream.table("b").where("NOT (rules)")` |
# MAGIC | Block full refresh | `TBLPROPERTIES ('pipelines.reset.allowed' = 'false')` | `@dp.table(table_properties={"pipelines.reset.allowed": "false"})` |
# MAGIC
# MAGIC ### Running and monitoring
# MAGIC
# MAGIC | Goal | Databricks SDK (Python) | CLI |
# MAGIC | --- | --- | --- |
# MAGIC | Save a source file | `w.workspace.upload(path, data, format=ImportFormat.AUTO, overwrite=True)` | `databricks workspace import` |
# MAGIC | Create a serverless pipeline | `w.pipelines.create(name=…, catalog="main", schema="…", serverless=True, libraries=[pl.PipelineLibrary(file=pl.FileLibrary(path=…))])` | `databricks pipelines create --json @spec.json` |
# MAGIC | Normal update | `w.pipelines.start_update(pid)` | `databricks pipelines start-update <pid>` |
# MAGIC | Full refresh | `w.pipelines.start_update(pid, full_refresh=True)` | `databricks pipelines start-update <pid> --full-refresh` |
# MAGIC | Dry run | `w.pipelines.start_update(pid, validate_only=True)` | `databricks pipelines start-update <pid> --validate-only` |
# MAGIC | Update state | `w.pipelines.get_update(pid, uid).update.state` | `databricks pipelines get-update <pid> <uid>` |
# MAGIC | Errors | `w.pipelines.list_pipeline_events(pid, filter="level='ERROR'")` | `databricks pipelines list-pipeline-events <pid>` |
# MAGIC | Delete | `w.pipelines.delete(pid)` (tables are kept) | `databricks pipelines delete <pid>` |
# MAGIC
# MAGIC ```sql
# MAGIC -- Expectation results of one update (pattern from the *Pipeline event log* page)
# MAGIC SELECT e.dataset, e.name, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
# MAGIC FROM (SELECT explode(from_json(details:flow_progress.data_quality.expectations,
# MAGIC              'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) AS e
# MAGIC       FROM event_log('<pipeline-id>')
# MAGIC       WHERE event_type = 'flow_progress' AND origin.update_id = '<update-id>')
# MAGIC GROUP BY ALL;
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,8 — Check Your Understanding
# MAGIC %md
# MAGIC ## 8 Check Your Understanding
# MAGIC
# MAGIC Answer in your own words. Answers are in section 11.
# MAGIC
# MAGIC 1. In E4, the silver rows of provider 3 kept the old specialty, but the gold view showed the new one. Explain why, and give two ways to make the old silver rows follow the change.
# MAGIC 2. A lab-results pipeline has the rule "result value must not be negative". When would you use `DROP ROW` plus a quarantine table, and when `FAIL UPDATE`?
# MAGIC 3. You built bronze with Auto Loader in a notebook in L06. Name four things you had to manage there that the pipeline in this lesson manages for you.

# COMMAND ----------

# DBTITLE 1,9 — Practice (intro)
# MAGIC %md
# MAGIC ## 9 Practice
# MAGIC
# MAGIC Do these yourself, without AI help. Each task writes pipeline source code as a Python string, and `run_practice(...)` runs it in **one** practice pipeline, `dbacademy_l07_practice` (always a full refresh, so you can run a task again and again). Each validation cell prints ✅ Passed or a hint. Solutions are in section 11.
# MAGIC
# MAGIC > The practice uses the claim files in `LANDING` and the tables of the main pipeline. Run sections 3, 4.7 and 5 first. Every practice update takes about a minute or more; when an update fails, the cell prints the error events — read the error class and compare it with E6.

# COMMAND ----------

# DBTITLE 1,9 — Practice helper (run first)
def run_practice(files):
    """Write the practice source files, point the practice pipeline at exactly these files, run a full refresh.
    Returns (pipeline_id, update_id, final state) and prints the error events of a failed update."""
    for file_name, text in files.items():
        write_source(file_name, text)                          # save each source file in the workspace
    pipeline_id = lesson_pipelines().get(f"{PREFIX}_practice")
    if pipeline_id:
        edit_pipeline(pipeline_id, "practice", list(files))   # reuse it, with only the files of this task
    else:
        pipeline_id = create_pipeline("practice", list(files))
    update_id, state, _ = run_update(pipeline_id, full_refresh=True)   # always start from zero
    for event_type, message, exceptions in update_errors(pipeline_id, update_id):
        print(f"ERROR in {event_type}: {message}\n    {exceptions[:700]}\n")
    return pipeline_id, update_id, state

# The numbers every practice task should reach (computed from the landing files, not typed by hand)
RULES = "claim_id IS NOT NULL AND claim_amount IS NOT NULL AND claim_amount > 0"
EXPECTED = spark.sql(f"""
    SELECT count(*)                                     AS total,
           count_if({RULES})                            AS valid,
           count_if(NOT ({RULES}))                      AS invalid,
           count_if(claim_id IS NULL)                   AS missing_id,
           count_if({RULES} AND claim_status = 'IN_REVIEW') AS valid_in_review,
           count(DISTINCT CASE WHEN {RULES} THEN claim_status END) AS valid_statuses
    FROM read_files('{LANDING}', format => 'json')""").first().asDict()
print("Practice targets:", EXPECTED)

# COMMAND ----------

# DBTITLE 1,P1 Task
# MAGIC %md
# MAGIC ### P1 — Fill in the blanks (3 blanks): expectations
# MAGIC Complete the source of the streaming table `p1_claims_clean`: rows without a `claim_id` and rows without a positive amount must be **dropped**; an unknown status must only be **counted** (the row stays). The table must read only **new** files on each update. Replace the three `<FILL_IN>` markers, then run the cell.

# COMMAND ----------

# DBTITLE 1,P1 Code
# >>> BLANK P1
P1_SQL = f"""
CREATE OR REFRESH STREAMING TABLE p1_claims_clean (
  CONSTRAINT valid_claim_id  <FILL_IN> (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (claim_amount IS NOT NULL AND claim_amount > 0) <FILL_IN>,
  CONSTRAINT known_status    EXPECT (claim_status IN ('SUBMITTED', 'APPROVED', 'DENIED', 'PENDING'))
)
AS SELECT * FROM <FILL_IN> read_files('{LANDING}', format => 'json')
"""
# >>> SOLUTION P1
P1_SQL = f"""
CREATE OR REFRESH STREAMING TABLE p1_claims_clean (
  CONSTRAINT valid_claim_id  EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (claim_amount IS NOT NULL AND claim_amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT known_status    EXPECT (claim_status IN ('SUBMITTED', 'APPROVED', 'DENIED', 'PENDING'))
)
AS SELECT * FROM STREAM read_files('{LANDING}', format => 'json')
"""
# <<< END P1
p1 = run_practice({"p1.sql": P1_SQL})

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    pid, uid, state = p1
    rows = spark.table(f"{FQ}.p1_claims_clean").count()
    names = {r["expectation"] for r in expectation_metrics(pid, uid).collect()}
    in_review = spark.table(f"{FQ}.p1_claims_clean").filter("claim_status = 'IN_REVIEW'").count()
    ok = (state == "COMPLETED" and rows == EXPECTED["valid"] and in_review == EXPECTED["valid_in_review"]
          and names == {"valid_claim_id", "positive_amount", "known_status"})
    PRACTICE_PASSED["P1"] = ok
    if ok:
        print("✅ Passed")
    elif state != "COMPLETED":
        print(f"Hint: the update ended in {state}. Read the ERROR lines printed by the code cell: is every blank a SQL keyword?")
    else:
        print(f"Hint: want {EXPECTED['valid']} rows with {EXPECTED['valid_in_review']} IN_REVIEW rows kept, and the "
              f"expectations valid_claim_id, positive_amount, known_status; got {rows} rows, {in_review} IN_REVIEW, {sorted(names)}.")
    METRICS["P1.state"], METRICS["P1.rows"], METRICS["P1.in_review"] = state, rows, in_review
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: run the P1 code cell first; the table p1_claims_clean must exist.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs): a silver table that never finishes
# MAGIC A colleague wrote the streaming table `p2_claims_silver`. It must keep the valid claims, **drop** the claims without an id or without a positive amount, and the update must **never stop** because of bad rows. The code has two bugs. Fix them in place and run the cell until the validation passes. Read the error class of each failed update (compare with E6).

# COMMAND ----------

# DBTITLE 1,P2 Code
# >>> BLANK P2
P2_SQL = f"""
CREATE OR REFRESH STREAMING TABLE p2_claims_silver (
  CONSTRAINT valid_claim_id  EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (claim_amount IS NOT NULL AND claim_amount > 0) ON VIOLATION FAIL UPDATE
)
AS SELECT claim_id, patient_id, provider_id, claim_amount, claim_status
FROM read_files('{LANDING}', format => 'json')
"""
# >>> SOLUTION P2
P2_SQL = f"""
CREATE OR REFRESH STREAMING TABLE p2_claims_silver (
  CONSTRAINT valid_claim_id  EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (claim_amount IS NOT NULL AND claim_amount > 0) ON VIOLATION DROP ROW
)
AS SELECT claim_id, patient_id, provider_id, claim_amount, claim_status
FROM STREAM read_files('{LANDING}', format => 'json')
"""
# <<< END P2
p2 = run_practice({"p2.sql": P2_SQL})

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    pid, uid, state = p2
    if state != "COMPLETED":
        classes = error_classes(" ".join(m + " " + x for _, m, x in update_errors(pid, uid)))
        PRACTICE_PASSED["P2"] = False
        print(f"Hint: the update ended in {state} with {classes}. One error is about the kind of source a streaming "
              "table needs (E6 part B); the other is about the action of a rule (E6 part A and 4.4).")
    else:
        rows = spark.table(f"{FQ}.p2_claims_silver").count()
        dropped = {r["flow"]: r["dropped_rows"] for r in flow_metrics(pid, uid).collect()}.get("p2_claims_silver", 0)
        ok = rows == EXPECTED["valid"] and dropped == EXPECTED["invalid"]
        PRACTICE_PASSED["P2"] = ok
        print("✅ Passed" if ok else
              f"Hint: want {EXPECTED['valid']} rows and {EXPECTED['invalid']} dropped rows; got {rows} rows, {dropped} dropped.")
        METRICS["P2.rows"], METRICS["P2.dropped"] = rows, dropped
    METRICS["P2.state"] = state
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: run the P2 code cell first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): quarantine and a gold materialized view
# MAGIC Write **one** SQL source file `P3_SQL` with three datasets, all reading the claim files in `LANDING`, and run it with `p3 = run_practice({"p3.sql": P3_SQL})`:
# MAGIC 1. `p3_claims_valid` — a streaming table with the valid claims. Use two `DROP ROW` expectations named `valid_claim_id` and `positive_amount`.
# MAGIC 2. `p3_claims_quarantine` — a streaming table with **all other** claims, plus a column `quarantine_reason`: `'missing claim_id'` or `'amount not positive'`.
# MAGIC 3. `p3_gold_by_status` — a materialized view with one row per `claim_status` of the valid claims: `claim_status`, `claims` (count) and `total_amount` (sum of `claim_amount`).
# MAGIC
# MAGIC **Hints:** `STREAM read_files('{LANDING}', format => 'json')` (an f-string) · the quarantine filter is `NOT (…the two rules…)`, written NULL-safe (4.4) · a `CASE WHEN` for the reason · the gold view reads `p3_claims_valid` **without** `STREAM` · separate the statements with `;` (E1, E5).

# COMMAND ----------

# DBTITLE 1,P3 Code
# >>> BLANK P3
# Your code here
# >>> SOLUTION P3
P3_SQL = f"""
CREATE OR REFRESH STREAMING TABLE p3_claims_valid (
  CONSTRAINT valid_claim_id  EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (claim_amount IS NOT NULL AND claim_amount > 0) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM read_files('{LANDING}', format => 'json');

CREATE OR REFRESH STREAMING TABLE p3_claims_quarantine
AS SELECT *,
          CASE WHEN claim_id IS NULL THEN 'missing claim_id' ELSE 'amount not positive' END AS quarantine_reason
FROM STREAM read_files('{LANDING}', format => 'json')
WHERE NOT (claim_id IS NOT NULL AND claim_amount IS NOT NULL AND claim_amount > 0);

CREATE OR REFRESH MATERIALIZED VIEW p3_gold_by_status
AS SELECT claim_status, count(*) AS claims, sum(claim_amount) AS total_amount
FROM p3_claims_valid
GROUP BY claim_status;
"""
p3 = run_practice({"p3.sql": P3_SQL})
# <<< END P3

# COMMAND ----------

# DBTITLE 1,P3 Validation
try:
    pid, uid, state = p3
    valid = spark.table(f"{FQ}.p3_claims_valid").count()
    quarantine = spark.table(f"{FQ}.p3_claims_quarantine")
    q_rows = quarantine.count()
    q_missing = quarantine.filter("quarantine_reason = 'missing claim_id'").count()
    gold = spark.table(f"{FQ}.p3_gold_by_status")
    gold_rows, gold_claims = gold.count(), gold.agg({"claims": "sum"}).first()[0]
    names = {r["expectation"] for r in expectation_metrics(pid, uid).collect() if r["dataset"] == "p3_claims_valid"}
    ok = (state == "COMPLETED" and valid == EXPECTED["valid"] and q_rows == EXPECTED["invalid"]
          and q_missing == EXPECTED["missing_id"] and gold_rows == EXPECTED["valid_statuses"]
          and gold_claims == EXPECTED["valid"] and names == {"valid_claim_id", "positive_amount"}
          and {"claim_status", "claims", "total_amount"} <= set(gold.columns))
    PRACTICE_PASSED["P3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {EXPECTED['valid']} valid rows, {EXPECTED['invalid']} quarantined ({EXPECTED['missing_id']} 'missing claim_id'), "
          f"{EXPECTED['valid_statuses']} gold rows summing to {EXPECTED['valid']} claims, expectations valid_claim_id and positive_amount; "
          f"got state {state}, {valid} valid, {q_rows} quarantined ({q_missing} missing id), {gold_rows} gold rows, "
          f"{gold_claims} claims, expectations {sorted(names)}, gold columns {gold.columns}.")
    METRICS["P3.state"], METRICS["P3.valid"], METRICS["P3.quarantine"] = state, valid, q_rows
    METRICS["P3.gold_rows"], METRICS["P3.gold_claims"] = gold_rows, gold_claims
except Exception as e:
    PRACTICE_PASSED["P3"] = False
    print("Hint: create P3_SQL and run p3 = run_practice({'p3.sql': P3_SQL}); all three datasets must exist.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints): the silver layer in Python
# MAGIC Write a **Python** pipeline source file (a string `P4_PY`) that defines the streaming table `p4_claims_silver`:
# MAGIC * read `main.dbacademy_l07.claims_bronze` as a stream, and add `specialty` from `main.dbacademy_l07.providers`;
# MAGIC * columns `claim_id`, `patient_id`, `provider_id`, `specialty`, `claim_amount` (`DECIMAL(10,2)`), `claim_status`, `service_date` (`DATE`), `source_file`;
# MAGIC * drop the rows that break `valid_claim_id` or `positive_amount` with **one** grouped decorator, and only count the rows that break `known_status`.
# MAGIC
# MAGIC Run it with `p4 = run_practice({"p4.py": P4_PY})`. Goal: the same rows and column types as `claims_silver`.

# COMMAND ----------

# DBTITLE 1,P4 Code
# >>> BLANK P4
# Your code here
# >>> SOLUTION P4
P4_PY = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F

drop_rules = {"valid_claim_id": "claim_id IS NOT NULL",
              "positive_amount": "claim_amount IS NOT NULL AND claim_amount > 0"}

@dp.table(name="p4_claims_silver", comment="Silver claims (Python practice)")
@dp.expect_all_or_drop(drop_rules)
@dp.expect("known_status", "claim_status IN ('SUBMITTED', 'APPROVED', 'DENIED', 'PENDING')")
def p4_claims_silver():
    providers = spark.read.table("main.dbacademy_l07.providers").select("provider_id", "specialty")
    return (spark.readStream.table("main.dbacademy_l07.claims_bronze")
            .join(providers, "provider_id", "left")
            .select("claim_id", "patient_id", "provider_id", "specialty",
                    F.col("claim_amount").cast("decimal(10,2)").alias("claim_amount"),
                    "claim_status",
                    F.col("service_date").cast("date").alias("service_date"),
                    "source_file"))
'''
p4 = run_practice({"p4.py": P4_PY})
# <<< END P4

# COMMAND ----------

# DBTITLE 1,P4 Validation
try:
    pid, uid, state = p4
    t = spark.table(f"{FQ}.p4_claims_silver")
    silver = spark.table(f"{FQ}.claims_silver")
    rows = t.count()
    diff = t.exceptAll(silver).count() + silver.exceptAll(t).count() if t.dtypes == silver.dtypes else -1
    names = {r["expectation"] for r in expectation_metrics(pid, uid).collect() if r["dataset"] == "p4_claims_silver"}
    dropped = {r["flow"]: r["dropped_rows"] for r in flow_metrics(pid, uid).collect()}.get("p4_claims_silver", 0)
    ok = (state == "COMPLETED" and rows == silver.count() and diff == 0 and dropped == EXPECTED["invalid"]
          and names == {"valid_claim_id", "positive_amount", "known_status"})
    PRACTICE_PASSED["P4"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {silver.count()} rows equal to claims_silver (same columns and types), {EXPECTED['invalid']} dropped rows "
          f"and the expectations valid_claim_id, positive_amount, known_status; got state {state}, {rows} rows, "
          f"{diff} different rows (-1 = different column types: {t.dtypes}), {dropped} dropped, {sorted(names)}.")
    METRICS["P4.state"], METRICS["P4.rows"], METRICS["P4.diff"], METRICS["P4.dropped"] = state, rows, diff, dropped
except Exception as e:
    PRACTICE_PASSED["P4"] = False
    print("Hint: create P4_PY and run p4 = run_practice({'p4.py': P4_PY}) first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,10 — Exam-Style Questions
# MAGIC %md
# MAGIC ## 10 Exam-Style Questions
# MAGIC
# MAGIC Choose the **one best answer**. Answers and explanations are in section 11.
# MAGIC
# MAGIC **Q1.** A pipeline has a silver streaming table of claims that joins a `payers` Delta table to add the payer name. A payer is renamed. The finance team needs a gold table where **all** claims, old and new, show the current payer name after each pipeline update. What should the gold dataset be?
# MAGIC A. A streaming table that reads `STREAM(silver)` · B. A materialized view that joins silver with `payers` · C. A temporary view on silver · D. A streaming table with `ON VIOLATION DROP ROW`
# MAGIC
# MAGIC **Q2.** Lab results with a negative value must not reach the silver table, but the pipeline must keep running and the team wants to know how many rows were removed. Which definition is best?
# MAGIC A. `CONSTRAINT non_negative EXPECT (result_value >= 0)` · B. `CONSTRAINT non_negative EXPECT (result_value >= 0) ON VIOLATION DROP ROW` · C. `CONSTRAINT non_negative EXPECT (result_value >= 0) ON VIOLATION FAIL UPDATE` · D. A `WHERE result_value >= 0` filter in the gold view
# MAGIC
# MAGIC **Q3.** A hospital's compliance team asks that encounter records rejected by data quality rules are **not lost** and can be reviewed later, while silver stays clean. What should the engineer add?
# MAGIC A. Change the rules to warn only · B. A second streaming table that selects the rows matching `NOT (rules)` from the same bronze source · C. A full refresh after every update · D. `pipelines.reset.allowed = false` on silver
# MAGIC
# MAGIC **Q4.** A triggered pipeline has a streaming table with `CONSTRAINT valid_npi EXPECT (npi IS NOT NULL) ON VIOLATION FAIL UPDATE`, and an independent materialized view in the same pipeline. One input row has a NULL `npi`. What happens?
# MAGIC A. The table writes all valid rows and skips the bad one · B. The update fails, the table write is rolled back, and the independent materialized view can still complete · C. The bad row is written and counted as failed in the event log · D. The pipeline retries until the row is fixed
# MAGIC
# MAGIC **Q5.** A new pipeline fails to start with `CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED` for `CREATE OR REFRESH STREAMING TABLE pharmacy_bronze AS SELECT * FROM read_files('/Volumes/…/pharmacy', format => 'csv')`. What is the fix?
# MAGIC A. Change the table to a temporary view · B. Add the `STREAM` keyword: `FROM STREAM read_files(…)` · C. Add `schemaLocation` to `read_files` · D. Run a full refresh
# MAGIC
# MAGIC **Q6.** A data engineer must report, for the last update of a claims pipeline, how many records failed each expectation. Where is this information?
# MAGIC A. `DESCRIBE HISTORY` of the silver table · B. `event_log('<pipeline-id>')`, `flow_progress` events, `details:flow_progress.data_quality.expectations` · C. `system.information_schema.tables` · D. The `_rescued_data` column of silver
# MAGIC
# MAGIC **Q7.** A streaming table reads device events from Kafka, which keeps data for 7 days. The table holds two years of history. An engineer wants to run a **full refresh** to apply a new column. What is the risk?
# MAGIC A. None: a full refresh only adds the new column · B. The table is cleared and only the data still in Kafka is reprocessed, so older history is lost · C. The full refresh fails because streaming tables cannot be refreshed · D. Rows are duplicated
# MAGIC
# MAGIC **Q8.** You migrate a legacy pipeline file that starts with `import dlt` and uses `@dlt.table` for a gold aggregate built with `spark.read.table(...)`. Which current code is correct?
# MAGIC A. `import pipelines` and `@pipelines.view` · B. `from pyspark import pipelines as dp` and `@dp.materialized_view` · C. `from pyspark import pipelines as dp` and `@dp.temporary_view` · D. Keep `@dlt.table`; the `pyspark.pipelines` module does not exist on Databricks

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — `COPY INTO` twice** (the second run skips the files it already loaded, so it inserts 0 rows)
# MAGIC {{solution:W1}}
# MAGIC
# MAGIC **W2 — Auto Loader with a typed column** (without `inferColumnTypes` every JSON column is a string; `schemaHints` forces `DOUBLE`)
# MAGIC {{solution:W2}}
# MAGIC
# MAGIC **W3 — rescued data** (a value that does not fit the type becomes NULL and is kept in `_rescued_data`)
# MAGIC {{solution:W3}}
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. A streaming table processes each input row once. The join with `providers` happened when the row arrived, so old silver rows keep the old value ({{E4.silver_p3}}); the materialized view re-joins `providers` on every update (E4). Fixes: compute the specialty in a materialized view (gold) instead of the streaming table, or run a full refresh of silver (E7: {{E7.silver_p3}}). In L08 you will also see AUTO CDC / SCD for dimensions.
# MAGIC 2. `DROP ROW` + quarantine when a few bad results are normal and the rest of the data must keep flowing (E2, E5: nothing lost, metrics in the event log). `FAIL UPDATE` when a bad value means something is seriously wrong and **no** data may be published until a person looks at it (E6: the update failed and the table got {{E6.strict_rows}} rows).
# MAGIC 3. Any four of: the checkpoint folder, the schema location, the trigger and `awaitTermination`, the restart after a schema change, the order of the steps, the data quality code and its metrics, the monitoring (event log), retries.
# MAGIC
# MAGIC ### P1 (blanks: `EXPECT`, `ON VIOLATION DROP ROW`, `STREAM`)
# MAGIC {{solution:P1}}
# MAGIC
# MAGIC ### P2 (fixes: `STREAM read_files` — without it the update fails like E6 part B; `ON VIOLATION DROP ROW` instead of `FAIL UPDATE` — otherwise the update fails like E6 part A)
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
# MAGIC **Q1 → B.** A materialized view returns the current result of its query, so it re-joins the dimension (E4: gold showed {{E4.oncology_gold}} Oncology claims while silver's column still said {{E4.silver_p3}}). A wrong: a streaming table keeps old rows as they were processed. C wrong: a temporary view is not published, the finance team cannot query it (E1). D wrong: an expectation action does not change join results.
# MAGIC
# MAGIC **Q2 → B.** `DROP ROW` removes the rows and the event log counts them (E2: {{E2.silver_dropped}} dropped). A wrong: warn keeps the rows (E2: {{E2.in_review_rows}} `IN_REVIEW` rows stayed). C wrong: the update fails (E6). D wrong: silver still contains the bad rows, and no metric is recorded.
# MAGIC
# MAGIC **Q3 → B.** The quarantine pattern (*Expectation recommendations and advanced patterns*; E5: bronze = silver + quarantine was {{E5.balanced}}). A wrong: silver would not stay clean. C wrong: a full refresh reprocesses but does not keep rejected rows. D wrong: it only blocks full refresh.
# MAGIC
# MAGIC **Q4 → B.** E6: update `{{E6.state}}`, `claims_strict` {{E6.strict_rows}} rows, flows {{E6.flow_status}}; docs: "In a triggered pipeline, the failure of a single flow does not cause other parallel flows to fail." A wrong: that is `DROP ROW`. C wrong: that is warn; fail records no metrics. D wrong: the docs say manual intervention is needed; in E6 the flow message said "{{E6.restart_message}}".
# MAGIC
# MAGIC **Q5 → B.** E6 part B: the same error class `{{E6.dry_error_classes}}`; the message says to use `STREAM()` or `readStream`. A wrong: the table must be published. C wrong: inside a pipeline the schema location is managed (*read_files* reference: "isn't required when used in a streaming table query"). D wrong: the definition itself is invalid.
# MAGIC
# MAGIC **Q6 → B.** E2 read these numbers from `flow_progress` events (`valid_claim_id`: {{E2.valid_claim_id.failed}} failed). A wrong: history has versions and operations, not expectation counts. C wrong: lists tables. D wrong: rescued data is about parsing (L06).
# MAGIC
# MAGIC **Q7 → B.** A full refresh "Clears data from streaming tables … and reprocesses all records from the data source"; records no longer in Kafka are dropped (*Run a pipeline update*). E7 showed the clear-and-reprocess ({{E7.bronze_out}} rows written, {{E7.bronze_total}} in the table) with files that were still there. A wrong: it rewrites the whole table. C wrong: E7 ran one. D wrong: E7 had no duplicates.
# MAGIC
# MAGIC **Q8 → B.** *Lakeflow pipelines Python language reference*: `dlt` "has been replaced by the `pyspark.pipelines` module"; `@dlt.table` with a batch DataFrame becomes `@dp.materialized_view`. A wrong: no such import. C wrong: a temporary view is not published. D wrong: `pyspark.pipelines` ran in E8 and P4.

# COMMAND ----------

# DBTITLE 1,12 — Cleanup
# Delete the pipelines first, then the runtime folder with the source files, then the schema
# (DROP SCHEMA ... CASCADE also removes the streaming tables and materialized views the pipelines kept).
for pipeline_id in lesson_pipelines().values():
    delete_pipeline(pipeline_id)
try:
    w.workspace.delete(RUNTIME, recursive=True)
except NotFound:
    pass
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
left = lesson_pipelines()
METRICS["cleanup.pipelines_left"] = len(left)
print(f"Deleted the lesson pipelines ({len(left)} left), the folder {RUNTIME} and the schema {FQ}.")

# COMMAND ----------

# DBTITLE 1,13 — Final Summary
# MAGIC %md
# MAGIC ## 13 Final Summary (numbers from the real run)
# MAGIC
# MAGIC | Example | Key result |
# MAGIC | --- | --- |
# MAGIC | Setup | {{setup.claim_files}} claim files with {{setup.claim_rows}} claims, {{setup.provider_rows}} providers; pipeline code in a workspace folder, data in Unity Catalog |
# MAGIC | E1 first update | `{{E1.state}}` in {{nd.E1.secs}} s; bronze {{E1.bronze}}, silver {{E1.silver}}, quarantine {{E1.quarantine}}, gold {{E1.gold_rows}} rows; temporary view published: {{E1.temp_view_published}} |
# MAGIC | E2 event log | dropped {{E2.silver_dropped}}, warned {{E2.silver_warned}}; `valid_claim_id` {{E2.valid_claim_id.passed}}/{{E2.valid_claim_id.failed}}, `positive_amount` {{E2.positive_amount.passed}}/{{E2.positive_amount.failed}}, `known_status` {{E2.known_status.passed}}/{{E2.known_status.failed}} (passed/failed) |
# MAGIC | E3 new file | bronze wrote {{E3.bronze_out}} rows (only the new file); gold technique `{{E3.gold_technique}}` ({{nd.E3.secs}} s) |
# MAGIC | E4 dimension change | silver kept {{E4.silver_p3}}; gold Pediatrics {{E4.pediatrics_gold}}, Oncology {{E4.oncology_gold}} |
# MAGIC | E5 quarantine | {{E5.quarantine}} rows ({{E5.reasons}}); bronze = silver + quarantine: {{E5.balanced}} |
# MAGIC | E6 failures | `FAIL UPDATE`: `{{E6.state}}`, `{{E6.error_classes}}`, `claims_strict` {{E6.strict_rows}} rows, parallel MV {{E6.status_rows}} rows; dry run without `STREAM`: `{{E6.dry_error_classes}}`; tables kept after delete: {{E6.tables_kept_after_delete}} |
# MAGIC | E7 full refresh | bronze wrote {{E7.bronze_out}} (table {{E7.bronze_total}}); silver provider 3 now {{E7.silver_p3}}; gold `{{E7.gold_technique}}` |
# MAGIC | E8 Python silver | {{E8.py_out}} rows, {{E8.py_dropped}} dropped; differences to SQL silver: {{E8.only_sql}} / {{E8.only_py}} |
# MAGIC | Practice | P1 {{P1.rows}} rows · P2 {{P2.rows}} rows, {{P2.dropped}} dropped · P3 {{P3.valid}} valid, {{P3.quarantine}} quarantined, {{P3.gold_rows}} gold rows · P4 {{P4.rows}} rows, {{P4.diff}} differences |
# MAGIC | Cleanup | lesson pipelines left: {{cleanup.pipelines_left}} |
# MAGIC
# MAGIC Update durations in this run (they change from run to run): E1 {{nd.E1.secs}} s, E3 {{nd.E3.secs}} s, E4 {{nd.E4.secs}} s, E6 {{nd.E6.secs}} s, dry run {{nd.E6.dry_secs}} s, E7 {{nd.E7.secs}} s, E8 {{nd.E8.secs}} s.
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **Continuous pipelines and continuous jobs** — not run (cost; the Free Edition limits serverless usage). Described from *Triggered vs. continuous pipeline mode*.
# MAGIC * **The effect of `development: true` on retries** — all pipelines here used `development: true`; we saw only the message "{{E6.restart_message}}", not a comparison with `false`.
# MAGIC * **Publishing the event log to a table (`event_log` setting) and the `system.lakeflow` pipeline system table** — not tested; this lesson used `event_log('<id>')` and the events API. System tables come in L16.
# MAGIC * **Reset streaming flow checkpoints (`reset_checkpoint_selection`), `refresh_selection`, `pipelines.reset.allowed = false`** — from *Run a pipeline update*; not run.
# MAGIC * **`@dp.expect_or_fail`, `@dp.expect_all`, `@dp.expect_all_or_fail`, `@dp.temporary_view`, `@dp.materialized_view` in Python** — syntax from the docs; the Python code that ran here used `@dp.table`, `@dp.expect`, `@dp.expect_or_drop` (E8) and `@dp.expect_all_or_drop` (P4).
# MAGIC * **The Lakeflow Pipelines Editor and the pipeline UI (graph, Data quality tab)** — this lesson works through the API only.
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. The SDK preinstalled on serverless job compute ({{nd.setup.sdk_version}} in this run) did not know `root_path` in `w.pipelines.create(...)` (`TypeError`), so the lesson uses the REST API through `w.api_client.do`.
# MAGIC 2. Free Edition: a third pipeline could not start while two lesson pipelines existed (`RESOURCE_EXHAUSTED … limit for severless compute for free usage`), even when all of them were idle; deleting one pipeline fixed it.
# MAGIC 3. Deleting a pipeline kept its tables ({{E6.tables_kept_after_delete}}): current docs say tables are retained by default; older material says they are deleted.
# MAGIC 4. The event log has a `warned_records` field (E2: {{E2.silver_warned}}) that the event log schema page does not list. Row counts of streaming flows come in `flow_progress` events with status `RUNNING` (the docs mention this: "metrics are reported in each micro-batch in a `flow_progress` event with the status `RUNNING`"), so `flow_metrics` sums all events of a flow.
# MAGIC 5. Every expectation counted every input row, also rows that another `DROP ROW` rule removed (E2).
# MAGIC 6. In E4 the streaming flows wrote nothing and still the gold view changed: the materialized view follows changes in a table **outside** the pipeline.
