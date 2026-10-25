# Databricks notebook source
# LESSON_TITLE: Lakeflow Declarative Pipelines II - AUTO CDC, SCD Type 1 and Type 2

# COMMAND ----------

# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L08 — Lakeflow Declarative Pipelines II: AUTO CDC, SCD Type 1 and Type 2
# MAGIC
# MAGIC **Exam objectives**
# MAGIC
# MAGIC * Professional (Oct 9, 2026), Section 1 *Developing code* (23%): "Implement CDC pipelines using AUTO CDC APIs (formerly APPLY CHANGES) in Lakeflow Pipelines, including SCD Type 1 and Type 2 via stored_as_scd_type."
# MAGIC * Professional Section 2 *Data Ingestion & Acquisition* (12%): "Configure incremental CDC pipelines using Lakeflow Pipelines with Delta or Iceberg as the target table format." (Lakeflow Connect, the managed database connectors, comes in L20.)
# MAGIC
# MAGIC **Names.** The AUTO CDC APIs were called **APPLY CHANGES** (SQL `APPLY CHANGES INTO`, Python `dlt.apply_changes()`). "The AUTO CDC APIs replace the APPLY CHANGES APIs and have the same syntax. The APPLY CHANGES APIs are still available, but Databricks recommends using the AUTO CDC APIs in their place" (*The AUTO CDC APIs: Simplify change data capture with pipelines*). The product is Lakeflow Declarative Pipelines (formerly Delta Live Tables), as in L07.
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** `CREATE FLOW … AS AUTO CDC INTO …` (SQL) and `dp.create_auto_cdc_flow()` (Python) to keep an SCD Type 1 or Type 2 table in sync with a change feed.
# MAGIC 2. **Explain** how `KEYS`, `SEQUENCE BY`, `APPLY AS DELETE WHEN` and `APPLY AS TRUNCATE WHEN` handle out-of-order events, duplicates, deletes and full reloads.
# MAGIC 3. **Compare** SCD Type 1 vs Type 2, full history vs `TRACK HISTORY ON` some columns, and AUTO CDC vs AUTO CDC FROM SNAPSHOT vs a hand-written `MERGE`.
# MAGIC 4. **Implement** an SCD Type 2 dimension with a hand-written `MERGE` in a normal notebook (no pipeline), and read `__START_AT` / `__END_AT` to get current rows and point-in-time rows.
# MAGIC 5. **Diagnose** a wrong sequence column and missing delete handling, and read `num_upserted_rows` / `num_deleted_rows` from the pipeline event log.
# MAGIC
# MAGIC > **Evidence-only rule:** every number, row and error class in this notebook comes from a real run of this notebook (the build fills them in), or from an official Databricks doc page named in the text. Things that cannot be tested on this workspace are marked **NOT VERIFIED — concept only (reason)**.
# MAGIC
# MAGIC > **Run time:** this lesson starts real pipeline updates (two for the examples, one per practice task P1–P3). Each update takes one to a few minutes. Run the cells in order and wait for each one.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up (intro)
# MAGIC %md
# MAGIC ## 2 Warm-up — recall L07 Lakeflow Declarative Pipelines I (no hints)
# MAGIC
# MAGIC Write each task from memory. Run the validation cell after each one. Solutions are in section 11.
# MAGIC
# MAGIC > Run the next cell first: it creates two small temporary views for the warm-up. No pipeline is started in the warm-up.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up prep (run first)
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

# W2: heart-rate readings to test a data quality rule
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wu_vitals AS
    SELECT * FROM VALUES (1, 72), (2, NULL), (3, 15), (4, 300), (5, 250), (6, 20), (7, 110)
    AS t(reading_id, heart_rate)""")

# W3: a few events shaped like the output of event_log('<pipeline-id>') (same column names and JSON layout)
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wu_event_log AS
    SELECT event_type, named_struct('update_id', update_id, 'flow_name', flow_name) AS origin, details
    FROM VALUES
      ('create_update',   'u1', NULL, '{"create_update":{"cause":"API_CALL"}}'),
      ('flow_progress',   'u1', 'main.clinic.vitals_bronze', '{"flow_progress":{"status":"RUNNING","metrics":{"num_output_rows":65}}}'),
      ('flow_progress',   'u1', 'main.clinic.vitals_bronze', '{"flow_progress":{"status":"COMPLETED"}}'),
      ('flow_progress',   'u1', 'main.clinic.vitals_silver', '{"flow_progress":{"status":"RUNNING","metrics":{"num_output_rows":40},"data_quality":{"dropped_records":3}}}'),
      ('flow_progress',   'u1', 'main.clinic.vitals_silver', '{"flow_progress":{"status":"RUNNING","metrics":{"num_output_rows":20},"data_quality":{"dropped_records":2}}}'),
      ('flow_progress',   'u1', 'main.clinic.vitals_silver', '{"flow_progress":{"status":"COMPLETED"}}'),
      ('flow_progress',   'u2', 'main.clinic.vitals_silver', '{"flow_progress":{"status":"RUNNING","metrics":{"num_output_rows":30},"data_quality":{"dropped_records":7}}}'),
      ('update_progress', 'u1', NULL, '{"update_progress":{"state":"COMPLETED"}}')
    AS t(event_type, update_id, flow_name, details)""")
print("Warm-up views ready: wu_vitals, wu_event_log")

# COMMAND ----------

# DBTITLE 1,W1 Task
# MAGIC %md
# MAGIC ### W1 — Streaming table or materialized view?
# MAGIC For each dataset of a hospital pipeline, choose `"ST"` (streaming table) or `"MV"` (materialized view). Store the answers in the dict `w1_choice` with exactly these four keys:
# MAGIC
# MAGIC | Key | Dataset |
# MAGIC | --- | --- |
# MAGIC | `lab_files_bronze` | Raw lab result JSON files arrive every hour; each file must be loaded exactly once. |
# MAGIC | `admissions_per_ward_daily` | Admissions per ward and day; the numbers must stay correct when old encounters are corrected in the source table. |
# MAGIC | `vitals_silver` | Append-only cleaning of the vitals stream (typed columns, `DROP ROW` rules). |
# MAGIC | `claims_by_current_specialty` | Claim totals per provider specialty, always computed with the **current** provider table. |

# COMMAND ----------

# DBTITLE 1,W1 Code
# >>> BLANK W1
# Your code here
# >>> SOLUTION W1
w1_choice = {
    "lab_files_bronze": "ST",              # new files only, each processed once
    "admissions_per_ward_daily": "MV",     # must reflect corrections of old source rows
    "vitals_silver": "ST",                 # append-only stream with expectations
    "claims_by_current_specialty": "MV",   # re-joins the current dimension on every update
}
# <<< END W1

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    w1_expected = {"lab_files_bronze": "ST", "admissions_per_ward_daily": "MV",
                   "vitals_silver": "ST", "claims_by_current_specialty": "MV"}
    w1_got = {k: str(v).strip().upper() for k, v in w1_choice.items()}
    w1_wrong = sorted(k for k in w1_expected if w1_got.get(k) != w1_expected[k])
    ok = not w1_wrong and set(w1_got) == set(w1_expected)
    WARMUP_PASSED["W1"] = ok
    print("✅ Passed" if ok else
          f"Hint: check these keys: {w1_wrong or sorted(set(w1_got) ^ set(w1_expected))}. A streaming table processes each "
          "input row once (new data only); a materialized view always shows the current result of its query (L07 E3, E4).")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create the dict w1_choice first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — A `DROP ROW` expectation
# MAGIC Write the pipeline SQL **constraint clause** (only the clause, as a Python string `w2_constraint`) for a silver table of vital signs: a rule named `valid_heart_rate` that **drops** every row where `heart_rate` is missing or not between 20 and 250 (both included). The rule must be NULL-safe: it must return `true` or `false`, never `NULL`. The validation tests your rule on the view `wu_vitals`.

# COMMAND ----------

# DBTITLE 1,W2 Code
# >>> BLANK W2
# Your code here
# >>> SOLUTION W2
w2_constraint = "CONSTRAINT valid_heart_rate EXPECT (heart_rate IS NOT NULL AND heart_rate BETWEEN 20 AND 250) ON VIOLATION DROP ROW"
# <<< END W2

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    m = re.match(r"^\s*CONSTRAINT\s+valid_heart_rate\s+EXPECT\s*\((.*)\)\s*ON\s+VIOLATION\s+DROP\s+ROW\s*,?\s*$",
                 w2_constraint, re.I | re.S)
    if not m:
        WARMUP_PASSED["W2"] = False
        print("Hint: the clause must look like CONSTRAINT <name> EXPECT (<rule>) ON VIOLATION <action> (L07 4.4).")
    else:
        checked = spark.sql(f"SELECT reading_id, ({m.group(1)}) AS passes FROM wu_vitals").collect()
        kept = sorted(r["reading_id"] for r in checked if r["passes"] is True)
        unknown = sorted(r["reading_id"] for r in checked if r["passes"] is None)
        ok = kept == [1, 5, 6, 7] and not unknown
        WARMUP_PASSED["W2"] = ok
        print("✅ Passed" if ok else
              f"Hint: want the readings [1, 5, 6, 7] to pass and no NULL results; your rule keeps {kept} and gives NULL "
              f"for {unknown}. BETWEEN includes both ends; a comparison with NULL gives NULL (L07 4.4).")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: create the string w2_constraint; the rule must be valid SQL.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — Query an event log
# MAGIC The view `wu_event_log` has the columns `event_type`, `origin` (a struct with `update_id` and `flow_name`) and `details` (a JSON **string**), like the output of `event_log('<pipeline-id>')`.
# MAGIC Create the DataFrame `w3_result` with one row per flow of update `u1`, from the `flow_progress` events only, with the columns `flow_name`, `output_rows` (sum of `num_output_rows`) and `dropped_rows` (sum of `dropped_records`). Use 0 where a value is missing.

# COMMAND ----------

# DBTITLE 1,W3 Code
# >>> BLANK W3
# Your code here
# >>> SOLUTION W3
w3_result = spark.sql("""
    SELECT origin.flow_name AS flow_name,
           coalesce(sum(details:flow_progress.metrics.num_output_rows::bigint), 0)      AS output_rows,
           coalesce(sum(details:flow_progress.data_quality.dropped_records::bigint), 0) AS dropped_rows
    FROM wu_event_log
    WHERE event_type = 'flow_progress' AND origin.update_id = 'u1'
    GROUP BY origin.flow_name
""")
# <<< END W3

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    got = sorted((r["flow_name"], int(r["output_rows"]), int(r["dropped_rows"])) for r in w3_result.collect())
    want = [("main.clinic.vitals_bronze", 65, 0), ("main.clinic.vitals_silver", 60, 5)]
    ok = got == want
    WARMUP_PASSED["W3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {want}; got {got}. Filter event_type and origin.update_id, read the JSON with the colon syntax "
          "(details:flow_progress.metrics.num_output_rows), cast, sum per flow and coalesce to 0 (L07 4.6).")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create w3_result with the columns flow_name, output_rows, dropped_rows.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent. It deletes the pipelines and the source-file folder of an earlier run, recreates the
# schema, and writes fake change feeds (no PHI) into normal Delta tables.
import io, json, time
from databricks.sdk import WorkspaceClient                  # the Databricks SDK for Python (preinstalled)
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

CATALOG, SCHEMA = "main", "dbacademy_l08"
FQ = f"{CATALOG}.{SCHEMA}"                                   # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"                    # the lesson volume (this lesson keeps its data in tables)
PREFIX = "dbacademy_l08"                                     # every pipeline of this lesson starts with this name

w = WorkspaceClient()                                        # inside a notebook it signs in as you
ME = w.current_user.me().user_name
RUNTIME = f"/Users/{ME}/_lesson_runtime/L08"                 # workspace folder for the pipeline source files

def api(method, path, body=None, query=None):
    """Call the Databricks REST API as the notebook user; the SDK client adds the host and the token (L07 4.7)."""
    return w.api_client.do(method, path, query=query, body=body) or {}

def lesson_pipelines():
    """{name: id} of every pipeline whose name starts with PREFIX."""
    found = api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{PREFIX}%'", "max_results": 100})
    return {p["name"]: p["pipeline_id"] for p in found.get("statuses", [])}

def delete_pipeline(pipeline_id):
    """Delete a pipeline. Its tables are kept by default (L07 E6)."""
    api("DELETE", f"/api/2.0/pipelines/{pipeline_id}")

# 1. Delete the pipelines and the source files of an earlier run
old = lesson_pipelines()
for pipeline_id in old.values():
    delete_pipeline(pipeline_id)
try:
    w.workspace.delete(RUNTIME, recursive=True)
except NotFound:
    pass                                                     # nothing to delete
w.workspace.mkdirs(RUNTIME)
# 2. Recreate the schema and the volume
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
spark.sql(f"CREATE SCHEMA {FQ}")
spark.sql(f"CREATE VOLUME {FQ}.data")

def q(value):
    """A SQL literal: NULL for None, otherwise a quoted string."""
    return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"

# 3. Patient demographics change feed (like the output of a CDC tool such as Debezium or Lakeflow Connect)
spark.sql(f"""
    CREATE TABLE {FQ}.patients_cdc (
      patient_id INT, first_name STRING, last_name STRING, city STRING, insurance_plan STRING,
      operation STRING,        -- INSERT, UPDATE or DELETE
      sequence_num BIGINT,     -- order of the changes in the source database
      event_ts TIMESTAMP,      -- when the change happened in the source (same order as sequence_num)
      arrival_ts TIMESTAMP)    -- when the row landed here (NOT the order of the changes)""")

def land_patients(rows, arrival_day):
    """Append one delivery of change events. event_ts = 2026-09-01 08:00 + sequence_num hours; arrival_ts = the delivery
    time + the position of the row in the delivery (an exact duplicate gets the time of its first copy)."""
    values = ",\n".join(
        f"({pid}, {q(first)}, {q(last)}, {q(city)}, {q(plan)}, '{op}', {seq}, "
        f"timestampadd(HOUR, {seq}, TIMESTAMP'2026-09-01 08:00:00'), "
        f"timestampadd(MINUTE, {rows.index(row)}, TIMESTAMP'{arrival_day} 06:00:00'))"
        for row in rows for (pid, first, last, city, plan, op, seq) in [row])
    spark.sql(f"INSERT INTO {FQ}.patients_cdc VALUES {values}")
    return len(rows)

#                 patient_id, first, last, city, insurance_plan, operation, sequence_num
PATIENTS_BATCH_1 = [
    (1, "Ana",  "Lopez",  "Austin",     "PLAN_A", "INSERT", 1),
    (2, "Ben",  "Okafor", "Boston",     "PLAN_B", "INSERT", 2),
    (3, "Chen", "Wu",     "Chicago",    "PLAN_A", "INSERT", 3),
    (4, "Dana", "Novak",  "Denver",     "PLAN_C", "INSERT", 4),
    (5, "Eli",  "Haddad", "El Paso",    "PLAN_B", "INSERT", 5),
    (6, "Fay",  "Ruiz",   "Fresno",     "PLAN_A", "INSERT", 6),
    (3, "Chen", "Wu",     "Chicago",    "PLAN_B", "UPDATE", 10),   # plan change ...
    (3, "Chen", "Wu",     "Chicago",    "PLAN_B", "UPDATE", 10),   # ... sent twice by the CDC tool (exact duplicate)
    (2, "Ben",  "Okafor", "Burlington", "PLAN_B", "UPDATE", 11),   # only the city changes
    (5, None,   None,     None,         None,     "DELETE", 12),   # patient 5 is removed from the source system
    (6, "Fay",  "Ruiz",   "Fresno",     "PLAN_C", "UPDATE", 13),
]
PATIENTS_BATCH_2 = [                                         # landed later, in E2
    (1, "Ana",  "Lopez",  "Austin",     "PLAN_C", "UPDATE", 20),
    (7, "Gia",  "Patel",  "Gary",       "PLAN_A", "INSERT", 21),
    (4, "Dana", "Novak",  "Dallas",     "PLAN_C", "UPDATE", 22),   # only the city changes
    (6, "Fay",  "Ruiz",   "Fresno",     "PLAN_B", "UPDATE", 8),    # late: older than the change at 13
    (5, "Eli",  "Haddad", "El Paso",    "PLAN_C", "UPDATE", 9),    # late: older than the DELETE at 12
    (3, "Chen", "Wu",     "Chicago",    "PLAN_B", "UPDATE", 10),   # the change at 10 delivered again
]

# 4. Insurance plan catalog feed with a full reload (TRUNCATE, then the current plans again)
spark.sql(f"""CREATE TABLE {FQ}.plans_cdc (plan_id INT, plan_name STRING, payer STRING,
                                         operation STRING, sequence_num BIGINT)""")

def land_plans(rows):
    """Append plan change events."""
    values = ", ".join(f"({pid}, {q(name)}, {q(payer)}, '{op}', {seq})" for pid, name, payer, op, seq in rows)
    spark.sql(f"INSERT INTO {FQ}.plans_cdc VALUES {values}")
    return len(rows)

PLANS_BATCH_1 = [(1, "Basic Care", "Acme Health", "INSERT", 1), (2, "Silver Care", "Acme Health", "INSERT", 2),
                 (3, "Gold Care", "Beacon Mutual", "INSERT", 3), (4, "Senior Plus", "Beacon Mutual", "INSERT", 4)]
PLANS_BATCH_2 = [(2, "Silver Care Plus", "Acme Health", "UPDATE", 8),
                 (0, None, None, "TRUNCATE", 10),                     # the payer reloads its whole catalog ...
                 (1, "Basic Care", "Acme Health", "INSERT", 11),      # ... and sends the plans that still exist
                 (3, "Gold Care", "Beacon Mutual", "INSERT", 12),
                 (5, "Family Care", "Cedar Health", "INSERT", 13)]

# 5. Payer directory: no change feed, only full snapshots with a version number
spark.sql(f"CREATE TABLE {FQ}.payer_snapshot (payer_id INT, payer_name STRING, network_status STRING)")
spark.sql(f"CREATE TABLE {FQ}.payer_snapshot_version (version BIGINT)")

def publish_payer_snapshot(version, rows):
    """Replace the snapshot table with a new full extract and set its version."""
    values = ", ".join(f"({pid}, {q(name)}, {q(status)})" for pid, name, status in rows)
    spark.sql(f"INSERT OVERWRITE {FQ}.payer_snapshot VALUES {values}")
    spark.sql(f"INSERT OVERWRITE {FQ}.payer_snapshot_version VALUES ({version})")
    return len(rows)

PAYERS_V1 = [(1, "Acme Health", "IN_NETWORK"), (2, "Beacon Mutual", "IN_NETWORK"), (3, "Cedar Health", "OUT_OF_NETWORK")]
PAYERS_V2 = [(1, "Acme Health", "IN_NETWORK"), (2, "Beacon Mutual", "OUT_OF_NETWORK"), (4, "Delta Care", "IN_NETWORK")]

# 6. Provider specialty feed for the practice (one delivery)
spark.sql(f"""
    CREATE TABLE {FQ}.providers_cdc AS
    SELECT provider_id, provider_name, specialty, operation, sequence_num,
           timestampadd(HOUR, sequence_num, TIMESTAMP'2026-09-01 08:00:00') AS event_ts
    FROM VALUES
      (1, 'Dr Fake 1',   'General',              'INSERT', 1L),
      (2, 'Dr Fake 2',   'Cardiology',           'INSERT', 2L),
      (3, 'Dr Fake 3',   'Neurology',            'INSERT', 3L),
      (4, 'Dr Fake 4',   'Pediatrics',           'INSERT', 4L),
      (1, 'Dr Fake 1',   'Oncology',             'UPDATE', 7L),
      (3, NULL,          NULL,                   'DELETE', 8L),
      (4, 'Dr Fake 4',   'Pediatric Cardiology', 'UPDATE', 9L),
      (4, 'Dr Fake 4',   'Pediatric Cardiology', 'UPDATE', 9L),
      (2, 'Dr Fake Two', 'Cardiology',           'UPDATE', 10L),
      (1, 'Dr Fake 1',   'Cardiology',           'UPDATE', 6L)
    AS t(provider_id, provider_name, specialty, operation, sequence_num)""")

# 7. First deliveries
land_patients(PATIENTS_BATCH_1, "2026-10-01")
land_plans(PLANS_BATCH_1)
publish_payer_snapshot(1, PAYERS_V1)

# Check what we created and record the real counts
patient_events = spark.table(f"{FQ}.patients_cdc").count()
plan_events = spark.table(f"{FQ}.plans_cdc").count()
payer_rows = spark.table(f"{FQ}.payer_snapshot").count()
provider_events = spark.table(f"{FQ}.providers_cdc").count()
METRICS["nd.setup.old_pipelines_deleted"] = len(old)
METRICS["setup.patient_events"] = patient_events
METRICS["setup.plan_events"] = plan_events
METRICS["setup.payer_rows"] = payer_rows
METRICS["setup.provider_events"] = provider_events
METRICS["nd.setup.sdk_version"] = __import__("databricks.sdk.version", fromlist=["__version__"]).__version__
assert (patient_events, plan_events, payer_rows, provider_events) == (11, 4, 3, 10)
print(f"patients_cdc {patient_events}, plans_cdc {plan_events}, payer_snapshot {payer_rows}, providers_cdc {provider_events}")
display(spark.table(f"{FQ}.patients_cdc").orderBy("arrival_ts"))

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):** four source tables (normal Delta tables, outside every pipeline):
# MAGIC
# MAGIC | Table | Rows now | What it is |
# MAGIC | --- | --- | --- |
# MAGIC | `patients_cdc` | {{setup.patient_events}} | the first delivery of patient demographic **changes** (E1–E6, E9); a second delivery lands in E2 |
# MAGIC | `plans_cdc` | {{setup.plan_events}} | insurance plan changes; the second delivery (E2) contains a full reload (`TRUNCATE`) (E7) |
# MAGIC | `payer_snapshot` + `payer_snapshot_version` | {{setup.payer_rows}} | a payer directory that only comes as **full snapshots** with a version number (E8) |
# MAGIC | `providers_cdc` | {{setup.provider_events}} | provider specialty changes for the practice (P3) |
# MAGIC
# MAGIC Each change row has `operation` (`INSERT`, `UPDATE`, `DELETE`), `sequence_num` (the order of the changes in the source database), `event_ts` (when the change happened, same order as `sequence_num`) and `arrival_ts` (when the row landed here). The data has the usual CDC problems **on purpose**:
# MAGIC
# MAGIC | Problem | Where |
# MAGIC | --- | --- |
# MAGIC | the same change delivered twice | patient 3, `sequence_num` 10: twice in delivery 1, once more in delivery 2 |
# MAGIC | a delete | patient 5 at `sequence_num` 12 (delivery 1) |
# MAGIC | late (out-of-order) changes | delivery 2: patient 6 at 8 (after 13 was applied), patient 5 at 9 (after the delete at 12) |
# MAGIC | a change of a column without history value | patient 2 (`city` at 11), patient 4 (`city` at 22) |
# MAGIC | a full reload of a reference table | plans: `TRUNCATE` at 10, then the plans that still exist |
# MAGIC
# MAGIC The setup also deletes every pipeline whose name starts with `dbacademy_l08` and the folder with the pipeline source files, so you can run it again at any time.

# COMMAND ----------

# DBTITLE 1,4.1 Concept — CDC and slowly changing dimensions
# MAGIC %md
# MAGIC ## 4 Concepts
# MAGIC
# MAGIC ### 4.1 Change data capture (CDC) and slowly changing dimensions (SCD)
# MAGIC
# MAGIC **What.** A CDC feed does not send whole tables. It sends one row per **change** in the source database. *Change data capture and snapshots* lists what each CDC record includes: "The operation type (INSERT, UPDATE, DELETE)", "The data values for the record", and "A sequence number or timestamp for deterministic ordering". Databases such as SQL Server, MySQL and Oracle create such feeds (for example through Debezium, Oracle GoldenGate or Lakeflow Connect), and a Delta table creates one with its Change Data Feed (L14).
# MAGIC
# MAGIC **Why it is hard.** Real feeds have late (out-of-order) rows, repeated rows, deletes, and sometimes "clear the table and reload" events. Applying them by hand needs staging tables, window functions and a careful `MERGE` (*Change data capture and snapshots*: "complex and error-prone").
# MAGIC
# MAGIC **SCD Type 1 vs SCD Type 2** (*Change data capture and snapshots*)
# MAGIC
# MAGIC | | SCD Type 1 | SCD Type 2 |
# MAGIC | --- | --- | --- |
# MAGIC | Keeps | only the latest values ("History is not retained") | every version of a row |
# MAGIC | Rows per key | one | one per version; the current one has `__END_AT = NULL` |
# MAGIC | Extra columns | none | `__START_AT`, `__END_AT` ("when each version was active") |
# MAGIC | Use it for | current state: patient address book, current plan per patient | audit, regulatory history, point-in-time reports ("which plan did the patient have on 1 March?") |
# MAGIC
# MAGIC **Snapshots instead of a feed.** When a source cannot send changes, it sends full **snapshots**. Then the changes must be found by comparing two snapshots: AUTO CDC FROM SNAPSHOT (4.5, E8).

# COMMAND ----------

# DBTITLE 1,4.2 Concept — MERGE vs AUTO CDC
# MAGIC %md
# MAGIC ### 4.2 Hand-written `MERGE` vs AUTO CDC
# MAGIC
# MAGIC | | Hand-written `MERGE INTO` (L01, E9, P4) | AUTO CDC flow in a pipeline (E1–E7) |
# MAGIC | --- | --- | --- |
# MAGIC | Runs in | any notebook, job or SQL warehouse | a Lakeflow pipeline (serverless, or the Pro / Advanced edition — *Requirements* on the AUTO CDC page) |
# MAGIC | Order of events | you reduce each batch to one row per key (`ROW_NUMBER`/`QUALIFY`): "MERGE operations fail with a `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` error if more than one row in the source table matches the same row in the target table" (*MERGE INTO*) | `SEQUENCE BY` column; "AUTO CDC automatically handles out-of-sequence records" (*Change data capture and snapshots*) |
# MAGIC | Late events | your own code: compare with what the target already has | handled by the sequence column |
# MAGIC | Deletes | `WHEN MATCHED … DELETE` (SCD1) or close the version (SCD2); a late update after a delete needs your own bookkeeping | `APPLY AS DELETE WHEN …`; the deleted key is kept as a **tombstone** (default two days, table property `pipelines.cdc.tombstoneGCThresholdInSeconds`) so late events can be ignored (*create_auto_cdc_flow*) |
# MAGIC | SCD Type 2 | a two-part `MERGE` source (close the old row, insert the new one) | `STORED AS SCD TYPE 2` |
# MAGIC | Several changes per key in one batch (SCD2) | one `MERGE` can only apply one change per key | every change becomes a version |
# MAGIC | Metrics | the `MERGE` result (`num_updated_rows`, `num_inserted_rows`, …) | event log: `num_upserted_rows`, `num_deleted_rows` (*Advanced AUTO CDC topics*) |
# MAGIC
# MAGIC **Rule of thumb:** a change feed with deletes and late rows into a Lakeflow pipeline → AUTO CDC. A small, clean daily batch with one change per key, outside a pipeline → a hand-written `MERGE` is fine (P4).

# COMMAND ----------

# DBTITLE 1,4.3 Concept — AUTO CDC syntax
# MAGIC %md
# MAGIC ### 4.3 AUTO CDC syntax (SQL and Python)
# MAGIC
# MAGIC First declare an **empty target streaming table**, then a **flow** that applies the changes into it. Syntax from *AUTO CDC INTO (pipelines)*:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE table_name;
# MAGIC
# MAGIC CREATE FLOW flow_name AS AUTO CDC [ONCE] INTO table_name
# MAGIC FROM source
# MAGIC KEYS (keys)
# MAGIC [IGNORE NULL UPDATES [ON {columnList | * EXCEPT (exceptColumnList)}]]
# MAGIC [APPLY AS DELETE WHEN condition]
# MAGIC [APPLY AS TRUNCATE WHEN condition]
# MAGIC SEQUENCE BY orderByColumn
# MAGIC [COLUMNS {columnList | * EXCEPT (exceptColumnList)}]
# MAGIC [STORED AS {SCD TYPE 1 | SCD TYPE 2 | BITEMPORAL}]
# MAGIC [TRACK HISTORY ON {columnList | * EXCEPT (exceptColumnList)}]
# MAGIC ```
# MAGIC
# MAGIC | Clause | Meaning (*AUTO CDC INTO (pipelines)*) |
# MAGIC | --- | --- |
# MAGIC | `FROM source` | must be a **streaming** source: `STREAM(table)` (or a streaming view). "It is safest to read from static or append-only sources." |
# MAGIC | `KEYS (…)` | required; the columns that identify a row. Keys must not be NULL (error `AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY`, *Error classes*). |
# MAGIC | `SEQUENCE BY col` | required; "the logical order of CDC events". Must be sortable and never NULL (`…NULL_SEQUENCE`). For ties, sequence by a struct: `SEQUENCE BY STRUCT(event_ts, event_id)`. |
# MAGIC | `APPLY AS DELETE WHEN cond` | rows that match are deletes, not upserts. Without it, a delete row is upserted like any other row (E3). |
# MAGIC | `APPLY AS TRUNCATE WHEN cond` | clears the whole table; "supported only for SCD type 1" (E7). |
# MAGIC | `COLUMNS * EXCEPT (…)` | which source columns go to the target; usually you drop the CDC metadata columns. |
# MAGIC | `STORED AS SCD TYPE 1 / 2` | "The default is SCD type 1." |
# MAGIC | `TRACK HISTORY ON …` | SCD Type 2 only: which columns create a new version when they change (E5). |
# MAGIC | `IGNORE NULL UPDATES` | partial updates: a NULL in an update keeps the old target value (not used in this lesson). |
# MAGIC
# MAGIC Default behaviour: "upsert CDC events from the source: update any rows in the target table that match the specified key(s) or insert a new row when a matching record does not exist in the target table."
# MAGIC
# MAGIC **Python** (*create_auto_cdc_flow*; the old name `apply_changes()` "has the same signature"):
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC dp.create_streaming_table("patients_current")             # the empty target
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="patients_current",
# MAGIC     source="patients_cdc_feed",                          # a streaming table or view of this pipeline
# MAGIC     keys=["patient_id"],
# MAGIC     sequence_by="sequence_num",                           # or col("…") or struct("ts", "id")
# MAGIC     apply_as_deletes="operation = 'DELETE'",              # or expr("operation = 'DELETE'")
# MAGIC     apply_as_truncates="operation = 'TRUNCATE'",          # SCD Type 1 only
# MAGIC     except_column_list=["operation", "sequence_num"],     # or column_list=[...]
# MAGIC     stored_as_scd_type=1,                                 # 1 (default) or 2
# MAGIC     track_history_except_column_list=None,                # or track_history_column_list=[...] (SCD Type 2)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Where the CDC feed comes from.** In this lesson the feed is a Delta table that the notebook appends to (`STREAM(main.dbacademy_l08.patients_cdc)`). In real projects it is often a bronze streaming table loaded with Auto Loader (L06, L07). Note from *create_auto_cdc_flow*: "Auto Loader does not guarantee file processing order" — one more reason why the order must come from `SEQUENCE BY`, never from the arrival of the data.

# COMMAND ----------

# DBTITLE 1,4.4 Concept — SCD Type 2 details
# MAGIC %md
# MAGIC ### 4.4 SCD Type 2 details: `__START_AT`, `__END_AT`, current rows, history columns
# MAGIC
# MAGIC * `__START_AT` and `__END_AT` get their values from the `SEQUENCE BY` column, so they have **its data type** ("you must also include the `__START_AT` and `__END_AT` columns with the same data type as the sequence_by field" when you declare the target schema — *AUTO CDC INTO (pipelines)*). Sequence by a timestamp → timestamps; by a number → numbers (E4, E8).
# MAGIC * **Current rows:** `WHERE __END_AT IS NULL`. There is no `is_current` column.
# MAGIC * **Point in time** (the state at time `t`): `WHERE __START_AT <= t AND (__END_AT IS NULL OR __END_AT > t)` (E4).
# MAGIC * A **delete** closes the current version (sets `__END_AT`); no new version is opened.
# MAGIC * **`TRACK HISTORY ON`**: "changes to other columns update the current version in place rather than generating a new history record" (*The AUTO CDC APIs…*, "Track a column subset with SCD Type 2"). Use it for columns where nobody needs history (phone number, a typo fix in the city).
# MAGIC * **Reading the target downstream:** an AUTO CDC target is updated in place, so it is not an append-only source. To stream from it, read its change data feed (*Advanced AUTO CDC topics*; L14). A materialized view on top of it is fine.

# COMMAND ----------

# DBTITLE 1,4.5 Concept — AUTO CDC FROM SNAPSHOT
# MAGIC %md
# MAGIC ### 4.5 AUTO CDC FROM SNAPSHOT
# MAGIC
# MAGIC **What.** "Use this when CDC is not enabled on the source database and only snapshots are available. This API compares snapshots to determine changes and then processes them. It is supported in both the pipeline SQL and Python interfaces." (*The AUTO CDC APIs…*). Keys that are missing from a newer snapshot become deletes; changed values become updates.
# MAGIC
# MAGIC **SQL** (*CREATE FLOW (pipelines)*): a required `FROM SNAPSHOT (snapshot_query)` and an optional `WITH VERSION (version_query)` that returns the **next** version to process (0 or 1 row). Inside it, `last_snapshot_version()` is the version that was processed last. There is no `SEQUENCE BY`: the version is the order.
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE payer_directory_scd2;
# MAGIC CREATE FLOW payer_directory_flow AS AUTO CDC INTO payer_directory_scd2
# MAGIC FROM SNAPSHOT (SELECT payer_id, payer_name, network_status FROM main.dbacademy_l08.payer_snapshot)
# MAGIC WITH VERSION (
# MAGIC   SELECT version FROM main.dbacademy_l08.payer_snapshot_version
# MAGIC   WHERE NOT EXISTS (SELECT 1 FROM last_snapshot_version())
# MAGIC      OR version > (SELECT version FROM last_snapshot_version()))
# MAGIC KEYS (payer_id)
# MAGIC STORED AS SCD TYPE 2;
# MAGIC ```
# MAGIC **Python:** `dp.create_auto_cdc_from_snapshot_flow(target=…, source=…, keys=[…], stored_as_scd_type=2)`; `source` is a view name (the pipeline update time becomes the version) or a function that returns `(DataFrame, version)` for the next snapshot.
# MAGIC
# MAGIC | | AUTO CDC | AUTO CDC FROM SNAPSHOT |
# MAGIC | --- | --- | --- |
# MAGIC | Input | a change feed (one row per change) | full copies of the table |
# MAGIC | Order | `SEQUENCE BY` | the snapshot version |
# MAGIC | Changes between two snapshots | every change | only the net difference: "if a user changes their address twice in one day … your change feed might go directly from A to C" (*Change data capture and snapshots*) |
# MAGIC | Event log metrics `num_upserted_rows` / `num_deleted_rows` | yes | "captured only by AUTO CDC queries and not by AUTO CDC FROM SNAPSHOT queries" (*Advanced AUTO CDC topics*) |

# COMMAND ----------

# DBTITLE 1,4.6 Concept — How this notebook runs pipelines
# MAGIC %md
# MAGIC ### 4.6 How this notebook runs pipelines (same as L07 4.7)
# MAGIC
# MAGIC Pipeline code cannot run in a normal notebook cell (L06 E11, L07). As in L07, this notebook writes the pipeline source files to a workspace folder (`RUNTIME`), creates serverless, triggered pipelines through the Pipelines REST API (`w.api_client.do`), starts updates and waits for them, and reads the tables and `event_log('<pipeline-id>')`.
# MAGIC
# MAGIC | Step | REST call | CLI |
# MAGIC | --- | --- | --- |
# MAGIC | Save a source file | `w.workspace.upload(path, data, format=ImportFormat.AUTO, overwrite=True)` | `databricks workspace import` |
# MAGIC | Create a pipeline | `POST /api/2.0/pipelines` | `databricks pipelines create --json …` |
# MAGIC | Change its settings | `PUT /api/2.0/pipelines/{id}` | `databricks pipelines update <id> --json …` |
# MAGIC | Start an update | `POST /api/2.0/pipelines/{id}/updates` (`full_refresh`, `validate_only`) | `databricks pipelines start-update <id>` |
# MAGIC | Wait for it | `GET /api/2.0/pipelines/{id}/updates/{update_id}` | `databricks pipelines get-update <id> <update-id>` |
# MAGIC | Read errors | `GET /api/2.0/pipelines/{id}/events?filter=level='ERROR'` | `databricks pipelines list-pipeline-events <id>` |
# MAGIC | Delete | `DELETE /api/2.0/pipelines/{id}` | `databricks pipelines delete <id>` |
# MAGIC
# MAGIC The SDK preinstalled on serverless job compute is `{{nd.setup.sdk_version}}` in this run; L07 showed that its typed `w.pipelines.create(...)` does not know newer fields such as `root_path`, so the lesson calls the REST API directly.
# MAGIC
# MAGIC **New helper for AUTO CDC:** `cdc_metrics(pipeline_id, update_id)` reads `num_upserted_rows` and `num_deleted_rows` per flow from the `flow_progress` events. "The `num_output_rows` metric, output for non-CDC flows, is not captured for AUTO CDC queries" (*Advanced AUTO CDC topics*). The last `flow_progress` events of an update can reach the event log some seconds **after** the update has ended, so `cdc_metrics` reads the log again every 10 s until every flow has a final status (in this run it waited {{nd.E1.event_wait_s}} s after update 1 and {{nd.E2.event_wait_s}} s after update 2).

# COMMAND ----------

# DBTITLE 1,4.6 Pipeline helpers (run first)
def write_source(file_name, text):
    """Save one pipeline source file (.sql or .py) in the RUNTIME workspace folder; return its full path."""
    w.workspace.upload(f"{RUNTIME}/{file_name}", io.BytesIO(text.encode("utf-8")),
                       format=ImportFormat.AUTO, overwrite=True)     # a plain workspace file, not a notebook
    return f"/Workspace{RUNTIME}/{file_name}"

def pipeline_settings(name, file_names):
    """All settings of a lesson pipeline (JSON): serverless, triggered, publishing to main.dbacademy_l08."""
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

def run_update(pipeline_id, full_refresh=False, validate_only=False, timeout_s=1800, tries=4):
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
            wait_s = 90 * attempt                             # wait longer each time: 90, 180, 270 s
            print(f"update {update_id}: no serverless capacity (RESOURCE_EXHAUSTED), trying again in {wait_s} s")
            time.sleep(wait_s)
            continue
        seconds = round(time.time() - started)
        print(f"update {update_id}: {state} after {seconds} s")
        if state != "COMPLETED":
            for event_type, message, exceptions in update_errors(pipeline_id, update_id):
                print(f"ERROR in {event_type}: {message}\n    {exceptions[:700]}\n")
        return update_id, state, seconds

FINAL_FLOW_STATES = {"COMPLETED", "FAILED", "STOPPED", "SKIPPED", "EXCLUDED", "IDLE"}   # a flow_progress status that ends a flow
event_wait_s = 0                                              # seconds the last cdc_metrics call waited for the event log

def cdc_metrics(pipeline_id, update_id, timeout_s=300):
    """One row per flow of one update: final status, rows upserted and deleted by AUTO CDC, other output rows.
    The last flow_progress events can reach the event log after the update has ended, so this waits until every flow
    has a final status (at most timeout_s seconds)."""
    global event_wait_s
    started = time.time()
    while True:
        df = flow_metrics_query(pipeline_id, update_id)
        waiting = [r["flow"] for r in df.collect() if r["final_status"] not in FINAL_FLOW_STATES]
        event_wait_s = round(time.time() - started)
        if not waiting:
            return df
        if time.time() - started > timeout_s:
            print(f"event log: still no final status after {event_wait_s} s for {waiting}")
            return df
        time.sleep(10)                                        # the events are still arriving: check again in 10 s

def flow_metrics_query(pipeline_id, update_id):
    """The event log query of cdc_metrics (one row per flow, read once)."""
    return spark.sql(f"""
        SELECT substring_index(origin.flow_name, '.', -1)                                 AS flow,
               max_by(details:flow_progress.status::string, timestamp)
                 FILTER (WHERE details:flow_progress.status IS NOT NULL)                   AS final_status,
               coalesce(sum(details:flow_progress.metrics.num_upserted_rows::bigint), 0) AS upserted_rows,
               coalesce(sum(details:flow_progress.metrics.num_deleted_rows::bigint), 0)  AS deleted_rows,
               coalesce(sum(details:flow_progress.metrics.num_output_rows::bigint), 0)   AS output_rows
        FROM event_log('{pipeline_id}')
        WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'
          AND origin.flow_name IS NOT NULL
        GROUP BY ALL
        HAVING final_status IS NOT NULL                                            -- skip events without a flow status
        ORDER BY flow""")

def metrics_text(df):
    """'flow: +upserted/-deleted' for every flow of a cdc_metrics DataFrame, in one string."""
    return "; ".join(f"{r['flow']}: +{r['upserted_rows']}/-{r['deleted_rows']}" for r in df.collect())

def error_classes(text):
    """All [ERROR_CLASS] names in a text, in order, without duplicates."""
    return list(dict.fromkeys(re.findall(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", text or "")))

def patient_rows(table, where="true"):
    """The patient columns of a target table, ordered; SCD Type 2 tables also show the version times as text."""
    cols = spark.table(f"{FQ}.{table}").columns
    times = (", date_format(__START_AT, 'MM-dd HH:mm') AS start_at, date_format(__END_AT, 'MM-dd HH:mm') AS end_at"
             if "__START_AT" in cols else "")
    order = "patient_id, __START_AT" if times else "patient_id"
    return spark.sql(f"""SELECT patient_id, first_name, last_name, city, insurance_plan{times}
                         FROM {FQ}.{table} WHERE {where} ORDER BY {order}""")

def rows_text(df):
    """All rows of a small DataFrame as one line of text (for the summary tables)."""
    return "; ".join(" ".join("-" if v is None else str(v) for v in r) for r in df.collect())

print("Helpers ready: write_source, create_pipeline, edit_pipeline, delete_pipeline, run_update, update_errors, "
      "cdc_metrics, metrics_text, patient_rows, rows_text")

# COMMAND ----------

# DBTITLE 1,5 — Examples (intro)
# MAGIC %md
# MAGIC ## 5 Examples
# MAGIC
# MAGIC Each example: **Predict** (answer before you run) → **code** (every line commented) → **Explained** (real numbers from the run).
# MAGIC
# MAGIC All pipeline examples use **one** pipeline, `dbacademy_l08_cdc`, with seven AUTO CDC flows that all read the same feeds. To save compute, it runs only **two** updates: E1 (delivery 1) and E2 (delivery 2). E3–E8 read the tables after these updates; E8 then deletes the pipeline (its tables stay). E9 needs no pipeline at all.
# MAGIC
# MAGIC | Target table | Source | Type | Example |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `patients_scd1` | `patients_cdc` | SCD Type 1, `SEQUENCE BY sequence_num` | E1, E2, E3, E6 |
# MAGIC | `patients_scd2` | `patients_cdc` | SCD Type 2, `SEQUENCE BY event_ts` | E3, E4, E6, E9 |
# MAGIC | `patients_scd2_tracked` | `patients_cdc` | SCD Type 2 in **Python**, no history for `city` | E5 |
# MAGIC | `patients_by_arrival` | `patients_cdc` | SCD Type 1 with the **wrong** sequence column `arrival_ts` | E2 |
# MAGIC | `patients_no_deletes` | `patients_cdc` | SCD Type 1 **without** `APPLY AS DELETE WHEN` | E3 |
# MAGIC | `plans_current` | `plans_cdc` | SCD Type 1 with `APPLY AS TRUNCATE WHEN` | E7 |
# MAGIC | `payer_directory_scd2` | `payer_snapshot` | AUTO CDC FROM SNAPSHOT, SCD Type 2 | E8 |
# MAGIC
# MAGIC > **Free Edition limit (L07):** at most two lesson pipelines at a time. The examples use one pipeline, the practice tasks share a second one.

# COMMAND ----------

# DBTITLE 1,E1 Predict
# MAGIC %md
# MAGIC ### E1 — SCD Type 1: the first delivery
# MAGIC
# MAGIC Delivery 1 has 11 change rows for 6 patients: 6 inserts, an update of patient 3 (twice), an update of patient 2, a delete of patient 5 and an update of patient 6.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. How many rows will `patients_scd1` have after the first update? Which plan does patient 3 have?
# MAGIC 2. Does `patients_scd1` have the columns `operation` and `sequence_num`? Does it have `__START_AT`?
# MAGIC 3. Which event log metric tells you how many rows AUTO CDC wrote: `num_output_rows` or something else?

# COMMAND ----------

# DBTITLE 1,E1 Code — the pipeline source files
PATIENTS_SQL = """
-- SCD Type 1: one row per patient with the latest values
CREATE OR REFRESH STREAMING TABLE patients_scd1;

CREATE FLOW patients_scd1_flow AS AUTO CDC INTO patients_scd1
FROM STREAM(main.dbacademy_l08.patients_cdc)                -- the change feed, read as a stream
KEYS (patient_id)                                            -- which row a change belongs to
APPLY AS DELETE WHEN operation = 'DELETE'                   -- these rows delete, they do not upsert
SEQUENCE BY sequence_num                                     -- the order of the changes in the source
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)   -- keep the CDC metadata out of the target
STORED AS SCD TYPE 1;                                        -- the default; written here to be clear

-- SCD Type 2: every version of every patient
CREATE OR REFRESH STREAMING TABLE patients_scd2;

CREATE FLOW patients_scd2_flow AS AUTO CDC INTO patients_scd2
FROM STREAM(main.dbacademy_l08.patients_cdc)
KEYS (patient_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_ts                                         -- a timestamp: __START_AT/__END_AT become timestamps
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)
STORED AS SCD TYPE 2;
"""

MISTAKES_SQL = """
-- Mistake 1 (E2): the sequence column is the ARRIVAL time, not the order of the changes in the source
CREATE OR REFRESH STREAMING TABLE patients_by_arrival;

CREATE FLOW patients_by_arrival_flow AS AUTO CDC INTO patients_by_arrival
FROM STREAM(main.dbacademy_l08.patients_cdc)
KEYS (patient_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY arrival_ts                                       -- wrong on purpose
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)
STORED AS SCD TYPE 1;

-- Mistake 2 (E3): no APPLY AS DELETE WHEN
CREATE OR REFRESH STREAMING TABLE patients_no_deletes;

CREATE FLOW patients_no_deletes_flow AS AUTO CDC INTO patients_no_deletes
FROM STREAM(main.dbacademy_l08.patients_cdc)
KEYS (patient_id)
SEQUENCE BY sequence_num                                     -- correct order, but deletes are missing
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)
STORED AS SCD TYPE 1;
"""

TRACKED_PY = '''
from pyspark import pipelines as dp                          # current API (legacy: import dlt)

@dp.temporary_view(name="patients_cdc_feed")                 # a view inside the pipeline (not published)
def patients_cdc_feed():
    return spark.readStream.table("main.dbacademy_l08.patients_cdc")   # STREAM(...) in SQL

dp.create_streaming_table("patients_scd2_tracked")           # the empty target

dp.create_auto_cdc_flow(                                     # SQL: CREATE FLOW ... AS AUTO CDC INTO
    target="patients_scd2_tracked",
    source="patients_cdc_feed",
    keys=["patient_id"],
    sequence_by="event_ts",
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "sequence_num", "event_ts", "arrival_ts"],
    stored_as_scd_type=2,
    track_history_except_column_list=["city"],               # a city change does not create a new version
)
'''

PLANS_SQL = """
-- SCD Type 1 with a full reload: TRUNCATE clears the table at its place in the sequence
CREATE OR REFRESH STREAMING TABLE plans_current;

CREATE FLOW plans_flow AS AUTO CDC INTO plans_current
FROM STREAM(main.dbacademy_l08.plans_cdc)
KEYS (plan_id)
APPLY AS DELETE WHEN operation = 'DELETE'
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'               -- SCD Type 1 only
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 1;
"""

PAYERS_SQL = """
-- AUTO CDC FROM SNAPSHOT: compare full snapshots, one version after the other
CREATE OR REFRESH STREAMING TABLE payer_directory_scd2;

CREATE FLOW payer_directory_flow AS AUTO CDC INTO payer_directory_scd2
FROM SNAPSHOT (
  SELECT payer_id, payer_name, network_status FROM main.dbacademy_l08.payer_snapshot
)
WITH VERSION (                                               -- the next version to process (0 or 1 row)
  SELECT version FROM main.dbacademy_l08.payer_snapshot_version
  WHERE (
    NOT EXISTS (SELECT 1 FROM last_snapshot_version())
    OR version > (SELECT version FROM last_snapshot_version())
  )
)
KEYS (payer_id)
STORED AS SCD TYPE 2;
"""
SOURCES = {"patients.sql": PATIENTS_SQL, "mistakes.sql": MISTAKES_SQL, "patients_tracked.py": TRACKED_PY,
           "plans.sql": PLANS_SQL, "payers_snapshot.sql": PAYERS_SQL}
print("Source files defined:", ", ".join(SOURCES))

# COMMAND ----------

# DBTITLE 1,E1 Code — create the pipeline and run update 1
for file_name, text in SOURCES.items():
    print("wrote", write_source(file_name, text))            # save each source file in the workspace
MAIN = create_pipeline("cdc", list(SOURCES))                  # one pipeline with all five files
print("pipeline id:", MAIN)
u1, u1_state, u1_secs = run_update(MAIN)                      # update 1: applies delivery 1
assert u1_state == "COMPLETED", update_errors(MAIN, u1)

e1_metrics = cdc_metrics(MAIN, u1)                            # upserted / deleted rows per flow (event log)
e1_event_wait_s = event_wait_s                                # how long the last events took to arrive
display(e1_metrics)
e1_scd1 = patient_rows("patients_scd1")                       # the SCD Type 1 table after delivery 1
display(e1_scd1)
published = spark.sql(f"""
    SELECT table_name, table_type FROM {CATALOG}.information_schema.tables
    WHERE table_schema = '{SCHEMA}' AND table_name NOT LIKE '\\_\\_%'
    ORDER BY table_name""")                                    # what is in the schema now
display(published)
history = spark.sql(f"DESCRIBE HISTORY {FQ}.patients_scd1").select("version", "operation")
display(history.orderBy("version"))                           # how AUTO CDC writes to the table

# State of the other targets after update 1 (E7 and E8 compare it with update 2)
plans_u1 = spark.sql(f"SELECT plan_id, plan_name FROM {FQ}.plans_current ORDER BY plan_id")
payers_u1 = spark.sql(f"""SELECT payer_id, network_status, __START_AT, __END_AT
                          FROM {FQ}.payer_directory_scd2 ORDER BY payer_id, __START_AT""")
e1_flows = {r["flow"]: r for r in e1_metrics.collect()}
raw_flow_names = [r[0] for r in spark.sql(f"""SELECT DISTINCT substring_index(origin.flow_name, '.', -1) FROM event_log('{MAIN}')
                  WHERE event_type = 'flow_progress' ORDER BY 1""").collect()]   # all names, also events without a status
METRICS["E1.state"] = u1_state
METRICS["nd.E1.secs"] = u1_secs
METRICS["nd.E1.event_wait_s"] = e1_event_wait_s
METRICS["E1.flow_names"] = ", ".join(sorted(e1_flows))
METRICS["E1.cdc_metrics"] = metrics_text(e1_metrics)
METRICS["E1.scd1_rows"] = e1_scd1.count()
METRICS["E1.scd1_table"] = rows_text(e1_scd1)
METRICS["E1.scd1_columns"] = ", ".join(spark.table(f"{FQ}.patients_scd1").columns)
METRICS["E1.scd2_columns"] = ", ".join(spark.table(f"{FQ}.patients_scd2").columns)
METRICS["E1.scd2_rows"] = spark.table(f"{FQ}.patients_scd2").count()
METRICS["E1.published"] = ", ".join(f"{r['table_name']} ({r['table_type']})" for r in published.collect() if not r["table_name"].startswith("event_log_"))
METRICS["E1.event_log_table"] = ", ".join(f"event_log_<pipeline id> ({r['table_type']})" for r in published.collect() if r["table_name"] == "event_log_" + MAIN.replace("-", "_"))
METRICS["nd.E1.raw_flow_names"] = ", ".join(raw_flow_names)
METRICS["nd.E1.history_ops"] = ", ".join(r["operation"] for r in history.orderBy("version").collect())
METRICS["E7.plans_u1"] = rows_text(plans_u1)
METRICS["E8.payers_u1"] = rows_text(payers_u1)
assert e1_scd1.count() == 5 and spark.table(f"{FQ}.patients_scd2").count() == 9 and e1_flows["patients_scd1_flow"]["upserted_rows"] == 5

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Update 1 ended in state **`{{E1.state}}`**. All seven flows ran in one update. Flow names in the event log: `{{E1.flow_names}}`. The SQL flows appear with their `CREATE FLOW` names, the Python flow with the name of its target (the default of `name`), and the snapshot flow also with the name of its target, not `payer_directory_flow`.
# MAGIC * `patients_scd1` has **{{E1.scd1_rows}}** rows (patient, name, city, plan): `{{E1.scd1_table}}`. One row per patient with the **latest** values; patient 5 is gone (deleted at 12); patient 3 has the plan from its update at 10.
# MAGIC * Columns of the SCD Type 1 table: `{{E1.scd1_columns}}` — the CDC metadata columns are not there (`COLUMNS * EXCEPT`), and there are no version columns. The SCD Type 2 table has `{{E1.scd2_columns}}` and already {{E1.scd2_rows}} rows (E4).
# MAGIC * Rows written per flow (`+num_upserted_rows/-num_deleted_rows` from `flow_progress`): `{{E1.cdc_metrics}}`. The SCD Type 1 flow upserted one row per patient that exists after the delivery; the SCD Type 2 flow one row per version. The snapshot flow reports 0: these metrics are "captured only by AUTO CDC queries and not by AUTO CDC FROM SNAPSHOT queries" (*Advanced AUTO CDC topics*).
# MAGIC * Tables in the schema: `{{E1.published}}` — the AUTO CDC targets are streaming tables; the feeds are normal managed Delta tables. The schema also got a table for the pipeline's event log: `{{E1.event_log_table}}`, although the lesson did not set the `event_log` pipeline setting.
# MAGIC * `DESCRIBE HISTORY patients_scd1` (operations in this run): {{nd.E1.history_ops}}.
# MAGIC
# MAGIC > **Key takeaway:** an AUTO CDC target is an empty streaming table plus one `AUTO CDC INTO` flow. You describe the keys, the order and the delete rule; the pipeline does the `MERGE` work.

# COMMAND ----------

# DBTITLE 1,E2 Predict
# MAGIC %md
# MAGIC ### E2 — Out-of-order events are resolved by `SEQUENCE BY`
# MAGIC
# MAGIC Delivery 2 lands. It contains new changes (patients 1, 4, 7) and two **late** changes: patient 6 at sequence 8 (its change at 13 was already applied in update 1) and patient 5 at sequence 9 (it was deleted at 12). The same update also brings plan delivery 2 (E7) and payer snapshot 2 (E8).
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which plan does patient 6 have in `patients_scd1` after update 2: `PLAN_B` (the change that arrived last) or `PLAN_C` (the change with the highest sequence)?
# MAGIC 2. The flow `patients_by_arrival` uses `SEQUENCE BY arrival_ts`. What does it show for patients 5 and 6?
# MAGIC 3. How many rows does `patients_scd1` have now?

# COMMAND ----------

# DBTITLE 1,E2 Code
land_patients(PATIENTS_BATCH_2, "2026-10-02")                 # delivery 2 of the patient feed (6 rows)
land_plans(PLANS_BATCH_2)                                     # plan delivery 2 with a TRUNCATE (E7)
publish_payer_snapshot(2, PAYERS_V2)                          # payer snapshot version 2 (E8)
u2, u2_state, u2_secs = run_update(MAIN)                      # update 2: a normal (incremental) update
assert u2_state == "COMPLETED", update_errors(MAIN, u2)

e2_metrics = cdc_metrics(MAIN, u2)                            # what each flow wrote in update 2
e2_event_wait_s = event_wait_s
display(e2_metrics)
e2_scd1 = patient_rows("patients_scd1")                       # sequence_num decides the order
display(e2_scd1)
e2_arrival = patient_rows("patients_by_arrival")              # arrival_ts decides the order (wrong)
display(e2_arrival)
e2_diff = spark.sql(f"""
    SELECT patient_id, first_name, last_name, city, insurance_plan FROM {FQ}.patients_by_arrival
    EXCEPT
    SELECT patient_id, first_name, last_name, city, insurance_plan FROM {FQ}.patients_scd1
    ORDER BY patient_id""")                                   # rows where the wrong order gives other values
display(e2_diff)

plan_of = lambda df, pid: next((r["insurance_plan"] for r in df.collect() if r["patient_id"] == pid), "no row")
METRICS["E2.state"] = u2_state
METRICS["nd.E2.secs"] = u2_secs
METRICS["nd.E2.event_wait_s"] = e2_event_wait_s
METRICS["E2.cdc_metrics"] = metrics_text(e2_metrics)
METRICS["E2.scd2_upserted"] = {r["flow"]: r["upserted_rows"] for r in e2_metrics.collect()}.get("patients_scd2_flow")
METRICS["E2.scd1_rows"] = e2_scd1.count()
METRICS["E2.scd1_table"] = rows_text(e2_scd1)
METRICS["E2.p6_scd1"] = plan_of(e2_scd1, 6)
METRICS["E2.p6_by_arrival"] = plan_of(e2_arrival, 6)
METRICS["E2.p5_scd1"] = plan_of(e2_scd1, 5)
METRICS["E2.p5_by_arrival"] = plan_of(e2_arrival, 5)
METRICS["E2.by_arrival_rows"] = e2_arrival.count()
METRICS["E2.diff"] = rows_text(e2_diff)
assert (plan_of(e2_scd1, 6), plan_of(e2_arrival, 6), plan_of(e2_scd1, 5), e2_scd1.count()) == ("PLAN_C", "PLAN_B", "no row", 6)

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Update 2: `{{E2.state}}`. Rows written per flow: `{{E2.cdc_metrics}}`. `patients_scd1_flow` wrote only the changes that won; the late ones changed nothing. The SCD Type 2 flow upserted {{E2.scd2_upserted}} rows for 6 change rows: the metric is not the number of change rows; in SCD Type 2 a change also closes the previous version, and a late change also changes an older version (E4).
# MAGIC * `patients_scd1` now has **{{E2.scd1_rows}}** rows: `{{E2.scd1_table}}`.
# MAGIC * Patient 6: `patients_scd1` shows **{{E2.p6_scd1}}**. The late change (sequence 8, `PLAN_B`) arrived after the change at 13, but its sequence is lower, so AUTO CDC did not apply it over the newer value.
# MAGIC * Patient 5 in `patients_scd1`: {{E2.p5_scd1}}. The late update at 9 is older than the delete at 12.
# MAGIC * With `SEQUENCE BY arrival_ts` (`patients_by_arrival`, {{E2.by_arrival_rows}} rows) patient 6 shows **{{E2.p6_by_arrival}}** and patient 5 shows **{{E2.p5_by_arrival}}**. Rows that differ from `patients_scd1`: `{{E2.diff}}`.
# MAGIC
# MAGIC > **Key takeaway:** `SEQUENCE BY` must be the order of the changes **in the source system** (a log sequence number, a commit timestamp, a version). The time when data arrived, a file name or `_metadata.file_modification_time` is the wrong column: "Auto Loader does not guarantee file processing order" (*create_auto_cdc_flow*). For ties, use `STRUCT(ts, id)`.

# COMMAND ----------

# DBTITLE 1,E3 Predict
# MAGIC %md
# MAGIC ### E3 — Deletes: `APPLY AS DELETE WHEN` and tombstones
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Patient 5 was deleted at 12 in delivery 1, and an update at 9 arrived in delivery 2. Is patient 5 in `patients_scd1`? In `patients_scd2`?
# MAGIC 2. `patients_no_deletes` has no `APPLY AS DELETE WHEN`. What does its row for patient 5 look like?
# MAGIC 3. How many rows did the SCD Type 1 flow delete according to the event log?

# COMMAND ----------

# DBTITLE 1,E3 Code
p5_scd1 = patient_rows("patients_scd1", "patient_id = 5")     # with APPLY AS DELETE WHEN
display(p5_scd1)
p5_scd2 = patient_rows("patients_scd2", "patient_id = 5")     # SCD Type 2: the versions of patient 5
display(p5_scd2)
p5_nodel = patient_rows("patients_no_deletes", "patient_id = 5")   # without APPLY AS DELETE WHEN
display(p5_nodel)
deleted_by_flow = spark.sql(f"""
    SELECT substring_index(origin.flow_name, '.', -1) AS flow,
           coalesce(sum(details:flow_progress.metrics.num_deleted_rows::bigint), 0) AS deleted_rows
    FROM event_log('{MAIN}')
    WHERE event_type = 'flow_progress' AND origin.update_id IN ('{u1}', '{u2}')
    GROUP BY ALL
    HAVING max(details:flow_progress.status::string) IS NOT NULL
    ORDER BY flow""")                                        # deletes in both updates, per flow
display(deleted_by_flow)

deleted = {r["flow"]: r["deleted_rows"] for r in deleted_by_flow.collect()}
METRICS["E3.p5_scd1_rows"] = p5_scd1.count()
METRICS["E3.p5_scd2"] = rows_text(p5_scd2)
METRICS["E3.p5_scd2_rows"] = p5_scd2.count()
METRICS["E3.p5_no_deletes"] = rows_text(p5_nodel)
METRICS["E3.no_deletes_rows"] = spark.table(f"{FQ}.patients_no_deletes").count()
METRICS["E3.deleted_scd1"] = deleted.get("patients_scd1_flow", 0)
METRICS["E3.deleted_no_deletes"] = deleted.get("patients_no_deletes_flow", 0)
METRICS["E3.deleted_plans"] = deleted.get("plans_flow", 0)
METRICS["E3.deleted_all"] = "; ".join(f"{k}: {v}" for k, v in sorted(deleted.items()))
assert p5_scd1.count() == 0 and p5_scd2.count() == 2 and p5_nodel.count() == 1

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `patients_scd1` has **{{E3.p5_scd1_rows}}** rows for patient 5. The late update at 9 did not bring the patient back: AUTO CDC keeps the deleted key as a **tombstone** with its sequence, so it can ignore older changes ("To handle out-of-order data, the deleted row is temporarily retained as a tombstone", default retention two days — *create_auto_cdc_flow*).
# MAGIC * `patients_scd2` keeps the history of patient 5 ({{E3.p5_scd2_rows}} rows; patient, name, city, plan, start, end): `{{E3.p5_scd2}}`. The late update at 9 (17:00) was placed **inside** the history, before the delete at 12 (20:00); the delete closed the last version, so no version is open (`__END_AT` is set).
# MAGIC * Without `APPLY AS DELETE WHEN`, the delete row is treated like any other change and **upserted**: `patients_no_deletes` has this row for patient 5: `{{E3.p5_no_deletes}}` (a dash is NULL), and {{E3.no_deletes_rows}} rows in total.
# MAGIC * Deleted rows in the event log (both updates, per flow): {{E3.deleted_all}}. `patients_scd1_flow` reports {{E3.deleted_scd1}}: `num_deleted_rows` is "The number of existing output rows deleted from the dataset during an update" (*Advanced AUTO CDC topics*), and patient 5 was inserted and deleted in the same delivery, so no existing row had to be removed. The plan `TRUNCATE` counted {{E3.deleted_plans}} deleted rows (E7).
# MAGIC
# MAGIC > **Key takeaway:** always map the delete operation of your feed (`'DELETE'`, Debezium `'d'`, …) with `APPLY AS DELETE WHEN`. If the source can send late events, a delete must win over older updates — AUTO CDC does this; a hand-written `MERGE` does not do it for free (E9).

# COMMAND ----------

# DBTITLE 1,E4 Predict
# MAGIC %md
# MAGIC ### E4 — SCD Type 2: history, current rows and a point-in-time query
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. How many versions does patient 6 have (changes at 6, 13 and the late one at 8)?
# MAGIC 2. How do you get exactly one current row per patient?
# MAGIC 3. What is the data type of `__START_AT` when the flow uses `SEQUENCE BY event_ts`?

# COMMAND ----------

# DBTITLE 1,E4 Code
e4_history = patient_rows("patients_scd2")                    # every version of every patient
display(e4_history)
e4_current = patient_rows("patients_scd2", "__END_AT IS NULL")   # the current version of each patient
display(e4_current)
e4_as_of = patient_rows("patients_scd2",                     # the state at 2026-09-01 20:30 (after the delete at 12)
    "__START_AT <= TIMESTAMP'2026-09-01 20:30:00' AND (__END_AT IS NULL OR __END_AT > TIMESTAMP'2026-09-01 20:30:00')")
display(e4_as_of)
start_type = dict(spark.table(f"{FQ}.patients_scd2").dtypes)["__START_AT"]   # type of the version columns
scd1_now = patient_rows("patients_scd1")                      # SCD Type 1 table, same columns
current_patients = e4_current.drop("start_at", "end_at")      # SCD Type 2 current rows without the times
same_as_scd1 = current_patients.exceptAll(scd1_now).count() == 0 and scd1_now.exceptAll(current_patients).count() == 0

METRICS["E4.history_rows"] = e4_history.count()
METRICS["E4.current_rows"] = e4_current.count()
METRICS["E4.p6_versions"] = rows_text(patient_rows("patients_scd2", "patient_id = 6"))
METRICS["E4.p1_versions"] = rows_text(patient_rows("patients_scd2", "patient_id = 1"))
METRICS["E4.current_equals_scd1"] = same_as_scd1
METRICS["E4.as_of_rows"] = e4_as_of.count()
METRICS["E4.as_of"] = rows_text(e4_as_of.select("patient_id", "insurance_plan", "city"))
METRICS["E4.start_type"] = start_type
assert e4_history.count() == 14 and e4_current.count() == 6 and same_as_scd1

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `patients_scd2` has **{{E4.history_rows}}** rows (versions) and **{{E4.current_rows}}** current rows (`__END_AT IS NULL`).
# MAGIC * Patient 6 (patient, name, city, plan, start, end): `{{E4.p6_versions}}`. The late change at 8 (it arrived in delivery 2, after the change at 13) became its own version **between** the versions of 6 and 13: AUTO CDC fixed the `__END_AT` of the older version.
# MAGIC * Patient 1: `{{E4.p1_versions}}`.
# MAGIC * The current rows are the same as `patients_scd1` (patient columns): **{{E4.current_equals_scd1}}**.
# MAGIC * Point in time 2026-09-01 20:30 (`__START_AT <= t AND (__END_AT IS NULL OR __END_AT > t)`): {{E4.as_of_rows}} patients (id, plan, city): `{{E4.as_of}}`.
# MAGIC * `__START_AT` has the type **`{{E4.start_type}}`** — the type of `event_ts`, the `SEQUENCE BY` column.
# MAGIC
# MAGIC > **Key takeaway:** SCD Type 2 = SCD Type 1 + history. Current rows: `__END_AT IS NULL`. Point in time: compare `t` with `__START_AT` and `__END_AT`.

# COMMAND ----------

# DBTITLE 1,E5 Predict
# MAGIC %md
# MAGIC ### E5 — History only for some columns (`TRACK HISTORY ON`, Python)
# MAGIC
# MAGIC `patients_scd2_tracked` is defined in **Python** (`patients_tracked.py`) with `track_history_except_column_list=["city"]` — SQL: `TRACK HISTORY ON * EXCEPT (city)`.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Patient 2 changed only the city (Boston → Burlington). How many versions does patient 2 have in `patients_scd2` and in `patients_scd2_tracked`? Which city does the tracked table show?
# MAGIC 2. Patient 1 changed the plan. Does the tracked table create a new version?

# COMMAND ----------

# DBTITLE 1,E5 Code
e5_compare = spark.sql(f"""
    SELECT f.patient_id, f.versions AS versions_full_history, t.versions AS versions_tracked
    FROM (SELECT patient_id, count(*) AS versions FROM {FQ}.patients_scd2 GROUP BY patient_id) AS f
    FULL JOIN (SELECT patient_id, count(*) AS versions FROM {FQ}.patients_scd2_tracked GROUP BY patient_id) AS t
      ON f.patient_id = t.patient_id
    ORDER BY f.patient_id""")                                 # versions per patient in both tables
display(e5_compare)
display(patient_rows("patients_scd2_tracked"))                # the tracked table

METRICS["E5.tracked_rows"] = spark.table(f"{FQ}.patients_scd2_tracked").count()
METRICS["E5.full_rows"] = spark.table(f"{FQ}.patients_scd2").count()
METRICS["E5.p2_full"] = rows_text(patient_rows("patients_scd2", "patient_id = 2"))
METRICS["E5.p2_tracked"] = rows_text(patient_rows("patients_scd2_tracked", "patient_id = 2"))
METRICS["E5.p4_tracked"] = rows_text(patient_rows("patients_scd2_tracked", "patient_id = 4"))
METRICS["E5.p1_tracked"] = rows_text(patient_rows("patients_scd2_tracked", "patient_id = 1"))
assert spark.table(f"{FQ}.patients_scd2_tracked").count() == 12
METRICS["E5.versions"] = "; ".join(f"{r['patient_id']}: {r['versions_full_history']}/{r['versions_tracked']}" for r in e5_compare.collect())

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Rows: full history **{{E5.full_rows}}**, tracked **{{E5.tracked_rows}}**. Versions per patient (full / tracked): {{E5.versions}}.
# MAGIC * Patient 2 with full history: `{{E5.p2_full}}`. Tracked: `{{E5.p2_tracked}}` — the city change updated the current version **in place** (one version, the new city, the original start).
# MAGIC * Patient 4 (city change at 22) in the tracked table: `{{E5.p4_tracked}}`.
# MAGIC * Patient 1 changed its plan, a tracked column: `{{E5.p1_tracked}}` — a new version, as in the full history.
# MAGIC * The same flow in SQL would end with `STORED AS SCD TYPE 2 TRACK HISTORY ON * EXCEPT (city)`; `TRACK HISTORY ON insurance_plan` lists the tracked columns instead (syntax: `TRACK HISTORY ON {columnList | * EXCEPT (exceptColumnList)}`; P3 uses a column list).
# MAGIC
# MAGIC > **Key takeaway:** `TRACK HISTORY ON` keeps SCD Type 2 small: only the columns someone must audit create versions; the other columns always show the latest value.

# COMMAND ----------

# DBTITLE 1,E6 Predict
# MAGIC %md
# MAGIC ### E6 — Duplicate events
# MAGIC
# MAGIC The change of patient 3 at sequence 10 is in the feed **three** times: twice in delivery 1 (exact copies) and once more in delivery 2.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. How many versions does patient 3 have in `patients_scd2`: 2, 3 or 4?
# MAGIC 2. Does the update fail because two rows have the same key and the same sequence value?

# COMMAND ----------

# DBTITLE 1,E6 Code
p3_events = spark.sql(f"""SELECT patient_id, insurance_plan, operation, sequence_num, arrival_ts
                          FROM {FQ}.patients_cdc WHERE patient_id = 3 ORDER BY arrival_ts""")   # patient 3 in the feed
display(p3_events)
p3_scd2 = patient_rows("patients_scd2", "patient_id = 3")     # its versions in SCD Type 2
display(p3_scd2)
dup_keys = spark.sql(f"""SELECT count(*) AS duplicate_rows FROM {FQ}.patients_scd1
                         GROUP BY patient_id HAVING count(*) > 1""")   # SCD Type 1 must have one row per key
METRICS["E6.p3_events"] = p3_events.count()
METRICS["E6.p3_seq10_events"] = p3_events.filter("sequence_num = 10").count()
METRICS["E6.p3_scd2_rows"] = p3_scd2.count()
METRICS["E6.p3_scd2"] = rows_text(p3_scd2)
METRICS["E6.p3_scd1"] = rows_text(patient_rows("patients_scd1", "patient_id = 3"))
METRICS["E6.scd1_duplicate_keys"] = dup_keys.count()
METRICS["E6.update_states"] = f"{u1_state}, {u2_state}"
assert p3_scd2.count() == 2 and dup_keys.count() == 0

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Patient 3 has {{E6.p3_events}} rows in the feed, {{E6.p3_seq10_events}} of them with sequence 10.
# MAGIC * `patients_scd2` has **{{E6.p3_scd2_rows}}** versions of patient 3: `{{E6.p3_scd2}}`. `patients_scd1`: `{{E6.p3_scd1}}`. Keys with more than one row in `patients_scd1`: {{E6.scd1_duplicate_keys}}.
# MAGIC * Both updates ended in `{{E6.update_states}}`: the exact duplicates did not stop the flows.
# MAGIC * Careful: these copies were **identical**. *Change data capture and snapshots* asks for "one distinct update per key at each sequencing value". Two **different** changes with the same key and the same sequence value have no defined order; make the order unique with `SEQUENCE BY STRUCT(event_ts, event_id)`.
# MAGIC
# MAGIC > **Key takeaway:** redelivered (identical) change events are harmless for AUTO CDC. Ties between different changes are your problem: give every change a unique sequence.

# COMMAND ----------

# DBTITLE 1,E7 Predict
# MAGIC %md
# MAGIC ### E7 — A full reload: `APPLY AS TRUNCATE WHEN` (SCD Type 1 only)
# MAGIC
# MAGIC Plan delivery 2: plan 2 is renamed (sequence 8), then the payer sends `TRUNCATE` (10) and the plans that still exist: 1, 3 and a new plan 5 (11–13).
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which plans are in `plans_current` after update 2? What happened to plans 2 and 4?
# MAGIC 2. Could you use the same clause in an SCD Type 2 flow?

# COMMAND ----------

# DBTITLE 1,E7 Code
plans_u2 = spark.sql(f"SELECT plan_id, plan_name FROM {FQ}.plans_current ORDER BY plan_id")   # after update 2
display(plans_u2)
display(spark.table(f"{FQ}.plans_cdc").orderBy("sequence_num"))   # the plan feed, in sequence order
METRICS["E7.plans_u2"] = rows_text(plans_u2)
METRICS["E7.plans_u2_rows"] = plans_u2.count()
assert [r["plan_id"] for r in plans_u2.collect()] == [1, 3, 5]

# COMMAND ----------

# DBTITLE 1,E7 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * After update 1 (plan id, name): `{{E7.plans_u1}}`.
# MAGIC * After update 2: **{{E7.plans_u2_rows}}** plans: `{{E7.plans_u2}}`. The `TRUNCATE` at 10 cleared everything older than 10 — also the rename of plan 2 at 8 — and the inserts at 11–13 came after it.
# MAGIC * The truncate row has `plan_id` 0 in our feed. The docs example (*The AUTO CDC APIs…*) sends a NULL key for its `TRUNCATE` row; the condition is only `operation = 'TRUNCATE'`.
# MAGIC * SCD Type 2: "The `APPLY AS TRUNCATE WHEN` clause is supported only for SCD type 1. SCD type 2 does not support the truncate operation" (*AUTO CDC INTO (pipelines)*). NOT VERIFIED here — concept only (it would cost one more failing pipeline update).
# MAGIC
# MAGIC > **Key takeaway:** use `APPLY AS TRUNCATE WHEN` only for a real "clear the table" event of an SCD Type 1 target (a reference table that the source reloads). It removes every row, so do not map normal deletes to it.

# COMMAND ----------

# DBTITLE 1,E8 Predict
# MAGIC %md
# MAGIC ### E8 — AUTO CDC FROM SNAPSHOT (SQL): changes from two full snapshots
# MAGIC
# MAGIC The payer directory has no change feed. Version 1 had payers 1, 2, 3. Version 2 (landed in E2): payer 1 unchanged, payer 2 now `OUT_OF_NETWORK`, payer 3 missing, payer 4 new.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which rows will `payer_directory_scd2` have, and what are `__START_AT` / `__END_AT` (there is no `SEQUENCE BY`)?
# MAGIC 2. What happens to payer 3, which is missing in version 2?

# COMMAND ----------

# DBTITLE 1,E8 Code
payers_u2 = spark.sql(f"""SELECT payer_id, network_status, __START_AT, __END_AT
                          FROM {FQ}.payer_directory_scd2 ORDER BY payer_id, __START_AT""")   # after update 2
display(payers_u2)
payer_flows = [r for r in e2_metrics.collect() if r["flow"].startswith("payer_directory")]   # the snapshot flow
payer_flow = payer_flows[0] if payer_flows else None
delete_pipeline(MAIN)                                         # Free Edition: free the pipeline slot for the practice
time.sleep(15)                                                # give the catalog a moment
after_delete = spark.table(f"{FQ}.patients_scd2").count()     # the tables stay (L07 E6) and can still be read
METRICS["E8.payers_u2"] = rows_text(payers_u2)
METRICS["E8.rows"] = payers_u2.count()
METRICS["E8.current_rows"] = payers_u2.filter("__END_AT IS NULL").count()
METRICS["E8.start_type"] = dict(payers_u2.dtypes)["__START_AT"]
METRICS["E8.flow_name"] = payer_flow["flow"] if payer_flow else "no flow_progress row"
METRICS["E8.flow_metrics"] = f"+{payer_flow['upserted_rows']}/-{payer_flow['deleted_rows']}" if payer_flow else "no flow_progress row"
METRICS["E8.rows_after_pipeline_delete"] = after_delete
assert payers_u2.count() == 5 and payers_u2.filter("__END_AT IS NULL").count() == 3 and after_delete == 14

# COMMAND ----------

# DBTITLE 1,E8 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * After update 1 (payer, status, start, end): `{{E8.payers_u1}}` — snapshot version 1.
# MAGIC * After update 2: **{{E8.rows}}** rows, {{E8.current_rows}} current: `{{E8.payers_u2}}`.
# MAGIC * Payer 2 got a new version, payer 3 (missing in version 2) was closed like a delete, payer 4 was inserted, payer 1 kept its version. `__START_AT` / `__END_AT` are the snapshot **versions** (type `{{E8.start_type}}`), because the version query decides the order.
# MAGIC * Event log of update 2: the snapshot flow is named `{{E8.flow_name}}` (its target) and reports `{{E8.flow_metrics}}` (upserted/deleted) — as *Advanced AUTO CDC topics* says, these metrics are "captured only by AUTO CDC queries and not by AUTO CDC FROM SNAPSHOT queries". Read the table to see what changed.
# MAGIC * **Pipeline deleted.** The code then deletes the example pipeline (Free Edition: at most two lesson pipelines at a time, L07; the practice needs its own). Its tables stay and can still be read: `patients_scd2` has {{E8.rows_after_pipeline_delete}} rows (L07 E6; *Use Unity Catalog with pipelines*). They can no longer be updated by that pipeline.
# MAGIC
# MAGIC > **Key takeaway:** no change feed, only full extracts → AUTO CDC FROM SNAPSHOT. It finds inserts, updates and deletes by comparing snapshots in version order, but it cannot see changes that happened between two snapshots.

# COMMAND ----------

# DBTITLE 1,E9 Predict
# MAGIC %md
# MAGIC ### E9 — The same SCD Type 2 by hand: `MERGE` in a normal notebook
# MAGIC
# MAGIC No pipeline here: a Delta table `patients_scd2_manual` and the classic SCD Type 2 `MERGE`, run once per delivery. Per delivery it (1) keeps the **latest** change per patient, (2) skips changes that are not newer than what the table already knows, (3) closes the current version and inserts the new one in **one** `MERGE` (the "merge key" trick: the same change twice, once with the key to close the old row, once with a NULL key so that it never matches and is inserted).
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Will the manual table have as many versions as `patients_scd2`?
# MAGIC 2. Patient 5 was inserted and deleted in delivery 1, and a late update came in delivery 2. What will the manual table show for patient 5?

# COMMAND ----------

# DBTITLE 1,E9 Code
MANUAL = f"{FQ}.patients_scd2_manual"
spark.sql(f"""CREATE OR REPLACE TABLE {MANUAL} (patient_id INT, first_name STRING, last_name STRING, city STRING,
              insurance_plan STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP)""")   # the SCD Type 2 target

def merge_scd2_delivery(arrival_day):
    """Apply one delivery of patients_cdc to the manual SCD Type 2 table; return the MERGE metrics."""
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW patient_changes AS
        WITH delivery AS (                                    -- 1. the changes of one delivery
          SELECT * FROM {FQ}.patients_cdc
          WHERE to_date(arrival_ts) = DATE'{arrival_day}'
          QUALIFY row_number() OVER (PARTITION BY patient_id ORDER BY event_ts DESC) = 1   -- latest per patient
        ),
        known AS (                                            -- 2. the newest time the table knows per patient
          SELECT patient_id, max(coalesce(__END_AT, __START_AT)) AS known_ts FROM {MANUAL} GROUP BY patient_id
        ),
        open_rows AS (SELECT * FROM {MANUAL} WHERE __END_AT IS NULL)   -- 3. the current version per patient
        SELECT d.*
        FROM delivery AS d
        LEFT JOIN known AS k ON d.patient_id = k.patient_id
        LEFT JOIN open_rows AS c ON d.patient_id = c.patient_id
        WHERE (k.known_ts IS NULL OR d.event_ts > k.known_ts)   -- skip changes that are not newer
          AND (d.operation = 'DELETE' OR c.patient_id IS NULL  -- a delete, a new patient ...
               OR NOT (d.first_name <=> c.first_name AND d.last_name <=> c.last_name
                       AND d.city <=> c.city AND d.insurance_plan <=> c.insurance_plan))   -- ... or a real change
    """)
    return spark.sql(f"""
        MERGE INTO {MANUAL} AS t
        USING (
          SELECT patient_id AS merge_key, * FROM patient_changes      -- closes the current version
          UNION ALL
          SELECT NULL AS merge_key, ch.*                               -- NULL key: never matches -> new version
          FROM patient_changes AS ch
          JOIN {MANUAL} AS cur ON ch.patient_id = cur.patient_id AND cur.__END_AT IS NULL
          WHERE ch.operation <> 'DELETE'
        ) AS s
        ON t.patient_id = s.merge_key AND t.__END_AT IS NULL
        WHEN MATCHED THEN UPDATE SET __END_AT = s.event_ts             -- close the old version (update or delete)
        WHEN NOT MATCHED AND s.operation <> 'DELETE' THEN INSERT       -- open the new version
          (patient_id, first_name, last_name, city, insurance_plan, __START_AT, __END_AT)
          VALUES (s.patient_id, s.first_name, s.last_name, s.city, s.insurance_plan, s.event_ts, NULL)
    """).first().asDict()

merge_1 = merge_scd2_delivery("2026-10-01")                   # delivery 1
merge_2 = merge_scd2_delivery("2026-10-02")                   # delivery 2
print("MERGE delivery 1:", merge_1, "\nMERGE delivery 2:", merge_2)
display(patient_rows("patients_scd2_manual"))                 # the hand-made history
cols = "patient_id, first_name, last_name, city, insurance_plan"
only_manual = spark.sql(f"""SELECT {cols} FROM {MANUAL} WHERE __END_AT IS NULL
                            EXCEPT SELECT {cols} FROM {FQ}.patients_scd2 WHERE __END_AT IS NULL""")   # current rows
only_auto = spark.sql(f"""SELECT {cols} FROM {FQ}.patients_scd2 WHERE __END_AT IS NULL
                          EXCEPT SELECT {cols} FROM {MANUAL} WHERE __END_AT IS NULL""")
display(only_manual)
manual_rows = spark.table(MANUAL).count()
METRICS["E9.merge_1"] = f"inserted {merge_1['num_inserted_rows']}, updated {merge_1['num_updated_rows']}"
METRICS["E9.merge_2"] = f"inserted {merge_2['num_inserted_rows']}, updated {merge_2['num_updated_rows']}"
METRICS["E9.manual_rows"] = manual_rows
METRICS["E9.manual_current"] = spark.table(MANUAL).filter("__END_AT IS NULL").count()
METRICS["E9.auto_rows"] = spark.table(f"{FQ}.patients_scd2").count()
METRICS["E9.only_manual"] = rows_text(only_manual)
METRICS["E9.only_auto_rows"] = only_auto.count()
METRICS["E9.p2_manual"] = rows_text(patient_rows("patients_scd2_manual", "patient_id = 2"))
METRICS["E9.p6_manual"] = rows_text(patient_rows("patients_scd2_manual", "patient_id = 6"))
assert manual_rows == 9 and only_manual.count() == 1 and only_auto.count() == 0

# COMMAND ----------

# DBTITLE 1,E9 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `MERGE` results: delivery 1 {{E9.merge_1}}; delivery 2 {{E9.merge_2}}.
# MAGIC * The manual table has **{{E9.manual_rows}}** rows ({{E9.manual_current}} current); `patients_scd2` has **{{E9.auto_rows}}**.
# MAGIC * **Lost versions.** One `MERGE` applies one change per key, so step 1 kept only the latest change per patient and delivery. Patient 2 by hand: `{{E9.p2_manual}}` (the Boston version is missing; compare E5). Patient 6 by hand: `{{E9.p6_manual}}` (only the latest version: the version from 6 was lost in delivery 1, and the late change at 8 was skipped in delivery 2; AUTO CDC has all three, E4).
# MAGIC * **A deleted patient came back.** Current rows only in the manual table: `{{E9.only_manual}}`. Patient 5 never had a row in the manual table (insert and delete came in the same delivery), so nothing remembered the delete, and the late update at 9 was inserted as a new current row. AUTO CDC kept a tombstone and ignored it (E3). Current rows only in `patients_scd2`: {{E9.only_auto_rows}}.
# MAGIC * To make the hand-written version correct you would need: all changes per delivery in sequence order (a loop or a window-function rebuild), your own tombstone table for deletes, and late changes inserted into the middle of the history.
# MAGIC
# MAGIC > **Key takeaway:** the two-part `MERGE` is the right tool for a clean batch with one change per key (P4). For a real CDC feed with deletes, duplicates and late events, use AUTO CDC.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "AUTO CDC needs the change rows in the right order." | `SEQUENCE BY` orders them: patient 6 kept `{{E2.p6_scd1}}` although the older change arrived later (E2). |
# MAGIC | 2 | "The arrival time (or file time) is a good sequence column." | With `SEQUENCE BY arrival_ts` patient 6 showed `{{E2.p6_by_arrival}}` and patient 5 `{{E2.p5_by_arrival}}` (E2). Use the source order; Auto Loader does not guarantee file order. |
# MAGIC | 3 | "A DELETE row deletes the row by itself." | Only with `APPLY AS DELETE WHEN`; without it the delete was upserted: `{{E3.p5_no_deletes}}` (E3). |
# MAGIC | 4 | "A late update after a delete brings the row back." | Not with AUTO CDC (tombstone; E3: {{E3.p5_scd1_rows}} rows). The simple hand-written `MERGE` did bring it back (E9). |
# MAGIC | 5 | "SCD Type 2 is the default." | "The default is SCD type 1" (*AUTO CDC INTO (pipelines)*). |
# MAGIC | 6 | "Every AUTO CDC target has `__START_AT` / `__END_AT`." | Only SCD Type 2. SCD Type 1 columns: `{{E1.scd1_columns}}` (E1). |
# MAGIC | 7 | "`__START_AT` is always a timestamp." | It has the type of the sequence: `{{E4.start_type}}` with `SEQUENCE BY event_ts` (E4), `{{E8.start_type}}` for the snapshot versions (E8). |
# MAGIC | 8 | "An SCD Type 2 target has an `is_current` flag." | Current rows are `__END_AT IS NULL` (E4: {{E4.current_rows}} rows). |
# MAGIC | 9 | "Every change creates a new SCD Type 2 version." | Not for columns outside `TRACK HISTORY ON`: patient 2's city change was applied in place (E5). |
# MAGIC | 10 | "`APPLY AS TRUNCATE WHEN` works with SCD Type 2." | "supported only for SCD type 1" (*AUTO CDC INTO (pipelines)*; E7). |
# MAGIC | 11 | "A redelivered change event creates a duplicate row or version." | Patient 3's change came three times and gave {{E6.p3_scd2_rows}} versions in total (E6). |
# MAGIC | 12 | "Different changes with the same key and the same sequence value are fine." | The docs ask for "one distinct update per key at each sequencing value"; break ties with `SEQUENCE BY STRUCT(ts, id)` (*Change data capture and snapshots*). |
# MAGIC | 13 | "AUTO CDC flows report `num_output_rows`." | They report `num_upserted_rows` and `num_deleted_rows` (E1: `{{E1.cdc_metrics}}`; *Advanced AUTO CDC topics*). |
# MAGIC | 14 | "Keys or the sequence column may be NULL." | The flow fails: `AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY` / `.NULL_SEQUENCE` (*Error classes*). NOT VERIFIED here — concept only. |
# MAGIC | 15 | "AUTO CDC FROM SNAPSHOT is Python only." | The SQL form ran in E8 (`FROM SNAPSHOT … WITH VERSION …`). |
# MAGIC | 16 | "The source of an AUTO CDC flow can be any batch query." | It must be a streaming source (`STREAM(...)`), except with `ONCE` (*AUTO CDC INTO (pipelines)*). |
# MAGIC | 17 | "A hand-written SCD Type 2 `MERGE` gives the same history." | {{E9.manual_rows}} rows by hand vs {{E9.auto_rows}} with AUTO CDC (E9). |
# MAGIC | 18 | "`APPLY CHANGES` no longer works." | It is still available; AUTO CDC has the same syntax and is recommended (*The AUTO CDC APIs…*). |

# COMMAND ----------

# DBTITLE 1,7 — Syntax Cheat Sheet
# MAGIC %md
# MAGIC ## 7 Syntax Cheat Sheet
# MAGIC
# MAGIC | Goal | Pipeline SQL | Pipeline Python (`from pyspark import pipelines as dp`) |
# MAGIC | --- | --- | --- |
# MAGIC | Empty target | `CREATE OR REFRESH STREAMING TABLE t;` | `dp.create_streaming_table("t")` |
# MAGIC | The flow | `CREATE FLOW f AS AUTO CDC INTO t FROM STREAM(src)` | `dp.create_auto_cdc_flow(target="t", source="src", name="f", …)` |
# MAGIC | Keys | `KEYS (patient_id)` | `keys=["patient_id"]` |
# MAGIC | Order | `SEQUENCE BY sequence_num` / `SEQUENCE BY STRUCT(ts, id)` | `sequence_by="sequence_num"` / `sequence_by=struct("ts", "id")` |
# MAGIC | Deletes | `APPLY AS DELETE WHEN operation = 'DELETE'` | `apply_as_deletes="operation = 'DELETE'"` |
# MAGIC | Full reload (SCD1) | `APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'` | `apply_as_truncates="operation = 'TRUNCATE'"` |
# MAGIC | Drop metadata columns | `COLUMNS * EXCEPT (operation, sequence_num)` | `except_column_list=["operation", "sequence_num"]` |
# MAGIC | Only some columns | `COLUMNS (patient_id, city)` | `column_list=["patient_id", "city"]` |
# MAGIC | SCD type | `STORED AS SCD TYPE 1` (default) / `STORED AS SCD TYPE 2` | `stored_as_scd_type=1` / `2` |
# MAGIC | History columns | `TRACK HISTORY ON insurance_plan` / `TRACK HISTORY ON * EXCEPT (city)` | `track_history_column_list=[…]` / `track_history_except_column_list=[…]` |
# MAGIC | Snapshots | `AUTO CDC INTO t FROM SNAPSHOT (query) WITH VERSION (query) KEYS (…)` | `dp.create_auto_cdc_from_snapshot_flow(target=…, source=…, keys=[…])` |
# MAGIC | Legacy name | `APPLY CHANGES INTO …` | `dlt.apply_changes(…)` |
# MAGIC
# MAGIC ### Reading SCD Type 2 and the event log
# MAGIC ```sql
# MAGIC -- current rows
# MAGIC SELECT * FROM patients_scd2 WHERE __END_AT IS NULL;
# MAGIC -- the state at time t
# MAGIC SELECT * FROM patients_scd2 WHERE __START_AT <= :t AND (__END_AT IS NULL OR __END_AT > :t);
# MAGIC -- rows written by AUTO CDC flows in one update
# MAGIC SELECT origin.flow_name,
# MAGIC        sum(details:flow_progress.metrics.num_upserted_rows::bigint) AS upserted,
# MAGIC        sum(details:flow_progress.metrics.num_deleted_rows::bigint)  AS deleted
# MAGIC FROM event_log('<pipeline-id>')
# MAGIC WHERE event_type = 'flow_progress' AND origin.update_id = '<update-id>'
# MAGIC GROUP BY ALL;
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 2 by hand (two-part `MERGE`, one change per key)
# MAGIC ```sql
# MAGIC MERGE INTO dim AS t
# MAGIC USING (
# MAGIC   SELECT id AS merge_key, * FROM changes                        -- closes the current version
# MAGIC   UNION ALL
# MAGIC   SELECT NULL AS merge_key, c.* FROM changes AS c               -- inserts the new version
# MAGIC   JOIN dim AS d ON c.id = d.id AND d.__END_AT IS NULL
# MAGIC   WHERE c.operation <> 'DELETE'
# MAGIC ) AS s
# MAGIC ON t.id = s.merge_key AND t.__END_AT IS NULL
# MAGIC WHEN MATCHED THEN UPDATE SET __END_AT = s.event_ts
# MAGIC WHEN NOT MATCHED AND s.operation <> 'DELETE' THEN
# MAGIC   INSERT (id, value, __START_AT, __END_AT) VALUES (s.id, s.value, s.event_ts, NULL);
# MAGIC ```
# MAGIC `changes` must have at most one row per key and only real changes (dedupe first).

# COMMAND ----------

# DBTITLE 1,8 — Check Your Understanding
# MAGIC %md
# MAGIC ## 8 Check Your Understanding
# MAGIC
# MAGIC Answer in your own words. Answers are in section 11.
# MAGIC
# MAGIC 1. Why must `SEQUENCE BY` be a column from the source system and not the time when the data arrived? Use E2 in your answer.
# MAGIC 2. An audit team needs to know which insurance plan a patient had on any date, but a corrected spelling of the city should not create a new version. Write the end of the AUTO CDC statement (`STORED AS …`).
# MAGIC 3. Name three things that the hand-written `MERGE` in E9 had to do itself or got wrong, which AUTO CDC handled.

# COMMAND ----------

# DBTITLE 1,9 — Practice (intro)
# MAGIC %md
# MAGIC ## 9 Practice
# MAGIC
# MAGIC Do these yourself, without AI help. P1–P3 write pipeline source code as a Python string, and `run_practice(...)` runs it in **one** practice pipeline, `dbacademy_l08_practice` (always a full refresh, so you can run a task again). P4 needs no pipeline. Each validation cell prints ✅ Passed or a hint. Solutions are in section 11.
# MAGIC
# MAGIC > The practice reads the feeds of section 3 (both deliveries must have landed: run E2 first) and compares your results with the example tables.

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
    return pipeline_id, update_id, state

def differences(table_a, table_b, cols):
    """(rows only in a, rows only in b) for the given columns; duplicates count."""
    a = spark.sql(f"SELECT {cols} FROM {FQ}.{table_a}")
    b = spark.sql(f"SELECT {cols} FROM {FQ}.{table_b}")
    return a.exceptAll(b).count(), b.exceptAll(a).count()

def reset_p4():
    """(Re)create the two tables of P4: an SCD Type 2 provider dimension and today's change batch."""
    spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.p4_provider_dim (provider_id INT, provider_name STRING, specialty STRING,
                  __START_AT TIMESTAMP, __END_AT TIMESTAMP)""")
    spark.sql(f"""INSERT INTO {FQ}.p4_provider_dim VALUES
        (1, 'Dr Fake 1', 'Internal Medicine', TIMESTAMP'2026-01-01 00:00:00', TIMESTAMP'2026-03-01 00:00:00'),
        (1, 'Dr Fake 1', 'General',           TIMESTAMP'2026-03-01 00:00:00', NULL),
        (2, 'Dr Fake 2', 'Cardiology',        TIMESTAMP'2026-01-01 00:00:00', NULL),
        (3, 'Dr Fake 3', 'Neurology',         TIMESTAMP'2026-01-01 00:00:00', NULL),
        (4, 'Dr Fake 4', 'Pediatrics',        TIMESTAMP'2026-01-01 00:00:00', NULL)""")
    spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.p4_provider_changes AS
        SELECT * FROM VALUES
          (1, 'Dr Fake 1', 'Oncology',    'UPDATE', TIMESTAMP'2026-10-01 09:00:00'),
          (2, 'Dr Fake 2', 'Cardiology',  'UPDATE', TIMESTAMP'2026-10-01 10:00:00'),
          (3, NULL,        NULL,          'DELETE', TIMESTAMP'2026-10-01 11:00:00'),
          (5, 'Dr Fake 5', 'Dermatology', 'INSERT', TIMESTAMP'2026-10-01 12:00:00'),
          (5, 'Dr Fake 5', 'Dermatology', 'INSERT', TIMESTAMP'2026-10-01 12:00:00')
        AS t(provider_id, provider_name, specialty, operation, event_ts)""")

reset_p4()
PATIENT_COLS = "patient_id, first_name, last_name, city, insurance_plan"
print("Practice helpers ready: run_practice, differences, reset_p4 (P4 tables created)")

# COMMAND ----------

# DBTITLE 1,P1 Task
# MAGIC %md
# MAGIC ### P1 — Fill in the blanks (3 blanks): an SCD Type 1 flow
# MAGIC Complete the source so that `p1_patients_current` holds the current patients: one row per patient, deletes applied, late changes resolved by `sequence_num`. Replace the three `<FILL_IN>` markers (each is a few SQL keywords), then run the cell. Goal: the same rows as `patients_scd1`.

# COMMAND ----------

# DBTITLE 1,P1 Code
# >>> BLANK P1
P1_SQL = """
CREATE OR REFRESH STREAMING TABLE p1_patients_current;

CREATE FLOW p1_flow AS <FILL_IN> p1_patients_current
FROM STREAM(main.dbacademy_l08.patients_cdc)
KEYS (patient_id)
<FILL_IN> operation = 'DELETE'
<FILL_IN> sequence_num
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)
STORED AS SCD TYPE 1;
"""
# >>> SOLUTION P1
P1_SQL = """
CREATE OR REFRESH STREAMING TABLE p1_patients_current;

CREATE FLOW p1_flow AS AUTO CDC INTO p1_patients_current
FROM STREAM(main.dbacademy_l08.patients_cdc)
KEYS (patient_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)
STORED AS SCD TYPE 1;
"""
# <<< END P1
p1 = run_practice({"p1.sql": P1_SQL})

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    pid, uid, state = p1
    only_p1, only_example = differences("p1_patients_current", "patients_scd1", PATIENT_COLS)
    rows = spark.table(f"{FQ}.p1_patients_current").count()
    ok = state == "COMPLETED" and only_p1 == 0 and only_example == 0
    PRACTICE_PASSED["P1"] = ok
    if ok:
        print("✅ Passed")
    elif state != "COMPLETED":
        print(f"Hint: the update ended in {state}. Read the ERROR lines printed by the code cell: is every blank a "
              "SQL clause from 4.3 (AUTO CDC INTO, APPLY AS DELETE WHEN, SEQUENCE BY)?")
    else:
        print(f"Hint: {only_p1} rows only in your table, {only_example} rows only in patients_scd1. Check the delete "
              "rule and the sequence column (E2, E3).")
    METRICS["P1.state"], METRICS["P1.rows"] = state, rows
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: run the P1 code cell first; the table p1_patients_current must exist.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs): a patient history that is wrong
# MAGIC A colleague wrote the SCD Type 2 flow below. The update completes, but the history is wrong: a deleted patient still has an open version, and patient 6 shows the wrong current plan. Fix the two bugs in place and run the cell until the validation passes. Goal: the same versions as `patients_scd2` (start and end values may use another type).

# COMMAND ----------

# DBTITLE 1,P2 Code
# >>> BLANK P2
P2_SQL = """
CREATE OR REFRESH STREAMING TABLE p2_patients_history;

CREATE FLOW p2_flow AS AUTO CDC INTO p2_patients_history
FROM STREAM(main.dbacademy_l08.patients_cdc)
KEYS (patient_id)
SEQUENCE BY arrival_ts
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)
STORED AS SCD TYPE 2;
"""
# >>> SOLUTION P2
P2_SQL = """
CREATE OR REFRESH STREAMING TABLE p2_patients_history;

CREATE FLOW p2_flow AS AUTO CDC INTO p2_patients_history
FROM STREAM(main.dbacademy_l08.patients_cdc)
KEYS (patient_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (operation, sequence_num, event_ts, arrival_ts)
STORED AS SCD TYPE 2;
"""
# <<< END P2
p2 = run_practice({"p2.sql": P2_SQL})

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    pid, uid, state = p2
    version_cols = PATIENT_COLS + ", __END_AT IS NULL AS is_current"
    only_p2, only_example = differences("p2_patients_history", "patients_scd2", version_cols)
    rows = spark.table(f"{FQ}.p2_patients_history").count()
    open_p5 = spark.table(f"{FQ}.p2_patients_history").filter("patient_id = 5 AND __END_AT IS NULL").count()
    ok = state == "COMPLETED" and only_p2 == 0 and only_example == 0
    PRACTICE_PASSED["P2"] = ok
    if ok:
        print("✅ Passed")
    elif state != "COMPLETED":
        print(f"Hint: the update ended in {state}. Read the ERROR lines printed by the code cell.")
    else:
        print(f"Hint: {only_p2} versions only in your table, {only_example} only in patients_scd2; patient 5 has "
              f"{open_p5} open versions (want 0). One bug is about deletes (E3), one about the order of the changes (E2).")
    METRICS["P2.state"], METRICS["P2.rows"] = state, rows
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: run the P2 code cell first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): provider specialty history
# MAGIC Write a pipeline source (SQL or Python) that builds `p3_provider_history` from `main.dbacademy_l08.providers_cdc`, and run it with `p3 = run_practice({"p3.sql": P3_SQL})` (or `{"p3.py": P3_PY}`):
# MAGIC * SCD Type 2, one key: `provider_id`; deletes are rows with `operation = 'DELETE'`; the order is `sequence_num`;
# MAGIC * columns `provider_id`, `provider_name`, `specialty` (+ the version columns);
# MAGIC * only a **specialty** change creates a new version; a name change updates the current version in place.
# MAGIC
# MAGIC **Hints:** empty streaming table + `CREATE FLOW … AS AUTO CDC INTO` (E1) · `STREAM(...)` · `COLUMNS * EXCEPT (…)` for the metadata columns `operation`, `sequence_num`, `event_ts` · `STORED AS SCD TYPE 2` · `TRACK HISTORY ON <column list>` (E5).

# COMMAND ----------

# DBTITLE 1,P3 Code
# >>> BLANK P3
# Your code here
# >>> SOLUTION P3
P3_SQL = """
CREATE OR REFRESH STREAMING TABLE p3_provider_history;

CREATE FLOW p3_flow AS AUTO CDC INTO p3_provider_history
FROM STREAM(main.dbacademy_l08.providers_cdc)
KEYS (provider_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num, event_ts)
STORED AS SCD TYPE 2
TRACK HISTORY ON specialty;
"""
p3 = run_practice({"p3.sql": P3_SQL})
# <<< END P3

# COMMAND ----------

# DBTITLE 1,P3 Validation
try:
    pid, uid, state = p3
    t = spark.table(f"{FQ}.p3_provider_history")
    current = sorted((r["provider_id"], r["provider_name"], r["specialty"])
                     for r in t.filter("__END_AT IS NULL").collect())
    want_current = [(1, "Dr Fake 1", "Oncology"), (2, "Dr Fake Two", "Cardiology"), (4, "Dr Fake 4", "Pediatric Cardiology")]
    versions = {r["provider_id"]: r["n"] for r in t.groupBy("provider_id").count().withColumnRenamed("count", "n").collect()}
    want_versions = {1: 3, 2: 1, 3: 1, 4: 2}
    ok = (state == "COMPLETED" and current == want_current and versions == want_versions
          and {"provider_id", "provider_name", "specialty", "__START_AT", "__END_AT"} <= set(t.columns)
          and not {"operation", "sequence_num"} & set(t.columns))
    PRACTICE_PASSED["P3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want current rows {want_current} and versions per provider {want_versions}; got state {state}, "
          f"current {current}, versions {versions}, columns {t.columns}. A name change must not create a version (E5); "
          "a delete closes the version (E3); the late change at 6 belongs between 1 and 7 (E4).")
    METRICS["P3.state"], METRICS["P3.rows"] = state, t.count()
    METRICS["P3.versions"] = "; ".join(f"{k}: {v}" for k, v in sorted(versions.items()))
except Exception as e:
    PRACTICE_PASSED["P3"] = False
    print("Hint: create P3_SQL (or P3_PY) and run p3 = run_practice(...) first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints): SCD Type 2 by hand with `MERGE`
# MAGIC `main.dbacademy_l08.p4_provider_dim` is an SCD Type 2 provider dimension (`provider_id`, `provider_name`, `specialty`, `__START_AT`, `__END_AT`; current rows have `__END_AT IS NULL`). `main.dbacademy_l08.p4_provider_changes` holds today's changes (`operation`, `event_ts`); it can contain exact duplicates. Without a pipeline, apply the changes to the dimension with **one** `MERGE` statement (preparation queries are allowed):
# MAGIC * a changed provider: close the current version at `event_ts` and add a new current version that starts at `event_ts`;
# MAGIC * an `UPDATE` without a real change: do nothing;
# MAGIC * a `DELETE`: close the current version;
# MAGIC * a new provider: add a current version.
# MAGIC
# MAGIC To start again, run `reset_p4()`.

# COMMAND ----------

# DBTITLE 1,P4 Code
# >>> BLANK P4
# Your code here
# >>> SOLUTION P4
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW p4_changes AS
    SELECT c.*
    FROM (SELECT DISTINCT * FROM {FQ}.p4_provider_changes) AS c
    LEFT JOIN {FQ}.p4_provider_dim AS d
      ON c.provider_id = d.provider_id AND d.__END_AT IS NULL
    WHERE c.operation = 'DELETE'
       OR d.provider_id IS NULL
       OR NOT (c.provider_name <=> d.provider_name AND c.specialty <=> d.specialty)
""")
p4_result = spark.sql(f"""
    MERGE INTO {FQ}.p4_provider_dim AS t
    USING (
      SELECT provider_id AS merge_key, * FROM p4_changes
      UNION ALL
      SELECT NULL AS merge_key, c.*
      FROM p4_changes AS c
      JOIN {FQ}.p4_provider_dim AS d ON c.provider_id = d.provider_id AND d.__END_AT IS NULL
      WHERE c.operation <> 'DELETE'
    ) AS s
    ON t.provider_id = s.merge_key AND t.__END_AT IS NULL
    WHEN MATCHED THEN UPDATE SET __END_AT = s.event_ts
    WHEN NOT MATCHED AND s.operation <> 'DELETE' THEN
      INSERT (provider_id, provider_name, specialty, __START_AT, __END_AT)
      VALUES (s.provider_id, s.provider_name, s.specialty, s.event_ts, NULL)
""").first()
# <<< END P4

# COMMAND ----------

# DBTITLE 1,P4 Validation
try:
    want = spark.sql("""
        SELECT * FROM VALUES
          (1, 'Dr Fake 1', 'Internal Medicine', TIMESTAMP'2026-01-01 00:00:00', TIMESTAMP'2026-03-01 00:00:00'),
          (1, 'Dr Fake 1', 'General',           TIMESTAMP'2026-03-01 00:00:00', TIMESTAMP'2026-10-01 09:00:00'),
          (1, 'Dr Fake 1', 'Oncology',          TIMESTAMP'2026-10-01 09:00:00', CAST(NULL AS TIMESTAMP)),
          (2, 'Dr Fake 2', 'Cardiology',        TIMESTAMP'2026-01-01 00:00:00', CAST(NULL AS TIMESTAMP)),
          (3, 'Dr Fake 3', 'Neurology',         TIMESTAMP'2026-01-01 00:00:00', TIMESTAMP'2026-10-01 11:00:00'),
          (4, 'Dr Fake 4', 'Pediatrics',        TIMESTAMP'2026-01-01 00:00:00', CAST(NULL AS TIMESTAMP)),
          (5, 'Dr Fake 5', 'Dermatology',       TIMESTAMP'2026-10-01 12:00:00', CAST(NULL AS TIMESTAMP))
        AS t(provider_id, provider_name, specialty, __START_AT, __END_AT)""")
    got = spark.table(f"{FQ}.p4_provider_dim").select("provider_id", "provider_name", "specialty", "__START_AT", "__END_AT")
    extra, missing = got.exceptAll(want).count(), want.exceptAll(got).count()
    ok = extra == 0 and missing == 0
    PRACTICE_PASSED["P4"] = ok
    print("✅ Passed" if ok else
          f"Hint: {extra} rows that should not be there, {missing} rows missing. Run reset_p4() and try again. Check: "
          "duplicates removed, no-change updates skipped, only current rows (__END_AT IS NULL) matched, deletes close "
          "the version without a new one.")
    METRICS["P4.rows"] = got.count()
    METRICS["P4.current"] = got.filter("__END_AT IS NULL").count()
except Exception as e:
    PRACTICE_PASSED["P4"] = False
    print("Hint: the table p4_provider_dim must exist (run reset_p4()).", error_class(e))

# COMMAND ----------

# DBTITLE 1,10 — Exam-Style Questions
# MAGIC %md
# MAGIC ## 10 Exam-Style Questions
# MAGIC
# MAGIC Choose the **one best answer**. Answers and explanations are in section 11.
# MAGIC
# MAGIC **Q1.** A hospital replicates its eligibility table from PostgreSQL into a Lakeflow pipeline. The feed has `op` (`c`, `u`, `d`) and `lsn` (the log sequence number). Reports need only the **current** coverage per member; updates sometimes arrive out of order. Which flow is best?
# MAGIC A. `AUTO CDC INTO elig FROM STREAM(feed) KEYS (member_id) APPLY AS DELETE WHEN op = 'd' SEQUENCE BY lsn STORED AS SCD TYPE 1` · B. The same with `SEQUENCE BY _metadata.file_modification_time` · C. A streaming table `SELECT * FROM STREAM(feed)` and a view with `max(lsn)` per member · D. The same as A with `STORED AS SCD TYPE 2` and no delete rule
# MAGIC
# MAGIC **Q2.** An AUTO CDC target of patients still shows patients that were deleted in the source; their name and address columns are NULL. What is the most likely cause?
# MAGIC A. The sequence column has ties · B. The flow has no `APPLY AS DELETE WHEN` clause · C. The target is SCD Type 2 · D. The tombstone retention is too short
# MAGIC
# MAGIC **Q3.** The compliance team must see which primary care provider (`pcp_id`) a patient had on any day. Phone number changes must **not** create new versions. Which clause ends the flow?
# MAGIC A. `STORED AS SCD TYPE 1` · B. `STORED AS SCD TYPE 2 TRACK HISTORY ON * EXCEPT (phone)` · C. `STORED AS SCD TYPE 2 COLUMNS * EXCEPT (phone)` · D. `STORED AS SCD TYPE 1 TRACK HISTORY ON pcp_id`
# MAGIC
# MAGIC **Q4.** A data analyst needs the current row of every provider from an SCD Type 2 table built with AUTO CDC. Which filter is correct?
# MAGIC A. `WHERE is_current = true` · B. `WHERE __END_AT IS NULL` · C. `WHERE __START_AT = max(__START_AT)` · D. `WHERE _change_type = 'insert'`
# MAGIC
# MAGIC **Q5.** A lab system sends its result catalog only as a full nightly extract (no change feed). The team wants an SCD Type 2 history of the catalog in a pipeline. What should they use?
# MAGIC A. `AUTO CDC … FROM SNAPSHOT (…) WITH VERSION (…) KEYS (test_code) STORED AS SCD TYPE 2` · B. A materialized view over the latest extract · C. `AUTO CDC … SEQUENCE BY load_date` on the extract, with `APPLY AS TRUNCATE WHEN` · D. Change data feed on the extract table
# MAGIC
# MAGIC **Q6.** A payer sends a `RELOAD` event before resending its full plan list. The target is an SCD Type 2 table. An engineer adds `APPLY AS TRUNCATE WHEN event = 'RELOAD'`. What is true?
# MAGIC A. It works and closes all versions · B. Truncate is supported only for SCD Type 1, so the design must change (for example an SCD Type 1 target, or treat missing plans as deletes with AUTO CDC FROM SNAPSHOT) · C. It deletes the history and starts again · D. It is ignored without an error
# MAGIC
# MAGIC **Q7.** An engineer must report how many rows an AUTO CDC flow changed in the last update. Which event log field should they read?
# MAGIC A. `details:flow_progress.metrics.num_output_rows` · B. `details:flow_progress.metrics.num_upserted_rows` and `num_deleted_rows` · C. `details:flow_progress.data_quality.dropped_records` · D. `operationMetrics` in `DESCRIBE HISTORY`
# MAGIC
# MAGIC **Q8.** A team keeps a provider dimension (SCD Type 2) with a nightly hand-written `MERGE`. A change batch now sometimes contains two changes of the same provider. Which statement is true?
# MAGIC A. `MERGE` applies both changes in order and creates two versions · B. `MERGE` can fail when several source rows match the same target row, so the batch must first be reduced to one row per key (or use AUTO CDC, which keeps every change as a version) · C. `MERGE` ignores the second change automatically · D. Add `SEQUENCE BY` to the `MERGE` statement

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — streaming table or materialized view** (ST: each input row once; MV: always the current result of the query, L07 E3/E4)
# MAGIC {{solution:W1}}
# MAGIC
# MAGIC **W2 — a NULL-safe `DROP ROW` rule** (`heart_rate BETWEEN 20 AND 250` alone gives NULL for a missing value)
# MAGIC {{solution:W2}}
# MAGIC
# MAGIC **W3 — event log query** (colon syntax on the JSON string, cast, sum per flow)
# MAGIC {{solution:W3}}
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. Changes can arrive late or in another order than they happened (network, retries, files that Auto Loader processes in any order). Only the source order says which value is the newest. In E2, `SEQUENCE BY arrival_ts` gave patient 6 `{{E2.p6_by_arrival}}` and patient 5 `{{E2.p5_by_arrival}}`; `SEQUENCE BY sequence_num` gave `{{E2.p6_scd1}}` and kept patient 5 deleted.
# MAGIC 2. `… STORED AS SCD TYPE 2 TRACK HISTORY ON * EXCEPT (city)` (or `TRACK HISTORY ON insurance_plan, …` with the columns that need history). E5: patient 2's city change did not create a version.
# MAGIC 3. Any three: reduce each delivery to one change per key (and lose the versions in between: E9 had {{E9.manual_rows}} rows vs {{E9.auto_rows}}); remove duplicates; skip late changes or insert them into the middle of the history; remember deletes (the manual table brought patient 5 back: `{{E9.only_manual}}`); compare values to skip no-change updates; close and insert versions with the merge-key trick.
# MAGIC
# MAGIC ### P1 (blanks: `AUTO CDC INTO`, `APPLY AS DELETE WHEN`, `SEQUENCE BY`)
# MAGIC {{solution:P1}}
# MAGIC
# MAGIC ### P2 (fixes: add `APPLY AS DELETE WHEN operation = 'DELETE'` — E3; sequence by the source order `event_ts` (or `sequence_num`) instead of `arrival_ts` — E2)
# MAGIC {{solution:P2}}
# MAGIC
# MAGIC ### P3
# MAGIC {{solution:P3}}
# MAGIC
# MAGIC Python version of the same flow:
# MAGIC ```python
# MAGIC P3_PY = '''
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC @dp.temporary_view(name="providers_feed")
# MAGIC def providers_feed():
# MAGIC     return spark.readStream.table("main.dbacademy_l08.providers_cdc")
# MAGIC
# MAGIC dp.create_streaming_table("p3_provider_history")
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     target="p3_provider_history", source="providers_feed", keys=["provider_id"],
# MAGIC     sequence_by="sequence_num", apply_as_deletes="operation = 'DELETE'",
# MAGIC     except_column_list=["operation", "sequence_num", "event_ts"],
# MAGIC     stored_as_scd_type=2, track_history_column_list=["specialty"])
# MAGIC '''
# MAGIC ```
# MAGIC
# MAGIC ### P4
# MAGIC {{solution:P4}}
# MAGIC
# MAGIC ### Exam answers
# MAGIC
# MAGIC **Q1 → A.** SCD Type 1 keeps the current state, `SEQUENCE BY lsn` resolves out-of-order events (E2: `{{E2.p6_scd1}}` kept), and the delete rule maps `d` (E3). B wrong: file time is arrival order (E2: `{{E2.p6_by_arrival}}`). C wrong: an append-only table keeps every change and deletes stay. D wrong: without a delete rule, deleted members stay (E3), and history is not needed.
# MAGIC
# MAGIC **Q2 → B.** Without `APPLY AS DELETE WHEN`, the delete row is upserted with its NULL columns (E3: `{{E3.p5_no_deletes}}`). A wrong: ties give an undefined order, not NULL rows. C wrong: SCD Type 2 closes deleted versions when the delete rule is there (E3). D wrong: tombstones only matter for late events after a delete.
# MAGIC
# MAGIC **Q3 → B.** History only where it is needed (E5). A wrong: no history. C wrong: `COLUMNS * EXCEPT (phone)` removes the phone column from the table. D wrong: `TRACK HISTORY ON` needs SCD Type 2 (*AUTO CDC INTO (pipelines)*; error class `AUTOCDC_TRACK_HISTORY_REQUIRES_SCD2`).
# MAGIC
# MAGIC **Q4 → B.** Current versions have `__END_AT = NULL` (E4: {{E4.current_rows}} current rows). A wrong: there is no such column (E1: `{{E1.scd2_columns}}`). C wrong: aggregate in WHERE is invalid, and a deleted key has no current row. D wrong: `_change_type` belongs to the change data feed (L14).
# MAGIC
# MAGIC **Q5 → A.** No change feed, only full extracts → AUTO CDC FROM SNAPSHOT (E8: missing keys were closed, changed keys got a version). B wrong: no history. C wrong: a full extract is not a change feed, and truncate is SCD Type 1 only. D wrong: the extract is replaced every night, so its change feed shows a full rewrite, not the business changes.
# MAGIC
# MAGIC **Q6 → B.** "SCD type 2 does not support the truncate operation" (*AUTO CDC INTO (pipelines)*; E7 used truncate with SCD Type 1). A, C, D wrong for the same reason.
# MAGIC
# MAGIC **Q7 → B.** AUTO CDC flows report `num_upserted_rows` and `num_deleted_rows`; "The `num_output_rows` metric … is not captured for AUTO CDC queries" (*Advanced AUTO CDC topics*; E1: `{{E1.cdc_metrics}}`). C wrong: that is for expectations (L07). D wrong: it is a table-level view, not the flow metric the event log reports per update.
# MAGIC
# MAGIC **Q8 → B.** *MERGE INTO*: "MERGE operations fail with a `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` error if more than one row in the source table matches the same row in the target table"; the page suggests to "retain only the latest change for each key". E9 therefore kept one change per key per delivery and lost versions ({{E9.manual_rows}} vs {{E9.auto_rows}} rows). A, C wrong for the same reason. D wrong: `SEQUENCE BY` is AUTO CDC syntax, not `MERGE`.

# COMMAND ----------

# DBTITLE 1,12 — Cleanup
# Delete the pipelines first, then the folder with the source files, then the schema
# (DROP SCHEMA ... CASCADE also removes the streaming tables that the deleted pipelines kept).
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
# MAGIC | Setup | `patients_cdc` {{setup.patient_events}} change rows (delivery 1), `plans_cdc` {{setup.plan_events}}, payer snapshot {{setup.payer_rows}} rows, `providers_cdc` {{setup.provider_events}} |
# MAGIC | E1 SCD Type 1 | update 1 `{{E1.state}}`; `patients_scd1` {{E1.scd1_rows}} rows; metrics `{{E1.cdc_metrics}}` |
# MAGIC | E2 out of order | update 2 `{{E2.state}}`; patient 6: `{{E2.p6_scd1}}` with `sequence_num`, `{{E2.p6_by_arrival}}` with `arrival_ts` |
# MAGIC | E3 deletes | patient 5 rows in SCD Type 1: {{E3.p5_scd1_rows}}; without a delete rule: `{{E3.p5_no_deletes}}` |
# MAGIC | E4 SCD Type 2 | {{E4.history_rows}} versions, {{E4.current_rows}} current; `__START_AT` type `{{E4.start_type}}`; current = SCD Type 1: {{E4.current_equals_scd1}} |
# MAGIC | E5 track history | full {{E5.full_rows}} vs tracked {{E5.tracked_rows}} rows |
# MAGIC | E6 duplicates | patient 3: {{E6.p3_seq10_events}} copies of one change → {{E6.p3_scd2_rows}} versions in total |
# MAGIC | E7 truncate | plans after update 2: `{{E7.plans_u2}}` |
# MAGIC | E8 snapshots | {{E8.rows}} rows, {{E8.current_rows}} current; version type `{{E8.start_type}}` |
# MAGIC | E9 manual MERGE | {{E9.manual_rows}} rows by hand vs {{E9.auto_rows}} with AUTO CDC; only in the manual current rows: `{{E9.only_manual}}` |
# MAGIC | Practice | P1 {{P1.rows}} rows · P2 {{P2.rows}} versions · P3 versions per provider {{P3.versions}} · P4 {{P4.rows}} rows, {{P4.current}} current |
# MAGIC | Cleanup | lesson pipelines left: {{cleanup.pipelines_left}} |
# MAGIC
# MAGIC Update durations in this run (they change from run to run): E1 {{nd.E1.secs}} s, E2 {{nd.E2.secs}} s.
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **`APPLY AS TRUNCATE WHEN` on an SCD Type 2 target** — the docs say it is not supported; not run (one more failing pipeline update on the Free Edition compute budget).
# MAGIC * **NULL keys or NULL sequence values** (`AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY` / `.NULL_SEQUENCE`) — from the *Error classes* page; not run.
# MAGIC * **Two different changes with the same key and sequence value** — undefined order per *Change data capture and snapshots*; not run (the result could differ between runs).
# MAGIC * **Tombstone retention** (`pipelines.cdc.tombstoneGCThresholdInSeconds`, default two days) — E3 showed a late event being ignored within the same day; the expiry was not tested.
# MAGIC * **`IGNORE NULL UPDATES`, `COLUMNS TO UPDATE`, `ONCE` flows, bitemporal tables, Python `create_auto_cdc_from_snapshot_flow`, reading the change data feed of an AUTO CDC target** — syntax from the docs; not run.
# MAGIC * **Lakeflow Connect** as the source of the CDC feed — L20.
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. The pipeline published its event log as a table in the target schema (`{{E1.event_log_table}}`) without the `event_log` setting.
# MAGIC 2. The event log has `flow_progress` events with the flow name `missingFlowName` and no status (all names: `{{nd.E1.raw_flow_names}}`); `cdc_metrics` skips them. The AUTO CDC FROM SNAPSHOT flow is listed under its **target** name (`{{E8.flow_name}}`), not under its `CREATE FLOW` name, and reports `{{E8.flow_metrics}}`.
# MAGIC 3. `num_deleted_rows` stayed {{E3.deleted_scd1}} for the patient delete (insert and delete in the same delivery: no existing row), while one `TRUNCATE` counted {{E3.deleted_plans}} deleted rows. `num_upserted_rows` of SCD Type 2 counts written versions: {{E2.scd2_upserted}} in update 2 for 6 change rows.
# MAGIC 4. Exact duplicate change rows (same key, same sequence) did not fail the flow and created no extra version (E6); the docs only define the case of "one distinct update per key at each sequencing value".
# MAGIC 5. The hand-written SCD Type 2 `MERGE` looked correct but brought back a deleted patient through a late update (E9: `{{E9.only_manual}}`).
