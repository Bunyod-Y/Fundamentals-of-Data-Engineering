# Databricks notebook source
# LESSON_TITLE: Lakeflow Jobs - Tasks, Parameters, Control Flow and Repair

# COMMAND ----------

# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L09 — Lakeflow Jobs: tasks, parameters, control flow and repair
# MAGIC
# MAGIC **Exam objectives** (Professional, Oct 9, 2026)
# MAGIC
# MAGIC * Section 1 *Developing code* (23%): build multi-task jobs with dependencies, parameters, task values, If/else and For each tasks.
# MAGIC * Section 4 *Monitoring & alerting* (10%): read run states, run history and durations through the API; notifications and duration warnings.
# MAGIC * Section 8 *Debugging & deploying* (10%): find the failed task and its error, repair a run, run a job with different parameters, export a job definition as JSON (the start of a bundle, L21).
# MAGIC
# MAGIC **Names.** Lakeflow Jobs is the current name of the orchestration product that was called **Databricks Workflows** (and before that simply "Jobs"). *Lakeflow Jobs*: "Lakeflow Jobs is workflow automation for Databricks, providing orchestration for data processing workloads". The REST API is still the **Jobs API** (`/api/2.2/jobs/...`).
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** the Jobs REST API (from Python, the CLI or the SDK) to create a job, start it with `run-now`, wait for it, and read task states and outputs.
# MAGIC 2. **Explain** job parameters, task parameters, dynamic value references and task values, and how a value moves from one task to the next.
# MAGIC 3. **Compare** `depends_on` + `run_if`, If/else (`condition_task`) and For each (`for_each_task`), and the trigger types (schedule, file arrival, table update, continuous).
# MAGIC 4. **Implement** a job with a failing task, an alert task that runs only on failure, and a **repair run** that re-runs only what failed.
# MAGIC 5. **Diagnose** a wrong `depends_on`, a wrong dynamic value reference and a failed task from its run output.
# MAGIC
# MAGIC > **Evidence-only rule:** every number, state and error class in this notebook comes from a real run of this notebook (the build fills them in), or from an official Databricks doc page named in the text. Things that cannot be tested on this workspace are marked **NOT VERIFIED — concept only (reason)**.
# MAGIC
# MAGIC > **Run time and compute:** this lesson creates small jobs and runs them on serverless compute: three example job runs, two repairs (one example, one practice) and one practice run. Each run takes one to a few minutes. Run the cells in order and wait for each one. Practice tasks P1–P3 only **create** jobs (no compute); only P4 runs one.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up (intro)
# MAGIC %md
# MAGIC ## 2 Warm-up — recall L08 AUTO CDC and SCD (no hints)
# MAGIC
# MAGIC Write each task from memory. Run the validation cell after each one. Solutions are in section 11.
# MAGIC
# MAGIC > Run the next cell first: it creates a small table and two temporary views for the warm-up. No pipeline is started.

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

# W2: a small patient table (the setup in section 3 drops it again) and today's changes
spark.sql("CREATE SCHEMA IF NOT EXISTS main.dbacademy_l09")
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l09.wu_patients AS
    SELECT * FROM VALUES (1, 'Austin', 'PLAN_A'), (2, 'Boston', 'PLAN_B'), (3, 'Chicago', 'PLAN_A')
    AS t(patient_id, city, insurance_plan)""")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wu_changes AS
    SELECT * FROM VALUES (2, 'Burlington', 'PLAN_B'), (3, 'Chicago', 'PLAN_C'), (4, 'Denver', 'PLAN_A')
    AS t(patient_id, city, insurance_plan)""")

# W3: a small change feed; arrival_order is the order in which the rows landed
spark.sql("""
    CREATE OR REPLACE TEMP VIEW wu_cdc AS
    SELECT * FROM VALUES
      (1, 'PLAN_A', 'INSERT', 1, 1), (1, 'PLAN_B', 'UPDATE', 5, 2), (1, 'PLAN_C', 'UPDATE', 3, 3),
      (2, 'PLAN_A', 'INSERT', 2, 4), (2, NULL,     'DELETE', 6, 5), (2, 'PLAN_B', 'UPDATE', 4, 6),
      (3, 'PLAN_C', 'INSERT', 7, 7)
    AS t(patient_id, insurance_plan, operation, sequence_num, arrival_order)""")
print("Warm-up objects ready: main.dbacademy_l09.wu_patients, wu_changes, wu_cdc")

# COMMAND ----------

# DBTITLE 1,W1 Task
# MAGIC %md
# MAGIC ### W1 — SCD Type 1 or SCD Type 2?
# MAGIC For each target table, choose `1` (SCD Type 1) or `2` (SCD Type 2). Store the answers in the dict `w1_scd` with exactly these four keys:
# MAGIC
# MAGIC | Key | Table |
# MAGIC | --- | --- |
# MAGIC | `billing_current_plan` | The billing team needs only the insurance plan each patient has **today**. |
# MAGIC | `pcp_history_for_audit` | Auditors must see which primary care provider a patient had on any past date. |
# MAGIC | `address_book` | A patient address book; old addresses are never needed. |
# MAGIC | `drug_price_point_in_time` | Claims must be priced with the drug price that was valid on the day of the claim. |

# COMMAND ----------

# DBTITLE 1,W1 Code
# >>> BLANK W1
# Your code here
# >>> SOLUTION W1
w1_scd = {
    "billing_current_plan": 1,        # only the latest value
    "pcp_history_for_audit": 2,       # every version with its time range
    "address_book": 1,                # history not needed
    "drug_price_point_in_time": 2,    # point-in-time lookup needs __START_AT / __END_AT
}
# <<< END W1

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    w1_expected = {"billing_current_plan": 1, "pcp_history_for_audit": 2, "address_book": 1, "drug_price_point_in_time": 2}
    w1_got = {k: int(v) for k, v in w1_scd.items()}
    w1_wrong = sorted(k for k in w1_expected if w1_got.get(k) != w1_expected[k])
    ok = not w1_wrong and set(w1_got) == set(w1_expected)
    WARMUP_PASSED["W1"] = ok
    print("✅ Passed" if ok else
          f"Hint: check these keys: {w1_wrong or sorted(set(w1_got) ^ set(w1_expected))}. SCD Type 1 keeps only the latest "
          "values; SCD Type 2 keeps every version with __START_AT / __END_AT (L08 4.1).")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create the dict w1_scd with the values 1 or 2.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — An upsert with `MERGE`
# MAGIC Without a pipeline, apply the rows of the view `wu_changes` to the table `main.dbacademy_l09.wu_patients` with **one** `MERGE` statement: update `city` and `insurance_plan` of existing patients (key `patient_id`) and insert new patients.

# COMMAND ----------

# DBTITLE 1,W2 Code
# >>> BLANK W2
# Your code here
# >>> SOLUTION W2
spark.sql("""
    MERGE INTO main.dbacademy_l09.wu_patients AS t
    USING wu_changes AS s
    ON t.patient_id = s.patient_id
    WHEN MATCHED THEN UPDATE SET city = s.city, insurance_plan = s.insurance_plan
    WHEN NOT MATCHED THEN INSERT (patient_id, city, insurance_plan) VALUES (s.patient_id, s.city, s.insurance_plan)
""")
# <<< END W2

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    got = sorted(tuple(r) for r in spark.table("main.dbacademy_l09.wu_patients").collect())
    want = [(1, "Austin", "PLAN_A"), (2, "Burlington", "PLAN_B"), (3, "Chicago", "PLAN_C"), (4, "Denver", "PLAN_A")]
    ok = got == want
    WARMUP_PASSED["W2"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {want}; got {got}. WHEN MATCHED → UPDATE SET, WHEN NOT MATCHED → INSERT (L01, L08 E9). "
          "Run the warm-up prep cell again to reset the table.")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: run the warm-up prep cell first; the MERGE must be valid SQL.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — What `SEQUENCE BY` does, by hand
# MAGIC The view `wu_cdc` is a change feed (`patient_id`, `insurance_plan`, `operation`, `sequence_num`, `arrival_order`). Rows arrived out of order. Create the DataFrame `w3_result` with the **current** state per patient, as AUTO CDC with `SEQUENCE BY sequence_num` and `APPLY AS DELETE WHEN operation = 'DELETE'` would give it: columns `patient_id`, `insurance_plan`; a patient whose latest change is a delete has no row.

# COMMAND ----------

# DBTITLE 1,W3 Code
# >>> BLANK W3
# Your code here
# >>> SOLUTION W3
w3_result = spark.sql("""
    SELECT patient_id, insurance_plan
    FROM wu_cdc
    QUALIFY row_number() OVER (PARTITION BY patient_id ORDER BY sequence_num DESC) = 1
          AND operation <> 'DELETE'
""")
# <<< END W3

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    got = sorted(tuple(r) for r in w3_result.select("patient_id", "insurance_plan").collect())
    want = [(1, "PLAN_B"), (3, "PLAN_C")]
    ok = got == want
    WARMUP_PASSED["W3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {want}; got {got}. Take the row with the highest sequence_num per patient (not the last arrival), "
          "then drop patients whose latest change is a DELETE (L08 E2, E3).")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create w3_result with the columns patient_id and insurance_plan.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent. It deletes the jobs and the task-code folder of an earlier run, recreates the schema,
# and writes fake encounter data (no PHI) into normal Delta tables.
import base64, io, json, random, time
from databricks.sdk import WorkspaceClient                  # the Databricks SDK for Python (preinstalled)
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

CATALOG, SCHEMA = "main", "dbacademy_l09"
FQ = f"{CATALOG}.{SCHEMA}"                                   # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"                    # the lesson volume
PREFIX = "dbacademy_l09"                                     # every job of this lesson starts with this name

w = WorkspaceClient()                                        # inside a notebook it signs in as you
ME = w.current_user.me().user_name
RUNTIME = f"/Users/{ME}/_lesson_runtime/L09"                 # workspace folder for the task notebooks and files

def api(method, path, body=None, query=None):
    """Call the Databricks REST API as the notebook user; the SDK client adds the host and the token (L07 4.7)."""
    return w.api_client.do(method, path, query=query, body=body) or {}

def lesson_jobs(name=None):
    """[(name, job_id)] of every job whose name starts with PREFIX (or has exactly this name)."""
    found, token = [], None
    while True:
        page = api("GET", "/api/2.2/jobs/list", query={"limit": 100, **({"page_token": token} if token else {})})
        for job in page.get("jobs", []):
            job_name = job.get("settings", {}).get("name", "")
            if (job_name == name) if name else job_name.startswith(PREFIX):
                found.append((job_name, job["job_id"]))
        token = page.get("next_page_token")
        if not token:
            return found

def delete_job(job_id):
    """Cancel the active runs of a job, then delete the job (its run history goes with it)."""
    api("POST", "/api/2.2/jobs/runs/cancel-all", body={"job_id": job_id})
    api("POST", "/api/2.2/jobs/delete", body={"job_id": job_id})

# 1. Delete the jobs and the task code of an earlier run
old = lesson_jobs()
for _, job_id in old:
    delete_job(job_id)
try:
    w.workspace.delete(RUNTIME, recursive=True)
except NotFound:
    pass                                                     # nothing to delete
w.workspace.mkdirs(RUNTIME)
# 2. Recreate the schema and the volume
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
spark.sql(f"CREATE SCHEMA {FQ}")
spark.sql(f"CREATE VOLUME {FQ}.data")

# 3. Source data: encounters of three fake practices on three days (2026-10-04 and 2026-10-05 have none)
rng = random.Random(42)                                       # own random generator: same data in every run
DAYS = {"2026-10-01": {"PR01": 5, "PR02": 4, "PR03": 3},
        "2026-10-02": {"PR01": 2, "PR02": 6},
        "2026-10-03": {"PR03": 4}}
rows, n = [], 0
for day, per_practice in DAYS.items():
    for practice, count in per_practice.items():
        for _ in range(count):
            n += 1
            rows.append(f"('E{n:04d}', 'P{rng.randint(1000, 1099)}', '{practice}', DATE'{day}', "
                        f"CAST({rng.randint(8000, 45000) / 100:.2f} AS DECIMAL(10,2)))")
spark.sql(f"""CREATE TABLE {FQ}.encounters_raw AS SELECT * FROM VALUES {", ".join(rows)}
              AS t(encounter_id, patient_id, practice_id, visit_date, cost)""")

# 4. Empty tables that the job tasks write to (created here, so parallel tasks never race to create them)
spark.sql(f"CREATE TABLE {FQ}.encounters_daily (encounter_id STRING, patient_id STRING, practice_id STRING, visit_date DATE, cost DECIMAL(10,2))")
spark.sql(f"""CREATE TABLE {FQ}.daily_summary (run_date DATE, encounters BIGINT, total_cost DECIMAL(12,2),
              rows_reported STRING, rows_from_get BIGINT, job_run_id STRING, task_name STRING)""")
spark.sql(f"CREATE TABLE {FQ}.practice_report (practice_id STRING, run_date DATE, encounters BIGINT, total_cost DECIMAL(12,2), task_name STRING)")
spark.sql(f"CREATE TABLE {FQ}.run_log (event STRING, detail STRING, logged_at TIMESTAMP)")
spark.sql(f"CREATE TABLE {FQ}.table_counts (table_name STRING, row_count BIGINT, counted_at TIMESTAMP)")
spark.sql(f"CREATE TABLE {FQ}.lab_report (lab_rows BIGINT, repair_count STRING, report_execution_count STRING, load_execution_count STRING)")

# Check what we created and record the real counts
raw_rows = spark.table(f"{FQ}.encounters_raw").count()
day1_rows = spark.table(f"{FQ}.encounters_raw").where("visit_date = DATE'2026-10-01'").count()
METRICS["nd.setup.old_jobs_deleted"] = len(old)
METRICS["setup.raw_rows"] = raw_rows
METRICS["setup.day1_rows"] = day1_rows
METRICS["nd.setup.sdk_version"] = __import__("databricks.sdk.version", fromlist=["__version__"]).__version__
assert (raw_rows, day1_rows) == (24, 12)
print(f"encounters_raw {raw_rows} rows, {day1_rows} on 2026-10-01")
display(spark.sql(f"""SELECT visit_date, practice_id, count(*) AS encounters, sum(cost) AS total_cost
                      FROM {FQ}.encounters_raw GROUP BY ALL ORDER BY ALL"""))

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):** `encounters_raw` has **{{setup.raw_rows}}** fake encounters of three practices (`PR01`–`PR03`) on 2026-10-01 to 2026-10-03; **{{setup.day1_rows}}** of them are on 2026-10-01. There are no encounters on 2026-10-04 or 2026-10-05 (the If/else example uses this).
# MAGIC
# MAGIC The other tables are empty. The job tasks write into them:
# MAGIC
# MAGIC | Table | Written by | Example |
# MAGIC | --- | --- | --- |
# MAGIC | `encounters_daily` | task `ingest` (one day of `encounters_raw`) | E1 |
# MAGIC | `daily_summary` | task `build_summary` (one row per run, with the values it received) | E2 |
# MAGIC | `practice_report` | the For each task, one row per practice | E3 |
# MAGIC | `run_log` | the alert task of E5 and practice P4 | E5, P4 |
# MAGIC | `table_counts` | the notebook `count_table` | P1–P4 |
# MAGIC | `lab_report` | task `report_labs` | E5 |
# MAGIC
# MAGIC The setup also deletes every job whose name starts with `dbacademy_l09` and the folder with the task code, so you can run it again at any time.

# COMMAND ----------

# DBTITLE 1,4.1 Concept — Jobs, tasks and task types
# MAGIC %md
# MAGIC ## 4 Concepts
# MAGIC
# MAGIC ### 4.1 Jobs, tasks and task types
# MAGIC
# MAGIC **What** (*Lakeflow Jobs*): "A job is the primary resource for coordinating, scheduling, and running your operations." "A task is a specific unit of work within a job." Tasks form a directed acyclic graph (DAG): a task can wait for other tasks (`depends_on`). A **trigger** starts the job. Each start of a job is a **job run**; each task in it is a **task run** with its own run id.
# MAGIC
# MAGIC **Limits** (*Lakeflow Jobs*): "A job can contain up to 1000 tasks." "A workspace is limited to 2000 concurrent task runs." Free Edition has much lower compute limits, so this lesson keeps every run small.
# MAGIC
# MAGIC **Task types** (*Configure and edit tasks in Lakeflow Jobs*, shortened):
# MAGIC
# MAGIC | Task type | JSON field | Runs | In this lesson |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Notebook | `notebook_task` | a notebook; parameters arrive as widgets | E1–E5, P1–P4 |
# MAGIC | Python script | `spark_python_task` | a `.py` file; parameters arrive as command-line arguments | E4 (`log_no_data.py`) |
# MAGIC | Python wheel, JAR, Spark Submit | `python_wheel_task`, `spark_jar_task`, `spark_submit_task` | packaged code | NOT VERIFIED — concept only (no package built) |
# MAGIC | SQL | `sql_task` | a query, file, alert or dashboard **on a SQL warehouse** | NOT VERIFIED — concept only (needs a SQL warehouse; compute budget) |
# MAGIC | Pipeline | `pipeline_task` | an update of a Lakeflow Declarative Pipeline (L07, L08) | NOT VERIFIED — concept only (L07/L08 ran pipelines through the API) |
# MAGIC | dbt | `dbt_task` | dbt commands | NOT VERIFIED — concept only |
# MAGIC | Run Job | `run_job_task` | another job (a job of jobs) | NOT VERIFIED — concept only (compute budget) |
# MAGIC | If/else | `condition_task` | compares two values; no code | E1, E4 |
# MAGIC | For each | `for_each_task` | runs one nested task once per input value | E3 |
# MAGIC
# MAGIC **Serverless** (*Run your Lakeflow Jobs with serverless compute for workflows*): "Serverless compute is supported with the notebook, Python script, dbt, Python wheel, and JAR task types." A task without any cluster setting runs on serverless compute (this workspace has only serverless).
# MAGIC
# MAGIC **Why jobs and not one big notebook?** Separate tasks can run in parallel, can be retried and **repaired** one by one, show their own state and duration, and can get different parameters. One big notebook fails as a whole and must be run again from the start.

# COMMAND ----------

# DBTITLE 1,4.2 Concept — Dependencies, run_if, If/else, For each
# MAGIC %md
# MAGIC ### 4.2 Control flow: `depends_on`, `run_if`, If/else, For each
# MAGIC
# MAGIC **`depends_on` + `run_if`** (*Configure task dependencies*). A task waits for all tasks in its `depends_on` list. `run_if` decides if it then runs:
# MAGIC
# MAGIC | `run_if` | Runs when | If not met, the task is |
# MAGIC | --- | --- | --- |
# MAGIC | `ALL_SUCCESS` (default) | "All dependencies have run and succeeded." | Upstream failed |
# MAGIC | `AT_LEAST_ONE_SUCCESS` | at least one dependency succeeded | Upstream failed |
# MAGIC | `NONE_FAILED` | no dependency failed and at least one ran | Upstream failed |
# MAGIC | `ALL_DONE` | all dependencies have run, whatever their result | — |
# MAGIC | `AT_LEAST_ONE_FAILED` | at least one dependency failed (alert / clean-up tasks, E5) | Excluded |
# MAGIC | `ALL_FAILED` | all dependencies failed | Excluded |
# MAGIC
# MAGIC The same page: "Upstream tasks with the Excluded result are treated as successful when evaluating Run if conditions", and tasks marked "Upstream failed" or "Upstream canceled" are treated as failed.
# MAGIC
# MAGIC **If/else** (`condition_task`, *Add branching logic to a job with the If/else task*). It compares `left` and `right` with `op`: `EQUAL_TO`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`. "The `==` and `!=` operators perform string comparison of their operands", so `12.0 == 12` is false; the other four compare **numbers**. A downstream task picks a branch with `"depends_on": [{"task_key": "check_rows", "outcome": "true"}]` (or `"false"`).
# MAGIC
# MAGIC **For each** (`for_each_task`, *Use a For each task to run another task in a loop*). `inputs` is a JSON array, a job parameter reference or a task value reference; the nested `task` runs once per element and reads the element with <code>&#123;&#123;input&#125;&#125;</code> (or <code>&#123;&#123;input.key&#125;&#125;</code> for objects). `concurrency` is how many iterations run at the same time ("The default value is 1"). "You cannot add another `For each` task as the nested task", and downstream tasks can depend on the For each task, not on the nested task.
# MAGIC
# MAGIC | Need | Use |
# MAGIC | --- | --- |
# MAGIC | B after A | `depends_on: [{task_key: A}]` |
# MAGIC | an alert or clean-up only when something failed | `run_if: AT_LEAST_ONE_FAILED` |
# MAGIC | a step that must run even after a failure (write a log) | `run_if: ALL_DONE` |
# MAGIC | one of two paths, decided by data (row count, a flag) | If/else + `outcome` |
# MAGIC | the same task for every practice / file / table | For each |

# COMMAND ----------

# DBTITLE 1,4.3 Concept — Parameters, dynamic value references, task values
# MAGIC %md
# MAGIC ### 4.3 Parameters, dynamic value references and task values
# MAGIC
# MAGIC **Job parameters** (*Configure job parameters*): "key-value pairs that allow you to parameterize jobs with default static or dynamic values". Defined once on the job (`"parameters": [{"name": "run_date", "default": "2026-10-01"}]`). For tasks with key-value parameters (notebooks) "job parameters are pushed down automatically": the notebook reads them with `dbutils.widgets.get("run_date")`. Tasks with a **list** of parameters (Python script) do not get them automatically: reference them, for example <code>["--run_date", "&#123;&#123;job.parameters.run_date&#125;&#125;"]</code>. "If a job parameter and a task parameter share a key, the job parameter takes precedence." Keys "can only contain `_ - .` or alphanumeric characters".
# MAGIC
# MAGIC **Task parameters**: `notebook_task.base_parameters` (a dict of widgets) or `spark_python_task.parameters` (a list of arguments) (*Access parameter values from a task*).
# MAGIC
# MAGIC **Dynamic value references** (*Dynamic value references*) are placeholders in double curly braces that the job fills in when the task starts:
# MAGIC
# MAGIC | Reference | Value |
# MAGIC | --- | --- |
# MAGIC | <code>&#123;&#123;job.id&#125;&#125;</code>, <code>&#123;&#123;job.name&#125;&#125;</code>, <code>&#123;&#123;job.run_id&#125;&#125;</code> | the job and the current job run |
# MAGIC | <code>&#123;&#123;job.repair_count&#125;&#125;</code> | "The number of repair attempts on the current job run." |
# MAGIC | <code>&#123;&#123;job.parameters.run_date&#125;&#125;</code> | the job parameter `run_date` |
# MAGIC | <code>&#123;&#123;job.start_time.iso_date&#125;&#125;</code> | the start date (UTC); also `iso_datetime`, `year`, `timestamp_ms`, … |
# MAGIC | <code>&#123;&#123;job.trigger.type&#125;&#125;</code> | what started the run |
# MAGIC | <code>&#123;&#123;task.name&#125;&#125;</code>, <code>&#123;&#123;task.run_id&#125;&#125;</code> | the current task |
# MAGIC | <code>&#123;&#123;task.execution_count&#125;&#125;</code> | "The number of times the current task was run (including retries and repairs)." |
# MAGIC | <code>&#123;&#123;tasks.ingest.values.row_count&#125;&#125;</code> | the task value `row_count` set by task `ingest` |
# MAGIC | <code>&#123;&#123;tasks.load_labs.result_state&#125;&#125;</code>, <code>&#123;&#123;tasks.load_labs.error_code&#125;&#125;</code> | the state / error of another task |
# MAGIC | <code>&#123;&#123;input&#125;&#125;</code> | the current element inside a For each task |
# MAGIC
# MAGIC A wrong reference is **not always an error**: "Syntax errors, including non-existent dynamic reference values and missing braces, are silently ignored" and "treated as literal strings". Only a reference in a known namespace that is not valid (the page's example: <code>&#123;&#123;job.notebook_url&#125;&#125;</code>) gives an error message. In E7, `jobs/create` rejected a typo inside the known namespace `tasks` (`value` instead of `values`). Old references such as <code>&#123;&#123;run_id&#125;&#125;</code> or <code>&#123;&#123;job_id&#125;&#125;</code> are deprecated.
# MAGIC
# MAGIC **Task values** (*Use task values to pass information between tasks*): a task writes `dbutils.jobs.taskValues.set(key="row_count", value=12)`; a later task reads it with the reference <code>&#123;&#123;tasks.ingest.values.row_count&#125;&#125;</code> in its parameters (recommended), or in code with `dbutils.jobs.taskValues.get(taskKey="ingest", key="row_count", debugValue=0)`. Values must be JSON; "The size of the JSON representation of the value cannot exceed 48 KiB." `debugValue` is the value returned when you run the notebook by hand, outside a job.
# MAGIC
# MAGIC | | Job parameter | Task parameter | Task value |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Set by | the job definition or the person / trigger that starts the run | the task definition | **code** in an upstream task, during the run |
# MAGIC | Typical use | run date, environment, table name | a constant for one task | a row count, a list of files, a status found at run time |
# MAGIC | Override at start | yes (run-now, repair) | only through the job definition | no |

# COMMAND ----------

# DBTITLE 1,4.4 Concept — Triggers
# MAGIC %md
# MAGIC ### 4.4 Triggers (*Automate jobs with schedules and triggers*)
# MAGIC
# MAGIC | Trigger | JSON | "Triggers a job run …" |
# MAGIC | --- | --- | --- |
# MAGIC | None (manual) | — | "Runs are triggered manually with the Run now button or programmatically" (this lesson: `run-now`) |
# MAGIC | Scheduled | `"schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "America/Chicago", "pause_status": "PAUSED"}` | "based on a time-based schedule" |
# MAGIC | File arrival | `"trigger": {"file_arrival": {"url": "/Volumes/…/incoming/"}}` | "when new files arrive in a monitored Unity Catalog storage location" |
# MAGIC | Table update | `"trigger": {"table_update": {"table_names": […], "condition": "ANY_UPDATED"}}` | "when source tables are updated" |
# MAGIC | Continuous | `"continuous": {"pause_status": "UNPAUSED"}` | "To keep the job always running, trigger another job run whenever a job run completes or fails." |
# MAGIC
# MAGIC * File arrival (*Trigger jobs when new files arrive*): a UC volume or external location; Databricks makes a "best effort to check for new files every minute"; options "Minimum time between triggers in seconds" and "Wait after last change in seconds"; without file events at most 50 such jobs per workspace and 10,000 files in the location. "Only new files start runs."
# MAGIC * Table update (*Trigger jobs when source tables are updated*): "You can select up to 10 managed or Delta tables per trigger"; <code>&#123;&#123;job.trigger.table_update.updated_tables&#125;&#125;</code> lists the changed tables.
# MAGIC * Every trigger can be **paused** (`pause_status: PAUSED`): no new runs, but run-now still works. E7 creates paused triggers only, so nothing starts by itself.
# MAGIC * NOT VERIFIED — concept only (compute budget): a trigger that really starts a run, and continuous jobs (a continuous job keeps compute busy all the time).

# COMMAND ----------

# DBTITLE 1,4.5 Concept — Retries, timeouts, concurrency, notifications
# MAGIC %md
# MAGIC ### 4.5 Retries, timeouts, concurrency and notifications
# MAGIC
# MAGIC | Setting | Level | JSON (Jobs API) | Meaning |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Retries | task | `max_retries`, `min_retry_interval_millis`, `retry_on_timeout` | "`-1` means to retry indefinitely and the value `0` means to never retry" (Jobs API reference). "By default, most job configurations don't retry failed tasks" (*Configure and edit tasks in Lakeflow Jobs*) |
# MAGIC | Timeout | task and job | `timeout_seconds` | "If the task does not complete in this time, Databricks sets its status to "Timed Out""; with retries, "the timeout applies to each retry" |
# MAGIC | Duration warning | task and job | `health: {rules: [{metric: RUN_DURATION_SECONDS, op: GREATER_THAN, value: 600}]}` | an expected duration; a notification `on_duration_warning_threshold_exceeded` fires when it is passed |
# MAGIC | Job concurrency | job | `max_concurrent_runs` | how many runs of the same job may run at once (this lesson: 1) |
# MAGIC | Queueing | job | `queue: {enabled: true}` | a run waits instead of being skipped when a limit is reached |
# MAGIC | For each concurrency | task | `for_each_task.concurrency` | iterations at the same time (default 1) |
# MAGIC
# MAGIC **Notifications** (*Add notifications on a job*): on start, success, failure, duration warning and streaming backlog; destinations are email addresses or system destinations (Slack, Microsoft Teams, PagerDuty, webhooks) that an admin sets up, "a maximum of three system destinations for each notification event type". "Job-level notifications aren't sent when failed tasks are retried" — use task notifications for that. "Mute notifications for skipped runs / canceled runs" = `notification_settings.no_alert_for_skipped_runs` / `no_alert_for_canceled_runs`.
# MAGIC
# MAGIC ```json
# MAGIC "email_notifications": {"on_failure": ["data-oncall@example.com"], "on_duration_warning_threshold_exceeded": ["data-oncall@example.com"]},
# MAGIC "notification_settings": {"no_alert_for_skipped_runs": true, "no_alert_for_canceled_runs": true}
# MAGIC ```
# MAGIC NOT VERIFIED — concept only (no real destination on this workspace): a notification that is really sent. P3 only stores a notification setting in a job that never runs (`example.com` is a reserved test domain).

# COMMAND ----------

# DBTITLE 1,4.6 Concept — Serverless jobs
# MAGIC %md
# MAGIC ### 4.6 Serverless jobs: auto-optimization, performance mode, environments
# MAGIC
# MAGIC *Run your Lakeflow Jobs with serverless compute for workflows*:
# MAGIC
# MAGIC * **Auto-optimization** "automatically optimizes the compute used to run your jobs and retries failed tasks". It is on by default. UI: clear "Enable serverless auto-optimization (may include additional retries)" in the task's retry policy. API: the task field `"disable_auto_optimization": true` ("An option to disable auto optimization in serverless", Jobs API reference). **Every task of this lesson sets it**: E5 fails on purpose, and an automatic retry would only cost compute. Switch it off for tasks that are not idempotent (a retry could load data twice).
# MAGIC * **Performance mode**: job field `performance_target`. Performance optimized: jobs "start and run faster". Standard: uses less compute (fewer DBUs, same SKU) with a startup latency of "4 to 6 minutes". The lesson jobs set `PERFORMANCE_OPTIMIZED` so that the notebook does not wait minutes for each run.
# MAGIC * **Environments**: "Notebook tasks default to Notebook Environment, which uses the notebook's own configured base environment and dependencies" (*Configure the serverless environment*). A Python script, wheel or dbt task needs an `environment_key` that points to an entry in the job's `environments` list (Jobs API reference):
# MAGIC ```json
# MAGIC "environments": [{"environment_key": "script_env", "spec": {"environment_version": "4", "dependencies": ["pyyaml==6.0.2"]}}]
# MAGIC ```
# MAGIC `environment_version` picks Python and the preinstalled libraries (the *Serverless environment versions* page lists versions 1–6 on 2026-10-18; version 4 has Python 3.12.3); `dependencies` are pip requirement lines. The lesson job uses version 4 **without** extra dependencies (NOT VERIFIED — concept only: installing a package, to save compute).

# COMMAND ----------

# DBTITLE 1,4.7 Concept — Repair and run with different parameters
# MAGIC %md
# MAGIC ### 4.7 Repair a run, run with different parameters
# MAGIC
# MAGIC **Repair** (*Troubleshoot and repair job failures*): re-runs the unsuccessful tasks of a failed or canceled run — "tasks that failed and tasks skipped because an upstream dependency failed" — and their dependents, **in the same job run**. "successful tasks and any tasks that depend on them are not re-run." Repaired tasks "use the current job and task settings" (you can fix the job first), and you can change parameters in the repair. Repair is for jobs with two or more tasks; for a single task use Run now. "Repairs re-run tasks from the beginning and are not idempotent": write with overwrite / `MERGE` / `REPLACE WHERE`, not blind appends.
# MAGIC
# MAGIC API (`POST /api/2.2/jobs/runs/repair`, Jobs API reference):
# MAGIC
# MAGIC | Field | Meaning |
# MAGIC | --- | --- |
# MAGIC | `rerun_all_failed_tasks: true` | "repair all failed tasks" |
# MAGIC | `rerun_tasks: ["load_labs"]` | exactly these tasks ("Only one of `rerun_tasks` or `rerun_all_failed_tasks` can be used") |
# MAGIC | `rerun_dependent_tasks: true` | also the tasks that depend on them, "even if they were previously successful" |
# MAGIC | `latest_repair_id` | "must be provided on subsequent requests to repair the same run" |
# MAGIC | `job_parameters` | new parameter values for the repaired tasks (P4) |
# MAGIC
# MAGIC **Run now with different parameters**: `POST /api/2.2/jobs/run-now` with `"job_parameters": {"run_date": "2026-10-05"}` starts a **new** run with these values; the job definition does not change (E4).
# MAGIC
# MAGIC | Situation | Use |
# MAGIC | --- | --- |
# MAGIC | task 4 of 5 failed after an hour; tasks 1–3 are fine | **repair** (tasks 1–3 are not run again) |
# MAGIC | the same job for another day | **run now** with other `job_parameters` |
# MAGIC | a one-time run of code without saving a job | `runs/submit` (this is how the lesson itself is tested) |

# COMMAND ----------

# DBTITLE 1,4.8 Concept — API, CLI, SDK, bundles
# MAGIC %md
# MAGIC ### 4.8 Four ways to do the same thing
# MAGIC
# MAGIC | Step | REST (this notebook) | CLI (text only here) | Python SDK |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Create | `POST /api/2.2/jobs/create` | `databricks jobs create --json @job.json` | `w.jobs.create(name=…, tasks=[…])` |
# MAGIC | Start | `POST /api/2.2/jobs/run-now` | `databricks jobs run-now <job-id> --json '{"job_parameters": {…}}'` | `w.jobs.run_now(job_id, job_parameters={…})` |
# MAGIC | Wait / read | `GET /api/2.2/jobs/runs/get?run_id=…&include_history=true` | `databricks jobs get-run <run-id>` | `w.jobs.get_run(run_id)` |
# MAGIC | Output of a task | `GET /api/2.2/jobs/runs/get-output?run_id=<task run id>` | `databricks jobs get-run-output <task-run-id>` | `w.jobs.get_run_output(run_id)` |
# MAGIC | Repair | `POST /api/2.2/jobs/runs/repair` | `databricks jobs repair-run <run-id> --rerun-all-failed-tasks` | `w.jobs.repair_run(run_id, rerun_all_failed_tasks=True)` |
# MAGIC | History | `GET /api/2.2/jobs/runs/list?job_id=…` | `databricks jobs list-runs --job-id <id> --completed-only` | `w.jobs.list_runs(job_id=…)` |
# MAGIC | Export | `GET /api/2.2/jobs/get?job_id=…` | `databricks jobs get <id>` · `databricks bundle generate job --existing-job-id <id>` | `w.jobs.get(job_id)` |
# MAGIC | Delete | `POST /api/2.2/jobs/delete` | `databricks jobs delete <id>` | `w.jobs.delete(job_id)` |
# MAGIC
# MAGIC The CLI commands and flags above are from `databricks jobs … --help` of CLI v1.21.0. Without `--no-wait`, `run-now` and `repair-run` wait for the end of the run. In production, keep the job JSON in git as a **Declarative Automation Bundle** (formerly Databricks Asset Bundles; L21): `bundle generate job` turns an existing job into bundle YAML.
# MAGIC
# MAGIC **Why REST here?** The SDK preinstalled on serverless job compute is `{{nd.setup.sdk_version}}` in this run (L07 found that its typed calls lack newer fields). The JSON you send to the REST API is the same JSON the CLI takes, and very close to bundle YAML, so it is the most useful form to learn.

# COMMAND ----------

# DBTITLE 1,4.9 Job helpers (run first)
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}         # life cycle states of a run that has ended

def nb(name):
    """Workspace path of a task notebook in RUNTIME."""
    return f"{RUNTIME}/{name}"

def write_notebook(name, code):
    """Save Python code as a notebook (SOURCE format) in RUNTIME; return its path."""
    source = "# Databricks notebook source\n" + code
    api("POST", "/api/2.0/workspace/import", body={
        "path": nb(name), "format": "SOURCE", "language": "PYTHON", "overwrite": True,
        "content": base64.b64encode(source.encode("utf-8")).decode("ascii")})
    return nb(name)

def write_file(name, text):
    """Save a plain workspace file (for example a .py script) in RUNTIME; return its /Workspace path."""
    w.workspace.upload(f"{RUNTIME}/{name}", io.BytesIO(text.encode("utf-8")), format=ImportFormat.AUTO, overwrite=True)
    return f"/Workspace{RUNTIME}/{name}"

def create_job(settings):
    """Create a job (POST /api/2.2/jobs/create) and return its id. A lesson job with the same name is deleted first."""
    for _, job_id in lesson_jobs(settings["name"]):
        delete_job(job_id)
    return api("POST", "/api/2.2/jobs/create", body=settings)["job_id"]

def get_job(job_id):
    """The job definition (GET /api/2.2/jobs/get)."""
    return api("GET", "/api/2.2/jobs/get", query={"job_id": job_id})

def start_job(job_id, job_parameters=None):
    """Start a run (POST /api/2.2/jobs/run-now), optionally with other job parameters; return the run id."""
    body = {"job_id": job_id, **({"job_parameters": job_parameters} if job_parameters else {})}
    return api("POST", "/api/2.2/jobs/run-now", body=body)["run_id"]

def get_run(run_id):
    """A run with its tasks, its repair history and the resolved parameter values (GET /api/2.2/jobs/runs/get)."""
    return api("GET", "/api/2.2/jobs/runs/get",
               query={"run_id": run_id, "include_history": "true", "include_resolved_values": "true"})

def life_cycle(obj):
    """Life cycle state of a run, task run or repair history item."""
    return (obj.get("state") or {}).get("life_cycle_state") or (obj.get("status") or {}).get("state")

def result_state(obj):
    """Result of a run, task run or repair history item (SUCCESS, FAILED, UPSTREAM_FAILED, EXCLUDED, ...)."""
    return (obj.get("state") or {}).get("result_state") or ((obj.get("status") or {}).get("termination_details") or {}).get("code")

def wait_run(run_id, repair_id=None, timeout_s=1500):
    """Wait until the run (or one repair of it) has ended; return the run. Cancels the run after timeout_s."""
    started = time.time()
    while True:
        run = get_run(run_id)
        item = run if repair_id is None else next((h for h in run.get("repair_history", []) if h.get("id") == repair_id), None)
        if item is not None and life_cycle(item) in TERMINAL and life_cycle(run) in TERMINAL:
            print(f"run {run_id}{' repair ' + str(repair_id) if repair_id else ''}: {result_state(item)} "
                  f"after {round(time.time() - started)} s")
            return run
        if time.time() - started > timeout_s:
            api("POST", "/api/2.2/jobs/runs/cancel", body={"run_id": run_id})   # do not leave compute running
            raise TimeoutError(f"run {run_id} still {life_cycle(run)} after {timeout_s} s")
        time.sleep(10)                                         # check again in 10 seconds

def repair(run_id, **fields):
    """Repair a finished run (POST /api/2.2/jobs/runs/repair); return the repair id."""
    return api("POST", "/api/2.2/jobs/runs/repair", body={"run_id": run_id, **fields})["repair_id"]

def task(run, task_key):
    """The latest task run of one task in a run (a dict from runs/get)."""
    found = [t for t in run.get("tasks", []) if t["task_key"] == task_key]
    return max(found, key=lambda t: t.get("start_time", 0)) if found else {}

def task_output(task_run_id):
    """Output of one task run (GET /api/2.2/jobs/runs/get-output): notebook result, error, logs."""
    return api("GET", "/api/2.2/jobs/runs/get-output", query={"run_id": task_run_id})

def task_type(t):
    """The kind of a task from its JSON fields."""
    return next((k for k in ("notebook_task", "spark_python_task", "condition_task", "for_each_task") if k in t), "other")

def task_table(run):
    """One row per task run: task, type, result, If/else outcome, seconds (DataFrame for display)."""
    rows = [(t["task_key"], task_type(t), result_state(t) or life_cycle(t), (t.get("condition_task") or {}).get("outcome"),
             round((t.get("run_duration") or (t.get("end_time", 0) - t.get("start_time", 0)) or 0) / 1000))
            for t in sorted(run.get("tasks", []), key=lambda t: (t.get("start_time", 0), t["task_key"]))]
    return spark.createDataFrame(rows, "task_key STRING, type STRING, result STRING, outcome STRING, seconds BIGINT")

def states_text(run):
    """'task: RESULT' for every task run of a run, in task order (outcome added for If/else tasks)."""
    parts = []
    for t in sorted(run.get("tasks", []), key=lambda t: (t["task_key"], t.get("start_time", 0))):
        outcome = (t.get("condition_task") or {}).get("outcome")
        parts.append(f"{t['task_key']}: {result_state(t) or life_cycle(t)}{' (' + outcome + ')' if outcome else ''}")
    return "; ".join(parts)

def error_classes(text):
    """All [ERROR_CLASS] names in a text, in order, without duplicates."""
    return list(dict.fromkeys(re.findall(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", text or "")))

def api_error(e):
    """The error code of a failed REST call (for example INVALID_PARAMETER_VALUE)."""
    return getattr(e, "error_code", None) or type(e).__name__

def no_braces(text):
    """Text without double curly braces (so it can be shown in markdown)."""
    return str(text).replace("{{", "{ {").replace("}}", "} }")

print("Helpers ready: write_notebook, write_file, create_job, get_job, start_job, get_run, wait_run, repair, task, "
      "task_output, task_table, states_text, delete_job, lesson_jobs")

# COMMAND ----------

# DBTITLE 1,4.9 Concept — How this notebook runs jobs
# MAGIC %md
# MAGIC ### 4.9 How this notebook runs jobs
# MAGIC
# MAGIC 1. The next cell writes the **task code** (seven small notebooks and one Python script) into the workspace folder `RUNTIME` (`/Users/<you>/_lesson_runtime/L09`). Read them: they are what the job tasks run.
# MAGIC 2. Each example builds the job definition as a Python dict (the same JSON as in the UI's "View as JSON", the CLI and bundles) and creates the job with `create_job`.
# MAGIC 3. `start_job` starts a run, `wait_run` checks the run every 10 s until it has ended, and `task_table` / `task_output` show what each task did.
# MAGIC 4. Cleanup (section 12) deletes all lesson jobs, the folder and the schema.
# MAGIC
# MAGIC Every job of the lesson: serverless (no cluster settings), `performance_target: PERFORMANCE_OPTIMIZED`, `max_concurrent_runs: 1`, `queue.enabled: true`, `disable_auto_optimization: true` on every task with code, and at most two tasks with code running at the same time.

# COMMAND ----------

# DBTITLE 1,4.9 Task code (run first)
# The notebooks below run as job tasks. Parameters arrive as widgets; task values go to the next tasks.
INGEST = '''
# Task "ingest": copy one day of encounters into encounters_daily and report what it found
from datetime import date
run_date = date.fromisoformat(dbutils.widgets.get("run_date"))     # the job parameter run_date (pushed down as a widget)
spark.sql(f"""
    INSERT INTO main.dbacademy_l09.encounters_daily REPLACE WHERE visit_date = DATE'{run_date}'
    SELECT * FROM main.dbacademy_l09.encounters_raw WHERE visit_date = DATE'{run_date}'
""")                                                              # idempotent: a repair or rerun replaces the same day
day = spark.table("main.dbacademy_l09.encounters_daily").where(f"visit_date = DATE'{run_date}'")
row_count = day.count()
practices = sorted(r["practice_id"] for r in day.select("practice_id").distinct().collect())
dbutils.jobs.taskValues.set(key="row_count", value=row_count)      # read later as {{tasks.ingest.values.row_count}}
dbutils.jobs.taskValues.set(key="practices", value=practices)      # a JSON list: the inputs of the For each task
print(f"{run_date}: {row_count} encounters, practices {practices}")
'''

BUILD_SUMMARY = '''
# Task "build_summary": one summary row for the day, with the values this task received
import json
from datetime import date
from pyspark.sql import functions as F
run_date = date.fromisoformat(dbutils.widgets.get("run_date"))     # job parameter
rows_reported = dbutils.widgets.get("rows_reported")              # base parameter {{tasks.ingest.values.row_count}}
job_run_id = dbutils.widgets.get("job_run_id")                    # base parameter {{job.run_id}}
task_name = dbutils.widgets.get("task_name")                      # base parameter {{task.name}}
rows_from_get = dbutils.jobs.taskValues.get(taskKey="ingest", key="row_count", default=-1, debugValue=-1)  # read in code
summary = (spark.table("main.dbacademy_l09.encounters_daily").where(F.col("visit_date") == F.lit(run_date))
           .agg(F.count("*").alias("encounters"), F.sum("cost").cast("decimal(12,2)").alias("total_cost"))
           .select(F.lit(run_date).alias("run_date"), "encounters", "total_cost",
                   F.lit(rows_reported).alias("rows_reported"), F.lit(int(rows_from_get)).cast("bigint").alias("rows_from_get"),
                   F.lit(job_run_id).alias("job_run_id"), F.lit(task_name).alias("task_name")))
summary.write.mode("append").saveAsTable("main.dbacademy_l09.daily_summary")
row = summary.first()
dbutils.notebook.exit(json.dumps({"encounters": row["encounters"], "total_cost": str(row["total_cost"])}))  # the run output
'''

PRACTICE_REPORT = '''
# Nested task of the For each task: one row per practice
from datetime import date
from pyspark.sql import functions as F
practice_id = dbutils.widgets.get("practice_id")                  # base parameter {{input}}: the current element
run_date = date.fromisoformat(dbutils.widgets.get("run_date"))    # job parameter
task_name = dbutils.widgets.get("task_name")                      # base parameter {{task.name}}
(spark.table("main.dbacademy_l09.encounters_daily")
 .where((F.col("visit_date") == F.lit(run_date)) & (F.col("practice_id") == practice_id))
 .agg(F.count("*").alias("encounters"), F.sum("cost").cast("decimal(12,2)").alias("total_cost"))
 .select(F.lit(practice_id).alias("practice_id"), F.lit(run_date).alias("run_date"), "encounters", "total_cost",
         F.lit(task_name).alias("task_name"))
 .write.mode("append").saveAsTable("main.dbacademy_l09.practice_report"))
'''

LOG_NO_DATA_PY = '''
# Python script task "log_no_data": parameters arrive as command-line arguments (no widgets, no notebook)
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--run_date", required=True)
args, _ = parser.parse_known_args()
print(f"No encounters for {args.run_date}: nothing to summarize.")
'''

LOAD_LABS = '''
# Task "load_labs" (E5): rebuild lab_results from the lab feed; fails while the feed table does not exist
spark.sql("CREATE OR REPLACE TABLE main.dbacademy_l09.lab_results AS SELECT * FROM main.dbacademy_l09.lab_feed")
lab_rows = spark.table("main.dbacademy_l09.lab_results").count()
dbutils.jobs.taskValues.set(key="lab_rows", value=lab_rows)
print(f"lab_results: {lab_rows} rows")
'''

REPORT_LABS = '''
# Task "report_labs" (E5): count the loaded labs and record the repair references it received
import json
row_count = spark.table("main.dbacademy_l09.lab_results").count()
(spark.createDataFrame([(row_count, dbutils.widgets.get("repair_count"), dbutils.widgets.get("execution_count"),
                         dbutils.widgets.get("load_execution_count"))],
                       "lab_rows BIGINT, repair_count STRING, report_execution_count STRING, load_execution_count STRING")
 .write.mode("append").saveAsTable("main.dbacademy_l09.lab_report"))
dbutils.notebook.exit(json.dumps({"lab_rows": row_count}))
'''

COUNT_TABLE = '''
# Task "count_table": count the rows of the table in the job parameter table_name (fails if it does not exist)
from pyspark.sql import functions as F
table_name = dbutils.widgets.get("table_name")
row_count = spark.table(table_name).count()
(spark.createDataFrame([(table_name, row_count)], "table_name STRING, row_count BIGINT")
 .withColumn("counted_at", F.current_timestamp())
 .write.mode("append").saveAsTable("main.dbacademy_l09.table_counts"))
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
'''

WRITE_LOG = '''
# Task "write_log": append one line to run_log (an audit or alert step)
from pyspark.sql import functions as F
dbutils.widgets.text("event", "")
dbutils.widgets.text("detail", "")
(spark.createDataFrame([(dbutils.widgets.get("event"), dbutils.widgets.get("detail"))], "event STRING, detail STRING")
 .withColumn("logged_at", F.current_timestamp())
 .write.mode("append").saveAsTable("main.dbacademy_l09.run_log"))
'''

for name, code in {"ingest": INGEST, "build_summary": BUILD_SUMMARY, "practice_report": PRACTICE_REPORT,
                   "load_labs": LOAD_LABS, "report_labs": REPORT_LABS, "count_table": COUNT_TABLE,
                   "write_log": WRITE_LOG}.items():
    print("notebook", write_notebook(name, code))           # save each task notebook in the workspace
LOG_NO_DATA = write_file("log_no_data.py", LOG_NO_DATA_PY)  # a plain .py file for the Python script task
print("file    ", LOG_NO_DATA)
runtime_items = sorted(o.path.rsplit("/", 1)[-1] for o in w.workspace.list(RUNTIME))
METRICS["setup.runtime_items"] = ", ".join(runtime_items)
assert len(runtime_items) == 8, runtime_items

# COMMAND ----------

# DBTITLE 1,5 — Examples (intro)
# MAGIC %md
# MAGIC ## 5 Examples
# MAGIC
# MAGIC Each example: **Predict** (answer before you run) → **code** (every line commented) → **Explained** (real values from the run).
# MAGIC
# MAGIC | Example | Job | Runs |
# MAGIC | --- | --- | --- |
# MAGIC | E1 a multi-task job | `dbacademy_l09_daily_encounters` | run 1 (default parameters) |
# MAGIC | E2 task values and dynamic value references | same | reads run 1 |
# MAGIC | E3 For each over practices | same | reads run 1 |
# MAGIC | E4 If/else + run with different parameters | same | run 2 (`run_date` = 2026-10-05) |
# MAGIC | E5 a failure, an alert task and a repair | `dbacademy_l09_lab_repair` | run 3 + one repair |
# MAGIC | E6 run history and durations | both | no new run |
# MAGIC | E7 export a job as JSON; mistakes the API rejects; triggers | definitions only | no run |

# COMMAND ----------

# DBTITLE 1,E1 Predict
# MAGIC %md
# MAGIC ### E1 — A multi-task job: ingest → check → summary and For each
# MAGIC
# MAGIC The job `dbacademy_l09_daily_encounters` has five tasks (DAG below). Run 1 uses the default `run_date` 2026-10-01 (12 encounters).
# MAGIC
# MAGIC ```
# MAGIC ingest (notebook) ──► check_rows (If/else: row_count > 0)
# MAGIC                          ├─ true  ──► build_summary (notebook) ──► per_practice (For each: one notebook run per practice)
# MAGIC                          └─ false ──► log_no_data (Python script)
# MAGIC ```
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which tasks run in run 1, and which result does `log_no_data` get?
# MAGIC 2. `check_rows` has no code. Does it get a task run and a result?
# MAGIC 3. Is the whole run `SUCCESS` although one task did not run?

# COMMAND ----------

# DBTITLE 1,E1 Code — the job definition
DAILY_JOB = {
    "name": f"{PREFIX}_daily_encounters",
    "description": "L09: daily encounter load for fake practices (lesson job, deleted in Cleanup)",
    "tags": {"lesson": "L09", "data": "fake"},
    "parameters": [{"name": "run_date", "default": "2026-10-01"}],   # job parameter, pushed down to notebook tasks
    "max_concurrent_runs": 1,                                         # one run of this job at a time
    "queue": {"enabled": True},                                       # wait instead of skip when a limit is reached
    "performance_target": "PERFORMANCE_OPTIMIZED",                    # serverless performance mode
    "timeout_seconds": 1800,                                          # the whole run
    "schedule": {"quartz_cron_expression": "0 0 6 * * ?",            # every day at 06:00 ...
                 "timezone_id": "America/Chicago", "pause_status": "PAUSED"},   # ... but paused: only run-now starts it
    "environments": [{"environment_key": "script_env",                # environment for the Python script task
                      "spec": {"environment_version": "4"}}],
    "tasks": [
        {"task_key": "ingest",
         "notebook_task": {"notebook_path": nb("ingest"), "source": "WORKSPACE"},
         "disable_auto_optimization": True, "max_retries": 0, "timeout_seconds": 900},
        {"task_key": "check_rows",                                    # If/else: compares two values, no code
         "depends_on": [{"task_key": "ingest"}],
         "condition_task": {"op": "GREATER_THAN",                     # numeric comparison
                            "left": "{{tasks.ingest.values.row_count}}", "right": "0"},
         "disable_auto_optimization": True},                           # set on every task (see E7)
        {"task_key": "build_summary",
         "depends_on": [{"task_key": "check_rows", "outcome": "true"}],   # only on the true branch
         "notebook_task": {"notebook_path": nb("build_summary"), "source": "WORKSPACE",
                           "base_parameters": {"rows_reported": "{{tasks.ingest.values.row_count}}",   # a task value
                                               "job_run_id": "{{job.run_id}}",                         # the job run
                                               "task_name": "{{task.name}}"}},                         # this task
         "disable_auto_optimization": True, "max_retries": 0, "timeout_seconds": 900},
        {"task_key": "per_practice",                                  # For each: one nested run per practice
         "depends_on": [{"task_key": "check_rows", "outcome": "true"}, {"task_key": "build_summary"}],
         "disable_auto_optimization": True,
         "for_each_task": {
             "inputs": "{{tasks.ingest.values.practices}}",            # the JSON list set by ingest
             "concurrency": 2,                                         # at most two iterations at the same time
             "task": {"task_key": "per_practice_iteration",
                      "notebook_task": {"notebook_path": nb("practice_report"), "source": "WORKSPACE",
                                        "base_parameters": {"practice_id": "{{input}}", "task_name": "{{task.name}}"}},
                      "disable_auto_optimization": True, "max_retries": 0, "timeout_seconds": 900}}},
        {"task_key": "log_no_data",
         "depends_on": [{"task_key": "check_rows", "outcome": "false"}],  # only on the false branch
         "spark_python_task": {"python_file": LOG_NO_DATA, "source": "WORKSPACE",
                               "parameters": ["--run_date", "{{job.parameters.run_date}}"]},  # no automatic pushdown
         "environment_key": "script_env",                              # required for a script on serverless
         "disable_auto_optimization": True, "max_retries": 0, "timeout_seconds": 600},
    ],
}
print(json.dumps(DAILY_JOB, indent=2)[:1500], "...")

# COMMAND ----------

# DBTITLE 1,E1 Code — create the job and run it
daily_job_id = create_job(DAILY_JOB)                          # POST /api/2.2/jobs/create
print("job id:", daily_job_id)
run1_id = start_job(daily_job_id)                             # run 1 with the default parameters
run1 = wait_run(run1_id)                                      # check every 10 s until the run has ended
display(task_table(run1))                                     # one row per task run
run1_states = states_text(run1)
check1 = task(run1, "check_rows")
METRICS["E1.result"] = result_state(run1)
METRICS["E1.states"] = run1_states
METRICS["E1.task_count"] = len(run1.get("tasks", []))
METRICS["E1.check_outcome"] = (check1.get("condition_task") or {}).get("outcome")
METRICS["E1.check_result"] = result_state(check1)
METRICS["nd.E1.run_id"] = run1_id
METRICS["nd.E1.job_id"] = daily_job_id
METRICS["E1.performance_target"] = run1.get("effective_performance_target")
METRICS["E1.trigger"] = run1.get("trigger")
assert result_state(run1) == "SUCCESS", run1_states
assert [result_state(task(run1, k)) for k in ("ingest", "build_summary", "per_practice")] == ["SUCCESS"] * 3, run1_states
assert (check1.get("condition_task") or {}).get("outcome") == "true", run1_states

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Run 1 ended with **`{{E1.result}}`**. Task runs (`task: result`): `{{E1.states}}` — {{E1.task_count}} entries in the run's `tasks` list.
# MAGIC * `check_rows` is a real task run with the result `{{E1.check_result}}` and the outcome **`{{E1.check_outcome}}`**: 12 encounters > 0. It runs no code of yours.
# MAGIC * `log_no_data` sits on the false branch, so it did not run (see its result above). The run is still `{{E1.result}}`: a branch that is not taken is not a failure.
# MAGIC * The run says how it was started (`trigger`): `{{E1.trigger}}` (run-now), and which serverless performance mode it got: `{{E1.performance_target}}`.
# MAGIC * The schedule is `PAUSED`, so the job never starts by itself; run-now works anyway.
# MAGIC
# MAGIC > **Key takeaway:** a job is JSON: tasks, `depends_on`, parameters. Branches (If/else) and loops (For each) are tasks too. You read results per task, not only per run.

# COMMAND ----------

# DBTITLE 1,E2 Predict
# MAGIC %md
# MAGIC ### E2 — Task values and dynamic value references
# MAGIC
# MAGIC `ingest` set two task values: `row_count` (a number) and `practices` (a list). `build_summary` received three base parameters: <code>&#123;&#123;tasks.ingest.values.row_count&#125;&#125;</code>, <code>&#123;&#123;job.run_id&#125;&#125;</code> and <code>&#123;&#123;task.name&#125;&#125;</code>, and also read `row_count` in code with `dbutils.jobs.taskValues.get`. It wrote what it received into `daily_summary` and returned a JSON string with `dbutils.notebook.exit`.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which type does the widget `rows_reported` have in the notebook: a number or a string?
# MAGIC 2. Is <code>&#123;&#123;job.run_id&#125;&#125;</code> the run id that `run-now` returned, or the id of the task run?
# MAGIC 3. Where can you read the value given to `dbutils.notebook.exit`?

# COMMAND ----------

# DBTITLE 1,E2 Code
summary = spark.table(f"{FQ}.daily_summary")                  # what build_summary received and computed
display(summary)
s = summary.first()
summary_task = task(run1, "build_summary")                    # the task run of build_summary in run 1
summary_output = task_output(summary_task["run_id"])          # runs/get-output needs the TASK run id
notebook_result = (summary_output.get("notebook_output") or {}).get("result")
print("notebook exit value:", notebook_result)
resolved = check1.get("resolved_values") or {}                # include_resolved_values=true: references filled in
resolved_summary = (summary_task.get("resolved_values") or {}).get("notebook_task", {}).get("base_parameters", {})
print("check_rows resolved:", resolved.get("condition_task"))
print("build_summary resolved:", resolved_summary)
METRICS["E2.encounters"] = s["encounters"]
METRICS["E2.total_cost"] = str(s["total_cost"])
METRICS["E2.rows_reported"] = s["rows_reported"]
METRICS["E2.rows_reported_type"] = type(s["rows_reported"]).__name__
METRICS["E2.rows_from_get"] = s["rows_from_get"]
METRICS["E2.job_run_id_is_run_id"] = s["job_run_id"] == str(run1_id)
METRICS["E2.job_run_id_is_task_run_id"] = s["job_run_id"] == str(summary_task["run_id"])
METRICS["E2.task_name"] = s["task_name"]
METRICS["E2.notebook_result"] = notebook_result
METRICS["E2.resolved_condition"] = no_braces(json.dumps(resolved.get("condition_task"), sort_keys=True))
METRICS["E2.resolved_has_values"] = bool(resolved_summary)
assert (s["encounters"], s["rows_reported"], s["rows_from_get"]) == (12, "12", 12)

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `daily_summary`: {{E2.encounters}} encounters, total cost {{E2.total_cost}}. The reference <code>&#123;&#123;tasks.ingest.values.row_count&#125;&#125;</code> arrived as `{{E2.rows_reported}}` (Python type `{{E2.rows_reported_type}}`: widgets are always strings); `taskValues.get` in code returned {{E2.rows_from_get}}.
# MAGIC * <code>&#123;&#123;job.run_id&#125;&#125;</code> = the run id from `run-now`: **{{E2.job_run_id_is_run_id}}**; = the task run id of `build_summary`: {{E2.job_run_id_is_task_run_id}}. Use <code>&#123;&#123;task.run_id&#125;&#125;</code> for the task run.
# MAGIC * <code>&#123;&#123;task.name&#125;&#125;</code> became `{{E2.task_name}}` (the `task_key`).
# MAGIC * The value of `dbutils.notebook.exit` is the task's **run output**: `runs/get-output` with the **task** run id returned `{{E2.notebook_result}}`.
# MAGIC * `runs/get?include_resolved_values=true` should add the resolved parameter values to each task run (Jobs API reference: "Whether to include resolved parameter values in the response"). In this run it returned `{{E2.resolved_condition}}` for `check_rows`, and resolved base parameters for `build_summary` were found: {{E2.resolved_has_values}}. So the reliable check is what the task itself received and wrote down (the `daily_summary` row above).
# MAGIC
# MAGIC > **Key takeaway:** pass small run-time facts (a count, a list, a status) between tasks with task values, and prefer the reference <code>&#123;&#123;tasks.<task>.values.<key>&#125;&#125;</code> in the task parameters over `taskValues.get` in code (*Use task values to pass information between tasks*). Everything arrives in a notebook as a **string**.

# COMMAND ----------

# DBTITLE 1,E3 Predict
# MAGIC %md
# MAGIC ### E3 — For each over practices
# MAGIC
# MAGIC `per_practice` loops over <code>&#123;&#123;tasks.ingest.values.practices&#125;&#125;</code> with `concurrency` 2. The nested task `per_practice_iteration` gets the current practice as <code>&#123;&#123;input&#125;&#125;</code>.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. How many nested runs: one per practice, or one per encounter?
# MAGIC 2. Can a task downstream of the loop depend directly on `per_practice_iteration`?
# MAGIC 3. What does <code>&#123;&#123;task.name&#125;&#125;</code> give inside the loop?

# COMMAND ----------

# DBTITLE 1,E3 Code
loop = task(run1, "per_practice")                             # the For each task run in run 1
stats = ((loop.get("for_each_task") or {}).get("stats") or {}).get("task_run_stats") or {}
print("iteration stats:", stats)
try:
    loop_run = get_run(loop["run_id"])                        # runs/get on the For each task run lists its iterations
    iterations, loop_error = loop_run.get("iterations", []), None
except Exception as e:
    iterations, loop_error = [], api_error(e)                 # keep going if the API does not allow it
    print("runs/get on the For each task run:", loop_error, str(e)[:200])
display(task_table({"tasks": iterations}))                    # one row per iteration
report = spark.table(f"{FQ}.practice_report").orderBy("practice_id")
display(report)
report_rows = [(r["practice_id"], r["encounters"]) for r in report.collect()]
METRICS["E3.report"] = "; ".join(f"{p}: {n}" for p, n in report_rows)
METRICS["E3.iterations"] = len(iterations)
METRICS["E3.loop_get_error"] = loop_error or "none"
METRICS["E3.iteration_results"] = ", ".join(sorted(str(result_state(t)) for t in iterations))
METRICS["E3.stats_total"] = stats.get("total_iterations")
METRICS["E3.stats_succeeded"] = stats.get("succeeded_iterations")
METRICS["E3.task_names"] = ", ".join(sorted({r["task_name"] for r in report.collect()}))
METRICS["E3.inputs_in_run"] = no_braces((loop.get("for_each_task") or {}).get("inputs"))
METRICS["E3.concurrency"] = (loop.get("for_each_task") or {}).get("concurrency")
assert report_rows == [("PR01", 5), ("PR02", 4), ("PR03", 3)], report_rows

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `practice_report`: `{{E3.report}}` — one nested run per element of the list `practices`.
# MAGIC * `runs/get` on the For each task run returned {{E3.iterations}} iterations with the results `{{E3.iteration_results}}`; the task's `stats` say total {{E3.stats_total}}, succeeded {{E3.stats_succeeded}}. Concurrency in the run: {{E3.concurrency}}.
# MAGIC * <code>&#123;&#123;task.name&#125;&#125;</code> inside the loop gave `{{E3.task_names}}`: the key of the nested task.
# MAGIC * In the run, `for_each_task.inputs` still shows the reference, not the list: `{{E3.inputs_in_run}}` (the braces are spaced out here so that the text is not read as a reference).
# MAGIC * Downstream tasks can depend on `per_practice` (the For each task), not on the nested task (*Use a For each task to run another task in a loop*).
# MAGIC
# MAGIC > **Key takeaway:** For each = one task definition, many runs. The list can come from a task value (found at run time) or a job parameter; <code>&#123;&#123;input&#125;&#125;</code> is the current element. Keep `concurrency` low on small compute.

# COMMAND ----------

# DBTITLE 1,E4 Predict
# MAGIC %md
# MAGIC ### E4 — If/else on the row count + run now with different parameters
# MAGIC
# MAGIC Run 2 of the same job overrides one job parameter: `{"run_date": "2026-10-05"}`. There are no encounters on that day.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which branch runs? What happens to `build_summary` and to the For each task?
# MAGIC 2. Does the override change the job definition (the default `run_date`)?
# MAGIC 3. How does the Python script get `run_date`?

# COMMAND ----------

# DBTITLE 1,E4 Code
run2_id = start_job(daily_job_id, {"run_date": "2026-10-05"})   # run now with a different job parameter
run2 = wait_run(run2_id)
display(task_table(run2))
run2_states = states_text(run2)
check2 = task(run2, "check_rows")
script_output = task_output(task(run2, "log_no_data")["run_id"])  # stdout of the Python script task
script_logs = (script_output.get("logs") or "").strip()
print("log_no_data logs:", script_logs)
run2_params = {p["name"]: p.get("value", p.get("default")) for p in run2.get("job_parameters", [])}
default_after = next(p["default"] for p in get_job(daily_job_id)["settings"]["parameters"] if p["name"] == "run_date")
day5_rows = spark.table(f"{FQ}.encounters_daily").where("visit_date = DATE'2026-10-05'").count()
summary_rows = spark.table(f"{FQ}.daily_summary").count()
print("job parameters of run 2:", run2_params, "| default in the job:", default_after,
      "| encounters_daily rows for 2026-10-05:", day5_rows, "| daily_summary rows:", summary_rows)
METRICS["E4.result"] = result_state(run2)
METRICS["E4.states"] = run2_states
METRICS["E4.check_outcome"] = (check2.get("condition_task") or {}).get("outcome")
METRICS["E4.resolved_condition"] = no_braces(json.dumps((check2.get("resolved_values") or {}).get("condition_task"), sort_keys=True))
METRICS["E4.script_logs"] = script_logs
METRICS["E4.run_params"] = json.dumps(run2_params, sort_keys=True)
METRICS["E4.default_after"] = default_after
METRICS["E4.daily_rows_for_day"] = day5_rows
METRICS["E4.summary_rows"] = summary_rows
assert (check2.get("condition_task") or {}).get("outcome") == "false", run2_states
assert result_state(task(run2, "log_no_data")) == "SUCCESS", run2_states
assert default_after == "2026-10-01"

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Run 2: **`{{E4.result}}`**. Task runs: `{{E4.states}}`.
# MAGIC * `check_rows` had the outcome **`{{E4.check_outcome}}`** (0 is not greater than 0), so only the false branch ran: the Python script `log_no_data` printed `{{E4.script_logs}}` (its stdout is the `logs` field of `runs/get-output`). `build_summary` and `per_practice` were `EXCLUDED` — also `per_practice`, which depends on `check_rows` (true) **and** on `build_summary`; `daily_summary` still has {{E4.summary_rows}} row(s).
# MAGIC * The script got `run_date` only because its parameter list references <code>&#123;&#123;job.parameters.run_date&#125;&#125;</code>: list parameters get no automatic pushdown (*Configure job parameters*).
# MAGIC * Job parameters of run 2: `{{E4.run_params}}`. The job definition still has the default `{{E4.default_after}}`: an override changes one run, not the job.
# MAGIC * `ingest` replaced the day 2026-10-05 in `encounters_daily` with {{E4.daily_rows_for_day}} rows (`REPLACE WHERE`), so a second run of the same day gives the same table.
# MAGIC
# MAGIC > **Key takeaway:** If/else lets the **data** choose the path, without code for the check itself. "Run now with different parameters" is the normal way to backfill or re-run one day.

# COMMAND ----------

# DBTITLE 1,E5 Predict
# MAGIC %md
# MAGIC ### E5 — A failed task, an alert task with `run_if`, and a repair
# MAGIC
# MAGIC The job `dbacademy_l09_lab_repair` loads a lab feed. The feed table `lab_feed` does **not exist yet** (the file is late), so `load_labs` fails.
# MAGIC
# MAGIC ```
# MAGIC load_labs ──► report_labs                     (run_if ALL_SUCCESS, the default)
# MAGIC          └──► alert_on_failure                 (run_if AT_LEAST_ONE_FAILED → write_log)
# MAGIC ```
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which result do `load_labs`, `report_labs` and `alert_on_failure` get in the first attempt? And the run?
# MAGIC 2. After the feed arrives, a repair with `rerun_all_failed_tasks` runs. Which tasks run again? Is `alert_on_failure` run again?
# MAGIC 3. Is the repaired run a new run id?

# COMMAND ----------

# DBTITLE 1,E5 Code — the job definition
REPAIR_JOB = {
    "name": f"{PREFIX}_lab_repair",
    "tags": {"lesson": "L09", "data": "fake"},
    "max_concurrent_runs": 1, "queue": {"enabled": True}, "performance_target": "PERFORMANCE_OPTIMIZED",
    "tasks": [
        {"task_key": "load_labs",
         "notebook_task": {"notebook_path": nb("load_labs"), "source": "WORKSPACE"},
         "disable_auto_optimization": True, "max_retries": 0, "timeout_seconds": 900},   # no retry: we want to see the failure
        {"task_key": "report_labs",
         "depends_on": [{"task_key": "load_labs"}],                          # run_if ALL_SUCCESS (default)
         "notebook_task": {"notebook_path": nb("report_labs"), "source": "WORKSPACE",
                           "base_parameters": {"repair_count": "{{job.repair_count}}",
                                               "execution_count": "{{task.execution_count}}",
                                               "load_execution_count": "{{tasks.load_labs.execution_count}}"}},
         "disable_auto_optimization": True, "max_retries": 0, "timeout_seconds": 900},
        {"task_key": "alert_on_failure",
         "depends_on": [{"task_key": "load_labs"}],
         "run_if": "AT_LEAST_ONE_FAILED",                                    # only when load_labs failed
         "notebook_task": {"notebook_path": nb("write_log"), "source": "WORKSPACE",
                           "base_parameters": {"event": "load_labs_failed",
                                               "detail": "{{tasks.load_labs.result_state}} | {{tasks.load_labs.error_code}}"}},
         "disable_auto_optimization": True, "max_retries": 0, "timeout_seconds": 600},
    ],
}
repair_job_id = create_job(REPAIR_JOB)
print("job id:", repair_job_id)

# COMMAND ----------

# DBTITLE 1,E5 Code — run, fail, fix, repair
run3_id = start_job(repair_job_id)                            # first attempt: lab_feed does not exist
run3 = wait_run(run3_id)
display(task_table(run3))
first_states = states_text(run3)
load_error = task_output(task(run3, "load_labs")["run_id"]).get("error", "")   # the error of the failed task
print("load_labs error:", load_error[:300])
alert_rows = [r["detail"] for r in spark.table(f"{FQ}.run_log").where("event = 'load_labs_failed'").collect()]
print("alert task wrote:", alert_rows)

# The late lab feed arrives (fake lab results)
spark.sql(f"""CREATE TABLE {FQ}.lab_feed AS SELECT * FROM VALUES
    ('L001', 'P1001', 'A1C', 6.1), ('L002', 'P1002', 'LDL', 131.0), ('L003', 'P1003', 'TSH', 2.4),
    ('L004', 'P1004', 'A1C', 7.8), ('L005', 'P1005', 'LDL', 98.0), ('L006', 'P1006', 'TSH', 0.9)
    AS t(lab_id, patient_id, test_code, result_value)""")

repair_id = repair(run3_id, rerun_all_failed_tasks=True)      # re-run only what did not succeed, in the same run
run3b = wait_run(run3_id, repair_id=repair_id)               # wait for this repair
display(task_table(run3b))
history = [(h.get("type"), result_state(h), len(h.get("task_run_ids", []))) for h in run3b.get("repair_history", [])]
print("repair history:", history)
lab = spark.table(f"{FQ}.lab_report").first()
repair_item = next((h for h in run3b.get("repair_history", []) if h.get("id") == repair_id), {})
repaired_ids = set(repair_item.get("task_run_ids", []))
repaired_tasks = sorted(t["task_key"] for t in run3b.get("tasks", []) if t["run_id"] in repaired_ids)
alert_runs = [t for t in run3b.get("tasks", []) if t["task_key"] == "alert_on_failure"]
print("tasks run by the repair:", repaired_tasks)
METRICS["E5.first_result"] = result_state(run3)
METRICS["E5.first_states"] = first_states
METRICS["E5.load_error_class"] = ", ".join(error_classes(load_error)) or load_error[:80]
METRICS["E5.alert_detail"] = no_braces(alert_rows[0]) if alert_rows else "no row"
METRICS["E5.final_result"] = result_state(run3b)
METRICS["E5.final_states"] = states_text(run3b)
METRICS["E5.history"] = "; ".join(f"{t}: {r}, {n} task runs" for t, r, n in history)
METRICS["E5.same_run_id"] = run3b.get("run_id") == run3_id
METRICS["E5.alert_task_runs"] = len(alert_runs)
METRICS["E5.repaired_tasks"] = ", ".join(repaired_tasks)
METRICS["E5.lab_rows"] = lab["lab_rows"]
METRICS["E5.repair_count"] = no_braces(lab["repair_count"])
METRICS["E5.report_execution_count"] = no_braces(lab["report_execution_count"])
METRICS["E5.load_execution_count"] = no_braces(lab["load_execution_count"])
METRICS["nd.E5.run_id"] = run3_id
METRICS["nd.E5.repair_id"] = repair_id
assert result_state(run3) == "FAILED" and result_state(task(run3, "load_labs")) == "FAILED", first_states
assert result_state(run3b) == "SUCCESS" and lab["lab_rows"] == 6, states_text(run3b)

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * First attempt: **`{{E5.first_result}}`** — `{{E5.first_states}}`.
# MAGIC * The error of the failed task (from `runs/get-output` of the `load_labs` task run): **`{{E5.load_error_class}}`**. This is where you start debugging: the run page shows the same error.
# MAGIC * `alert_on_failure` (`run_if: AT_LEAST_ONE_FAILED`) ran and wrote `{{E5.alert_detail}}`: the result state arrives in lower case, and the error code is a general code, not the Spark error class (the references <code>&#123;&#123;tasks.load_labs.result_state&#125;&#125;</code> | <code>&#123;&#123;tasks.load_labs.error_code&#125;&#125;</code>).
# MAGIC * After the feed arrived, the repair ended with **`{{E5.final_result}}`**: `{{E5.final_states}}`. Repair history: `{{E5.history}}`.
# MAGIC * Same run id after the repair: {{E5.same_run_id}} — a repair is part of the original run, not a new run. Task runs started by the repair: `{{E5.repaired_tasks}}`. `alert_on_failure` had succeeded, so it was not run again ({{E5.alert_task_runs}} task run of it in the run).
# MAGIC * `report_labs` counted {{E5.lab_rows}} lab rows and received <code>&#123;&#123;job.repair_count&#125;&#125;</code> = `{{E5.repair_count}}`, its own <code>&#123;&#123;task.execution_count&#125;&#125;</code> = `{{E5.report_execution_count}}`, and <code>&#123;&#123;tasks.load_labs.execution_count&#125;&#125;</code> = `{{E5.load_execution_count}}`. The execution count of `report_labs` includes its first attempt, which was only `UPSTREAM_FAILED`.
# MAGIC * The run's `tasks` list keeps **both** attempts of each repaired task (`{{E5.final_states}}`): read the latest one (largest `start_time`), as the helper `task()` does.
# MAGIC
# MAGIC > **Key takeaway:** fix the cause, then **repair** — only the failed task and the tasks that were skipped because of it run again, inside the same run. `run_if: AT_LEAST_ONE_FAILED` gives you an alert or clean-up step without an external tool.

# COMMAND ----------

# DBTITLE 1,E6 Predict
# MAGIC %md
# MAGIC ### E6 — Run history and durations through the API
# MAGIC
# MAGIC `GET /api/2.2/jobs/runs/list?job_id=…&completed_only=true` returns the finished runs of a job, newest first.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. How many runs does the daily job have now, and how many the repair job?
# MAGIC 2. Does the duration of the repaired run include the time before the repair?

# COMMAND ----------

# DBTITLE 1,E6 Code
def run_history(job_id):
    """Finished runs of a job (newest first) as rows: run id, trigger, run_date parameter, result, seconds."""
    runs = api("GET", "/api/2.2/jobs/runs/list", query={"job_id": job_id, "completed_only": "true", "limit": 25}).get("runs", [])
    return [(r["run_id"], r.get("trigger"),
             next((p.get("value", p.get("default")) for p in r.get("job_parameters", []) if p["name"] == "run_date"), None),
             result_state(r), round((r.get("run_duration") or 0) / 1000), round((r.get("queue_duration") or 0) / 1000))
            for r in runs]

daily_history = run_history(daily_job_id)
repair_history_rows = run_history(repair_job_id)
history_df = spark.createDataFrame(daily_history + repair_history_rows,
                                   "run_id BIGINT, trigger STRING, run_date STRING, result STRING, seconds BIGINT, queue_seconds BIGINT")
display(history_df)                                           # seconds change from run to run
task_secs = {t["task_key"]: round(((t.get("end_time") or 0) - (t.get("start_time") or 0)) / 1000)   # end - start
             for t in run1.get("tasks", []) if t.get("start_time")}                                    # tasks that ran
print("run 1 task durations (s):", task_secs)
METRICS["E6.daily_runs"] = len(daily_history)
METRICS["E6.repair_job_runs"] = len(repair_history_rows)
METRICS["E6.daily_run_dates"] = ", ".join(str(r[2]) for r in daily_history)
METRICS["E6.daily_results"] = ", ".join(str(r[3]) for r in daily_history)
METRICS["E6.triggers"] = ", ".join(sorted({str(r[1]) for r in daily_history + repair_history_rows}))
METRICS["nd.E6.run1_secs"] = next((r[4] for r in daily_history if r[0] == run1_id), None)
METRICS["nd.E6.run2_secs"] = next((r[4] for r in daily_history if r[0] == run2_id), None)
METRICS["nd.E6.run3_secs"] = next((r[4] for r in repair_history_rows if r[0] == run3_id), None)
METRICS["nd.E6.run1_task_secs"] = json.dumps(task_secs, sort_keys=True)
assert len(daily_history) == 2 and len(repair_history_rows) == 1

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * The daily job has {{E6.daily_runs}} finished runs (newest first, `run_date` {{E6.daily_run_dates}}, results {{E6.daily_results}}); the repair job has {{E6.repair_job_runs}} — the repair added no new run. Triggers seen: `{{E6.triggers}}`.
# MAGIC * Durations in this run (they change every time): run 1 {{nd.E6.run1_secs}} s, run 2 {{nd.E6.run2_secs}} s, the repaired run 3 {{nd.E6.run3_secs}} s. Per task in run 1 (s, end − start; `log_no_data` was excluded): `{{nd.E6.run1_task_secs}}`.
# MAGIC * `run_duration` of a run is "The time in milliseconds it took the job run and all of its repairs to finish" (Jobs API reference), so run 3 includes the failed attempt, the wait for the feed and the repair. The UI says the same: "The Duration value covers the time from the first run's start to the latest repair run's finish" (*Troubleshoot and repair job failures*).
# MAGIC
# MAGIC > **Key takeaway:** the run list gives you state, trigger, parameters and duration per run — enough for a simple SLA check or a cost report. System tables (`system.lakeflow.job_run_timeline`, L16) keep this history for the whole account.

# COMMAND ----------

# DBTITLE 1,E7 Predict
# MAGIC %md
# MAGIC ### E7 — Export a job as JSON; mistakes the API rejects; triggers
# MAGIC
# MAGIC Creating, changing and deleting a job costs no compute; only runs do. This example uses only definitions.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `jobs/get` returns the settings you sent. Does it add fields you did not send?
# MAGIC 2. Which of these mistakes does `jobs/create` reject: a `depends_on` to a task that does not exist; a reference <code>&#123;&#123;job.notebook_url&#125;&#125;</code>; a typo <code>&#123;&#123;tasks.ingest.value.row_count&#125;&#125;</code>; a dependency cycle?
# MAGIC 3. Can you create a file arrival or table update trigger in the `PAUSED` state?

# COMMAND ----------

# DBTITLE 1,E7 Code — export the job
exported = get_job(daily_job_id)["settings"]                   # the job definition as JSON
print(json.dumps(exported, indent=2)[:3000])
sent_keys, got_keys = set(DAILY_JOB), set(exported)
METRICS["E7.added_keys"] = ", ".join(sorted(got_keys - sent_keys)) or "none"
METRICS["E7.missing_keys"] = ", ".join(sorted(sent_keys - got_keys)) or "none"
METRICS["E7.exported_tasks"] = len(exported.get("tasks", []))
METRICS["E7.format"] = exported.get("format")
METRICS["E7.schedule"] = json.dumps(exported.get("schedule"), sort_keys=True)
METRICS["E7.flag_kept"] = ", ".join(t["task_key"] for t in exported["tasks"] if t.get("disable_auto_optimization"))
METRICS["E7.flag_dropped"] = ", ".join(t["task_key"] for t in exported["tasks"] if not t.get("disable_auto_optimization"))
METRICS["E7.nested_flag"] = next(t["for_each_task"]["task"].get("disable_auto_optimization") for t in exported["tasks"] if "for_each_task" in t)

# COMMAND ----------

# DBTITLE 1,E7 Code — mistakes and paused triggers
def one_task(task_key, depends_on=None, base_parameters=None):
    """A small notebook task (write_log) for test definitions that are never run."""
    t = {"task_key": task_key, "disable_auto_optimization": True,
         "notebook_task": {"notebook_path": nb("write_log"), "source": "WORKSPACE"}}
    if depends_on:
        t["depends_on"] = depends_on
    if base_parameters:
        t["notebook_task"]["base_parameters"] = base_parameters
    return t

MISTAKES = {
    "unknown depends_on": [one_task("load"), one_task("report", depends_on=[{"task_key": "Load"}])],      # wrong key (case)
    "known namespace, invalid field": [one_task("load", base_parameters={"detail": "{{job.notebook_url}}"})],
    "typo in a task value reference": [one_task("load"), one_task("report", depends_on=[{"task_key": "load"}],
                                       base_parameters={"detail": "{{tasks.load.value.row_count}}"})],
    "dependency cycle": [one_task("a", depends_on=[{"task_key": "b"}]), one_task("b", depends_on=[{"task_key": "a"}])],
    "outcome on a non-condition task": [one_task("load"), one_task("report", depends_on=[{"task_key": "load", "outcome": "true"}])],
}
mistake_results = {}
for label, tasks in MISTAKES.items():
    try:
        bad_id = create_job({"name": f"{PREFIX}_mistake", "tasks": tasks})
        mistake_results[label] = "accepted"                    # created: the mistake shows up only at run time
        delete_job(bad_id)
    except Exception as e:
        mistake_results[label] = api_error(e)                  # rejected at create time
        print(f"{label}: {api_error(e)} - {str(e)[:200]}")
display(spark.createDataFrame(list(mistake_results.items()), "mistake STRING, create_result STRING"))

# Triggers: created PAUSED, so they never start a run; then the job is deleted
trigger_results = {}
dbutils.fs.mkdirs(f"{VOL}/incoming")                          # the folder a file arrival trigger watches
TRIGGER_JOB = {"name": f"{PREFIX}_trigger_demo", "tasks": [one_task("count")],
               "trigger": {"pause_status": "PAUSED", "file_arrival": {"url": f"{VOL}/incoming/"}}}
trigger_job_id = None
try:
    trigger_job_id = create_job(TRIGGER_JOB)
    trigger_results["file_arrival"] = sorted((get_job(trigger_job_id)["settings"].get("trigger") or {}).keys())
except Exception as e:
    trigger_results["file_arrival"] = api_error(e)
    print("file arrival:", str(e)[:300])
try:
    table_trigger = {"pause_status": "PAUSED", "table_update": {"table_names": [f"{FQ}.encounters_raw"], "condition": "ANY_UPDATED"}}
    if trigger_job_id is None:
        trigger_job_id = create_job({**TRIGGER_JOB, "trigger": table_trigger})
    else:
        api("POST", "/api/2.2/jobs/update", body={"job_id": trigger_job_id, "new_settings": {"trigger": table_trigger}})
    trigger_results["table_update"] = sorted((get_job(trigger_job_id)["settings"].get("trigger") or {}).keys())
except Exception as e:
    trigger_results["table_update"] = api_error(e)
    print("table update:", str(e)[:300])
if trigger_job_id is not None:
    delete_job(trigger_job_id)
print("trigger settings read back:", trigger_results)
METRICS["E7.mistakes"] = "; ".join(f"{k}: {v}" for k, v in mistake_results.items())
METRICS["E7.unknown_depends_on"] = mistake_results["unknown depends_on"]
METRICS["E7.invalid_known_ref"] = mistake_results["known namespace, invalid field"]
METRICS["E7.typo_ref"] = mistake_results["typo in a task value reference"]
METRICS["E7.cycle"] = mistake_results["dependency cycle"]
METRICS["E7.outcome_on_notebook"] = mistake_results["outcome on a non-condition task"]
METRICS["E7.file_arrival"] = str(trigger_results.get("file_arrival"))
METRICS["E7.table_update"] = str(trigger_results.get("table_update"))
METRICS["E7.jobs_left"] = ", ".join(sorted(n for n, _ in lesson_jobs()))

# COMMAND ----------

# DBTITLE 1,E7 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `jobs/get` returned the settings with {{E7.exported_tasks}} tasks. Keys the service added: `{{E7.added_keys}}`; keys that were sent but not returned: `{{E7.missing_keys}}`; `format` = `{{E7.format}}`. The schedule came back as `{{E7.schedule}}`. Save this JSON in git, or run `databricks bundle generate job --existing-job-id <id>` to get bundle YAML (L21).
# MAGIC * `disable_auto_optimization: true` was sent on all five tasks. `jobs/get` returns it on `{{E7.flag_kept}}`, but not on `{{E7.flag_dropped}}` (the If/else task and the For each task itself, which run no code). The nested task of the loop has it: {{E7.nested_flag}}.
# MAGIC * `jobs/create` with mistakes (`accepted` = the job was created; otherwise the error code): `{{E7.mistakes}}`.
# MAGIC * So every one of these mistakes is found when the job is saved — also the typo `value` for `values`. `job` and `tasks` are known namespaces, and the docs say: "if you provide a reference that belongs to a known namespace but is not valid … an error message is displayed". What the docs call "silently ignored" (references outside the known namespaces, missing braces) was not tested here.
# MAGIC * Paused triggers, read back from `jobs/get` (`trigger` keys, or the error code): file arrival on the volume folder `{{E7.file_arrival}}`; table update on `encounters_raw` `{{E7.table_update}}`. The trigger job was deleted right after. Lesson jobs left now: `{{E7.jobs_left}}`.
# MAGIC
# MAGIC > **Key takeaway:** the job definition is plain JSON that you can export, review and version. The API checks task keys, dependencies and references in the known namespaces when you save; problems in the data or the code (a missing table, E5) show up only in a run, so test new jobs with a real run on small data.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "Repair starts a new run." | It re-runs tasks inside the **same** run (E5: same run id {{E5.same_run_id}}; the job still has {{E6.repair_job_runs}} run). |
# MAGIC | 2 | "Repair runs every task again." | Only unsuccessful tasks and the tasks skipped because of them: the repair ran `{{E5.repaired_tasks}}` (E5; *Troubleshoot and repair job failures*). |
# MAGIC | 3 | "Repair uses the job settings of the first attempt." | "Repaired tasks use the current job and task settings" — fix the job, then repair. |
# MAGIC | 4 | "A typo in a dynamic value reference is only found at run time." | A typo inside a known namespace (`tasks.load.value.…`) was rejected at create time: `{{E7.typo_ref}}` (E7). Only references outside the known namespaces and missing braces are "treated as literal strings" (*Dynamic value references*). |
# MAGIC | 5 | "`depends_on` with a wrong task key is found only at run time." | `jobs/create` answered `{{E7.unknown_depends_on}}` (E7). |
# MAGIC | 6 | "`EQUAL_TO` compares numbers." | `==` / `!=` compare strings (`12.0 == 12` is false); `>`, `>=`, `<`, `<=` compare numbers (*If/else task*). E1 used `GREATER_THAN`. |
# MAGIC | 7 | "A task value arrives as an int." | Through a widget it is a string: `{{E2.rows_reported_type}}` (E2). |
# MAGIC | 8 | "Job parameters reach every task automatically." | Only key-value tasks (notebooks). A Python script got `run_date` only through <code>&#123;&#123;job.parameters.run_date&#125;&#125;</code> in its list (E4). |
# MAGIC | 9 | "A task parameter overrides a job parameter with the same name." | "the job parameter takes precedence" (*Configure job parameters*). |
# MAGIC | 10 | "Run now with other parameters changes the job." | The default stayed `{{E4.default_after}}` (E4). |
# MAGIC | 11 | "A branch that is not taken makes the run fail." | Run 1 was `{{E1.result}}`, run 2 `{{E4.result}}` (E1, E4). |
# MAGIC | 12 | "`run_if: ALL_DONE` and `AT_LEAST_ONE_FAILED` are the same." | `ALL_DONE` runs after any result; `AT_LEAST_ONE_FAILED` only after a failure, else the task is Excluded (*Configure task dependencies*). |
# MAGIC | 13 | "A For each task can contain another For each task." | "You cannot add another `For each` task as the nested task." |
# MAGIC | 14 | "Serverless tasks never retry on their own." | Auto-optimization "retries failed tasks" unless `disable_auto_optimization` is set (*Run your Lakeflow Jobs with serverless compute for workflows*). |
# MAGIC | 15 | "Standard performance mode is just a cheaper SKU." | Same SKU, fewer DBUs, startup latency of "4 to 6 minutes". |
# MAGIC | 16 | "<code>&#123;&#123;run_id&#125;&#125;</code> is the current way to get the run id." | Deprecated; use <code>&#123;&#123;job.run_id&#125;&#125;</code> (job run) or <code>&#123;&#123;task.run_id&#125;&#125;</code> (task run) (*Dynamic value references*). |
# MAGIC | 17 | "`runs/get-output` works with the job run id." | It needs the **task** run id (E2, E5). |
# MAGIC | 18 | "A paused schedule blocks run-now." | Run 1 and 2 started with run-now while the schedule was `PAUSED` (E1). |

# COMMAND ----------

# DBTITLE 1,7 — Syntax Cheat Sheet
# MAGIC %md
# MAGIC ## 7 Syntax Cheat Sheet
# MAGIC
# MAGIC ### Job JSON (REST, CLI `--json`, close to bundle YAML)
# MAGIC
# MAGIC | Goal | JSON |
# MAGIC | --- | --- |
# MAGIC | Job parameter | `"parameters": [{"name": "run_date", "default": "2026-10-01"}]` |
# MAGIC | Notebook task | `{"task_key": "ingest", "notebook_task": {"notebook_path": "/Users/…/ingest", "source": "WORKSPACE", "base_parameters": {…}}}` |
# MAGIC | Python script task | `{"spark_python_task": {"python_file": "/Workspace/…/x.py", "parameters": ["--run_date", "…"]}, "environment_key": "script_env"}` |
# MAGIC | Dependency | `"depends_on": [{"task_key": "ingest"}]` |
# MAGIC | Run if | `"run_if": "AT_LEAST_ONE_FAILED"` (`ALL_SUCCESS`, `ALL_DONE`, `NONE_FAILED`, `AT_LEAST_ONE_SUCCESS`, `ALL_FAILED`) |
# MAGIC | If/else | `"condition_task": {"op": "GREATER_THAN", "left": "…", "right": "0"}` + `"depends_on": [{"task_key": "check_rows", "outcome": "true"}]` |
# MAGIC | For each | `"for_each_task": {"inputs": "[\"PR01\", \"PR02\"]", "concurrency": 2, "task": {…}}` |
# MAGIC | Retries / timeout | `"max_retries": 2, "min_retry_interval_millis": 60000, "retry_on_timeout": false, "timeout_seconds": 900` |
# MAGIC | Serverless | `"disable_auto_optimization": true` (task), `"performance_target": "PERFORMANCE_OPTIMIZED"` (job) |
# MAGIC | Schedule | `"schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC", "pause_status": "PAUSED"}` |
# MAGIC | Notifications | `"email_notifications": {"on_failure": ["…"]}`, `"notification_settings": {"no_alert_for_skipped_runs": true}` |
# MAGIC
# MAGIC ### References and task values
# MAGIC
# MAGIC | Goal | Write |
# MAGIC | --- | --- |
# MAGIC | job parameter in a task parameter | <code>&#123;&#123;job.parameters.run_date&#125;&#125;</code> |
# MAGIC | job run / task run id | <code>&#123;&#123;job.run_id&#125;&#125;</code> / <code>&#123;&#123;task.run_id&#125;&#125;</code> |
# MAGIC | a task value | <code>&#123;&#123;tasks.ingest.values.row_count&#125;&#125;</code> |
# MAGIC | upstream state / error | <code>&#123;&#123;tasks.load_labs.result_state&#125;&#125;</code> / <code>&#123;&#123;tasks.load_labs.error_code&#125;&#125;</code> |
# MAGIC | For each element | <code>&#123;&#123;input&#125;&#125;</code> (or <code>&#123;&#123;input.field&#125;&#125;</code>) |
# MAGIC | start date (UTC) | <code>&#123;&#123;job.start_time.iso_date&#125;&#125;</code> |
# MAGIC
# MAGIC ### Inside a task notebook
# MAGIC ```python
# MAGIC run_date = dbutils.widgets.get("run_date")                                   # a job or task parameter (always a string)
# MAGIC dbutils.jobs.taskValues.set(key="row_count", value=12)                       # JSON value, at most 48 KiB
# MAGIC n = dbutils.jobs.taskValues.get(taskKey="ingest", key="row_count", debugValue=0)  # debugValue: when run by hand
# MAGIC dbutils.notebook.exit(json.dumps({"rows": 12}))                              # the task's run output
# MAGIC ```
# MAGIC
# MAGIC ### REST calls of this notebook
# MAGIC `POST jobs/create` · `POST jobs/run-now {job_id, job_parameters}` · `GET jobs/runs/get?run_id&include_history&include_resolved_values` · `GET jobs/runs/get-output?run_id=<task run>` · `POST jobs/runs/repair {run_id, rerun_all_failed_tasks | rerun_tasks, rerun_dependent_tasks, latest_repair_id, job_parameters}` · `GET jobs/runs/list?job_id&completed_only` · `GET jobs/get` · `POST jobs/update {job_id, new_settings}` · `POST jobs/delete` (all under `/api/2.2/`).

# COMMAND ----------

# DBTITLE 1,8 — Check Your Understanding
# MAGIC %md
# MAGIC ## 8 Check Your Understanding
# MAGIC
# MAGIC Answer in your own words. Answers are in section 11.
# MAGIC
# MAGIC 1. A nightly job has five tasks. Task 4 failed after 50 minutes because a table name was wrong. What do you do, and which tasks run again?
# MAGIC 2. Task `ingest` finds the list of new lab files at run time; one task per file must run, at most three at a time. How do you build this, and how does the nested task get the file name?
# MAGIC 3. Which mistakes in a job definition does the Jobs API reject when you save the job, and which ones does only a run show? Use E5 and E7.

# COMMAND ----------

# DBTITLE 1,9 — Practice (intro)
# MAGIC %md
# MAGIC ## 9 Practice
# MAGIC
# MAGIC Do these yourself, without AI help. P1–P3 only **create** jobs (no run, no compute); the validation reads the job back with `get_job`. P4 runs a job, lets it fail and repairs it. Use the helpers of 4.9 (`create_job`, `start_job`, `wait_run`, `repair`, `task`, `nb(...)`). Each validation cell prints ✅ Passed or a hint. Solutions are in section 11.
# MAGIC
# MAGIC > The task notebooks of 4.9 must exist (run that cell first), and P4 needs the table `lab_results` from E5.

# COMMAND ----------

# DBTITLE 1,9 — Practice helper (run first)
def job_settings(name):
    """The settings of the lesson job with this name (None if there is none)."""
    found = lesson_jobs(name)
    return get_job(found[-1][1])["settings"] if found else None

def by_key(settings):
    """{task_key: task} of a job definition."""
    return {t["task_key"]: t for t in (settings or {}).get("tasks", [])}

def code_tasks(settings):
    """All tasks that run code (notebook or script), also the nested task of a For each task."""
    out = []
    for t in (settings or {}).get("tasks", []):
        if "for_each_task" in t:
            out.append(t["for_each_task"].get("task", {}))
        elif "notebook_task" in t or "spark_python_task" in t:
            out.append(t)
    return out

print("Practice helpers ready: job_settings, by_key, code_tasks")

# COMMAND ----------

# DBTITLE 1,P1 Task
# MAGIC %md
# MAGIC ### P1 — Fill in the blanks (3 blanks): a weekly lab count job
# MAGIC Complete the job definition: `log_count` must run **after** `count_labs`, must run **even when `count_labs` failed** (it writes an audit line), and must pass the task value `row_count` of `count_labs` as `detail`. Replace the three `<FILL_IN>` markers, then run the cell (it creates the job; nothing runs).

# COMMAND ----------

# DBTITLE 1,P1 Code
# >>> BLANK P1
P1_JOB = {
    "name": f"{PREFIX}_p1_weekly_labs",
    "parameters": [{"name": "table_name", "default": f"{FQ}.lab_results"}],
    "performance_target": "PERFORMANCE_OPTIMIZED",
    "tasks": [
        {"task_key": "count_labs", "disable_auto_optimization": True,
         "notebook_task": {"notebook_path": nb("count_table"), "source": "WORKSPACE"}},
        {"task_key": "log_count", "disable_auto_optimization": True,
         "depends_on": <FILL_IN>,
         "run_if": <FILL_IN>,
         "notebook_task": {"notebook_path": nb("write_log"), "source": "WORKSPACE",
                           "base_parameters": {"event": "weekly_lab_count", "detail": <FILL_IN>}}},
    ],
}
# >>> SOLUTION P1
P1_JOB = {
    "name": f"{PREFIX}_p1_weekly_labs",
    "parameters": [{"name": "table_name", "default": f"{FQ}.lab_results"}],
    "performance_target": "PERFORMANCE_OPTIMIZED",
    "tasks": [
        {"task_key": "count_labs", "disable_auto_optimization": True,
         "notebook_task": {"notebook_path": nb("count_table"), "source": "WORKSPACE"}},
        {"task_key": "log_count", "disable_auto_optimization": True,
         "depends_on": [{"task_key": "count_labs"}],
         "run_if": "ALL_DONE",
         "notebook_task": {"notebook_path": nb("write_log"), "source": "WORKSPACE",
                           "base_parameters": {"event": "weekly_lab_count", "detail": "{{tasks.count_labs.values.row_count}}"}}},
    ],
}
# <<< END P1
p1_job_id = create_job(P1_JOB)
print("created job", p1_job_id)

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    t = by_key(job_settings(f"{PREFIX}_p1_weekly_labs")).get("log_count", {})
    deps = [d.get("task_key") for d in t.get("depends_on", [])]
    detail = t.get("notebook_task", {}).get("base_parameters", {}).get("detail", "")
    checks = {"depends_on": deps == ["count_labs"], "run_if": t.get("run_if") == "ALL_DONE",
              "detail": detail.replace(" ", "") == "{{tasks.count_labs.values.row_count}}"}
    ok = all(checks.values())
    PRACTICE_PASSED["P1"] = ok
    print("✅ Passed" if ok else
          f"Hint: check {[k for k, v in checks.items() if not v]}. depends_on is a list of {{'task_key': ...}}; the run_if "
          "value that runs after any result is in 4.2; a task value reference has the form tasks.<task>.values.<key> in "
          "double curly braces (4.3).")
    METRICS["P1.run_if"], METRICS["P1.deps"] = t.get("run_if"), ", ".join(deps)
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: run the P1 code cell first; the job must be created.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs): a lab job that a colleague wrote
# MAGIC The colleague's job should count `lab_results` and then write the count to `run_log`. The Jobs API refuses to create it: read the error message of the code cell. There are two bugs; both are mistakes that `jobs/create` rejects (E7). Fix both in place and run the cell until the validation passes.

# COMMAND ----------

# DBTITLE 1,P2 Code
# >>> BLANK P2
P2_JOB = {
    "name": f"{PREFIX}_p2_lab_log",
    "parameters": [{"name": "table_name", "default": f"{FQ}.lab_results"}],
    "performance_target": "PERFORMANCE_OPTIMIZED",
    "tasks": [
        {"task_key": "count_labs", "disable_auto_optimization": True,
         "notebook_task": {"notebook_path": nb("count_table"), "source": "WORKSPACE"}},
        {"task_key": "log_count", "disable_auto_optimization": True,
         "depends_on": [{"task_key": "count-labs"}],
         "notebook_task": {"notebook_path": nb("write_log"), "source": "WORKSPACE",
                           "base_parameters": {"event": "lab_count", "detail": "{{tasks.count_labs.value.row_count}}"}}},
    ],
}
# >>> SOLUTION P2
P2_JOB = {
    "name": f"{PREFIX}_p2_lab_log",
    "parameters": [{"name": "table_name", "default": f"{FQ}.lab_results"}],
    "performance_target": "PERFORMANCE_OPTIMIZED",
    "tasks": [
        {"task_key": "count_labs", "disable_auto_optimization": True,
         "notebook_task": {"notebook_path": nb("count_table"), "source": "WORKSPACE"}},
        {"task_key": "log_count", "disable_auto_optimization": True,
         "depends_on": [{"task_key": "count_labs"}],
         "notebook_task": {"notebook_path": nb("write_log"), "source": "WORKSPACE",
                           "base_parameters": {"event": "lab_count", "detail": "{{tasks.count_labs.values.row_count}}"}}},
    ],
}
# <<< END P2
p2_job_id = create_job(P2_JOB)
print("created job", p2_job_id)

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    t = by_key(job_settings(f"{PREFIX}_p2_lab_log")).get("log_count", {})
    deps = [d.get("task_key") for d in t.get("depends_on", [])]
    detail = t.get("notebook_task", {}).get("base_parameters", {}).get("detail", "")
    checks = {"depends_on": deps == ["count_labs"],
              "reference": detail.replace(" ", "") == "{{tasks.count_labs.values.row_count}}"}
    ok = all(checks.values())
    PRACTICE_PASSED["P2"] = ok
    print("✅ Passed" if ok else
          f"Hint: still wrong: {[k for k, v in checks.items() if not v]}. A task key must match exactly (E7); a task value "
          "reference is tasks.<task_key>.values.<key>; the API rejects a wrong one when the job is saved (E7).")
    METRICS["P2.deps"], METRICS["P2.fixed"] = ", ".join(deps), ok
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: the job was not created. Read the error of the code cell: which task key does not exist?", error_class(e))

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): a For each job with notifications
# MAGIC Create the job `dbacademy_l09_p3_practice_reports` (store the id in `p3_job_id`; do **not** run it):
# MAGIC * job parameter `run_date` with the default `2026-10-02`;
# MAGIC * one For each task `per_practice` over the fixed list `PR01`, `PR02`, `PR03`, at most **2** at a time;
# MAGIC * the nested task `practice` runs the notebook `nb("practice_report")` with the base parameters `practice_id` = the current element and `task_name` = the task name (both as references);
# MAGIC * a job-level email notification **on failure** to `data-oncall@example.com`, and no alerts for skipped runs;
# MAGIC * every task with code has `disable_auto_optimization` set.
# MAGIC
# MAGIC **Hints:** E1 `per_practice` · `inputs` is a JSON **string** (`json.dumps([...])`) · <code>&#123;&#123;input&#125;&#125;</code>, <code>&#123;&#123;task.name&#125;&#125;</code> · `email_notifications.on_failure` · `notification_settings.no_alert_for_skipped_runs` (4.5).

# COMMAND ----------

# DBTITLE 1,P3 Code
# >>> BLANK P3
# Your code here
# >>> SOLUTION P3
P3_JOB = {
    "name": f"{PREFIX}_p3_practice_reports",
    "parameters": [{"name": "run_date", "default": "2026-10-02"}],
    "performance_target": "PERFORMANCE_OPTIMIZED",
    "email_notifications": {"on_failure": ["data-oncall@example.com"]},
    "notification_settings": {"no_alert_for_skipped_runs": True},
    "tasks": [
        {"task_key": "per_practice",
         "for_each_task": {
             "inputs": json.dumps(["PR01", "PR02", "PR03"]),
             "concurrency": 2,
             "task": {"task_key": "practice", "disable_auto_optimization": True,
                      "notebook_task": {"notebook_path": nb("practice_report"), "source": "WORKSPACE",
                                        "base_parameters": {"practice_id": "{{input}}", "task_name": "{{task.name}}"}}}}},
    ],
}
p3_job_id = create_job(P3_JOB)
# <<< END P3

# COMMAND ----------

# DBTITLE 1,P3 Validation
try:
    s3 = job_settings(f"{PREFIX}_p3_practice_reports") or {}
    loop3 = by_key(s3).get("per_practice", {}).get("for_each_task", {})
    nested = loop3.get("task", {})
    params = nested.get("notebook_task", {}).get("base_parameters", {})
    checks = {
        "run_date default": [p.get("default") for p in s3.get("parameters", []) if p.get("name") == "run_date"] == ["2026-10-02"],
        "inputs": sorted(json.loads(loop3.get("inputs", "[]"))) == ["PR01", "PR02", "PR03"],
        "concurrency": loop3.get("concurrency") == 2,
        "notebook": nested.get("notebook_task", {}).get("notebook_path") == nb("practice_report"),
        "practice_id": params.get("practice_id", "").replace(" ", "") == "{{input}}",
        "task_name": params.get("task_name", "").replace(" ", "") == "{{task.name}}",
        "on_failure": s3.get("email_notifications", {}).get("on_failure") == ["data-oncall@example.com"],
        "no_alert_for_skipped_runs": s3.get("notification_settings", {}).get("no_alert_for_skipped_runs") is True,
        "disable_auto_optimization": bool(code_tasks(s3)) and all(t.get("disable_auto_optimization") is True for t in code_tasks(s3)),
    }
    ok = all(checks.values())
    PRACTICE_PASSED["P3"] = ok
    print("✅ Passed" if ok else f"Hint: these checks failed: {[k for k, v in checks.items() if not v]} (E1 per_practice, 4.5).")
    METRICS["P3.checks"] = sum(checks.values())
    METRICS["P3.concurrency"] = loop3.get("concurrency")
except Exception as e:
    PRACTICE_PASSED["P3"] = False
    print("Hint: create the job with create_job(...) first; inputs must be a JSON string.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints): create, run, fail, repair
# MAGIC Create the job `dbacademy_l09_p4_lab_counts` with the job parameter `table_name` (default `main.dbacademy_l09.lab_feed_v2`, a table that does **not** exist) and two notebook tasks: `count_source` runs `nb("count_table")`; `log_result` runs `nb("write_log")` after it, with `event` = `p4_count` and `detail` = the task value `row_count` of `count_source`. Run the job and wait: it fails. Then, **without starting a new run**, make the same run succeed so that it counts `main.dbacademy_l09.lab_results`. Store the run id in `p4_run_id`.

# COMMAND ----------

# DBTITLE 1,P4 Code
# >>> BLANK P4
# Your code here
# >>> SOLUTION P4
P4_JOB = {
    "name": f"{PREFIX}_p4_lab_counts",
    "parameters": [{"name": "table_name", "default": f"{FQ}.lab_feed_v2"}],
    "max_concurrent_runs": 1, "queue": {"enabled": True}, "performance_target": "PERFORMANCE_OPTIMIZED",
    "tasks": [
        {"task_key": "count_source", "disable_auto_optimization": True, "max_retries": 0,
         "notebook_task": {"notebook_path": nb("count_table"), "source": "WORKSPACE"}},
        {"task_key": "log_result", "disable_auto_optimization": True, "max_retries": 0,
         "depends_on": [{"task_key": "count_source"}],
         "notebook_task": {"notebook_path": nb("write_log"), "source": "WORKSPACE",
                           "base_parameters": {"event": "p4_count", "detail": "{{tasks.count_source.values.row_count}}"}}},
    ],
}
p4_job_id = create_job(P4_JOB)
p4_run_id = start_job(p4_job_id)
p4_first = wait_run(p4_run_id)                                       # fails: lab_feed_v2 does not exist
p4_repair_id = repair(p4_run_id, rerun_all_failed_tasks=True,
                      job_parameters={"table_name": f"{FQ}.lab_results"})   # new value for the repaired tasks
p4_final = wait_run(p4_run_id, repair_id=p4_repair_id)
# <<< END P4

# COMMAND ----------

# DBTITLE 1,P4 Validation
try:
    r4 = get_run(p4_run_id)
    h4 = r4.get("repair_history", [])
    original = [result_state(h) for h in h4 if h.get("type") == "ORIGINAL"]
    repairs = [result_state(h) for h in h4 if h.get("type") == "REPAIR"]
    counted = [r["row_count"] for r in spark.table(f"{FQ}.table_counts").where(f"table_name = '{FQ}.lab_results'").collect()]
    logged = [r["detail"] for r in spark.table(f"{FQ}.run_log").where("event = 'p4_count'").collect()]
    checks = {"job name": r4.get("job_id") in [i for _, i in lesson_jobs(f"{PREFIX}_p4_lab_counts")],
              "first attempt failed": original == ["FAILED"],
              "repaired, same run": len(repairs) >= 1 and result_state(r4) == "SUCCESS",
              "counted lab_results": 6 in counted, "logged the count": "6" in logged}
    ok = all(checks.values())
    PRACTICE_PASSED["P4"] = ok
    print("✅ Passed" if ok else f"Hint: these checks failed: {[k for k, v in checks.items() if not v]}.")
    METRICS["P4.original"], METRICS["P4.repairs"] = ", ".join(map(str, original)), ", ".join(map(str, repairs))
    METRICS["P4.result"], METRICS["P4.logged"] = result_state(r4), ", ".join(logged)
    METRICS["P4.states"] = states_text(r4)
except Exception as e:
    PRACTICE_PASSED["P4"] = False
    print("Hint: store the run id of your job run in p4_run_id.", error_class(e))

# COMMAND ----------

# DBTITLE 1,10 — Exam-Style Questions
# MAGIC %md
# MAGIC ## 10 Exam-Style Questions
# MAGIC
# MAGIC Choose the **one best answer**. Answers and explanations are in section 11.
# MAGIC
# MAGIC **Q1.** A claims job loads a file, then must run the transformation only when the load found at least one row; otherwise it writes a "no data" log. The load task sets the task value `rows`. Which design is best?
# MAGIC A. An If/else task with `GREATER_THAN`, left <code>&#123;&#123;tasks.load.values.rows&#125;&#125;</code>, right `0`, and two tasks that depend on its `true` / `false` outcome · B. An If/else task with `EQUAL_TO`, left <code>&#123;&#123;tasks.load.values.rows&#125;&#125;</code>, right `"0.0"` · C. Two tasks with `run_if: ALL_DONE` · D. Read the value in each downstream notebook and call `dbutils.notebook.exit()` when it is 0
# MAGIC
# MAGIC **Q2.** A five-task nightly job for a hospital data mart failed in task 4 after 70 minutes because of a wrong table name. Tasks 1–3 succeeded. The engineer fixed the table name in the job. What is the cheapest correct action?
# MAGIC A. Run now; all five tasks run again · B. Repair the run: task 4 and the tasks that were skipped because of it run again, in the same run, with the fixed settings · C. Repair the run; it uses the settings from the first attempt, so the fix needs a new run · D. Create a new job with tasks 4 and 5
# MAGIC
# MAGIC **Q3.** Task `find_new_labs` finds the list of new lab files at run time. Task `parse_lab` must run once per file, at most four at a time. What should the engineer configure?
# MAGIC A. A job parameter with all file names, and four copies of `parse_lab` · B. `find_new_labs` sets a task value `files` (a JSON list); a For each task with `inputs` <code>&#123;&#123;tasks.find_new_labs.values.files&#125;&#125;</code>, `concurrency` 4, and the nested task reads <code>&#123;&#123;input&#125;&#125;</code> · C. A For each task inside another For each task · D. `max_concurrent_runs: 4` on the job and four run-now calls
# MAGIC
# MAGIC **Q4.** A job has the job parameter `env = prod`. A notebook task also has the base parameter `env = dev`. Which value does `dbutils.widgets.get("env")` return in that task?
# MAGIC A. `dev`, task parameters are more specific · B. `prod`, the job parameter takes precedence · C. The job fails to start because of the duplicate key · D. Both values as a list
# MAGIC
# MAGIC **Q5.** An engineer wrote the base parameter <code>"detail": "&#123;&#123;tasks.load.value.rows&#125;&#125;"</code> (`value` instead of `values`). What happens?
# MAGIC A. `jobs/create` rejects the job with `INVALID_PARAMETER_VALUE` · B. The job is created and the task gets the text as a literal string · C. The job is created and the task fails at run time · D. The reference is corrected automatically
# MAGIC
# MAGIC **Q6.** A pharmacy job must send an incident row to an audit table only when the load task fails; on success that task must not run. Which `run_if` fits the audit task that depends on the load task?
# MAGIC A. `ALL_DONE` · B. `ALL_SUCCESS` · C. `AT_LEAST_ONE_FAILED` · D. `NONE_FAILED`
# MAGIC
# MAGIC **Q7.** A serverless job appends billing rows with `INSERT INTO`; a retry after a failure in the middle could load rows twice. The engineer wants no automatic retries. What should they set?
# MAGIC A. `max_concurrent_runs: 1` · B. Turn off serverless auto-optimization for the task (`disable_auto_optimization: true`) and keep `max_retries` at 0 · C. `performance_target: STANDARD` · D. `queue.enabled: false`
# MAGIC
# MAGIC **Q8.** Lab result files land in the Unity Catalog volume `/Volumes/main/labs/raw/incoming/`. A job must start soon after new files arrive, without running all the time. Which trigger?
# MAGIC A. Continuous · B. A schedule every minute · C. A file arrival trigger on the volume folder · D. A table update trigger on the volume

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — SCD type** (SCD Type 1: only the latest values; SCD Type 2: every version with `__START_AT` / `__END_AT`, L08 4.1)
# MAGIC {{solution:W1}}
# MAGIC
# MAGIC **W2 — an upsert `MERGE`**
# MAGIC {{solution:W2}}
# MAGIC
# MAGIC **W3 — the latest change per key by the source order** (what `SEQUENCE BY` does; patient 1's newest change by `sequence_num` is `PLAN_B`, although `PLAN_C` arrived last; patient 2's newest change is a delete)
# MAGIC {{solution:W3}}
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. Fix the table name in the job (repairs use the current job and task settings), then **repair** the run with `rerun_all_failed_tasks` (or select task 4): task 4 and the tasks that were skipped because of it run again, inside the same run; tasks 1–3 are not run again (E5: `{{E5.final_states}}`).
# MAGIC 2. `ingest` sets a task value with the JSON list of files; a For each task uses it as `inputs` (<code>&#123;&#123;tasks.ingest.values.files&#125;&#125;</code>) with `concurrency` 3; the nested task gets <code>&#123;&#123;input&#125;&#125;</code> as a base parameter and reads it with `dbutils.widgets.get` (E3: `{{E3.report}}`).
# MAGIC 3. Rejected at save time (E7, all `INVALID_PARAMETER_VALUE`): a `depends_on` to a task that does not exist, a dependency cycle, an `outcome` on a dependency that is not an If/else task, and invalid references inside the known namespaces (`job`, `tasks`), such as a typo `value` for `values`. Only a run shows problems in the data or the code: a table that does not exist (E5 `{{E5.load_error_class}}`), a wrong parameter **value** (P4). Per the docs, references outside the known namespaces and missing braces are also kept as literal text (not tested here).
# MAGIC
# MAGIC ### P1 (blanks: `[{"task_key": "count_labs"}]`, `"ALL_DONE"`, the task value reference)
# MAGIC {{solution:P1}}
# MAGIC
# MAGIC ### P2 (fixes: the task key is `count_labs`, not `count-labs` — E7; `values`, not `value` — 4.3)
# MAGIC {{solution:P2}}
# MAGIC
# MAGIC ### P3
# MAGIC {{solution:P3}}
# MAGIC
# MAGIC ### P4 (repair with new `job_parameters`; a new run-now would be a second run)
# MAGIC {{solution:P4}}
# MAGIC
# MAGIC ### Exam answers
# MAGIC
# MAGIC **Q1 → A.** If/else with a numeric operator and two outcome branches (E1 `{{E1.check_outcome}}`, E4 `{{E4.check_outcome}}`). B wrong: `EQUAL_TO` compares strings, so `"0"` and `"0.0"` differ (*If/else task*). C wrong: `ALL_DONE` runs both tasks after any result. D wrong: both tasks would start (compute) and the run would still show both as run.
# MAGIC
# MAGIC **Q2 → B.** Repair re-runs only unsuccessful tasks and their dependents, in the same run, with the current settings (*Troubleshoot and repair job failures*; E5 `{{E5.history}}`). A wrong: repeats 70 minutes of work. C wrong: repaired tasks "use the current job and task settings". D wrong: unnecessary.
# MAGIC
# MAGIC **Q3 → B.** For each with a task value list and `concurrency` (E3). A wrong: the list is only known at run time. C wrong: "You cannot add another `For each` task as the nested task." D wrong: that runs the whole job four times.
# MAGIC
# MAGIC **Q4 → B.** "if a job parameter and a task parameter share a key, the job parameter takes precedence" (*Configure job parameters*). A, C, D wrong for the same reason.
# MAGIC
# MAGIC **Q5 → A.** E7: the same typo was rejected at create time with `{{E7.typo_ref}}`; `tasks` is a known namespace, and for an invalid reference in a known namespace "an error message is displayed" (*Dynamic value references*). B wrong: literal text is what the docs describe for unknown namespaces and missing braces. C wrong: the job is never created. D wrong: nothing is corrected.
# MAGIC
# MAGIC **Q6 → C.** `AT_LEAST_ONE_FAILED` runs only after a failure; otherwise the task is Excluded (*Configure task dependencies*; E5 `{{E5.alert_detail}}`). A wrong: runs after success too. B and D wrong: they run after success, not after failure.
# MAGIC
# MAGIC **Q7 → B.** Serverless auto-optimization "retries failed tasks"; switch it off for non-idempotent tasks (*Run your Lakeflow Jobs with serverless compute for workflows*); `max_retries` 0 means "never retry" (Jobs API reference). A wrong: limits parallel runs, not retries. C wrong: cost/latency, not retries. D wrong: queueing, not retries.
# MAGIC
# MAGIC **Q8 → C.** File arrival triggers watch a UC volume or external location and start a run when new files arrive (*Trigger jobs when new files arrive*; E7 created one paused: `{{E7.file_arrival}}`). A wrong: continuous keeps a run going all the time. B wrong: runs every minute even without files. D wrong: a volume folder is not a table.

# COMMAND ----------

# DBTITLE 1,12 — Cleanup
# Delete every lesson job (their runs go with them), then the task-code folder, then the schema
for _, job_id in lesson_jobs():
    delete_job(job_id)
try:
    w.workspace.delete(RUNTIME, recursive=True)
except NotFound:
    pass
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
jobs_left = lesson_jobs()
METRICS["cleanup.jobs_left"] = len(jobs_left)
print(f"Deleted the lesson jobs ({len(jobs_left)} left), the folder {RUNTIME} and the schema {FQ}.")

# COMMAND ----------

# DBTITLE 1,13 — Final Summary
# MAGIC %md
# MAGIC ## 13 Final Summary (values from the real run)
# MAGIC
# MAGIC | Example | Key result |
# MAGIC | --- | --- |
# MAGIC | Setup | `encounters_raw` {{setup.raw_rows}} rows, {{setup.day1_rows}} on 2026-10-01; task code: {{setup.runtime_items}} |
# MAGIC | E1 multi-task job | run 1 `{{E1.result}}`: `{{E1.states}}` |
# MAGIC | E2 task values | `rows_reported` = `{{E2.rows_reported}}` ({{E2.rows_reported_type}}), `taskValues.get` = {{E2.rows_from_get}}; <code>&#123;&#123;job.run_id&#125;&#125;</code> = run id: {{E2.job_run_id_is_run_id}} |
# MAGIC | E3 For each | `{{E3.report}}`; {{E3.iterations}} iterations |
# MAGIC | E4 If/else + override | run 2 `{{E4.result}}`, outcome `{{E4.check_outcome}}`; job default still `{{E4.default_after}}` |
# MAGIC | E5 failure + repair | first `{{E5.first_result}}` (`{{E5.load_error_class}}`), after repair `{{E5.final_result}}`; history `{{E5.history}}` |
# MAGIC | E6 history | daily job {{E6.daily_runs}} runs, repair job {{E6.repair_job_runs}} run |
# MAGIC | E7 export / create checks | `{{E7.mistakes}}` |
# MAGIC | Practice | P1 `run_if` {{P1.run_if}} · P2 fixed {{P2.fixed}} · P3 {{P3.checks}} checks passed · P4 original `{{P4.original}}`, repairs `{{P4.repairs}}`, final `{{P4.result}}` |
# MAGIC | Cleanup | lesson jobs left: {{cleanup.jobs_left}} |
# MAGIC
# MAGIC Durations in this run (they change from run to run): run 1 {{nd.E6.run1_secs}} s, run 2 {{nd.E6.run2_secs}} s, run 3 with its repair {{nd.E6.run3_secs}} s.
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **SQL, pipeline, dbt, Run Job, Python wheel and JAR tasks** — not run (compute budget; a SQL task needs a SQL warehouse).
# MAGIC * **Triggers that really start a run** (schedule, file arrival, table update) and **continuous jobs** — E7 only created paused triggers.
# MAGIC * **Notifications that are really sent** and **duration warnings** (`health` rules) — no real destination here; P3 only stores the setting.
# MAGIC * **Retries** (`max_retries` > 0, `min_retry_interval_millis`, `retry_on_timeout`), **timeouts that fire**, and the effect of serverless auto-optimization retries — every lesson task disables auto-optimization and uses `max_retries` 0.
# MAGIC * **Standard performance mode** (4–6 minutes startup per the docs) and **environment dependencies** (`spec.dependencies`) — not run, to save compute.
# MAGIC * **References outside the known namespaces and references with missing braces** — the docs say they are "treated as literal strings"; not tested (E7 only tested references inside `job` and `tasks`).
# MAGIC * **`rerun_tasks` + `rerun_dependent_tasks`, `latest_repair_id` for a second repair, multiple triggers per job (Beta)** — syntax from the Jobs API reference; not run.
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. A typo in a task value reference (<code>&#123;&#123;tasks.load.value.row_count&#125;&#125;</code>) was **rejected** by `jobs/create` (`{{E7.typo_ref}}`). The first expectation, from the docs sentence about "silently ignored" references, was a job that saves and passes literal text at run time (E7).
# MAGIC 2. `runs/get` with `include_resolved_values=true` returned no resolved values for the If/else and notebook tasks (`{{E2.resolved_condition}}`; base parameters found: {{E2.resolved_has_values}}) (E2).
# MAGIC 3. `disable_auto_optimization: true` sent on every task was kept only on the tasks with code (`{{E7.flag_kept}}`); the API dropped it on `{{E7.flag_dropped}}` without an error (E7).
# MAGIC 4. The alert task received <code>&#123;&#123;tasks.load_labs.result_state&#125;&#125;</code> | <code>&#123;&#123;tasks.load_labs.error_code&#125;&#125;</code> as `{{E5.alert_detail}}`: lower case, and a general error code instead of `{{E5.load_error_class}}` (E5).
# MAGIC 5. After the repair, the run's `tasks` list holds both attempts of each repaired task (`{{E5.final_states}}`), and <code>&#123;&#123;task.execution_count&#125;&#125;</code> of `report_labs` was `{{E5.report_execution_count}}` although its first attempt only got `UPSTREAM_FAILED` (E5).
