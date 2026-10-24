# Databricks learning notebooks — rules for Claude Code

Long-term memory of this repo: `_authoring/CURRICULUM.md` (the full plan, sections A–G),
`_authoring/LNN.md` (one spec per lesson), `_authoring/PROGRESS.md` (status + lessons learned).

**Before building or changing lesson NN, read `_authoring/CURRICULUM.md`, `_authoring/LNN.md`,
`_authoring/PROGRESS.md` and the previous lesson notebook. After finishing a lesson, update
`_authoring/PROGRESS.md` (status + lessons learned) and add any new verified serverless limitation to
the Environment section of this file.**

## A. Goal and learner
DBAcademy-style lessons for an experienced data engineer (Snowflake, dbt, Python/Django, Azure,
Tableau; works on healthcare Databricks pipelines, new to Databricks fundamentals). Goal: Databricks
Certified Data Engineer Associate (optional checkpoint after L10), then Professional (outline live
from Oct 9, 2026: S1 Developing code 23%, S2 Ingestion 12%, S3 Data manipulation 12%, S4 Monitoring
& alerting 10%, S5 Cost & performance 15%, S6 Security & compliance 8%, S7 Governance 5%,
S8 Debugging & deploying 10%, S9 Data modeling 5%; 60 scored questions, 120 minutes). Above all:
the learner must be able to write Databricks code without AI. Plain B1/B2 English. Use current names
(Lakeflow Declarative Pipelines, Lakeflow Jobs, Declarative Automation Bundles) and mention the
old name once.

## B. Evidence-only rule (most important)
- No guesses. Every number, output, error class and behaviour claim in markdown comes from a real
  run (_authoring/LNN.json) or an official Databricks doc page (name the page).
- Numbers in Explained cells are ALWAYS placeholders {{key}} filled by the build from METRICS.
  Never type a result number by hand. The build fails if a placeholder has no real value.
- Expected errors: catch with try/except, store the real error class in METRICS
  (e.g. METRICS["E2.error"] = "DELTA_FAILED_TO_MERGE_FIELDS") and reference it with a placeholder.
- Things that cannot be verified here: "NOT VERIFIED — concept only (reason)", listed in the summary.
- Check current syntax in the docs before writing it.
- After a run, read _authoring/LNN.json and make sure every Explained text matches the real
  values (e.g. do not say "fails" if the metric shows it succeeded).

## Notebook rules
- NOTEBOOK FORMAT: the learner notebook is Jupyter .ipynb ONLY ("Databricks - Learning/LNN - <title>.ipynb",
  the same shape as L01: Databricks cell metadata application/vnd.databricks.v1+cell with showTitle
  true and a short title per cell; markdown cells are normal markdown cells; notebook language python;
  committed WITHOUT outputs). The build produces it. Decided 2026-10-22: the SOURCE stays Databricks
  .py text in _authoring/LNN.py (simpler BLANK/SOLUTION blocks, placeholders and diffs); the learner
  never sees it.
- Default language Python. %sql only for single-statement cells (first line of the cell).
- Every result an Explained cell talks about must be visible: display(spark.sql(...)).
- Every example also records its key numbers: METRICS["E3.after_insert"] = cnt, and asserts them
  where the value is known from the setup data.
- Each cell has a title line (# DBTITLE 1,<title>).
- One lesson = one notebook. No helper notebooks.
- METRICS[...] / WARMUP_PASSED[...] / PRACTICE_PASSED[...] lines are removed from the learner notebook:
  keep each on one line and only record values (store stream/write results in a variable first).
- A cell that must fail (e.g. a stream that stops on a new column): first code line
  `# EXPECT_FAILURE: <key>`. The learner sees the real error; test runs record METRICS["<key>.errors"].

## C. Environment
- Serverless. Catalog main, schema main.dbacademy_lNN, volume /Volumes/main/dbacademy_lNN/data.
- The workspace default catalog is `bunyod`: use fully qualified names or USE CATALOG main.
- Setup is idempotent (DROP SCHEMA ... CASCADE, recreate). Fake healthcare data only, no PHI.
- Use rng = random.Random(42). Never use the global random.seed() (Spark calls consume it and counts
  change — broke L01 once). For big data use spark.range with deterministic hash/modulo expressions.
- Predictive optimization is ON and cannot be disabled: extra OPTIMIZE versions, automatic
  compaction, possible ConcurrentWriteException. Read the `operation` column, never assume version
  numbers, retry where needed, and use "nd." keys for values that can change between runs.
- dbutils.fs for files, not shutil. Inside foreachBatch use batch_df.sparkSession.
- Verified serverless facts (L01): no global temp views (NOT_SUPPORTED_WITH_SERVERLESS); streaming
  only with trigger(availableNow=True) and an explicit checkpoint; RESTORE syntax is
  TO VERSION AS OF n and can race with auto-OPTIMIZE; CREATE OR REPLACE TABLE keeps history;
  UNDROP restores a dropped managed table with history; TIMESTAMP AS OF before the first commit
  fails with DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION; saveAsTable(overwrite) shows as
  CREATE OR REPLACE TABLE AS SELECT; UPDATE/DELETE use deletion vectors.
- Verified (L06, 2026-10-21): a stream that fails inside a notebook cell fails the cell and the job
  run even inside try/except ("Some streams terminated before this command could finish!") — use
  `# EXPECT_FAILURE`; Auto Loader addNewColumns stops with UNKNOWN_FIELD_EXCEPTION and a restart
  with the same code fails with DELTA_METADATA_MISMATCH until the writer has mergeSchema; Auto
  Loader infers all JSON/CSV columns as strings without cloudFiles.inferColumnTypes; COPY INTO
  returns num_affected_rows, num_inserted_rows, num_skipped_corrupt_files.
- Verified (L04, 2026-10-22): serverless job notebooks run the PySpark client 3.5.0 on Python 3.11;
  `df.cache()`/`persist()` fail with CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC, `CACHE TABLE` and
  `spark.catalog.cacheTable` with NOT_SUPPORTED_WITH_SERVERLESS; `spark.sql.execution.pythonUDF.arrow.enabled`
  is false; a Python UDF whose value does not fit its returnType gives NULL without an error;
  `SHOW USER FUNCTIONS IN main.<schema>` fails with CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED (query
  `main.information_schema.routines`); `groupBy("name")` on a column made by a UC SQL function fails with
  CANNOT_RESOLVE_DATAFRAME_COLUMN (`groupBy(F.col("name"))` works); Python/Pandas UDFs fail through local
  Databricks Connect (sandbox startup), so test them only in job runs.
- Verified (L05, 2026-10-22): ANSI mode is on, so `arr[i]` out of bounds (also `filter(...)[0]` on an empty
  array) fails with INVALID_ARRAY_INDEX — use `get(arr, i)` / `try_element_at`; counting rows that use only
  `_corrupt_record` from JSON files fails with UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN and the
  usual `cache()` fix is unavailable (select another column or save a table); VARIANT cannot be a CLUSTER BY
  key (DELTA_CLUSTERING_COLUMNS_DATATYPE_NOT_SUPPORTED); the 3.5.0 job client still has `F.parse_json` and
  collects VARIANT values as VariantVal.
- Verified (L06 rebuild, 2026-10-22): `CREATE OR REFRESH STREAMING TABLE` from serverless notebook compute fails with
  STREAMING_TABLE_OPERATION_NOT_ALLOWED.ST_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE (build streaming tables in a
  Lakeflow pipeline); `spark.sql("SELECT … FROM STREAM read_files(…)")` is a streaming DataFrame but needs
  `schemaLocation => '…'` (else CLOUDFILES_SCHEMA_LOCATION_REQUIRED, the same error as Auto Loader without
  `cloudFiles.schemaLocation`); Auto Loader errors at stream start (that one, DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES
  for a wrong format) are raised by `toTable()` and can be caught with try/except; `_metadata.file_path` and the
  binaryFile `path` start with `dbfs:/Volumes/…`; a `COPY INTO` rerun that loads no files writes no table version.
- Verified (L07, 2026-10-22): Lakeflow pipelines run fine when created from a serverless job notebook via the Pipelines REST
  API (`w.api_client.do`; the preinstalled databricks-sdk is 0.40.0 and its typed `pipelines.create` lacks `root_path`);
  source files are plain workspace files (`w.workspace.upload(..., format=ImportFormat.AUTO)`); `event_log('<id>')` works
  from the notebook as the run-as user; Free Edition: at most two lesson pipelines at once (a third fails with
  RESOURCE_EXHAUSTED, then `event_log()` gives EVENT_LOG_UNAVAILABLE — use the events API); FAIL UPDATE ->
  EXPECTATION_VIOLATION.VERBOSITY_ALL, update FAILED, the flow writes 0 rows, a parallel MV completes; a streaming table
  without STREAM -> CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED (a dry run finds it); deleting a pipeline keeps
  its tables; full refresh shows `DLT REFRESH` in DESCRIBE HISTORY.
- Free Edition compute (checked 2026-10-23 in system.billing.usage + system.access.audit): the limit is serverless
  compute, not storage ("You've hit the limit for serverless compute for free usage"); no number is published
  (fair usage policy). In 12 days GENIE (Genie Code in the browser UI) used ~389 DBU, all lesson builds ~23 DBU.
  Serverless auto-optimization retried a failed one-time lesson run (doubling its compute): run_lesson.py now
  submits the task with `disable_auto_optimization: true`. Build lessons when nothing else uses compute.
- Verified (L08, 2026-10-24): AUTO CDC SQL `TRACK HISTORY ON specialty` works without parentheses (with
  `(specialty)` the pre-test failed with PARSE_SYNTAX_ERROR); a pipeline publishes its event log as a MANAGED table
  `event_log_<pipeline id with _>` in the target schema without the `event_log` setting; `event_log()` read right after an
  update is COMPLETED can still show flows `RUNNING` with 0 metrics (poll until every flow has a final status); AUTO CDC flows
  report `num_upserted_rows`/`num_deleted_rows` (a Python flow and a FROM SNAPSHOT flow are named after their target; FROM
  SNAPSHOT reports 0), plus `flow_progress` events named `missingFlowName` without a status; `__START_AT` has the SEQUENCE BY
  type (timestamp) or bigint for snapshot versions.
- Workspace limits: the public DBFS root is disabled (use /Volumes only); no storage credential or
  external location of our own (external tables, s3://, abfss://, gs:// sources, file-notification
  mode are NOT VERIFIED — concept only).

## D. Standard lesson structure (numbered, no gaps, every cell titled)
1 Title (exam sections, 5 learning objectives: Use / Explain / Compare / Implement / Diagnose) ·
2 Warm-up (3 recall tasks from the previous lesson, no hints, validation sets WARMUP_PASSED) ·
3 Setup (asserted real counts) · 4 Concepts (what / why / when vs alternative table / syntax SQL +
PySpark) · 5 Examples (Predict → code with every line commented, stores METRICS → Explained with
placeholders only) · 6 Exam traps (12+ rows, Trap | Truth) · 7 Cheat sheet (SQL ↔ PySpark) ·
8 Check your understanding (3) · 9 Practice P1 fill-in (3 blanks), P2 fix-the-bug (2 bugs), P3 from
scratch with hints, P4 blind; validation cells print ✅ Passed or a specific hint and set
PRACTICE_PASSED · 10 Exam questions (8, healthcare scenarios, A–D, each verifiable by an example or
a named doc page) · 11 Solutions (generated from SOLUTION blocks + written answers, why each wrong
option is wrong) · 12 Cleanup (schema, jobs, pipelines, files) · 13 Final summary (placeholders,
NOT VERIFIED list, surprises).

## E. Toolchain and workflow
- Source of truth: _authoring/LNN.py (Databricks source format). Practice cells use
  `# >>> BLANK P1` … `# >>> SOLUTION P1` … `# <<< END P1` blocks (ids W1–W3, P1–P4).
- `python _authoring/build_notebook.py LNN --mode solved|learner`: solved → _authoring/build/LNN_solved.py
  (SOLUTION parts, state cell, assert cell that all WARMUP_PASSED/PRACTICE_PASSED are True, final
  dbutils.notebook.exit(json.dumps(METRICS))); learner → "Databricks - Learning/LNN - <title>.ipynb"
  (BLANK parts, {{key}} filled from _authoring/LNN.json, Solutions generated from SOLUTION blocks,
  test lines removed, no outputs). The learner build refuses a run record from older code.
- `python _authoring/run_lesson.py LNN [--twice] [--publish]`: imports the solved build to
  "<home>/_lesson_build/" (outside the workspace Git folder), runs it as a one-time serverless
  notebook run, saves _authoring/build/runs/LNN_<timestamp>.json, and _authoring/LNN.json only on
  success. --twice runs twice and diffs METRICS except "nd." keys. --publish builds the learner
  notebook; it reaches the workspace through git (push, then Pull in the Git folder).
  Check every CLI flag with --help before using it. Use the .venv (Python 3.12) interpreter.
- Workflow per lesson:
  1. Read _authoring/CURRICULUM.md, _authoring/LNN.md, _authoring/PROGRESS.md and the previous lesson.
  2. Write _authoring/LNN.py in stages: setup → examples → rest. After each stage:
     build solved → run → read results → fix. Do not continue on a failed run.
  3. Final: run with --twice; all asserts pass and METRICS are identical except "nd." keys.
  4. Build learner (--publish). Update _authoring/PROGRESS.md and this file. Commit _authoring/LNN.*
     and the notebook in "Databricks - Learning/".
  5. Report: final METRICS, NOT VERIFIED items, surprises, and suggested PR title + description.
- The learner then does the practice by hand without AI before the next lesson starts.

## Lesson index (details in _authoring/CURRICULUM.md section G and _authoring/LNN.md)
- L01 Lakehouse Basics: UC namespace, notebooks, first Delta tables, DML, history, time travel — legacy, done
- L02 Delta Lake Deep Dive: schema, constraints, table types, clones, maintenance — legacy, done
- L03 Spark SQL Transformations: joins, aggregations, window functions
- L04 PySpark DataFrame API, reusable transforms, UDFs
- L05 Semi-structured data: JSON, nested types, VARIANT
- L06 Ingestion: read_files, COPY INTO, Auto Loader, Parquet, `_metadata`, binaryFile — done (rebuilt to the plan 2026-10-22)
- L07 Lakeflow Declarative Pipelines I: medallion, streaming tables, MVs, expectations — done
- L08 LDP II: AUTO CDC, SCD Type 1 / Type 2 — done
- L09 Lakeflow Jobs: tasks, parameters, control flow, repair
- L10 Unity Catalog governance
- L11 Structured Streaming fundamentals — legacy notebook kept; rebuild per spec
- L12 Streaming advanced: joins, Delta source options, state, checkpoint compatibility
- L13 Delta optimization: liquid clustering, CLUSTER BY AUTO, deletion vectors, data skipping
- L14 Change Data Feed and incremental processing
- L15 Testing data pipelines
- L16 Monitoring and alerting
- L17 Security and compliance (PHI patterns, fake data)
- L18 Data modeling for analytics
- L19 Performance troubleshooting
- L20 Advanced ingestion and sharing
- L21 Declarative Automation Bundles and CI/CD
- L22 Mock exam (Professional)
