# A. Goal and learner
- Learner: experienced data engineer (Snowflake, dbt, Python/Django, Azure, Tableau), works on
  healthcare Databricks pipelines, new to Databricks fundamentals.
- Goal: pass Databricks Certified Data Engineer Associate (optional checkpoint after L10), then
  Professional (new outline, live from Oct 9, 2026). Above all: write Databricks code WITHOUT AI.
- Format: DBAcademy-style notebooks: explanation → examples → hands-on practice.
- Plain B1/B2 English. Current product names (Lakeflow Declarative Pipelines, Lakeflow Jobs,
  Declarative Automation Bundles); mention the old name once.
- Professional exam sections and weights: S1 Developing code 23%, S2 Ingestion 12%,
  S3 Data manipulation 12%, S4 Monitoring & alerting 10%, S5 Cost & performance 15%,
  S6 Security & compliance 8%, S7 Governance 5%, S8 Debugging & deploying 10%, S9 Data modeling 5%.
  60 scored questions, 120 minutes.

# B. Evidence-only rule (most important)
- No guesses. Every number, output, error class and behaviour claim in markdown comes from a real
  run of the notebook, or from an official Databricks doc page (name the page).
- Numbers in Explained cells are placeholders {{key}} filled from the run's METRICS JSON. Never type
  a result number by hand. The build fails if a placeholder has no real value.
- Expected errors: catch with try/except, store the real error class in METRICS, quote it.
- Not verifiable here (compute or permissions): "NOT VERIFIED — concept only (reason)", listed in
  the final summary.
- Check current syntax in the docs before writing it.
- After each run, read the results and make sure every sentence matches them.

# C. Environment
- NOTEBOOK FORMAT: Jupyter .ipynb ONLY, for source, solved and learner notebooks. Never .py source
  format. Use Databricks cell metadata like Lesson 1 (application/vnd.databricks.v1+cell with
  showTitle true and a short title per cell). SQL cells start with %sql as the first line; markdown
  cells are normal markdown cells. Notebook metadata language = python.
  Learner notebooks are committed WITHOUT outputs (clear outputs before commit).
- Serverless compute. Default language Python. %sql only for single-statement cells; every result an
  Explained cell mentions must be visible with display(spark.sql(...)).
- Workspace folder "<home>/Databricks - Learning/", notebook name "LNN - <title>".
- Catalog main, schema main.dbacademy_lNN, volume /Volumes/main/dbacademy_lNN/data.
- Workspace default catalog is `bunyod`, not `main`: use fully qualified names or USE CATALOG main.
- Setup idempotent (DROP SCHEMA ... CASCADE, recreate). Fake healthcare data only — never PHI.
- Deterministic data: rng = random.Random(42); never the global random.seed() (Spark calls consume
  it and counts change — broke L01 once). Large data: spark.range + hash/modulo expressions.
- Predictive optimization is ON and cannot be disabled: extra OPTIMIZE versions, automatic
  compaction, possible ConcurrentWriteException. Read the `operation` column, never assume version
  numbers, retry where needed, prefix values that can vary between runs with "nd.".
- Verified serverless facts (L01): no global temp views (NOT_SUPPORTED_WITH_SERVERLESS); streaming
  only with trigger(availableNow=True) and an explicit checkpoint; RESTORE syntax is
  TO VERSION AS OF n and can race with auto-OPTIMIZE; CREATE OR REPLACE TABLE keeps history;
  UNDROP restores a dropped managed table with history; TIMESTAMP AS OF before the first commit
  fails with DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION; saveAsTable(overwrite) shows as
  CREATE OR REPLACE TABLE AS SELECT; UPDATE/DELETE use deletion vectors.
- dbutils.fs for files, not shutil. Inside foreachBatch use batch_df.sparkSession.

# D. Standard lesson structure (numbered sections, no gaps, every cell titled)
1. Title: lesson name, exam sections, 5 learning objectives (Use / Explain / Compare / Implement / Diagnose).
2. Warm-up: 3 recall tasks from the previous lesson, no hints; validation sets WARMUP_PASSED.
3. Classroom Setup: real counts with asserts; defines METRICS = {}, PRACTICE_PASSED = {}, WARMUP_PASSED = {}.
4. Concepts: what it is / why it exists / when vs alternative (table) / minimal syntax SQL + PySpark.
5. Examples: Predict (2–3 questions) → code (every line commented, stores METRICS) →
   Explained (placeholders only).
6. Exam traps: 12+ rows (Trap | Truth), consistent with the examples.
7. Cheat sheet: SQL ↔ PySpark side by side + quick reference.
8. Check your understanding: 3 open questions.
9. Practice: P1 fill-in (3 blanks), P2 fix-the-bug (2 bugs), P3 from scratch with hints,
   P4 BLIND (task text only). Validation cells print ✅ Passed or a specific hint and set PRACTICE_PASSED.
10. Exam questions: 8 scenario questions (healthcare context), A–D, each verifiable by an example
    or a named doc page.
11. Solutions: warm-up, section 8 answers, P1–P4 code (generated from the SOLUTION blocks), exam
    answers with why each wrong option is wrong.
12. Cleanup: drops everything the lesson created (schema, jobs, pipelines, files).
13. Final summary: key numbers (placeholders), NOT VERIFIED list, surprises vs first expectation.

# E. Toolchain and workflow
- lessons/src/LNN.ipynb is the source of truth. Practice code cells contain:
    # >>> BLANK P1
    ...learner code with <FILL_IN>...
    # >>> SOLUTION P1
    ...solution code...
    # <<< END P1
- tools/build_notebook.py LNN --mode solved|learner (reads and writes .ipynb JSON with nbformat):
  solved → build/LNN_solved.ipynb: keeps SOLUTION parts, appends an assert cell (all PRACTICE_PASSED
  and WARMUP_PASSED are True) and a final cell dbutils.notebook.exit(json.dumps(METRICS, default=str)).
  learner → notebooks/LNN - <title>.ipynb: keeps BLANK parts, fills {{key}} placeholders from
  runs/LNN_latest.json (fails if one is missing), generates Solutions code from SOLUTION blocks,
  clears all outputs.
- tools/run_lesson.py LNN [--twice] [--publish]: imports build/LNN_solved.ipynb to
  "<home>/Databricks - Learning/_build/" (databricks workspace import, JUPYTER format), runs it as a
  one-time serverless notebook run, saves metrics or the full error to runs/LNN_<timestamp>.json
  (runs/LNN_latest.json only on success). --twice runs two times and diffs METRICS except "nd." keys.
  --publish imports the learner notebook to "<home>/Databricks - Learning/". Check every CLI flag
  with --help before using it.
- Workflow per lesson: read spec + previous lesson → build in stages (setup → examples → rest),
  after each stage build solved → run → read results → fix; never continue on a failed run →
  final run with --twice → build learner → publish → update PROGRESS.md and CLAUDE.md →
  commit (src, learner notebook, spec, runs/LNN_latest.json) → report final METRICS, NOT VERIFIED
  items, surprises, and a suggested PR title + description.
- The learner then does the practice by hand without AI before the next lesson starts.

# F. Progress
| # | Lesson | Status |
| L01 | Lakehouse Basics | DONE (built with Genie Code, .ipynb, pushed) |
| L02 | Delta Lake Deep Dive | IN PROGRESS |
| L03–L22 | see section G | PLANNED |

# G. Lessons (each becomes lessons/specs/LNN.md)

## L01 — Lakehouse Basics: UC namespace, notebooks, first Delta tables — DONE
Topics: catalog.schema.table, USE/SHOW, magic commands, dbutils.fs, widgets, %run concept,
temp vs global temp view, compute types, Delta basics, CTAS/INSERT/UPDATE/DELETE/MERGE (3 branches,
SQL + DeltaTable API), DESCRIBE HISTORY/DETAIL, VERSION/TIMESTAMP AS OF, RESTORE, INSERT OVERWRITE vs
CREATE OR REPLACE vs DROP+CREATE vs UNDROP.

## L02 — Delta Lake Deep Dive: schema, constraints, table types, clones, maintenance
Exam: Associate; Pro S5, S9 basics. Warm-up: MERGE 3 branches; time travel to first version; RESTORE.
Data: patients 50, encounters 300, lab_results 500 (some NULL patient_id, some negative result_value).
Concepts: _delta_log commits/checkpoints; schema enforcement vs evolution (mergeSchema vs
overwriteSchema, autoMerge, ALTER ADD/RENAME/DROP COLUMN, column mapping, MERGE WITH SCHEMA
EVOLUTION, type widening); NOT NULL / CHECK; PK/FK informational; generated, identity (ALWAYS vs BY
DEFAULT), DEFAULT; managed vs external, DROP effect, UNDROP; SHALLOW vs DEEP CLONE; OPTIMIZE, ZORDER
(brief), VACUUM (retention, DRY RUN, time travel effect), deletion vectors + REORG PURGE,
TBLPROPERTIES, predictive optimization.
Examples: E1 _delta_log listing (or real error + DESCRIBE HISTORY); E2 extra column append error →
mergeSchema; E3 type mismatch error, type widening; E4 RENAME without/with column mapping; E5 CHECK
with existing bad rows → error → clean → add → violating insert error; NOT NULL; E6 generated /
identity / DEFAULT, insert into ALWAYS identity error; E7 MERGE WITH SCHEMA EVOLUTION; E8 DROP/UNDROP;
external table with LOCATION on a volume (record result); E9 shallow vs deep clone, update clone,
source unchanged; E10 20 small appends → OPTIMIZE metrics; E11 VACUUM DRY RUN, RETAIN 0 HOURS
behaviour, retention check config on serverless; E12 deletion vectors + REORG PURGE.
Practice: P1 mergeSchema + ADD COLUMN + CHECK; P2 identity bug + VACUUM/time travel order bug;
P3 lab_results_clean with constraints + generated column, DEEP CLONE backup, OPTIMIZE; P4 blind:
shallow clone dev copy, MERGE WITH SCHEMA EVOLUTION, source unchanged, DROP/UNDROP clone.

## L03 — Spark SQL Transformations: joins, aggregations, window functions
Exam: Pro S3. Warm-up: mergeSchema; CHECK after cleanup; shallow clone.
Data: patients 200, providers 20, encounters 2000, claims 1500 (duplicate claim_ids with later
updated_at), lab_results 3000, diagnosis_codes; NULL keys and ties on purpose.
Concepts: SQL evaluation order; all joins incl. SEMI/ANTI/CROSS, NULL keys, <=>; GROUP BY/HAVING,
FILTER clause, COUNT variants, approx_count_distinct, GROUPING SETS/ROLLUP/CUBE; ROW_NUMBER/RANK/
DENSE_RANK, LAG/LEAD, running totals, ROWS vs RANGE default frame, QUALIFY; CTEs; set operations;
PIVOT/UNPIVOT; higher-order functions; dates; CASE; NULL functions.
Examples: anti join vs NOT IN with NULL; semi join; NULL-key joins vs <=>; QUALIFY dedup; LAG days
between visits; running total default vs ROWS frame with ties; rank functions on ties; ROLLUP;
PIVOT/UNPIVOT; collect_list + transform/filter/aggregate; UNION vs UNION ALL; FILTER vs CASE.
Practice: P1 window blanks; P2 NOT IN + NULL bug, wrong frame bug; P3 30-day readmission flag;
P4 blind: latest lab per patient per test + change from previous.

## L04 — PySpark DataFrame API, reusable transforms, UDFs
Exam: Pro S1 (Pandas/Python/SQL/UC UDFs), S3. Warm-up: QUALIFY dedup; anti join; LAG.
Concepts: DataFrame vs SQL (Catalyst); lazy evaluation; col/expr/selectExpr/when/withColumns;
join API + broadcast hint; groupBy/agg; Window API; DataFrame.transform; StructType vs DDL;
explain(); caching on serverless (verify); Python UDF, Pandas UDF, SQL UDF, UC Python UDF;
built-ins first.
Examples: 3 L03 queries in PySpark; lazy evaluation proof; explain with/without broadcast; transform
chain; timing built-in vs Python UDF vs Pandas UDF on 5M rows (run twice, "nd." keys); wrong
returnType → silent NULLs; SQL UDF age_group; UC Python function + DESCRIBE FUNCTION EXTENDED.
Practice: P1 withColumns/when blanks; P2 when without otherwise, returnType mismatch; P3 cleaning
module of 4 functions with transform(); P4 blind: UC SQL function + Pandas UDF in one query.

## L05 — Semi-structured data: JSON, nested types, VARIANT
Exam: Pro S3 (VARIANT), S2. Warm-up: Window dedup in PySpark; SQL UDF; transform().
Data: FHIR-like Patient/Observation JSON files (nested arrays, optional fields, mixed shapes,
malformed records) + a bronze table with a raw JSON STRING column.
Concepts: STRUCT/ARRAY/MAP; dot notation; colon notation on JSON strings and its case rules
(verify); from_json, schema_of_json, to_json; explode/explode_outer/posexplode/inline; VARIANT:
parse_json, try_parse_json, variant_get, colon paths, schema_of_variant, variant_explode (verify);
STRING vs STRUCT vs VARIANT choice; bad records.
Examples: nested JSON to structs; explode observations; explode vs explode_outer counts; colon
extraction; from_json with schema; VARIANT + variant_get; malformed handling; schema_of_variant;
flatten to silver patients + observations.
Practice: P1 colon/explode blanks; P2 explode drops patients, wrong path; P3 FHIR → 2 silver tables;
P4 blind: same with VARIANT.

## L06 — Ingestion: read_files, COPY INTO, Auto Loader
Exam: Pro S1 (Auto Loader), S2. Warm-up: explode_outer; colon extraction; parse_json.
Data: landing files in batches: claims CSV (batch 2 new column, batch 3 type mismatches),
encounters JSON, labs Parquet.
Concepts: spark.read vs read_files; formats (CSV, JSON, Parquet, Avro, ORC, text, binaryFile, XML —
verify); COPY INTO idempotency, mergeSchema, FORCE; Auto Loader: schemaLocation, inference,
inferColumnTypes, schemaHints, schemaEvolutionMode (addNewColumns/rescue/failOnNewColumns/none),
_rescued_data, listing vs notification, availableNow, checkpoint; _metadata columns; streaming
tables with read_files in SQL.
Examples: read_files CSV; COPY INTO twice (second loads 0); only new files loaded; Auto Loader first
run; new column under addNewColumns (real stop/restart); rescue mode; type mismatches into
_rescued_data; _metadata columns; binaryFile listing.
Practice: P1 cloudFiles option blanks; P2 missing schemaLocation, wrong format; P3 bronze claims with
Auto Loader + rescued data + file metadata; P4 blind: COPY INTO version + idempotency proof.

## L07 — Lakeflow Declarative Pipelines I: medallion, streaming tables, MVs, expectations
Exam: Pro S1, S3 (expectations/quarantine), S4 (event log). Warm-up: COPY INTO; Auto Loader;
_rescued_data.
Special: pipeline code cannot run in a normal notebook cell. Create pipeline source files (SQL and
Python, current pyspark.pipelines API; mention legacy `import dlt`) and a serverless pipeline with
the CLI/SDK, run it, and verify through tables and event_log() from the solved notebook.
Concepts: declarative vs imperative; flows/datasets; streaming table vs MV vs temporary view;
SQL and Python syntax; expectations (warn, DROP ROW, FAIL UPDATE); quarantine; triggered vs
continuous; dev vs prod mode; refresh vs full refresh; incremental MV refresh; event log metrics;
Structured Streaming vs Declarative Pipelines choice.
Examples: bronze → silver → gold MV; event log expectation counts; new batch → streaming table
processes only new files; source update → streaming table vs MV behaviour; quarantine table;
FAIL UPDATE in event log; full refresh effect; Python version of silver.
Practice: P1 expectation blanks; P2 missing STREAM, wrong ON VIOLATION; P3 gold MV + quarantine;
P4 blind: silver layer in Python.

## L08 — LDP II: AUTO CDC, SCD Type 1 / Type 2
Exam: Pro S1, S2. Warm-up: ST vs MV choice; DROP ROW; event log query. Special: same as L07.
Data: CDC feeds (patient demographics, provider specialty) with operation, sequence_num, event_ts;
out-of-order events, duplicates, truncate-like event.
Concepts: CDC basics; MERGE vs AUTO CDC (formerly APPLY CHANGES); keys, sequence by, apply as
deletes/truncates, column lists, SCD type 1 vs 2, __START_AT/__END_AT, track history columns;
AUTO CDC FROM SNAPSHOT (verify); current syntax from docs.
Examples: SCD1; out-of-order resolved; deletes; SCD2 history + current rows; track history on some
columns; duplicates; manual SCD2 MERGE in a normal notebook compared with AUTO CDC.
Practice: P1 AUTO CDC blanks; P2 wrong sequence column, missing deletes; P3 SCD2 provider
specialty; P4 blind: manual SCD2 MERGE without a pipeline.

## L09 — Lakeflow Jobs: tasks, parameters, control flow, repair
Exam: Pro S1, S4, S8. Warm-up: SCD1 vs SCD2; MERGE upsert; sequence-by purpose.
Special: create task notebooks in "Databricks - Learning/L09_tasks", create/run jobs with the
Databricks SDK from the notebook, poll runs, delete jobs in Cleanup; CLI equivalents as text.
Concepts: task types (notebook, script, SQL, pipeline, run job, If/else, For each); dependencies and
run-if; job vs task parameters; dynamic value references; task values; triggers (schedule, file
arrival, table update, continuous); retries/timeouts/concurrency; notifications; serverless
environments, dependencies, performance mode; repair run; run with overrides; API/CLI/SDK.
Examples: 3-task job; task values; If/Else on row count; For Each over practices; forced failure +
repair; parameter override; run history and durations via SDK; job JSON export.
Practice: P1 job definition blanks; P2 wrong depends_on, wrong dynamic reference; P3 For Each job with
notifications; P4 blind: create, run, fail, repair with SDK.

## L10 — Unity Catalog governance
Exam: Pro S6, S7. Warm-up: task values; repair run; For Each use case.
Special: first check SHOW GRANTS, current_user(), is_account_group_member(); anything needing another
principal or admin rights is concept only with the real error.
Concepts: securable hierarchy; privileges (USE CATALOG/SCHEMA, SELECT, MODIFY, CREATE TABLE,
READ/WRITE VOLUME, EXECUTE, MANAGE, ALL PRIVILEGES); ownership; inheritance incl. future objects;
GRANT/REVOKE/SHOW GRANTS; dynamic views; row filters, column masks; tags, comments;
information_schema; lineage system table; workspace-catalog binding (concept).
Examples: SHOW GRANTS levels; GRANT + inheritance; row filter by practice; mask on a fake SSN column;
dynamic view; tags/comments + information_schema; lineage query.
Practice: P1 GRANT blanks; P2 wrong mask return type, missing USE SCHEMA; P3 row filter + mask on
claims; P4 blind: tagged, documented, masked view.

## L11 — Structured Streaming fundamentals (rebuild of the learner's earlier notebook)
Exam: Pro S1. Warm-up: GRANT + SHOW GRANTS; row filter; COMMENT ON.
Topics: readStream/writeStream, availableNow, output modes, watermarks + windows, foreachBatch +
MERGE, checkpoint recovery, dropDuplicates vs dropDuplicatesWithinWatermark.
Required fixes vs the old notebook: write the source as several commits + maxFilesPerTrigger=1 so the
watermark REALLY drops late rows (watermark moves only between micro-batches); explain why the last
windows are not emitted in append mode; demos that change a source use their own copy; foreachBatch
MERGE keeps sum + count, is idempotent (txnAppId/txnVersion), uses batch_df.sparkSession;
update mode allowed without aggregation, complete needs aggregation, Delta sink supports append and
complete only; dedup labs use event_id, never event_time; SQL WATERMARK ... DELAY OF exists;
AUTO CDC is the pipeline alternative to foreachBatch MERGE.

## L12 — Streaming advanced: joins, Delta source options, state, checkpoint compatibility
Exam: Pro S1. Warm-up: watermark window; foreachBatch MERGE; checkpoint restart.
Data: vital_sign_events in 5 commits with late rows; alert_events; nurse_ack_events (within 15 min,
after 15 min, missing); static patients.
Examples: maxFilesPerTrigger/maxBytesPerTrigger with recentProgress; stream-static join (static side
re-read each batch — prove with an update); stream-stream inner join with watermarks + time range;
left outer join NULL rows only after the watermark (multiple runs); UPDATE/DELETE commit error →
skipChangeCommits; checkpoint compatibility (filter change OK, groupBy key change error);
lastProgress/stateOperators/state size; state reader if supported. Concept only: RocksDB,
transformWithState. CDF lives in L14.

## L13 — Delta optimization: liquid clustering, CLUSTER BY AUTO, deletion vectors, data skipping
Exam: Pro S5, S9. Warm-up: stream-static join; skipChangeCommits; incompatible checkpoint change.
Data: encounters_big via spark.range (20M+ rows, skewed practice_id).
Measurement: files read/pruned per query from system.query.history (verify columns/access) or query
profile; else DESCRIBE DETAIL + EXPLAIN and state what could not be measured.
Concepts: file stats and data skipping (dataSkippingNumIndexedCols / StatsColumns); partitioning;
ZORDER (legacy); liquid clustering, changing keys, incremental OPTIMIZE; CLUSTER BY AUTO; deletion
vectors vs MERGE write amplification; predictive optimization; managed tables; disk cache (verify).
Examples: none vs partitioned vs liquid; files read per filter; high-cardinality partition problem;
ALTER CLUSTER BY + OPTIMIZE; CLUSTER BY AUTO; MERGE with/without deletion vectors; stats columns;
repeated query timing ("nd.").
Practice: P1 CLUSTER BY/OPTIMIZE blanks; P2 high-cardinality partition, ZORDER on liquid table;
P3 justify + prove a claims fact layout; P4 blind: partitioned → liquid migration.

## L14 — Change Data Feed and incremental processing
Exam: Pro S5, S1. Warm-up: CLUSTER BY change; deletion vectors; data skipping fact.
Concepts: enabling CDF (only later changes); _change_type values, _commit_version/_timestamp;
table_changes(); readChangeFeed stream + startingVersion; silver → gold propagation; idempotent
foreachBatch (txnAppId/txnVersion); dedup before MERGE; CDF vs skipChangeCommits; retention/VACUUM;
CDF vs AUTO CDC.
Examples: table_changes after UPDATE/DELETE/MERGE; changes before enabling not available; CDF stream
into gold with foreachBatch MERGE (postimage/insert/delete only); multiple-match MERGE error → dedup;
retry the same batch to prove idempotency; incremental aggregate for changed keys; retention effect.
Practice: P1 table_changes blanks; P2 multiple-match MERGE, preimage applied; P3 incremental gold;
P4 blind: CDF stream + idempotent MERGE.

## L15 — Testing data pipelines
Exam: Pro S1. Warm-up: table_changes; dedup before MERGE; _change_type question.
Special: mini project in the repo under lessons/L15_project (src/transforms.py, tests/, conftest.py),
synced to the workspace; run pytest from the notebook and locally with Databricks Connect.
Concepts: pure transforms + DataFrame.transform; assertDataFrameEqual (row order, tolerances),
assertSchemaEqual; pytest fixtures; test data builders; integration tests on a test schema; testing
MERGE; notebook debugger (concept); Databricks Connect.
Examples: refactor into functions; assert pass/fail diff; schema nullability diff; pytest with one
failing test then fix; integration MERGE test; parametrized edge cases.
Practice: P1 assert blanks; P2 row-order dependence, wrong expected schema; P3 tests for 3 transforms;
P4 blind: test an SCD1 MERGE function.

## L16 — Monitoring and alerting
Exam: Pro S4. Warm-up: assertDataFrameEqual; transform(); pytest.
Special: first check which system tables are readable; others are concept only with the real error.
Concepts: system.billing.usage + list_prices, system.compute.*, system.lakeflow.*,
system.access.audit, system.query.history; pipeline event log; SDK/REST/CLI for runs; Lakehouse
alerts (verify current features); job notifications; query profile / Spark UI (concept on serverless).
Examples: DBU cost by job/warehouse; most expensive workloads; failed runs + durations; table access
audit; slowest queries; pipeline data quality trend; SDK recent failures; data quality alert.
Practice: P1 billing query blanks; P2 wrong price time-window join, wrong date filter; P3 job health
report; P4 blind: cost per pipeline per day.

## L17 — Security and compliance (PHI patterns, fake data)
Exam: Pro S6. Warm-up: billing query; audit query; event log query.
Special: ABAC and governed tags may need admin; on refusal record the real error, show per-table
filter/mask fallback, ABAC as concept.
Concepts: ABAC with governed tags; anonymization vs pseudonymization; salted hashing, tokenization
with a mapping table, suppression, generalization, k-anonymity (concept); PII detection (verify
features); masking before silver in batch + streaming; right-to-erasure: DELETE + VACUUM, deletion
vectors + REORG PURGE, CDF and downstream, streaming consumers, clones/backups.
Examples: mask by tag (or fallback); salted vs plain hash; tokenization + controlled re-identification;
generalization + small-group check; streaming masking; erasure across bronze/silver/gold with proof;
leftover data in clones/CDF.
Practice: P1 mask function blanks; P2 unsalted hash, purge missing a clone; P3 de-identified research
extract; P4 blind: full erasure workflow with verification.

## L18 — Data modeling for analytics
Exam: Pro S9. Warm-up: column mask; salted hash; erasure order.
Concepts: medallion vs dimensional; star vs snowflake; fact grain; surrogate keys (identity); SCD
recap; PK/FK with RELY (verify); MVs for pre-aggregation; UC metric views (verify syntax, MEASURE());
per-table layout from access patterns; file sizes and compaction.
Examples: dims + facts from L03-style data; surrogate key joins; RELY effect on the plan; monthly
claims MV + refresh; metric view queried by two dimensions; clustering key choice per table.
Practice: P1 fact CTAS blanks; P2 wrong grain double counting, missing date key; P3 lab results star
schema; P4 blind: metric view + MV for a dashboard.

## L19 — Performance troubleshooting
Exam: Pro S5, S8. Warm-up: fact grain; MV refresh; clustering keys.
Special: check what serverless exposes (query profile, explain, query history); Spark UI parts may be
concept only.
Data: skewed spark.range tables (one key ~40%), small dimension, many small files.
Concepts: jobs/stages/tasks, narrow vs wide, shuffle; Catalyst + Photon; AQE (coalesce, skew join,
broadcast conversion); join strategies + hints; spill; small files; explain('formatted'); query
profile reading.
Examples: sort-merge vs broadcast plans; broadcast hint timing ("nd."); skew join behaviour; small vs
compacted reads; UDF vs built-in plan; query profile guide (no invented numbers); slow-query diagnosis.
Practice: P1 hint blanks; P2 broadcast of a large table, filter after join; P3 fix a slow report;
P4 blind: explain slowness from a plan.

## L20 — Advanced ingestion and sharing
Exam: Pro S2. Warm-up: broadcast join; skew symptom; small-files fix.
Special: run what is possible (Iceberg/UniForm, CREATE SHARE / ADD TABLE / SHOW if allowed, existing
connections); Kafka/Kinesis/Pub/Sub, Lakeflow Connect, Federation, Clean Rooms as correct syntax +
concept only; strong exam questions here.
Concepts: Kafka source options and decoding; Kinesis/Pub/Sub overview; Iceberg on Databricks (verify);
Lakeflow Connect CDC connectors (SQL Server, MySQL, PostgreSQL); Lakehouse Federation (connections,
foreign catalogs, UC permissions); Delta Sharing D2D vs open sharing; Clean Rooms.
Examples: Kafka readStream code; Iceberg table; share + add table; federation steps; Lakeflow Connect
steps; decision table for source → ingestion option.
Practice: P1 Kafka option blanks; P2 missing checkpoint, wrong decoding; P3 ingestion plan for 4
healthcare sources; P4 blind: share a gold table with a partner.

## L21 — Declarative Automation Bundles and CI/CD
Exam: Pro S1, S8. Warm-up: Kafka options; share vs federation; Lakeflow Connect use case.
Special: bundle files under lessons/L21_bundle (databricks.yml with dev/prod targets, resources for
one job and one pipeline, src/, tests/); Claude Code runs bundle validate/deploy/run/destroy against
dev and shows real output; notebook verifies deployed resources with the SDK.
Concepts: bundle structure; targets, variables, presets, development vs production mode; resources;
validate/deploy/run/destroy; Python project structure and wheels; serverless/pipeline dependencies
and common errors; Git folders; branching and promotion; CI example (GitHub Actions or Azure DevOps).
Examples: databricks.yml walkthrough; validate; deploy to dev + SDK check; per-target override proof;
dependency conflict + fix; CI YAML walkthrough.
Practice: P1 databricks.yml blanks; P2 wrong target name, missing variable; P3 prod-only schedule;
P4 blind: bundle for the L07 pipeline.

## L22 — Mock exam (Professional)
60 scenario questions, single best answer, weighted S1 14, S2 7, S3 7, S4 6, S5 9, S6 5, S7 3, S8 6,
S9 3. Rules cell (120 min, no notes, no AI); questions in blocks of 10; answer dict cell; scoring cell
saves the attempt to a Delta table with timestamp and shows score per section; weak-areas cell maps
wrong answers to L01–L21; answers with explanations (why each wrong option is wrong, doc names).
Every answer backed by a lesson example or doc page; do not reuse earlier lesson questions.
A second set of 60 questions for a re-take.
