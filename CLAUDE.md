# Databricks learning notebooks — rules for Claude Code

## Goal
DBAcademy-style lessons for a learner (experienced data engineer: Snowflake/dbt/Python) moving
from Databricks foundations to Data Engineer Professional (outline live from Oct 9, 2026). The
learner must be able to write Databricks code without AI. Plain B1/B2 English. Use current names
(Lakeflow Declarative Pipelines, Lakeflow Jobs, Declarative Automation Bundles) and mention the
old name once.

## Evidence-only rule (most important)
- No guesses. Every number, output, error class and behaviour claim in markdown comes from a real
  run (_authoring/LNN.json) or an official Databricks doc page (name the page).
- Numbers in Explained cells are ALWAYS placeholders {{key}} filled by the build from METRICS.
  Never type a result number by hand.
- Expected errors: catch with try/except, store the real error class in METRICS
  (e.g. METRICS["E2.error"] = "DELTA_FAILED_TO_MERGE_FIELDS") and reference it with a placeholder.
- Things that cannot be verified here: "NOT VERIFIED — concept only (reason)", listed in the summary.
- Check current syntax in the docs before writing it.
- After a run, read _authoring/LNN.json and make sure every Explained text matches the real
  values (e.g. do not say "fails" if the metric shows it succeeded).

## Notebook rules
- Databricks source format .py. Default language Python. %sql only for single-statement cells.
- Every result an Explained cell talks about must be visible: display(spark.sql(...)).
- Every example also records its key numbers: METRICS["E3.after_insert"] = cnt, and asserts them
  where the value is known from the setup data.
- Each cell has a title line (# DBTITLE 1,<title>).
- One lesson = one notebook: "Databricks - Learning/LNN - <title>.ipynb", like L01. No helper notebooks.
- METRICS[...] / WARMUP_PASSED[...] / PRACTICE_PASSED[...] lines are removed from the learner notebook:
  keep each on one line and only record values (store stream/write results in a variable first).
- A cell that must fail (e.g. a stream that stops on a new column): first code line
  `# EXPECT_FAILURE: <key>`. The learner sees the real error; test runs record METRICS["<key>.errors"].

## Environment
- Serverless. Catalog main, schema main.dbacademy_lNN, volume /Volumes/main/dbacademy_lNN/data.
- The workspace default catalog is `bunyod`: use fully qualified names or USE CATALOG main.
- Setup is idempotent (DROP SCHEMA ... CASCADE, recreate). Fake healthcare data only, no PHI.
- Use rng = random.Random(42). Never use the global random.seed(). For big data use spark.range
  with deterministic expressions.
- Predictive optimization is ON: extra OPTIMIZE versions, automatic compaction, possible
  ConcurrentWriteException. Read the `operation` column, never assume version numbers, retry
  where needed, and use "nd." keys for values that can change between runs.
- dbutils.fs for files, not shutil. Serverless limits discovered so far: no global temp views;
  RESTORE needs TO VERSION AS OF; RESTORE can race with auto-OPTIMIZE.

## Standard lesson structure (numbered, no gaps)
1 Title (exam objectives, 5 learning objectives) · 2 Warm-up (3 recall tasks from the previous
lesson, no hints, validation sets WARMUP_PASSED) · 3 Setup (asserted counts) · 4 Concepts
(what / why / when vs alternative table / syntax SQL + PySpark) · 5 Examples (Predict → code with
every line commented → Explained with placeholders) · 6 Exam traps (12+ rows) · 7 Cheat sheet
(SQL ↔ PySpark) · 8 Check your understanding (3) · 9 Practice P1 fill-in (3 blanks), P2 fix-the-bug
(2 bugs), P3 from scratch with hints, P4 blind; validation cells print ✅ Passed or a specific hint
and set PRACTICE_PASSED · 10 Exam questions (8, healthcare scenarios, A–D) · 11 Solutions
(generated from SOLUTION blocks + written answers) · 12 Cleanup · 13 Final summary (placeholders,
NOT VERIFIED list, surprises).

## Workflow per lesson
1. Read _authoring/LNN.md and the previous lesson (for the warm-up).
2. Write _authoring/LNN.py in stages: setup → examples → rest. After each stage:
   build solved → run → read results → fix. Do not continue on a failed run.
3. Final: run with --twice; all asserts pass and METRICS are identical except "nd." keys.
4. Build learner and publish. Commit _authoring/LNN.* and the notebooks in "Databricks - Learning/".
5. Report: final METRICS, NOT VERIFIED items, surprises, and suggested PR title + description.
