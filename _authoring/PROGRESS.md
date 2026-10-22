# Progress

Statuses: DONE = built, verified with two identical serverless runs, published · LEGACY = hand-written
before the toolchain (no placeholders, no run record) · PLANNED = spec only.

| # | Lesson | Status |
| --- | --- | --- |
| L01 | Lakehouse Basics | LEGACY DONE (.ipynb, in git) |
| L02 | Delta Lake Deep Dive | LEGACY DONE (.ipynb, exported from the workspace into git 2026-10-22) |
| L03 | Spark SQL Transformations | DONE 2026-10-22 |
| L04 | PySpark DataFrame API, transforms, UDFs | DONE 2026-10-22 |
| L05 | Semi-structured data: JSON, nested types, VARIANT | DONE 2026-10-22 |
| L06 | Ingestion: read_files, COPY INTO, Auto Loader | DONE 2026-10-21 (built as "L03", renumbered 2026-10-22; see note) |
| L07 | Lakeflow Declarative Pipelines I | PLANNED |
| L08 | LDP II: AUTO CDC, SCD 1/2 | PLANNED |
| L09 | Lakeflow Jobs | PLANNED |
| L10 | Unity Catalog governance | PLANNED |
| L11 | Structured Streaming fundamentals | LEGACY (old notebook kept as "L11 - Structured Streaming Fundamentals.ipynb"; rebuild per spec) |
| L12 | Streaming advanced | PLANNED |
| L13 | Delta optimization | PLANNED |
| L14 | Change Data Feed | PLANNED |
| L15 | Testing data pipelines | PLANNED |
| L16 | Monitoring and alerting | PLANNED |
| L17 | Security and compliance | PLANNED |
| L18 | Data modeling for analytics | PLANNED |
| L19 | Performance troubleshooting | PLANNED |
| L20 | Advanced ingestion and sharing | PLANNED |
| L21 | Declarative Automation Bundles and CI/CD | PLANNED |
| L22 | Mock exam (Professional) | PLANNED |

## Open items

- L06 was built and verified as "L03" (before CURRICULUM.md existed). Only file names, titles and
  notebook metadata were renumbered. Still to do when L05 exists: change the schema inside the
  notebook from `main.dbacademy_l03` to `main.dbacademy_l06`, rewrite the warm-up to recall L05
  (it recalls L02 now), then rerun with `--twice` and republish.
- L11 is the old hand-written streaming notebook; the spec lists the required fixes for the rebuild.

## Lessons learned

(Add one line per lesson after it is finished: what behaved differently from the first expectation,
and what the next lesson should know.)

- L03: pre-test new example/practice cells with Databricks Connect (.venv, `serverless(True)`) against the
  tables a failed run leaves behind; it saves full 3-5 minute job runs. Do not assert counts that depend on
  random coverage (E9 first assumed NULLs only in `tsh`; some patients also lacked A1C/LDL).
- L03: `approx_count_distinct` was not exact even at ~190 distinct values; ORDER BY without a frame behaved like
  RANGE (the docs page does not state the default). L04 can reuse the L03 tables and queries for its PySpark versions.
- L04: Python and Pandas UDFs cannot run through local Databricks Connect (ISOLATION_STARTUP_FAILURE.SANDBOX_STARTUP),
  so stage (a) used a temporary probe cell (METRICS["probe.*"], removed later) to learn UDF and cache behaviour in a real job.
- L04: the job notebook client is PySpark 3.5.0 / Python 3.11 and behaves differently from Databricks Connect 18.3 on the
  laptop (`df.cache()` and `groupBy("name")` after a UC SQL function worked locally, failed in the job). Only job runs are evidence.
- L05: a local Databricks Connect harness that execs the solved build's code cells (display stubbed) caught most problems
  before the first job run; job runs were only needed for client-specific facts. The job client is PySpark 3.5.0 but has
  `F.parse_json` and returns VARIANT values (VariantVal) — still prefer SQL expressions for VARIANT and cast before display.
- L05: "bad record" depends on the parser: the JSON reader/colon/`from_json` accept single-quoted JSON, `parse_json` does not;
  `from_json` puts a type mismatch into `_corrupt_record` too. One `schema_of_json` sample gave BIGINT and lost decimals.
- L03 and L06 both use the schema `main.dbacademy_l03` until L06 is renumbered: running one lesson drops the other's tables.
