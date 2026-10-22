# Progress

Statuses: DONE = built, verified with two identical serverless runs, published · LEGACY = hand-written
before the toolchain (no placeholders, no run record) · PLANNED = spec only.

| # | Lesson | Status |
| --- | --- | --- |
| L01 | Lakehouse Basics | LEGACY DONE (.ipynb, in git) |
| L02 | Delta Lake Deep Dive | LEGACY DONE (.ipynb, exported from the workspace into git 2026-10-22) |
| L03 | Spark SQL Transformations | PLANNED |
| L04 | PySpark DataFrame API, transforms, UDFs | PLANNED |
| L05 | Semi-structured data: JSON, nested types, VARIANT | PLANNED |
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
