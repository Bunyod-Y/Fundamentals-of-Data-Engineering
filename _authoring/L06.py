# Databricks notebook source
# LESSON_TITLE: Ingestion with Auto Loader, read_files and COPY INTO

# COMMAND ----------

# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L06 — Ingestion with Auto Loader, read_files and COPY INTO
# MAGIC
# MAGIC **Exam objectives** (quoted from the official exam guides):
# MAGIC
# MAGIC * Associate (May 4, 2026), Section 2 *Development and Ingestion*: "Classify valid Auto Loader sources and use cases", "Demonstrate knowledge of Auto Loader syntax", "Use Databricks' built-in debugging tools to troubleshoot a given issue".
# MAGIC * Associate, Section 3: "Identify DDL (Data Definition Language)/DML features" — here: `COPY INTO`.
# MAGIC * Professional (Oct 9, 2026), Section 2: "Develop data ingestion pipelines to ingest a variety of data formats, including Delta Lake, Parquet, JSON, Iceberg, CSV, and Binary from diverse sources such as message buses (Kafka, Kinesis, Pub/Sub) and cloud storage." This lesson: JSON, CSV, Parquet and binary files from cloud storage (a Unity Catalog volume). Delta, Iceberg and message buses come in later lessons.
# MAGIC * Professional, Section 1: "Create production-ready data pipelines for streaming data using Lakeflow Pipelines and Auto Loader." This lesson covers the Auto Loader part. Lakeflow Declarative Pipelines (formerly Delta Live Tables, DLT) come in a later lesson.
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** `read_files` to query JSON, CSV and Parquet files in a Unity Catalog volume, with schema inference, `schemaHints`, the `_metadata` column, and the `binaryFile` format to list files.
# MAGIC 2. **Explain** how Auto Loader remembers which files it has already loaded (checkpoint) and where it keeps the inferred schema (`cloudFiles.schemaLocation`), and how `STREAM read_files` and streaming tables relate to it.
# MAGIC 3. **Compare** `read_files`, `COPY INTO` and Auto Loader: batch vs incremental, idempotency, scale, schema evolution, typed (Parquet) vs text (JSON/CSV) formats.
# MAGIC 4. **Implement** incremental ingestion with Auto Loader (`format("cloudFiles")`) and `trigger(availableNow=True)` into a Delta table, with schema evolution, rescued data and file metadata columns.
# MAGIC 5. **Diagnose** ingestion problems with `cloud_files_state`, the `_schemas` folder, `DESCRIBE HISTORY`, the error class of a stream that does not start or stops, and the `_rescued_data` column.
# MAGIC
# MAGIC > **Evidence-only rule:** every number and error class in this notebook comes from a real run of this notebook (the build fills them in), or from an official Databricks doc page named in the text. Things that cannot be tested on this workspace are marked **NOT VERIFIED — concept only (reason)**.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up (intro)
# MAGIC %md
# MAGIC ## 2 Warm-up — recall L05 Semi-structured Data: JSON, Nested Types and VARIANT (no hints)
# MAGIC
# MAGIC Write each task from memory. Run the validation cell after each one. Solutions are in section 11.
# MAGIC
# MAGIC > The warm-up uses its own small tables. Run the next cell first: it creates the lesson schema, the tables `wu_patients` and `wu_labs_raw`, and a helper.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up prep (run first)
spark.sql("CREATE SCHEMA IF NOT EXISTS main.dbacademy_l06")   # the warm-up tables live here; setup recreates the schema later

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
    found = re.search(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", str(e))   # e.g. [CLOUDFILES_SCHEMA_LOCATION_REQUIRED]
    return found.group(1) if found else type(e).__name__

# Patients with an array of phone numbers: one with two, one with an empty array, one with NULL, one with one
spark.sql("CREATE OR REPLACE TABLE main.dbacademy_l06.wu_patients (patient_id INT, phones ARRAY<STRING>)")
spark.sql("""INSERT INTO main.dbacademy_l06.wu_patients VALUES
    (1, array('555-0101', '555-0102')), (2, array()), (3, NULL), (4, array('555-0104'))""")
# Raw FHIR-like lab observations as JSON strings (fake values)
spark.sql("CREATE OR REPLACE TABLE main.dbacademy_l06.wu_labs_raw (raw STRING)")
spark.sql("""INSERT INTO main.dbacademy_l06.wu_labs_raw VALUES
    ('{"id":"obs-1","code":{"coding":[{"code":"4548-4"}]},"valueQuantity":{"value":7.2,"unit":"%"}}'),
    ('{"id":"obs-2","code":{"coding":[{"code":"2345-7"}]},"valueQuantity":{"value":105,"unit":"mg/dL"}}'),
    ('{"id":"obs-3","code":{"coding":[{"code":"2093-3"}]},"valueQuantity":{"value":182.5,"unit":"mg/dL"}}')""")
print("Warm-up tables: main.dbacademy_l06.wu_patients, main.dbacademy_l06.wu_labs_raw")

# COMMAND ----------

# DBTITLE 1,W1 Task
# MAGIC %md
# MAGIC ### W1 — One row per phone number, keep every patient
# MAGIC `main.dbacademy_l06.wu_patients` has the columns `patient_id` and `phones` (`ARRAY<STRING>`). Some patients have an empty array or NULL.
# MAGIC Create the table `main.dbacademy_l06.wu1_phones` with the columns `patient_id` and `phone`: one row per phone number, and **one row with `phone` = NULL** for each patient without phone numbers.

# COMMAND ----------

# DBTITLE 1,W1 Code
# >>> BLANK W1
# Your code here
# >>> SOLUTION W1
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l06.wu1_phones AS
    SELECT p.patient_id, t.phone
    FROM main.dbacademy_l06.wu_patients p,
         LATERAL explode_outer(p.phones) AS t(phone)
""")
# <<< END W1

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    t = spark.table("main.dbacademy_l06.wu1_phones")
    got = sorted((r["patient_id"], r["phone"] or "") for r in t.collect())
    want = [(1, "555-0101"), (1, "555-0102"), (2, ""), (3, ""), (4, "555-0104")]
    ok = got == want and t.columns == ["patient_id", "phone"]
    WARMUP_PASSED["W1"] = ok
    print("✅ Passed" if ok else
          f"Hint: want 5 rows (patients 2 and 3 with phone NULL) and the columns patient_id, phone; got {got}, {t.columns}.")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create main.dbacademy_l06.wu1_phones first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — Colon notation on a JSON string
# MAGIC `main.dbacademy_l06.wu_labs_raw` has one column `raw` (a JSON **string**). Without `from_json` and without `VARIANT`, create `main.dbacademy_l06.wu2_labs` with three columns:
# MAGIC `id` (`STRING`, from `id`), `loinc` (`STRING`, the `code` of the **first** element of `code.coding`) and `value` (`DOUBLE`, from `valueQuantity.value`).

# COMMAND ----------

# DBTITLE 1,W2 Code
# >>> BLANK W2
# Your code here
# >>> SOLUTION W2
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l06.wu2_labs AS
    SELECT raw:id                           AS id,
           raw:code.coding[0].code          AS loinc,
           raw:valueQuantity.value::double  AS value
    FROM main.dbacademy_l06.wu_labs_raw
""")
# <<< END W2

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    t = spark.table("main.dbacademy_l06.wu2_labs")
    got = sorted(tuple(r) for r in t.select("id", "loinc", "value").collect())
    want = [("obs-1", "4548-4", 7.2), ("obs-2", "2345-7", 105.0), ("obs-3", "2093-3", 182.5)]
    types = dict(t.dtypes)
    ok = got == want and types == {"id": "string", "loinc": "string", "value": "double"}
    WARMUP_PASSED["W2"] = ok
    print("✅ Passed" if ok else f"Hint: want {want} with types string, string, double; got {got}, {types}.")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: create main.dbacademy_l06.wu2_labs with the columns id, loinc, value first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — `VARIANT` with `parse_json`
# MAGIC Create `main.dbacademy_l06.wu3_labs` from `wu_labs_raw` with two columns: `v` — the raw JSON as a **`VARIANT`** (strict parser, the input is valid JSON), and `unit` — a `STRING` read from `v` at the path `valueQuantity.unit`.

# COMMAND ----------

# DBTITLE 1,W3 Code
# >>> BLANK W3
# Your code here
# >>> SOLUTION W3
spark.sql("""
    CREATE OR REPLACE TABLE main.dbacademy_l06.wu3_labs AS
    SELECT v, v:valueQuantity.unit::string AS unit
    FROM (SELECT parse_json(raw) AS v FROM main.dbacademy_l06.wu_labs_raw)
""")
# <<< END W3

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    r = spark.sql("""SELECT count(*) AS n, count_if(typeof(v) = 'variant') AS variants,
                            sort_array(collect_list(unit)) AS units
                     FROM main.dbacademy_l06.wu3_labs""").first()
    cols = spark.table("main.dbacademy_l06.wu3_labs").columns
    ok = r["n"] == 3 and r["variants"] == 3 and list(r["units"]) == ["%", "mg/dL", "mg/dL"] and cols == ["v", "unit"]
    WARMUP_PASSED["W3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want 3 rows, v of type variant, units ['%', 'mg/dL', 'mg/dL'], columns ['v', 'unit']; "
          f"got {r['n']} rows, {r['variants']} variants, units {list(r['units'])}, columns {cols}.")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create main.dbacademy_l06.wu3_labs with the columns v and unit first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent (drop the schema, recreate it), fake healthcare data only (no PHI).
import json, random                                    # json builds the JSON lines; random makes fake values

CATALOG, SCHEMA = "main", "dbacademy_l06"
FQ = f"{CATALOG}.{SCHEMA}"                              # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"               # Unity Catalog volume for all files
CLAIMS_DIR = f"{VOL}/landing/claims_json"               # the clearinghouse drops claim files here
PROVIDERS_DIR = f"{VOL}/landing/providers_csv"          # and the provider file here
LABS_DIR = f"{VOL}/landing/labs_parquet"                # the lab system drops Parquet files here
CHECKPOINTS = f"{VOL}/_checkpoints"                     # one sub-folder per streaming query

spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")        # removes warm-up tables, volume and files
spark.sql(f"CREATE SCHEMA {FQ}")
spark.sql(f"CREATE VOLUME {FQ}.data")

STATUSES = ["SUBMITTED", "APPROVED", "DENIED", "PENDING"]

def claim_row(claim_id):
    """One fake claim. The generator is seeded per claim, so a claim always gets the same values."""
    rng = random.Random(f"claim-{claim_id}")            # isolated generator (string seeds are deterministic)
    return {"claim_id": claim_id,
            "patient_id": rng.randint(1, 50),
            "provider_id": rng.randint(1, 10),
            "claim_amount": round(rng.uniform(50, 5000), 2),
            "claim_status": rng.choice(STATUSES),
            "service_date": f"2026-{rng.randint(1, 9):02d}-{rng.randint(1, 28):02d}"}

def land_claims(file_name, first_id, n, extra=None, bad_amount_ids=()):
    """Write one newline-delimited JSON file with n claims into the landing folder."""
    lines = []
    for cid in range(first_id, first_id + n):
        row = claim_row(cid)
        if extra:
            row.update(extra)                           # e.g. {"payer": "Medicare"}: a new column
        if cid in bad_amount_ids:
            row["claim_amount"] = "N/A"                 # a value that does not fit the DOUBLE type
        lines.append(json.dumps(row))
    dbutils.fs.put(f"{CLAIMS_DIR}/{file_name}", "\n".join(lines) + "\n", overwrite=True)
    return n

# First delivery: 3 JSON files with 100 claims each
for i in range(3):
    land_claims(f"claims_{i + 1:03d}.json", first_id=i * 100 + 1, n=100)

# Provider reference file (CSV with a header row)
rng = random.Random(42)
specialties = ["Cardiology", "Neurology", "Oncology", "Pediatrics", "Orthopedics", "General"]
csv_lines = ["provider_id,provider_name,specialty,npi"]
for pid in range(1, 11):
    csv_lines.append(f"{pid},Dr Provider {pid},{rng.choice(specialties)},{1000000000 + pid}")
dbutils.fs.put(f"{PROVIDERS_DIR}/providers_001.csv", "\n".join(csv_lines) + "\n", overwrite=True)

def land_labs(file_name, first_id, n):
    """Write one Parquet file with n fake lab results (deterministic values from the id) into LABS_DIR."""
    tmp = f"{VOL}/_tmp_parquet"                         # Spark writes a folder; we keep only its data file
    labs = spark.range(first_id, first_id + n).selectExpr(
        "CAST(id AS INT) AS lab_id",
        "CAST(id % 50 + 1 AS INT) AS patient_id",
        "CASE id % 3 WHEN 0 THEN '4548-4' WHEN 1 THEN '2345-7' ELSE '2093-3' END AS loinc_code",
        "CAST(round(4 + (id * 37 % 1000) / 100.0, 2) AS DOUBLE) AS result_value",
        "TIMESTAMP'2026-03-01 08:00:00' + make_interval(0, 0, 0, 0, CAST(id % 72 AS INT), 0, 0) AS collected_at")
    labs.coalesce(1).write.mode("overwrite").parquet(tmp)   # one part file
    part = [f.path for f in dbutils.fs.ls(tmp) if f.name.startswith("part-")][0]
    dbutils.fs.cp(part, f"{LABS_DIR}/{file_name}")      # a clean file name in the landing folder
    dbutils.fs.rm(tmp, True)                            # remove _SUCCESS, _committed_... and the folder
    return n

land_labs("labs_001.parquet", first_id=1, n=100)        # first lab delivery: 100 results

# Check the files and record the real counts
claim_files = [f.name for f in dbutils.fs.ls(CLAIMS_DIR)]
claim_lines = spark.read.text(CLAIMS_DIR).count()       # one JSON object per line
approved = spark.read.json(CLAIMS_DIR).filter("claim_status = 'APPROVED'").count()
METRICS["setup.claim_files"] = len(claim_files)
METRICS["setup.claim_rows"] = claim_lines
METRICS["setup.approved"] = approved
METRICS["setup.provider_rows"] = len(csv_lines) - 1
lab_files = [f.name for f in dbutils.fs.ls(LABS_DIR)]
lab_rows = spark.read.parquet(LABS_DIR).count()
METRICS["setup.lab_files"], METRICS["setup.lab_rows"] = len(lab_files), lab_rows
assert claim_files == ["claims_001.json", "claims_002.json", "claims_003.json"], claim_files
assert claim_lines == 300, claim_lines
assert lab_files == ["labs_001.parquet"] and lab_rows == 100, (lab_files, lab_rows)
print(f"claim files: {claim_files}, claim rows: {claim_lines}, APPROVED: {approved}, providers: {len(csv_lines) - 1}, "
      f"lab files: {lab_files}, lab rows: {lab_rows}")
display(dbutils.fs.ls(CLAIMS_DIR))

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):** {{setup.claim_files}} JSON files with {{setup.claim_rows}} claims in total ({{setup.approved}} of them `APPROVED`), one CSV file with {{setup.provider_rows}} providers, and {{setup.lab_files}} Parquet file with {{setup.lab_rows}} lab results.
# MAGIC
# MAGIC The helper `land_claims(file_name, first_id, n, extra=None, bad_amount_ids=())` writes one more JSON file into the landing folder. The examples call it to simulate new deliveries:
# MAGIC * `extra={"payer": "Medicare"}` adds a **new column**;
# MAGIC * `bad_amount_ids=(…)` writes `"N/A"` into `claim_amount` for those claims (a **type mismatch**).
# MAGIC
# MAGIC The helper `land_labs(file_name, first_id, n)` writes one Parquet file with lab results (`lab_id`, `patient_id`, `loinc_code`, `result_value`, `collected_at`). Spark always writes a **folder** of files, so the helper writes to a temporary folder and copies only the data file into `landing/labs_parquet`.

# COMMAND ----------

# DBTITLE 1,4.1 Concept — Three ways to load files
# MAGIC %md
# MAGIC ## 4 Concepts
# MAGIC
# MAGIC ### 4.1 Three ways to load files: `read_files`, `COPY INTO`, Auto Loader
# MAGIC
# MAGIC **What.** All three read files (JSON, CSV, Parquet, …) from cloud storage or a Unity Catalog volume.
# MAGIC
# MAGIC **Why three?** They answer different questions:
# MAGIC * `read_files` — "What is in these files right now?" A table-valued SQL function. Every query reads the files again.
# MAGIC * `COPY INTO` — "Load the files I have not loaded yet into this table." A SQL command that remembers loaded files.
# MAGIC * Auto Loader — "Keep loading new files as they arrive." A Structured Streaming source (`format("cloudFiles")`).
# MAGIC
# MAGIC **When to use which**
# MAGIC
# MAGIC | | `read_files` | `COPY INTO` | Auto Loader |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Type | batch query (or `STREAM read_files` in a pipeline) | batch SQL command | streaming source |
# MAGIC | Remembers loaded files? | no | yes — "Files in the source location that have already been loaded are skipped" (*COPY INTO* reference) | yes — file metadata in RocksDB in the checkpoint, "processed exactly once" (*What is Auto Loader?*) |
# MAGIC | Scale (*Ingest data from cloud object storage* page) | — | "files in the order of thousands" | "files in the order of millions or more" |
# MAGIC | Schema change | inferred on every read | `COPY_OPTIONS ('mergeSchema' = 'true')` | `cloudFiles.schemaEvolutionMode` |
# MAGIC | Language | SQL | SQL | Python/Scala; SQL inside Lakeflow pipelines |
# MAGIC
# MAGIC **Sources** (*What is Auto Loader?*): Amazon S3 `s3://`, ADLS `abfss://`, Google Cloud Storage `gs://`, Azure Blob Storage `wasbs://`, and Unity Catalog volumes `/Volumes/`.
# MAGIC
# MAGIC **Formats**
# MAGIC * `read_files` and Auto Loader: `JSON`, `CSV`, `XML`, `TEXT`, `BINARYFILE`, `PARQUET`, `AVRO`, `ORC` (*read_files table-valued function*).
# MAGIC * `COPY INTO` `FILEFORMAT`: `CSV`, `JSON`, `AVRO`, `ORC`, `PARQUET`, `TEXT`, `BINARYFILE` — no `XML` in the list (*COPY INTO* reference).
# MAGIC * Self-describing formats (Parquet, Avro, ORC) store column types in the file. Text formats (JSON, CSV, XML) do not: the reader must infer or be told the types. This lesson uses JSON claims, a CSV provider file and Parquet lab results.
# MAGIC
# MAGIC > **NOT VERIFIED — concept only (no storage credential or external location of our own on this workspace):** loading from `s3://`, `abfss://` or `gs://` paths. This lesson uses a Unity Catalog volume only.

# COMMAND ----------

# DBTITLE 1,4.2 Concept — read_files
# MAGIC %md
# MAGIC ### 4.2 `read_files` — query files with SQL
# MAGIC
# MAGIC **What.** A table-valued function: `read_files(path [, option_key => option_value ] [...])`. Options use **named parameters** (`=>`). Source: *read_files table-valued function* (SQL reference).
# MAGIC
# MAGIC **Why.** You can look at raw files before you build a table, and you can create a table from files in one statement (`CREATE TABLE … AS SELECT * FROM read_files(…)`).
# MAGIC
# MAGIC **Important options**
# MAGIC
# MAGIC | Option | Meaning (docs) |
# MAGIC | --- | --- |
# MAGIC | `format` | file format; "Auto-inferred if omitted" |
# MAGIC | `schema` | a DDL string, e.g. `'id int, ts timestamp'` |
# MAGIC | `schemaHints` | override the type of some columns during inference |
# MAGIC | `inferColumnTypes` | default `true` (Auto Loader's default is different, see 4.4) |
# MAGIC | `header`, `sep`, … | format options of the DataFrame reader, e.g. for CSV |
# MAGIC
# MAGIC **File metadata column `_metadata`** (*File metadata column* page). A hidden `STRUCT` column on every file source (`read_files`, `spark.read`, Auto Loader, `COPY INTO` with a `SELECT`). Fields: `file_path`, `file_name`, `file_size`, `file_block_start`, `file_block_length`, `file_modification_time`. "You must explicitly select `_metadata`" — `SELECT *` does not return it. Bronze tables usually keep `_metadata.file_name` (or `file_path`) and `file_modification_time` for lineage and debugging.
# MAGIC
# MAGIC **`binaryFile` format.** Reads each file as one row with the fixed columns `path`, `modificationTime`, `length` and `content` (the bytes). "Use `* EXCEPT (content)` to exclude binary content when querying file metadata" (*read_files* reference). Use it for images and PDFs, or simply to **list** a landing folder with SQL. Options from the *Binary file* page: `pathGlobFilter` (for example `'*.json'`) and `recursiveFileLookup`.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *, _metadata.file_name
# MAGIC FROM read_files('/Volumes/main/dbacademy_l06/data/landing/claims_json', format => 'json');
# MAGIC ```
# MAGIC ```python
# MAGIC spark.read.format("json").load("/Volumes/main/dbacademy_l06/data/landing/claims_json")   # PySpark batch read
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.3 Concept — COPY INTO
# MAGIC %md
# MAGIC ### 4.3 `COPY INTO` — idempotent batch load
# MAGIC
# MAGIC **What.** A SQL command that loads files into an existing Delta table. Source: *COPY INTO* (SQL reference).
# MAGIC
# MAGIC **Why.** It is "a retryable and idempotent operation": if you run it again, files that were already loaded are skipped. You can schedule it every hour and only new files are loaded.
# MAGIC
# MAGIC **When.** Thousands of files, SQL-only teams, or when you sometimes need to re-load a few files (`COPY_OPTIONS ('force' = 'true')` turns idempotency off).
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS main.dbacademy_l06.claims_copy;     -- a table with no columns yet
# MAGIC COPY INTO main.dbacademy_l06.claims_copy
# MAGIC FROM '/Volumes/main/dbacademy_l06/data/landing/claims_json'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');                         -- let the first load create the columns
# MAGIC ```
# MAGIC Other clauses: `FILES = ('a.json', …)` or `PATTERN = '*.json'` (not both), `FORMAT_OPTIONS (…)` for reader options such as `'header' = 'true'`, and `FROM (SELECT expression_list FROM '<path>')`, which "selects the specified columns or expressions from the source data before copying" — for example `_metadata.file_name AS source_file` (E9).
# MAGIC
# MAGIC **Proof of idempotency.** Every `COPY INTO` that loads files commits a table version with the operation `COPY INTO` in `DESCRIBE HISTORY`. E3 checks what a rerun writes there.
# MAGIC
# MAGIC PySpark has no `COPY INTO` API. Run it with `spark.sql("COPY INTO …")`.

# COMMAND ----------

# DBTITLE 1,4.4 Concept — Auto Loader
# MAGIC %md
# MAGIC ### 4.4 Auto Loader — incremental loading with `cloudFiles`
# MAGIC
# MAGIC **What.** A Structured Streaming source. It finds new files in a folder and loads each file once. Source: *What is Auto Loader?*
# MAGIC
# MAGIC **Two locations you must give it**
# MAGIC
# MAGIC | Option | What is stored there |
# MAGIC | --- | --- |
# MAGIC | `checkpointLocation` (on the writer) | stream progress, including which files were loaded (RocksDB) |
# MAGIC | `cloudFiles.schemaLocation` (on the reader) | the inferred schema and its later versions, in a `_schemas` folder (*Configure schema inference and evolution in Auto Loader*) |
# MAGIC
# MAGIC Many teams use the same folder for both.
# MAGIC
# MAGIC **Type inference.** "For formats that don't encode data types (JSON, CSV, and XML), Auto Loader infers all columns as strings" unless you set `cloudFiles.inferColumnTypes` to `true`. Inference samples "the first 50 GB or 1000 files that it discovers, whichever limit is crossed first."
# MAGIC
# MAGIC **File detection.** "By default, Auto Loader uses directory listing mode." Databricks recommends file notification mode with file events for most workloads.
# MAGIC
# MAGIC > **NOT VERIFIED — concept only (needs a cloud storage location with file events; we only have a volume):** file notification / file events mode.
# MAGIC
# MAGIC ```python
# MAGIC (spark.readStream
# MAGIC     .format("cloudFiles")                                   # Auto Loader
# MAGIC     .option("cloudFiles.format", "json")                    # format of the files
# MAGIC     .option("cloudFiles.schemaLocation", ckpt)              # where the inferred schema is kept
# MAGIC     .load(landing_dir)
# MAGIC  .writeStream
# MAGIC     .option("checkpointLocation", ckpt)                     # which files were loaded
# MAGIC     .trigger(availableNow=True)                             # load everything new, then stop
# MAGIC     .toTable("main.dbacademy_l06.claims_bronze"))
# MAGIC ```
# MAGIC **Parquet.** For typed formats "Auto Loader samples a subset of files and merges the schemas of individual files" (*Configure schema inference and evolution in Auto Loader*): the types come from the files, so `inferColumnTypes` is not needed (E8).
# MAGIC
# MAGIC SQL forms of Auto Loader (`STREAM read_files`, streaming tables): see 4.6.

# COMMAND ----------

# DBTITLE 1,4.5 Concept — Schema evolution and rescued data
# MAGIC %md
# MAGIC ### 4.5 Schema evolution and the rescued data column
# MAGIC
# MAGIC **What.** Source files change: a new column appears, or a value does not fit the column type. Auto Loader decides what to do with `cloudFiles.schemaEvolutionMode`. Source: *Configure schema inference and evolution in Auto Loader*.
# MAGIC
# MAGIC | Mode | When a new column appears |
# MAGIC | --- | --- |
# MAGIC | `addNewColumns` (default when you give **no** schema) | "the stream stops with an `UnknownFieldException`"; the new column is added to the schema first, so a **restart** continues with the new schema |
# MAGIC | `rescue` | the schema never changes and the stream does not fail; the new column goes into the rescued data column |
# MAGIC | `failOnNewColumns` | the stream fails and does not continue until you change the schema or remove the file |
# MAGIC | `none` (default when you **give** a schema) | the stream does not fail; new columns are ignored |
# MAGIC
# MAGIC **Rescued data column `_rescued_data`.** Auto Loader adds it automatically. It holds "a JSON blob with the rescued columns and the source file path of the record": columns that are not in the schema, values with the wrong type, and columns with different upper/lower case.
# MAGIC
# MAGIC **Why it matters.** With rescued data, a bad value never stops the pipeline, and no data is lost: you can fix it later from `_rescued_data`.

# COMMAND ----------

# DBTITLE 1,4.6 Concept — Auto Loader in SQL - STREAM read_files and streaming tables
# MAGIC %md
# MAGIC ### 4.6 Auto Loader in SQL: `STREAM read_files` and streaming tables
# MAGIC
# MAGIC **What.** "`read_files` leverages Auto Loader when used in a streaming table query", and "You must use the `STREAM` keyword with `read_files`" for streaming (*read_files table-valued function*). The usual place is a **streaming table**:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE main.dbacademy_l06.claims_st
# MAGIC AS SELECT *, _metadata.file_name AS source_file
# MAGIC FROM STREAM read_files('/Volumes/main/dbacademy_l06/data/landing/claims_json', format => 'json');
# MAGIC ```
# MAGIC
# MAGIC **Streaming options of `read_files`** (same page): `schemaLocation` ("isn't required when used in a streaming table query" — the table manages it), `schemaEvolutionMode`, `includeExistingFiles` (default `true`), `maxFilesPerTrigger` (default 1000), `maxBytesPerTrigger`, `allowOverwrites`.
# MAGIC
# MAGIC **Where it runs.** "Streaming tables are only supported in Lakeflow pipelines and on Databricks SQL with Unity Catalog" (*CREATE STREAMING TABLE* reference). Lakeflow Declarative Pipelines (formerly Delta Live Tables) are lesson L07. In a notebook on serverless compute, E11 tests two things for real: a `STREAM read_files` query as a streaming DataFrame, and `CREATE OR REFRESH STREAMING TABLE`.
# MAGIC
# MAGIC | | Python Auto Loader | `STREAM read_files` in a notebook | Streaming table (`CREATE OR REFRESH STREAMING TABLE`) |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Code | `spark.readStream.format("cloudFiles")` | `spark.sql("SELECT … FROM STREAM read_files(…)")` | SQL DDL |
# MAGIC | Checkpoint / schema location | you choose both | you choose both (E11) | managed for you |
# MAGIC | Refresh | your trigger (`availableNow`) | your trigger | `REFRESH` or the pipeline / schedule |

# COMMAND ----------

# DBTITLE 1,5 — Examples (intro)
# MAGIC %md
# MAGIC ## 5 Examples
# MAGIC
# MAGIC Each example: **Predict** (answer before you run) → **code** (every line commented) → **Explained** (real numbers from the run).
# MAGIC
# MAGIC The examples build on each other: files that one example lands are still there for the next one. Run them in order.

# COMMAND ----------

# DBTITLE 1,E1 Predict
# MAGIC %md
# MAGIC ### E1 — `read_files` on JSON files
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which type will `read_files` give `claim_amount` and `service_date`: `STRING`, `DOUBLE`, `DATE`?
# MAGIC 2. Will there be an extra column that is not in the JSON files?
# MAGIC 3. How many rows does each file have?

# COMMAND ----------

# DBTITLE 1,E1 Code
claims_query = f"SELECT * FROM read_files('{CLAIMS_DIR}', format => 'json')"   # all JSON files in the folder
e1 = spark.sql(claims_query)                                   # a normal DataFrame; nothing is stored
display(spark.sql(f"DESCRIBE QUERY {claims_query}"))          # column names and inferred types
display(e1.orderBy("claim_id").limit(5))                       # first rows

per_file = spark.sql(f"""
    SELECT _metadata.file_name AS file_name, count(*) AS row_count     -- _metadata: hidden file column
    FROM read_files('{CLAIMS_DIR}', format => 'json')
    GROUP BY ALL
    ORDER BY file_name""")
display(per_file)                                              # rows per file

types = dict(e1.dtypes)                                        # {column: type}
rows, files = e1.count(), per_file.count()
print(f"{rows} rows from {files} files; claim_amount = {types['claim_amount']}, service_date = {types['service_date']}")
METRICS["E1.rows"] = rows
METRICS["E1.columns"] = ", ".join(e1.columns)
METRICS["E1.type_claim_id"] = types["claim_id"]
METRICS["E1.type_claim_amount"] = types["claim_amount"]
METRICS["E1.type_service_date"] = types["service_date"]
METRICS["E1.files"] = files
METRICS["E1.rows_per_file"] = ", ".join(str(r["row_count"]) for r in per_file.collect())
assert rows == 300 and files == 3

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `read_files` returned **{{E1.rows}}** rows from **{{E1.files}}** files ({{E1.rows_per_file}} rows per file, from `_metadata.file_name`).
# MAGIC * Columns: `{{E1.columns}}`. They come in alphabetical order, not in the order of the JSON keys.
# MAGIC * The last column, `_rescued_data`, is not in the files. `read_files` adds a rescued data column by default (*read_files* reference).
# MAGIC * Inferred types: `claim_id` = `{{E1.type_claim_id}}`, `claim_amount` = `{{E1.type_claim_amount}}`, `service_date` = `{{E1.type_service_date}}`. `read_files` infers types (`inferColumnTypes` is `true` by default), but the date text `"2026-03-05"` stays a string. Use `schemaHints => 'service_date DATE'` or a cast if you need a date.
# MAGIC
# MAGIC > **Key takeaway:** `read_files` is the fastest way to look at raw files with SQL. It reads all files on every query and stores nothing.

# COMMAND ----------

# DBTITLE 1,E2 Predict
# MAGIC %md
# MAGIC ### E2 — CSV: `spark.read.csv` without options vs `read_files`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `spark.read.csv(path)` with **no options**: what are the column names, and how many rows do you get from a file with a header and 10 data rows?
# MAGIC 2. `read_files(…, format => 'csv', header => true)`: which type does `npi` get?
# MAGIC 3. How do you make `npi` a `STRING` without writing the full schema?

# COMMAND ----------

# DBTITLE 1,E2 Code
plain = spark.read.csv(PROVIDERS_DIR)                          # no options at all
display(plain)                                                 # look at the first row and the column names
METRICS["E2.plain_columns"] = ", ".join(plain.columns)
METRICS["E2.plain_rows"] = plain.count()
METRICS["E2.plain_type_first"] = plain.dtypes[0][1]           # type of the first column

rf_query = f"SELECT * FROM read_files('{PROVIDERS_DIR}', format => 'csv', header => true)"
rf = spark.sql(rf_query)                                       # header row gives the column names
display(spark.sql(f"DESCRIBE QUERY {rf_query}"))               # inferred types
rf_rows = rf.count()
METRICS["E2.rf_rows"] = rf_rows
METRICS["E2.rf_type_provider_id"] = dict(rf.dtypes)["provider_id"]
METRICS["E2.rf_type_npi"] = dict(rf.dtypes)["npi"]

hinted = spark.sql(f"""
    SELECT * FROM read_files('{PROVIDERS_DIR}', format => 'csv', header => true,
                             schemaHints => 'npi STRING')      -- change one column, infer the rest
""")
display(hinted)
METRICS["E2.hint_type_npi"] = dict(hinted.dtypes)["npi"]
METRICS["E2.hint_type_provider_id"] = dict(hinted.dtypes)["provider_id"]
assert rf_rows == 10

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `spark.read.csv(path)` with no options: columns `{{E2.plain_columns}}` and **{{E2.plain_rows}}** rows. The header line became a normal data row, and the first column has type `{{E2.plain_type_first}}`. The plain CSV reader does not use the header and does not infer types unless you ask for it.
# MAGIC * `read_files(…, format => 'csv', header => true)`: **{{E2.rf_rows}}** rows, real column names, and inferred types: `provider_id` = `{{E2.rf_type_provider_id}}`, `npi` = `{{E2.rf_type_npi}}`.
# MAGIC * `schemaHints => 'npi STRING'` changed only that column: `npi` = `{{E2.hint_type_npi}}`, while `provider_id` was still inferred as `{{E2.hint_type_provider_id}}`.
# MAGIC
# MAGIC > **Key takeaway:** an identifier such as an NPI is a code, not a number: you never add NPIs, and some codes start with 0. Keep such columns as `STRING` with `schemaHints`; you do not need to write the whole schema.

# COMMAND ----------

# DBTITLE 1,E3 Predict
# MAGIC %md
# MAGIC ### E3 — `COPY INTO` is idempotent
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. The target table is created with **no columns**. Can `COPY INTO` still load it?
# MAGIC 2. You run the same `COPY INTO` a second time. How many rows are inserted?
# MAGIC 3. A new file with 50 claims arrives. How many rows does the third run insert?

# COMMAND ----------

# DBTITLE 1,E3 Code
copy_sql = f"""
    COPY INTO {FQ}.claims_copy
    FROM '{CLAIMS_DIR}'
    FILEFORMAT = JSON
    COPY_OPTIONS ('mergeSchema' = 'true')                      -- the first load creates the columns
"""
spark.sql(f"CREATE TABLE {FQ}.claims_copy")                    # schemaless Delta table (no columns)

r1 = spark.sql(copy_sql)                                       # run 1: all 3 files
display(r1)                                                    # COPY INTO returns a small result table
METRICS["E3.result_columns"] = ", ".join(r1.columns)
METRICS["E3.run1_inserted"] = r1.first()["num_inserted_rows"]
after_run1 = spark.table(f"{FQ}.claims_copy").count()

r2 = spark.sql(copy_sql)                                       # run 2: same files again
display(r2)
METRICS["E3.run2_inserted"] = r2.first()["num_inserted_rows"]
after_run2 = spark.table(f"{FQ}.claims_copy").count()

land_claims("claims_004.json", first_id=301, n=50)             # a new delivery arrives
r3 = spark.sql(copy_sql)                                       # run 3: only the new file
display(r3)
METRICS["E3.run3_inserted"] = r3.first()["num_inserted_rows"]
after_run3 = spark.table(f"{FQ}.claims_copy").count()
print(f"table rows after each run: {after_run1}, {after_run2}, {after_run3}")
METRICS["E3.after_run1"], METRICS["E3.after_run2"], METRICS["E3.after_run3"] = after_run1, after_run2, after_run3
METRICS["E3.type_claim_amount"] = dict(spark.table(f"{FQ}.claims_copy").dtypes)["claim_amount"]
history = spark.sql(f"DESCRIBE HISTORY {FQ}.claims_copy")      # one row per table version
display(history.select("version", "operation", "operationMetrics").orderBy("version"))
copy_versions = [r["version"] for r in history.filter("operation = 'COPY INTO'").orderBy("version").collect()]
METRICS["E3.copy_ops"] = len(copy_versions)                    # how many COPY INTO commits
METRICS["nd.E3.copy_versions"] = ", ".join(map(str, copy_versions))   # version numbers can shift (auto-OPTIMIZE)
assert after_run1 == 300 and after_run2 == 300 and after_run3 == 350

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `COPY INTO` returns one row with the columns `{{E3.result_columns}}`.
# MAGIC * Run 1 loaded the table that had **no columns**: `num_inserted_rows` = **{{E3.run1_inserted}}**, table rows = {{E3.after_run1}}. `mergeSchema` let the first load create the columns.
# MAGIC * Run 2 with the same files: `num_inserted_rows` = **{{E3.run2_inserted}}**, table rows = {{E3.after_run2}}. The files were already loaded, so they were skipped. This is the idempotency from the docs.
# MAGIC * After a new 50-claim file arrived, run 3 inserted **{{E3.run3_inserted}}** rows (table rows = {{E3.after_run3}}). Only the new file was loaded.
# MAGIC * `claim_amount` in the table is `{{E3.type_claim_amount}}`: the JSON reader behind `COPY INTO` infers types. Compare with Auto Loader in E4.
# MAGIC * `DESCRIBE HISTORY` has **{{E3.copy_ops}}** versions with the operation `COPY INTO` (in this run: versions {{nd.E3.copy_versions}}), for three runs. Run 2 loaded nothing and wrote **no** table version. This is how you prove idempotency from the table history (P4).
# MAGIC
# MAGIC > **Key takeaway:** `COPY INTO` can run again and again (a nightly job, a retry after an error) without duplicates. `COPY_OPTIONS ('force' = 'true')` switches this off.

# COMMAND ----------

# DBTITLE 1,E4 Predict
# MAGIC %md
# MAGIC ### E4 — Auto Loader basics
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. The landing folder now has 4 files (350 claims). How many rows does the first Auto Loader run load?
# MAGIC 2. We do **not** set `cloudFiles.inferColumnTypes`. Which type does `claim_amount` get?
# MAGIC 3. We run the stream again with no new files, and then once more after a new file with 25 claims. How many rows does each run add?

# COMMAND ----------

# DBTITLE 1,E4 Code
BRONZE = f"{FQ}.claims_bronze"
BRONZE_CKPT = f"{CHECKPOINTS}/claims_bronze"                  # checkpoint AND schema location of this stream

def table_count(name):
    """Row count of a table, or 0 if it does not exist yet."""
    return spark.table(name).count() if spark.catalog.tableExists(name) else 0

def load_bronze():
    """Run the bronze Auto Loader stream once (availableNow) and return how many rows it added."""
    before = table_count(BRONZE)
    query = (spark.readStream
        .format("cloudFiles")                                  # Auto Loader source
        .option("cloudFiles.format", "json")                   # the files are JSON
        .option("cloudFiles.schemaLocation", BRONZE_CKPT)      # keep the inferred schema here (_schemas)
        .load(CLAIMS_DIR)                                      # the landing folder
        .writeStream
        .option("checkpointLocation", BRONZE_CKPT)             # remember which files were loaded
        .trigger(availableNow=True)                            # load all new files, then stop
        .toTable(BRONZE))                                      # start the stream into a managed table
    query.awaitTermination()                                   # wait until availableNow is done
    return table_count(BRONZE) - before

run1 = load_bronze()                                           # first run: every file in the folder
run2 = load_bronze()                                           # second run: no new files
land_claims("claims_005.json", first_id=351, n=25)             # a new delivery
run3 = load_bronze()                                           # third run: only the new file
print(f"rows added: run 1 = {run1}, run 2 = {run2}, run 3 = {run3}")
METRICS["E4.run1_added"], METRICS["E4.run2_added"], METRICS["E4.run3_added"] = run1, run2, run3
METRICS["E4.total"] = table_count(BRONZE)

bronze_types = dict(spark.table(BRONZE).dtypes)                # {column: type}
METRICS["E4.columns"] = ", ".join(spark.table(BRONZE).columns)
METRICS["E4.type_claim_amount"] = bronze_types["claim_amount"]
METRICS["E4.type_claim_id"] = bronze_types["claim_id"]
display(spark.table(BRONZE).orderBy("claim_id").limit(5))

state = spark.sql(f"SELECT * FROM cloud_files_state('{BRONZE_CKPT}')")   # which files Auto Loader loaded
display(state)
METRICS["E4.state_files"] = state.count()
METRICS["E4.state_columns"] = ", ".join(state.columns)
assert run1 == 350 and run2 == 0 and run3 == 25

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Run 1 added **{{E4.run1_added}}** rows: every file that was in the folder (the 3 setup files and the file from E3).
# MAGIC * Run 2 added **{{E4.run2_added}}** rows: no new files. The checkpoint remembers the loaded files, so nothing is read twice.
# MAGIC * After a new 25-claim file, run 3 added **{{E4.run3_added}}** rows. Total: {{E4.total}} rows.
# MAGIC * Types without `cloudFiles.inferColumnTypes`: `claim_amount` = `{{E4.type_claim_amount}}`, `claim_id` = `{{E4.type_claim_id}}`. As the docs say, Auto Loader infers **all columns as strings** for JSON, CSV and XML. `read_files` (E1) and `COPY INTO` (E3) inferred numbers from the same files.
# MAGIC * Columns: `{{E4.columns}}`. Auto Loader added `_rescued_data` by itself.
# MAGIC * `cloud_files_state(checkpoint)` listed **{{E4.state_files}}** files, with the columns `{{E4.state_columns}}`. This is the built-in way to see which files a stream has loaded.
# MAGIC
# MAGIC > **Key takeaway:** the same folder (checkpoint + `cloudFiles.schemaLocation`) is the memory of the stream. If you delete it, the next run loads every file again.

# COMMAND ----------

# DBTITLE 1,E5 Predict
# MAGIC %md
# MAGIC ### E5 — A new column arrives (`addNewColumns`)
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. A file with 20 claims and a new column `payer` lands. What happens when the bronze stream runs?
# MAGIC 2. We restart the stream exactly as before. Does it load the file now?
# MAGIC 3. What is inside the `_schemas` folder of the checkpoint?

# COMMAND ----------

# DBTITLE 1,E5 Code — a file with a new column arrives
land_claims("claims_006.json", first_id=376, n=20, extra={"payer": "Medicare"})   # 20 claims with a new column payer
e5_before = table_count(BRONZE)                                # bronze rows before the three runs
print("bronze rows before run A:", e5_before)

# COMMAND ----------

# DBTITLE 1,E5 Note — the next two cells fail on purpose
# MAGIC %md
# MAGIC **The next two cells are expected to fail.** Run each one, read the error class in the red message, then go on. In a job, the task would stop here: "Databricks recommends configuring Auto Loader streams with Lakeflow Jobs to restart automatically after such schema changes" (*Configure schema inference and evolution in Auto Loader*). In this notebook, **you** do the restart by running the next cell.
# MAGIC
# MAGIC Runs A and B write the full Auto Loader code again (the same as `load_bronze()` in E4), so you can see exactly what changes in run C.

# COMMAND ----------

# DBTITLE 1,E5 Run A — the stream sees the new column
# EXPECT_FAILURE: E5.runA
(spark.readStream
    .format("cloudFiles")                                                             # Auto Loader
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/dbacademy_l06/data/_checkpoints/claims_bronze")
    .load("/Volumes/main/dbacademy_l06/data/landing/claims_json")
    .writeStream
    .option("checkpointLocation", "/Volumes/main/dbacademy_l06/data/_checkpoints/claims_bronze")
    .trigger(availableNow=True)
    .toTable("main.dbacademy_l06.claims_bronze")
    .awaitTermination())                                                              # wait for the result

# COMMAND ----------

# DBTITLE 1,E5 Run B — restart with exactly the same code
# EXPECT_FAILURE: E5.runB
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/dbacademy_l06/data/_checkpoints/claims_bronze")
    .load("/Volumes/main/dbacademy_l06/data/landing/claims_json")
    .writeStream
    .option("checkpointLocation", "/Volumes/main/dbacademy_l06/data/_checkpoints/claims_bronze")
    .trigger(availableNow=True)
    .toTable("main.dbacademy_l06.claims_bronze")
    .awaitTermination())

# COMMAND ----------

# DBTITLE 1,E5 Run C — restart with mergeSchema on the writer
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/dbacademy_l06/data/_checkpoints/claims_bronze")
    .load("/Volumes/main/dbacademy_l06/data/landing/claims_json")
    .writeStream
    .option("checkpointLocation", "/Volumes/main/dbacademy_l06/data/_checkpoints/claims_bronze")
    .option("mergeSchema", "true")                                                    # the only change: the table may get new columns
    .trigger(availableNow=True)
    .toTable("main.dbacademy_l06.claims_bronze")
    .awaitTermination())

run_c_added = table_count(BRONZE) - e5_before                 # runs A and B added nothing
payer_filled = spark.table(BRONZE).filter("payer IS NOT NULL").count()
print(f"run C added {run_c_added} rows; payer filled in {payer_filled} rows")
display(spark.table(BRONZE).filter("claim_id >= 370").orderBy("claim_id"))

schema_files = sorted(f.name for f in dbutils.fs.ls(f"{BRONZE_CKPT}/_schemas"))   # one file per schema version
print("schema versions:", schema_files)
print(dbutils.fs.head(f"{BRONZE_CKPT}/_schemas/{schema_files[-1]}", 1000))   # the latest stored schema
METRICS["E5.runC_added"] = run_c_added
METRICS["E5.total"] = table_count(BRONZE)
METRICS["E5.columns"] = ", ".join(spark.table(BRONZE).columns)
METRICS["E5.payer_filled"] = payer_filled
METRICS["E5.schema_files"] = ", ".join(schema_files)
assert run_c_added == 20

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC | Run | Writer `mergeSchema` | Result | Error classes |
# MAGIC | --- | --- | --- | --- |
# MAGIC | A | no | failed | `{{E5.runA.errors}}` |
# MAGIC | B (restart, same code) | no | failed again | `{{E5.runB.errors}}` |
# MAGIC | C (restart) | yes | added {{E5.runC_added}} rows | — |
# MAGIC
# MAGIC * **Run A** stopped as the docs describe: the new column `payer` raised `UNKNOWN_FIELD_EXCEPTION` (wrapped in `STREAM_FAILED`). Before it stopped, Auto Loader saved a new schema version. The `_schemas` folder now holds the files `{{E5.schema_files}}`: version 0 from the first run and version 1 with `payer`.
# MAGIC * **Run B** shows something the docs page does not say directly: a restart with the same code fails again, now with `DELTA_METADATA_MISMATCH`. The **source** schema has the new column, but the **Delta table** does not, and the writer may not add columns.
# MAGIC * **Run C** added `.option("mergeSchema", "true")` on the **writer**. Now the table has the columns `{{E5.columns}}`. `payer` is filled for {{E5.payer_filled}} rows (the new file only); older rows have NULL. Total rows: {{E5.total}}.
# MAGIC
# MAGIC > **Key takeaway:** for `addNewColumns`, plan the restart (a job that restarts the task) **and** set `mergeSchema` on the writer. Otherwise the restart fails too. `try/except` around the stream does not help: in a test run of this lesson the cell still failed with `Some streams terminated before this command could finish!` (run 771734934782354, see `_authoring/L06.md`).

# COMMAND ----------

# DBTITLE 1,E6 Predict
# MAGIC %md
# MAGIC ### E6 — Rescued data
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. A stream with `cloudFiles.inferColumnTypes = true` has `claim_amount` as `DOUBLE`. A file arrives where 3 claims have `claim_amount = "N/A"`. Does the stream fail? What is in `claim_amount` and `_rescued_data` for those rows?
# MAGIC 2. A stream with `schemaEvolutionMode = rescue` sees a new column `plan_type`. Does the table get a `plan_type` column?

# COMMAND ----------

# DBTITLE 1,E6 Code
TYPED, TYPED_CKPT = f"{FQ}.claims_typed", f"{CHECKPOINTS}/claims_typed"
RESCUE, RESCUE_CKPT = f"{FQ}.claims_rescue", f"{CHECKPOINTS}/claims_rescue"

def load_with(table, ckpt, **options):
    """Run an Auto Loader stream with extra cloudFiles options once; return rows added."""
    before = table_count(table)
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", ckpt))
    for key, value in options.items():
        reader = reader.option(f"cloudFiles.{key}", value)   # e.g. cloudFiles.inferColumnTypes
    query = (reader.load(CLAIMS_DIR).writeStream
             .option("checkpointLocation", ckpt)
             .trigger(availableNow=True)
             .toTable(table))
    query.awaitTermination()
    return table_count(table) - before

# Part A: typed stream, then a file with bad amounts
typed_run1 = load_with(TYPED, TYPED_CKPT, inferColumnTypes="true")
METRICS["E6.typed_run1_added"] = typed_run1
METRICS["E6.typed_type_claim_amount"] = dict(spark.table(TYPED).dtypes)["claim_amount"]
land_claims("claims_007.json", first_id=396, n=10, bad_amount_ids=(396, 397, 398))   # 3 bad values
typed_run2 = load_with(TYPED, TYPED_CKPT, inferColumnTypes="true")
print(f"typed stream: run 1 added {typed_run1}, run 2 added {typed_run2}")
METRICS["E6.typed_run2_added"] = typed_run2
bad = spark.table(TYPED).filter("claim_id IN (396, 397, 398)").orderBy("claim_id")
display(bad.select("claim_id", "claim_amount", "_rescued_data"))
METRICS["E6.bad_amount_null"] = bad.filter("claim_amount IS NULL").count()
METRICS["E6.bad_rescued"] = bad.filter("_rescued_data IS NOT NULL").count()
METRICS["E6.rescued_example"] = bad.first()["_rescued_data"]

# Part B: rescue mode, then a file with a new column
rescue_run1 = load_with(RESCUE, RESCUE_CKPT, schemaEvolutionMode="rescue")
land_claims("claims_008.json", first_id=406, n=10, extra={"plan_type": "HMO"})      # new column plan_type
rescue_run2 = load_with(RESCUE, RESCUE_CKPT, schemaEvolutionMode="rescue")
print(f"rescue stream: run 1 added {rescue_run1}, run 2 added {rescue_run2}")
METRICS["E6.rescue_run1_added"], METRICS["E6.rescue_run2_added"] = rescue_run1, rescue_run2
rescue_cols = spark.table(RESCUE).columns
print("columns of the rescue table:", rescue_cols)
METRICS["E6.rescue_has_plan_type"] = "plan_type" in rescue_cols
METRICS["E6.rescue_rows_with_rescued"] = spark.table(RESCUE).filter("_rescued_data IS NOT NULL").count()
METRICS["E6.rescue_example"] = (spark.table(RESCUE).filter("claim_id = 406").first()["_rescued_data"])
display(spark.table(RESCUE).filter("claim_id >= 406").select("claim_id", "claim_amount", "_rescued_data"))

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC **Part A — type mismatch.** With `cloudFiles.inferColumnTypes = true`, the first run added {{E6.typed_run1_added}} rows and `claim_amount` became `{{E6.typed_type_claim_amount}}`. Then a file with 3 `"N/A"` amounts arrived:
# MAGIC * The stream did **not** fail. It added {{E6.typed_run2_added}} rows.
# MAGIC * For the 3 bad claims, `claim_amount` is NULL in {{E6.bad_amount_null}} rows, and `_rescued_data` is filled in {{E6.bad_rescued}} rows.
# MAGIC * Example `_rescued_data`: `{{E6.rescued_example}}`. It keeps the original value and the source file path, so you can fix the data later.
# MAGIC
# MAGIC **Part B — `schemaEvolutionMode = rescue`.** The first run added {{E6.rescue_run1_added}} rows. A file with the new column `plan_type` arrived, and the second run added {{E6.rescue_run2_added}} rows **without failing**.
# MAGIC * Does the table have a `plan_type` column? **{{E6.rescue_has_plan_type}}**.
# MAGIC * Rows with `_rescued_data` filled: {{E6.rescue_rows_with_rescued}}. Example: `{{E6.rescue_example}}`.
# MAGIC
# MAGIC > **Key takeaway:** rescued data means "never stop, never lose data". `addNewColumns` (E5) changes the table but stops the stream. `rescue` keeps the table schema stable and never stops for a new column.

# COMMAND ----------

# DBTITLE 1,E7 Predict
# MAGIC %md
# MAGIC ### E7 — Two common Auto Loader mistakes
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. An Auto Loader stream has a `checkpointLocation` on the writer, but **no** `cloudFiles.schemaLocation` and no schema. Does it start?
# MAGIC 2. The claim files are JSON, but the code says `cloudFiles.format = csv`. What happens?

# COMMAND ----------

# DBTITLE 1,E7 Code
def start_stream(fmt, schema_location, name):
    """Try to start an Auto Loader stream on the claim files; return the error class, or 'started'."""
    try:
        reader = (spark.readStream
            .format("cloudFiles")                              # Auto Loader
            .option("cloudFiles.format", fmt))                 # the format we claim the files have
        if schema_location:
            reader = reader.option("cloudFiles.schemaLocation", schema_location)   # where the schema is kept
        query = (reader.load(CLAIMS_DIR)
            .writeStream
            .option("checkpointLocation", f"{CHECKPOINTS}/{name}")   # a checkpoint is always given
            .trigger(availableNow=True)
            .toTable(f"{FQ}.{name}"))                          # the error comes when the stream starts
        query.awaitTermination()
        return "started"
    except Exception as e:
        return error_class(e)                                  # the real error class

no_location = start_stream("json", None, "e7_no_location")            # mistake 1: no schemaLocation
wrong_format = start_stream("csv", f"{CHECKPOINTS}/e7_csv", "e7_csv")  # mistake 2: wrong format
print("no schemaLocation:", no_location, "| csv on JSON files:", wrong_format)

csv_columns = (spark.readStream.format("cloudFiles")           # what the CSV parser made of a JSON line
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/e7_csv_look")
    .load(CLAIMS_DIR).columns)                                 # only the inferred schema, no stream starts
print("first inferred CSV column names:", csv_columns[:3])
METRICS["E7.no_location_error"] = no_location
METRICS["E7.wrong_format_error"] = wrong_format
METRICS["E7.csv_first_column"] = csv_columns[0]
METRICS["E7.csv_column_count"] = len(csv_columns)
METRICS["E7.tables_created"] = sum(spark.catalog.tableExists(f"{FQ}.{t}") for t in ("e7_no_location", "e7_csv"))
assert no_location != "started" and wrong_format != "started"

# COMMAND ----------

# DBTITLE 1,E7 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * **No `cloudFiles.schemaLocation`:** the stream did not start: **`{{E7.no_location_error}}`**. The checkpoint alone is not enough: without a given schema, Auto Loader needs a place to store the inferred schema.
# MAGIC * **`csv` on JSON files:** the stream did not start either: **`{{E7.wrong_format_error}}`**. The CSV parser split the JSON lines at the commas and used pieces of JSON text as column names: it inferred {{E7.csv_column_count}} columns, and the first one is named `{{E7.csv_first_column}}`. Delta does not allow such names, so the table could not be created.
# MAGIC * Both errors came when the stream **started**, before any file was processed. That is why a normal `try/except` caught them (a stream that fails **while running** fails the cell anyway — E5). Tables created: {{E7.tables_created}}.
# MAGIC
# MAGIC > **Key takeaway:** read the error class first. `…SCHEMA_LOCATION_REQUIRED` → add `cloudFiles.schemaLocation`. Strange column names or `…INVALID_CHARACTERS_IN_COLUMN_NAMES` → check `cloudFiles.format`. You will fix both bugs in P2.

# COMMAND ----------

# DBTITLE 1,E8 Predict
# MAGIC %md
# MAGIC ### E8 — Parquet lab results with Auto Loader and file metadata columns
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Auto Loader reads the Parquet lab files **without** `cloudFiles.inferColumnTypes`. Is `result_value` a `STRING` (like JSON in E4) or a `DOUBLE`?
# MAGIC 2. We add `_metadata.file_name` and `_metadata.file_modification_time` as columns. A second Parquet file with 50 results arrives. How many rows does each file contribute after the second run?

# COMMAND ----------

# DBTITLE 1,E8 Code
LABS_BRONZE = f"{FQ}.labs_bronze"
LABS_CKPT = f"{CHECKPOINTS}/labs_bronze"                       # checkpoint AND schema location

def load_labs():
    """Run the labs Auto Loader stream once (availableNow); return how many rows it added."""
    before = table_count(LABS_BRONZE)
    query = (spark.readStream
        .format("cloudFiles")                                  # Auto Loader
        .option("cloudFiles.format", "parquet")                # typed files: no inferColumnTypes needed
        .option("cloudFiles.schemaLocation", LABS_CKPT)        # keep the schema here
        .load(LABS_DIR)
        .selectExpr("*",                                       # all data columns + _rescued_data
                    "_metadata.file_name AS source_file",      # which file the row came from
                    "_metadata.file_modification_time AS source_file_time")   # when that file was written
        .writeStream
        .option("checkpointLocation", LABS_CKPT)               # which files were loaded
        .trigger(availableNow=True)                            # load all new files, then stop
        .toTable(LABS_BRONZE))
    query.awaitTermination()
    return table_count(LABS_BRONZE) - before

labs_run1 = load_labs()                                        # labs_001.parquet
land_labs("labs_002.parquet", first_id=101, n=50)              # a second delivery
labs_run2 = load_labs()                                        # only labs_002.parquet
labs_types = dict(spark.table(LABS_BRONZE).dtypes)             # {column: type}
per_source = spark.sql(f"""
    SELECT source_file, count(*) AS row_count, count(DISTINCT source_file_time) AS file_times
    FROM {LABS_BRONZE} GROUP BY source_file ORDER BY source_file""")
display(per_source)                                            # rows per source file
display(spark.table(LABS_BRONZE).orderBy("lab_id").limit(5))
per_source_rows = per_source.collect()
print(f"run 1 added {labs_run1}, run 2 added {labs_run2}; types {labs_types}")
METRICS["E8.run1_added"], METRICS["E8.run2_added"] = labs_run1, labs_run2
METRICS["E8.columns"] = ", ".join(spark.table(LABS_BRONZE).columns)
METRICS["E8.type_result_value"] = labs_types["result_value"]
METRICS["E8.type_collected_at"] = labs_types["collected_at"]
METRICS["E8.type_lab_id"] = labs_types["lab_id"]
METRICS["E8.rows_per_file"] = ", ".join(f"{r['source_file']}: {r['row_count']}" for r in per_source_rows)
assert labs_run1 == 100 and labs_run2 == 50

# COMMAND ----------

# DBTITLE 1,E8 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Run 1 added **{{E8.run1_added}}** rows, run 2 (after the new file) **{{E8.run2_added}}**. Same incremental behaviour as JSON in E4.
# MAGIC * Types **without** `inferColumnTypes`: `result_value` = `{{E8.type_result_value}}`, `collected_at` = `{{E8.type_collected_at}}`, `lab_id` = `{{E8.type_lab_id}}`. Parquet stores the types, so Auto Loader keeps them. The "all strings" rule of E4 is only for JSON, CSV and XML.
# MAGIC * Columns: `{{E8.columns}}`. `_rescued_data` is there for Parquet too; `source_file` and `source_file_time` come from `_metadata`.
# MAGIC * Rows per source file: {{E8.rows_per_file}}. With these two columns, every bronze row can be traced back to its file.
# MAGIC
# MAGIC > **Key takeaway:** add `_metadata.file_name` (or `file_path`) and `_metadata.file_modification_time` in the `select` of the stream. You cannot get this information back after the load.

# COMMAND ----------

# DBTITLE 1,E9 Predict
# MAGIC %md
# MAGIC ### E9 — The `_metadata` column in detail, and `COPY INTO` with a `SELECT`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Does `SELECT *` from `read_files` return a `_metadata` column?
# MAGIC 2. You pass the path `/Volumes/main/...`. Does `_metadata.file_path` look exactly the same?
# MAGIC 3. Can `COPY INTO` add the source file name as a column?

# COMMAND ----------

# DBTITLE 1,E9 Code
star_cols = spark.sql(f"SELECT * FROM read_files('{CLAIMS_DIR}', format => 'json')").columns   # SELECT *
meta = spark.sql(f"""
    SELECT _metadata.*                                         -- every field of the struct as a column
    FROM read_files('{CLAIMS_DIR}', format => 'json')
    WHERE _metadata.file_name = 'claims_001.json'              -- you can filter on file metadata
    LIMIT 1""")
display(meta)
meta_row = meta.first()
print("SELECT * has _metadata:", "_metadata" in star_cols, "| fields:", meta.columns)

spark.sql(f"CREATE TABLE {FQ}.claims_meta")                    # schemaless target
copy_meta_sql = f"""
    COPY INTO {FQ}.claims_meta
    FROM (SELECT claim_id, claim_amount, claim_status,
                 _metadata.file_name AS source_file            -- file name as a normal column
          FROM '{CLAIMS_DIR}')
    FILEFORMAT = JSON
    PATTERN = 'claims_00[1-3].json'                            -- only the first delivery
    COPY_OPTIONS ('mergeSchema' = 'true')"""
meta_result = spark.sql(copy_meta_sql).first()                 # num_affected_rows, num_inserted_rows, ...
per_file = spark.sql(f"SELECT source_file, count(*) AS row_count FROM {FQ}.claims_meta GROUP BY ALL ORDER BY 1")
display(per_file)
METRICS["E9.star_has_metadata"] = "_metadata" in star_cols
METRICS["E9.fields"] = ", ".join(meta.columns)
METRICS["E9.file_path"] = meta_row["file_path"]
METRICS["E9.file_size"] = meta_row["file_size"]
METRICS["E9.copy_inserted"] = meta_result["num_inserted_rows"]
METRICS["E9.copy_columns"] = ", ".join(spark.table(f"{FQ}.claims_meta").columns)
METRICS["E9.copy_files"] = per_file.count()
assert meta_result["num_inserted_rows"] == 300

# COMMAND ----------

# DBTITLE 1,E9 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Does `SELECT *` return a `_metadata` column? **{{E9.star_has_metadata}}**. You must name it, as the *File metadata column* page says.
# MAGIC * `_metadata.*` gave the fields `{{E9.fields}}`.
# MAGIC * `file_path` for `claims_001.json` was `{{E9.file_path}}` ({{E9.file_size}} bytes). Note the `dbfs:` prefix: compare paths with `endswith` or use `file_name`, not an exact match on the path you typed.
# MAGIC * `COPY INTO … FROM (SELECT …, _metadata.file_name AS source_file FROM '<path>')` worked: {{E9.copy_inserted}} rows from {{E9.copy_files}} files, table columns `{{E9.copy_columns}}`. The `SELECT` can also choose and rename columns.
# MAGIC
# MAGIC > **Key takeaway:** `_metadata` is free lineage, but only when you select it. In `COPY INTO`, put it in the `SELECT` of the `FROM (…)` clause.

# COMMAND ----------

# DBTITLE 1,E10 Predict
# MAGIC %md
# MAGIC ### E10 — List the landing folders with `binaryFile`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Which columns does `read_files(..., format => 'binaryFile')` return?
# MAGIC 2. How many files are in the claims folder now (count the deliveries of E3 to E6)?
# MAGIC 3. How do you list the files of **all** sub-folders of `landing` in one read?

# COMMAND ----------

# DBTITLE 1,E10 Code
bin_query = f"SELECT * FROM read_files('{CLAIMS_DIR}', format => 'binaryFile')"
bin_cols = spark.sql(f"DESCRIBE QUERY {bin_query}")           # column names and types
display(bin_cols)
listing = spark.sql(f"""
    SELECT * EXCEPT (content)                                  -- the docs' advice: skip the bytes
    FROM read_files('{CLAIMS_DIR}', format => 'binaryFile')
    ORDER BY path""")
display(listing)
bin_stats = listing.selectExpr("count(*) AS files", "sum(length) AS bytes").first()

all_files = (spark.read.format("binaryFile")                   # the same in PySpark
    .option("recursiveFileLookup", "true")                     # also read the sub-folders
    .load(f"{VOL}/landing")
    .select("path", "length"))                                 # never collect the content column
by_folder = all_files.selectExpr("regexp_extract(path, 'landing/([^/]+)/', 1) AS folder").groupBy("folder").count().orderBy("folder")
display(by_folder)
parquet_only = (spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")                     # only file names that match the glob
    .load(f"{VOL}/landing")
    .select("path")
    .count())
folder_counts = ", ".join(f"{r['folder']}: {r['count']}" for r in by_folder.collect())
print(f"claims folder: {bin_stats['files']} files, {bin_stats['bytes']} bytes | all landing files: {folder_counts} | "
      f"*.parquet files: {parquet_only}")
METRICS["E10.columns"] = ", ".join(f"{r['col_name']} {r['data_type']}" for r in bin_cols.collect())
METRICS["E10.claim_files"] = bin_stats["files"]
METRICS["E10.claim_bytes"] = bin_stats["bytes"]
METRICS["E10.landing_files"] = folder_counts
METRICS["E10.glob_parquet"] = parquet_only
assert bin_stats["files"] == 8

# COMMAND ----------

# DBTITLE 1,E10 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `binaryFile` columns: `{{E10.columns}}` — the fixed schema from the docs. One row per file, not per record.
# MAGIC * The claims folder holds **{{E10.claim_files}}** files with {{E10.claim_bytes}} bytes: the 3 setup files plus the deliveries of E3, E4, E5 and E6 (two files).
# MAGIC * `recursiveFileLookup` listed every landing sub-folder in one read: {{E10.landing_files}}.
# MAGIC * `pathGlobFilter = '*.parquet'` (with `recursiveFileLookup`) found {{E10.glob_parquet}} files: the filter works on file names.
# MAGIC
# MAGIC > **Key takeaway:** `binaryFile` is the format for images, PDFs and other non-tabular files, and a quick SQL way to list a folder. Always drop `content` when you only need the listing.

# COMMAND ----------

# DBTITLE 1,E11 Predict
# MAGIC %md
# MAGIC ### E11 — `STREAM read_files` in a notebook, and a streaming table
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `spark.sql("SELECT * FROM STREAM read_files(...)")` without a schema location — does it work in a notebook?
# MAGIC 2. With `schemaLocation => '…'` — can we write it to a table with `availableNow`?
# MAGIC 3. Does `CREATE OR REFRESH STREAMING TABLE … AS SELECT * FROM STREAM read_files(…)` work in this serverless notebook?

# COMMAND ----------

# DBTITLE 1,E11 Code
SQL_BRONZE, SQL_CKPT = f"{FQ}.labs_bronze_sql", f"{CHECKPOINTS}/labs_bronze_sql"

try:                                                           # 1: no schema location
    spark.sql(f"SELECT * FROM STREAM read_files('{LABS_DIR}', format => 'parquet')").columns
    stream_no_location = "worked"
except Exception as e:
    stream_no_location = error_class(e)

labs_stream = spark.sql(f"""
    SELECT *, _metadata.file_name AS source_file               -- the same lineage column as in E8
    FROM STREAM read_files('{LABS_DIR}',
                           format => 'parquet',
                           schemaLocation => '{SQL_CKPT}')      -- 2: with a schema location
""")
sql_query = (labs_stream.writeStream                           # a normal streaming DataFrame
    .option("checkpointLocation", SQL_CKPT)
    .trigger(availableNow=True)
    .toTable(SQL_BRONZE))
sql_query.awaitTermination()
sql_rows = spark.table(SQL_BRONZE).count()

try:                                                           # 3: a streaming table from a notebook
    spark.sql(f"""CREATE OR REFRESH STREAMING TABLE {FQ}.labs_st
                  AS SELECT * FROM STREAM read_files('{LABS_DIR}', format => 'parquet')""")
    st_result = "created"
except Exception as e:
    st_result = error_class(e)
print(f"no schemaLocation: {stream_no_location} | is streaming: {labs_stream.isStreaming}, rows: {sql_rows} | "
      f"streaming table: {st_result}")
display(spark.table(SQL_BRONZE).groupBy("source_file").count().orderBy("source_file"))
METRICS["E11.no_location_error"] = stream_no_location
METRICS["E11.is_streaming"] = labs_stream.isStreaming
METRICS["E11.rows"] = sql_rows
METRICS["E11.type_result_value"] = dict(spark.table(SQL_BRONZE).dtypes)["result_value"]
METRICS["E11.streaming_table"] = st_result
assert sql_rows == 150

# COMMAND ----------

# DBTITLE 1,E11 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Without a schema location, `STREAM read_files` in a notebook failed: **`{{E11.no_location_error}}`** — the same error as Python Auto Loader in E7. The docs say the schema location "isn't required when used in a streaming table query"; a notebook stream is not a streaming table.
# MAGIC * With `schemaLocation => …` it worked: a streaming DataFrame (`isStreaming` = {{E11.is_streaming}}) that we wrote with `availableNow`: **{{E11.rows}}** rows, `result_value` = `{{E11.type_result_value}}`. So `STREAM read_files` is Auto Loader with SQL syntax.
# MAGIC * `CREATE OR REFRESH STREAMING TABLE` in this serverless notebook: **`{{E11.streaming_table}}`**. On this workspace a streaming table cannot be created from serverless notebook compute. Streaming tables belong to Lakeflow Declarative Pipelines (L07) and Databricks SQL (*CREATE STREAMING TABLE* reference).
# MAGIC
# MAGIC > **Key takeaway:** the exam's SQL form of Auto Loader is `CREATE OR REFRESH STREAMING TABLE … AS SELECT … FROM STREAM read_files(…)`. In a notebook, `STREAM read_files` needs `schemaLocation` and a `writeStream` with a checkpoint, like Python Auto Loader.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "Auto Loader infers numbers from JSON by itself." | No: without `cloudFiles.inferColumnTypes` every JSON/CSV/XML column is a string (E4, Auto Loader schema docs). |
# MAGIC | 2 | "Auto Loader on Parquet also gives strings without `inferColumnTypes`." | Parquet stores types: `result_value` stayed `{{E8.type_result_value}}` (E8). |
# MAGIC | 3 | "`read_files` and Auto Loader have the same type defaults." | `read_files` has `inferColumnTypes = true` by default; Auto Loader does not (E1 vs E4). |
# MAGIC | 4 | "A date text in JSON becomes `DATE` automatically." | `read_files` kept `service_date` as a string (E1). Use `schemaHints` or a cast. |
# MAGIC | 5 | "Running `COPY INTO` again duplicates the rows." | Loaded files are skipped; the second run inserted {{E3.run2_inserted}} rows and wrote no table version (E3). |
# MAGIC | 6 | "`COPY INTO` needs a table with columns." | A schemaless `CREATE TABLE t` plus `COPY_OPTIONS ('mergeSchema' = 'true')` works (E3). |
# MAGIC | 7 | "`'force' = 'true'` only forces a schema update." | `force` turns idempotency off: files load again (*COPY INTO* reference). |
# MAGIC | 8 | "`addNewColumns` adds a new column without stopping." | The stream stops with `UNKNOWN_FIELD_EXCEPTION`; a restart is needed (E5). |
# MAGIC | 9 | "After the schema change, a restart with the same code works." | It failed with `DELTA_METADATA_MISMATCH` until the writer had `mergeSchema` (E5). |
# MAGIC | 10 | "`rescue` mode adds new columns to the table." | No: the table keeps its schema; the new column goes into `_rescued_data` (E6). |
# MAGIC | 11 | "A wrong type (`"N/A"` in a number column) fails the stream or drops the row." | The value becomes NULL and the original value is saved in `_rescued_data` with the file path (E6). |
# MAGIC | 12 | "The checkpoint folder alone is enough for Auto Loader." | Without a schema, the stream did not start: `{{E7.no_location_error}}` (E7). |
# MAGIC | 13 | "A wrong `cloudFiles.format` gives a clear format error." | CSV on JSON files gave `{{E7.wrong_format_error}}`: JSON text became column names (E7). |
# MAGIC | 14 | "`SELECT *` returns the file name column." | `_metadata` must be selected by name; `SELECT *` did not return it (E9). |
# MAGIC | 15 | "`_metadata.file_path` is exactly the path I typed." | It was `{{E9.file_path}}` — with a `dbfs:` prefix (E9). |
# MAGIC | 16 | "`binaryFile` only lists file names." | It also reads the bytes into `content`; use `* EXCEPT (content)` for a listing (E10, *read_files* reference). |
# MAGIC | 17 | "`STREAM read_files` works only inside a pipeline." | In a notebook it worked as a streaming DataFrame with `schemaLocation`; without it: `{{E11.no_location_error}}` (E11). |
# MAGIC | 18 | "`CREATE OR REFRESH STREAMING TABLE` runs anywhere." | In this serverless notebook it failed with `{{E11.streaming_table}}` (E11); streaming tables belong to Lakeflow pipelines and Databricks SQL. |
# MAGIC | 19 | "`try/except` around a failing stream keeps a notebook job alive." | Only for errors at start (E7). A stream that fails while running failed the cell anyway: `Some streams terminated before this command could finish!` (test run in `_authoring/L06.md`, E5). |
# MAGIC | 20 | "Auto Loader uses file notification mode by default." | The default is directory listing mode (*What is Auto Loader?*). |
# MAGIC | 21 | "`DESCRIBE HISTORY` of the target lists the loaded files." | Use `cloud_files_state('<checkpoint>')` (E4) or a `source_file` column from `_metadata` (E8). |
# MAGIC | 22 | "`spark.read.csv(path)` uses the header row." | Not without `header`: you get `_c0, _c1, …` and the header as a data row (E2). |

# COMMAND ----------

# DBTITLE 1,7 — Syntax Cheat Sheet
# MAGIC %md
# MAGIC ## 7 Syntax Cheat Sheet
# MAGIC
# MAGIC | Goal | SQL | PySpark |
# MAGIC | --- | --- | --- |
# MAGIC | Look at files | `SELECT * FROM read_files('/Volumes/…', format => 'json')` | `spark.read.format("json").load("/Volumes/…")` |
# MAGIC | CSV with header + types | `read_files(path, format => 'csv', header => true)` | `spark.read.option("header", "true").option("inferSchema", "true").csv(path)` |
# MAGIC | Change one type | `schemaHints => 'npi STRING'` | `.option("cloudFiles.schemaHints", "npi STRING")` (Auto Loader) |
# MAGIC | Parquet files | `read_files(path, format => 'parquet')` | `spark.read.parquet(path)` · Auto Loader `.option("cloudFiles.format", "parquet")` |
# MAGIC | Source file name | `SELECT *, _metadata.file_name AS source_file` | `.selectExpr("*", "_metadata.file_name AS source_file")` |
# MAGIC | List files | `SELECT * EXCEPT (content) FROM read_files(path, format => 'binaryFile')` | `spark.read.format("binaryFile").option("recursiveFileLookup", "true").option("pathGlobFilter", "*.parquet").load(path).select("path", "length")` |
# MAGIC | Idempotent batch load | `COPY INTO t FROM '/Volumes/…' FILEFORMAT = JSON COPY_OPTIONS ('mergeSchema' = 'true')` | `spark.sql("COPY INTO …")` |
# MAGIC | `COPY INTO` with file name | `COPY INTO t FROM (SELECT *, _metadata.file_name AS source_file FROM '/Volumes/…') FILEFORMAT = PARQUET` | `spark.sql(…)` |
# MAGIC | Prove idempotency | `DESCRIBE HISTORY t` (count `operation = 'COPY INTO'`), `SELECT count(*) FROM t VERSION AS OF n` | `spark.sql("DESCRIBE HISTORY t").filter("operation = 'COPY INTO'")` |
# MAGIC | Incremental load | `CREATE OR REFRESH STREAMING TABLE t AS SELECT * FROM STREAM read_files('…')` (Lakeflow pipelines / Databricks SQL) | `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", ckpt).load(dir)` |
# MAGIC | SQL stream in a notebook | `SELECT * FROM STREAM read_files('…', format => 'parquet', schemaLocation => '…')` | `spark.sql(that).writeStream.option("checkpointLocation", ckpt).trigger(availableNow=True).toTable("t")` |
# MAGIC | Run once and stop | — | `.writeStream.option("checkpointLocation", ckpt).trigger(availableNow=True).toTable("t")` |
# MAGIC | Allow new columns in the table | `COPY_OPTIONS ('mergeSchema' = 'true')` | `.writeStream.option("mergeSchema", "true")` |
# MAGIC | Loaded files of a stream | `SELECT * FROM cloud_files_state('<checkpoint>')` | `spark.sql("SELECT * FROM cloud_files_state('…')")` |
# MAGIC | Run a notebook with parameters | — | `dbutils.notebook.run("./child", 600, {"key": "value"})` |
# MAGIC
# MAGIC ### Auto Loader options — quick reference
# MAGIC
# MAGIC | Option | Default | Use |
# MAGIC | --- | --- | --- |
# MAGIC | `cloudFiles.format` | — (required) | `json`, `csv`, `parquet`, `avro`, `orc`, `text`, `binaryFile`, `xml` |
# MAGIC | `cloudFiles.schemaLocation` | — | where `_schemas` is stored; needed for inference and evolution |
# MAGIC | `cloudFiles.inferColumnTypes` | `false` | infer numbers and booleans instead of strings |
# MAGIC | `cloudFiles.schemaHints` | — | set the type of some columns |
# MAGIC | `cloudFiles.schemaEvolutionMode` | `addNewColumns` (no schema given) / `none` (schema given) | `addNewColumns`, `rescue`, `failOnNewColumns`, `none` |

# COMMAND ----------

# DBTITLE 1,8 — Check Your Understanding
# MAGIC %md
# MAGIC ## 8 Check Your Understanding
# MAGIC
# MAGIC Answer in your own words. Answers are in section 11.
# MAGIC
# MAGIC 1. A colleague deletes the checkpoint folder of an Auto Loader stream "to clean up" and runs the stream again. What happens, and why?
# MAGIC 2. Your team receives about 200 claim files per month and only writes SQL. Another team receives millions of device files per day. Which loading method do you suggest to each team, and why?
# MAGIC 3. Explain the difference between `schemaEvolutionMode = addNewColumns` and `rescue` for a bronze table that a dashboard reads every hour.

# COMMAND ----------

# DBTITLE 1,9 — Practice (intro)
# MAGIC %md
# MAGIC ## 9 Practice
# MAGIC
# MAGIC Do these yourself, without AI help. Each task has a validation cell that prints ✅ Passed or a hint. Solutions are in section 11.
# MAGIC
# MAGIC > The practice uses the files that the examples landed. Run sections 3 and 5 first.

# COMMAND ----------

# DBTITLE 1,P1 Task
# MAGIC %md
# MAGIC ### P1 — Fill in the blanks (3 blanks): Auto Loader syntax
# MAGIC Load the provider CSV files from `PROVIDERS_DIR` into `main.dbacademy_l06.p1_providers` with Auto Loader. Replace the three `<FILL_IN>` markers.

# COMMAND ----------

# DBTITLE 1,P1 Code
# >>> BLANK P1
p1_query = (spark.readStream
    .format(<FILL_IN>)                                      # 1: the Auto Loader source
    .option(<FILL_IN>, "csv")                               # 2: the option that sets the file format
    .option("header", "true")                               # the CSV files have a header row
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/p1")
    .load(PROVIDERS_DIR)
    .writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/p1")
    .trigger(<FILL_IN>)                                     # 3: load everything new, then stop
    .toTable(f"{FQ}.p1_providers"))
p1_query.awaitTermination()
# >>> SOLUTION P1
p1_query = (spark.readStream
    .format("cloudFiles")                                   # 1: the Auto Loader source
    .option("cloudFiles.format", "csv")                     # 2: the option that sets the file format
    .option("header", "true")                               # the CSV files have a header row
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/p1")
    .load(PROVIDERS_DIR)
    .writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/p1")
    .trigger(availableNow=True)                             # 3: load everything new, then stop
    .toTable(f"{FQ}.p1_providers"))
p1_query.awaitTermination()
# <<< END P1

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    t = spark.table(f"{FQ}.p1_providers")
    rows, cols = t.count(), t.columns
    ok = rows == 10 and "provider_id" in cols
    PRACTICE_PASSED["P1"] = ok
    print("✅ Passed" if ok else
          f"Hint: want 10 rows and a provider_id column; got {rows} rows and columns {cols}. "
          "Did the header option reach the reader?")
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: the table main.dbacademy_l06.p1_providers does not exist yet. Check the three blanks.", error_class(e))
METRICS["P1.rows"] = spark.table(f"{FQ}.p1_providers").count() if spark.catalog.tableExists(f"{FQ}.p1_providers") else 0

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs): a bronze claims stream that does not start
# MAGIC A colleague wrote an Auto Loader stream that should load **all** claim files from `CLAIMS_DIR` into `main.dbacademy_l06.p2_claims` (one `availableNow` run). It does not start. The code has two bugs. Fix them in place.
# MAGIC The first lines of the cell reset the table and the checkpoint, so you can run the cell as often as you like.

# COMMAND ----------

# DBTITLE 1,P2 Code
spark.sql(f"DROP TABLE IF EXISTS {FQ}.p2_claims")              # reset: every run starts from zero
dbutils.fs.rm(f"{CHECKPOINTS}/p2", True)                       # reset the checkpoint folder too
# >>> BLANK P2
p2_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load(CLAIMS_DIR)
    .writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/p2")
    .trigger(availableNow=True)
    .toTable(f"{FQ}.p2_claims"))
p2_query.awaitTermination()
# >>> SOLUTION P2
p2_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")                          # fix 1: the claim files are JSON
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/p2")     # fix 2: where the inferred schema is kept
    .load(CLAIMS_DIR)
    .writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/p2")
    .trigger(availableNow=True)
    .toTable(f"{FQ}.p2_claims"))
p2_query.awaitTermination()
# <<< END P2

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    t = spark.table(f"{FQ}.p2_claims")
    expected = spark.read.text(CLAIMS_DIR).count()                # one claim per line in all landed files
    rows, cols = t.count(), t.columns
    ok = rows == expected and "claim_amount" in cols
    PRACTICE_PASSED["P2"] = ok
    if ok:
        print("✅ Passed")
    elif "claim_amount" not in cols:
        print(f"Hint: the columns are {cols}. Which cloudFiles.format do the claim files have?")
    else:
        print(f"Hint: {rows} rows instead of {expected}. Did the stream read every file in CLAIMS_DIR?")
    METRICS["P2.rows"] = rows
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: the table main.dbacademy_l06.p2_claims does not exist: the stream did not start. "
          "Read the error class of the code cell and compare it with E7.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): a typed bronze claims table with file metadata
# MAGIC Load **all** claim files from `CLAIMS_DIR` into `main.dbacademy_l06.p3_claims` with Auto Loader, in one `availableNow` run. Requirements:
# MAGIC * `claim_amount` must be `DOUBLE` and `service_date` must be `DATE`;
# MAGIC * the 3 claims with `"N/A"` must not stop the load (their original value must be kept);
# MAGIC * two lineage columns: `source_file` (the file name) and `source_file_time` (the file modification time).
# MAGIC
# MAGIC **Hints:** checkpoint and schema location `f"{CHECKPOINTS}/p3"` · `cloudFiles.inferColumnTypes` · `cloudFiles.schemaHints` with two columns · the `"N/A"` values go to the rescued data column by themselves · `_metadata` must be selected by name (E8).

# COMMAND ----------

# DBTITLE 1,P3 Code
# >>> BLANK P3
# Your code here
# >>> SOLUTION P3
p3_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")                              # numbers instead of strings
    .option("cloudFiles.schemaHints", "claim_amount DOUBLE, service_date DATE") # fix two types
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/p3")
    .load(CLAIMS_DIR)
    .selectExpr("*",
                "_metadata.file_name AS source_file",                          # lineage: which file
                "_metadata.file_modification_time AS source_file_time")        # lineage: when it was written
    .writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/p3")
    .trigger(availableNow=True)
    .toTable(f"{FQ}.p3_claims"))
p3_query.awaitTermination()
# <<< END P3

# COMMAND ----------

# DBTITLE 1,P3 Validation
try:
    t = spark.table(f"{FQ}.p3_claims")
    expected = spark.read.text(CLAIMS_DIR).count()                # one claim per line in all landed files
    n_files = len([f for f in dbutils.fs.ls(CLAIMS_DIR) if f.name.endswith(".json")])
    rows, types = t.count(), dict(t.dtypes)
    null_amounts = t.filter("claim_amount IS NULL").count()
    rescued = t.filter("_rescued_data LIKE '%claim_amount%'").count() if "_rescued_data" in types else 0
    files = t.select("source_file").distinct().count() if "source_file" in types else 0
    ok = (rows == expected and types.get("claim_amount") == "double" and types.get("service_date") == "date"
          and null_amounts == 3 and rescued == 3 and files == n_files
          and types.get("source_file_time") == "timestamp")
    PRACTICE_PASSED["P3"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {expected} rows, claim_amount double, service_date date, 3 NULL amounts kept in _rescued_data, "
          f"{n_files} distinct source_file values and source_file_time timestamp; got {rows} rows, "
          f"claim_amount {types.get('claim_amount')}, service_date {types.get('service_date')}, {null_amounts} NULL amounts, "
          f"{rescued} rescued, {files} source files, source_file_time {types.get('source_file_time')}.")
    METRICS["P3.rows"], METRICS["P3.null_amounts"], METRICS["P3.rescued"], METRICS["P3.files"] = rows, null_amounts, rescued, files
except Exception as e:
    PRACTICE_PASSED["P3"] = False
    print("Hint: the table main.dbacademy_l06.p3_claims does not exist yet.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints): `COPY INTO` version and idempotency proof
# MAGIC 1. Create the empty table `main.dbacademy_l06.p4_labs` and load **all** Parquet lab files from `LABS_DIR` into it with `COPY INTO`, with an extra column `source_file` (the file name).
# MAGIC 2. Run exactly the same `COPY INTO` a second time. Store its `num_inserted_rows` in the variable `p4_second_inserted`.
# MAGIC 3. Prove from the table history that the second run changed nothing: store the list of table versions whose operation is `COPY INTO` in `p4_copy_versions`, and the row count of the table **at the last of these versions** (time travel) in `p4_rows_at_copy`.
# MAGIC
# MAGIC Goal: the second run inserts 0 rows, there is exactly one `COPY INTO` version, and the table at that version has as many rows as the table now.

# COMMAND ----------

# DBTITLE 1,P4 Code
# >>> BLANK P4
# Your code here
# >>> SOLUTION P4
spark.sql(f"DROP TABLE IF EXISTS {FQ}.p4_labs")
spark.sql(f"CREATE TABLE {FQ}.p4_labs")
p4_sql = f"""
    COPY INTO {FQ}.p4_labs
    FROM (SELECT *, _metadata.file_name AS source_file FROM '{LABS_DIR}')
    FILEFORMAT = PARQUET
    COPY_OPTIONS ('mergeSchema' = 'true')
"""
spark.sql(p4_sql)                                                        # first load
p4_second_inserted = spark.sql(p4_sql).first()["num_inserted_rows"]      # the retry
p4_copy_versions = [r["version"] for r in spark.sql(f"DESCRIBE HISTORY {FQ}.p4_labs")
                    .filter("operation = 'COPY INTO'").orderBy("version").collect()]
p4_rows_at_copy = spark.sql(f"SELECT count(*) AS n FROM {FQ}.p4_labs VERSION AS OF {p4_copy_versions[-1]}").first()["n"]
print(p4_second_inserted, p4_copy_versions, p4_rows_at_copy)
# <<< END P4

# COMMAND ----------

# DBTITLE 1,P4 Validation
try:
    t = spark.table(f"{FQ}.p4_labs")
    rows, cols = t.count(), t.columns
    expected = spark.read.parquet(LABS_DIR).count()
    real_versions = [r["version"] for r in spark.sql(f"DESCRIBE HISTORY {FQ}.p4_labs")
                     .filter("operation = 'COPY INTO'").orderBy("version").collect()]
    files = t.select("source_file").distinct().count() if "source_file" in cols else 0
    ok = (rows == expected and files == 2 and p4_second_inserted == 0
          and list(p4_copy_versions) == real_versions and len(real_versions) == 1 and p4_rows_at_copy == rows)
    PRACTICE_PASSED["P4"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {expected} rows from 2 source files, second run 0 rows, one COPY INTO version {real_versions}, "
          f"and the same row count at that version; got {rows} rows, {files} files, p4_second_inserted={p4_second_inserted}, "
          f"p4_copy_versions={p4_copy_versions}, p4_rows_at_copy={p4_rows_at_copy}.")
    METRICS["P4.rows"], METRICS["P4.second_inserted"], METRICS["P4.copy_ops"] = rows, p4_second_inserted, len(real_versions)
except Exception as e:
    PRACTICE_PASSED["P4"] = False
    print("Hint: create main.dbacademy_l06.p4_labs and the variables p4_second_inserted, p4_copy_versions, "
          "p4_rows_at_copy first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,10 — Exam-Style Questions
# MAGIC %md
# MAGIC ## 10 Exam-Style Questions
# MAGIC
# MAGIC Choose the **one best answer**. Answers and explanations are in section 11.
# MAGIC
# MAGIC **Q1.** A hospital loads lab-result JSON files with Auto Loader. The code sets `cloudFiles.format` and `cloudFiles.schemaLocation`, but no schema and no other options. Which type does the numeric column `result_value` get in the bronze table?
# MAGIC A. `DOUBLE` · B. `STRING` · C. `DECIMAL(10,2)` · D. the type of the value in the first file
# MAGIC
# MAGIC **Q2.** A nightly `COPY INTO` job loads claim files into a Delta table. It fails after the load has committed, and the scheduler runs it again on the same files. What happens?
# MAGIC A. Every row is inserted a second time. · B. The files are skipped and no rows are inserted. · C. The command fails because the files were already loaded. · D. Nothing is skipped unless `'force' = 'true'` is set.
# MAGIC
# MAGIC **Q3.** A bronze Auto Loader stream (no schema given) writes claims to a Delta table. A new column `payer` appears; the stream stops. The engineer restarts the job with **unchanged** code, and it fails again with `DELTA_METADATA_MISMATCH`. What is the fix?
# MAGIC A. Set `cloudFiles.schemaEvolutionMode` to `none`. · B. Add `.option("mergeSchema", "true")` to the `writeStream`. · C. Delete the checkpoint folder. · D. Set `cloudFiles.inferColumnTypes` to `true`.
# MAGIC
# MAGIC **Q4.** A new Auto Loader stream for pharmacy claim JSON files sets `cloudFiles.format` and a `checkpointLocation`, but no schema. It does not start: `CLOUDFILES_SCHEMA_LOCATION_REQUIRED`. What is the fix, and what will that location contain?
# MAGIC A. Add `cloudFiles.schemaLocation`; it stores the inferred schema and its later versions in a `_schemas` folder. · B. Add `cloudFiles.inferColumnTypes = true`; the error is about types. · C. Remove `checkpointLocation`; Auto Loader then uses it as the schema location. · D. Switch to `cloudFiles.format = 'csv'`, which needs no schema.
# MAGIC
# MAGIC **Q5.** An Auto Loader stream with an inferred `DOUBLE` column `heart_rate` receives a record with `"heart_rate": "N/A"`. What happens?
# MAGIC A. The stream fails. · B. The record is dropped. · C. `heart_rate` is NULL and the original value is kept in `_rescued_data` with the file path. · D. The whole file is skipped.
# MAGIC
# MAGIC **Q6.** A payer sends millions of small eligibility files per day to cloud storage. The team wants incremental loading with schema evolution. What should they use?
# MAGIC A. `COPY INTO` · B. Auto Loader · C. a view on `read_files` · D. `CREATE TABLE AS SELECT` every day
# MAGIC
# MAGIC **Q7.** An engineer must find out which landing files an Auto Loader stream has already ingested. What is the best tool?
# MAGIC A. `DESCRIBE HISTORY` on the target table · B. `SELECT * FROM cloud_files_state('<checkpoint>')` · C. `dbutils.fs.ls` on the landing folder · D. `SHOW TABLES`
# MAGIC
# MAGIC **Q8.** An auditor asks from which landing file each row of a bronze lab-results table came. The team loads Parquet files with Auto Loader. What should the stream do?
# MAGIC A. Nothing: `SELECT *` on the source already returns `_metadata`. · B. Add `.selectExpr("*", "_metadata.file_name AS source_file")` before `writeStream`. · C. Read `DESCRIBE HISTORY` of the bronze table later. · D. Set `cloudFiles.format = 'binaryFile'`.

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — `explode_outer`** (`explode` would drop patients 2 and 3: empty array and NULL)
# MAGIC {{solution:W1}}
# MAGIC
# MAGIC **W2 — colon notation** (the result of a colon path is a `STRING`; `::double` casts it)
# MAGIC {{solution:W2}}
# MAGIC
# MAGIC **W3 — `parse_json`** (a path on a `VARIANT` returns a `VARIANT`; `::string` casts it)
# MAGIC {{solution:W3}}
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. The checkpoint is the memory of the stream: it lists the loaded files (and the schema, if `schemaLocation` points there). Without it, the stream is a new stream. It loads **every** file in the folder again, so the table gets duplicates (E4: run 2 added 0 rows only because the checkpoint existed).
# MAGIC 2. 200 files per month and SQL only: `COPY INTO`. It is idempotent SQL and fine for "files in the order of thousands". Millions of files per day: Auto Loader, recommended "for files in the order of millions or more" and with better schema inference and evolution (*Ingest data from cloud object storage*).
# MAGIC 3. `addNewColumns` adds the new column to the table, but the stream stops first and needs a restart with writer `mergeSchema` (E5): the dashboard gets the column, but data is late until the restart. `rescue` never stops and never changes the table: the dashboard always gets fresh data, but the new column is only inside `_rescued_data` until someone changes the schema (E6).
# MAGIC
# MAGIC ### P1
# MAGIC {{solution:P1}}
# MAGIC
# MAGIC ### P2 (fixes: `cloudFiles.format` = `json`; add `cloudFiles.schemaLocation` — see E7 for the two error classes)
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
# MAGIC **Q1 → B.** Without `cloudFiles.inferColumnTypes`, Auto Loader infers all JSON columns as strings (E4: `claim_amount` = `{{E4.type_claim_amount}}`). A wrong: needs `inferColumnTypes`. C wrong: no decimal inference here. D wrong: there is no "first file decides" rule.
# MAGIC
# MAGIC **Q2 → B.** `COPY INTO` skips files it already loaded (E3: second run inserted {{E3.run2_inserted}} rows). A wrong: that is the behaviour with `force`. C wrong: it does not fail. D wrong: skipping is the default; `force` turns it **off**.
# MAGIC
# MAGIC **Q3 → B.** The source schema was already updated; the Delta table also needs permission to add the column (E5: run B `{{E5.runB.errors}}`, run C with `mergeSchema` added {{E5.runC_added}} rows). A wrong: `none` ignores new columns, it does not fix the table. C wrong: deleting the checkpoint reloads every file (duplicates). D wrong: types are not the problem.
# MAGIC
# MAGIC **Q4 → A.** E7: without it the stream failed with `{{E7.no_location_error}}`; E5: the schema location holds the `_schemas` folder (files `{{E5.schema_files}}`). B wrong: the error is about the missing location, not types. C wrong: the checkpoint is still needed for the processed files, and the location is not taken from it. D wrong: the files are JSON, and reading them as CSV failed too (E7: `{{E7.wrong_format_error}}`).
# MAGIC
# MAGIC **Q5 → C.** E6: {{E6.bad_amount_null}} NULL values and the original in `_rescued_data`, for example `{{E6.rescued_example}}`. A/B/D wrong: the stream continued and added all {{E6.typed_run2_added}} rows of the file.
# MAGIC
# MAGIC **Q6 → B.** Auto Loader for "files in the order of millions or more over time" (*Ingest data from cloud object storage*). A wrong: `COPY INTO` is for thousands of files. C wrong: `read_files` reads all files on every query. D wrong: a full reload every day.
# MAGIC
# MAGIC **Q7 → B.** `cloud_files_state` lists the files of the stream (E4: {{E4.state_files}} files). A wrong: history shows operations, not file names. C wrong: lists the folder, not what was ingested. D wrong: unrelated.
# MAGIC
# MAGIC **Q8 → B.** E8 kept `source_file` per row ({{E8.rows_per_file}}). A wrong: `SELECT *` did not return `_metadata` (E9: {{E9.star_has_metadata}}). C wrong: history shows operations, not file names. D wrong: `binaryFile` reads files as bytes, not lab rows.

# COMMAND ----------

# DBTITLE 1,12 — Cleanup
for q in spark.streams.active:                          # stop any stream that is still running
    q.stop()
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")        # drops tables, the volume and all its files
print(f"Dropped {FQ} (tables, volume, files, checkpoints).")

# COMMAND ----------

# DBTITLE 1,13 — Final Summary
# MAGIC %md
# MAGIC ## 13 Final Summary (numbers from the real run)
# MAGIC
# MAGIC | Example | Key result |
# MAGIC | --- | --- |
# MAGIC | Setup | {{setup.claim_files}} JSON files, {{setup.claim_rows}} claims ({{setup.approved}} APPROVED), {{setup.provider_rows}} providers, {{setup.lab_files}} Parquet file with {{setup.lab_rows}} labs |
# MAGIC | E1 `read_files` JSON | {{E1.rows}} rows from {{E1.files}} files; `claim_amount` {{E1.type_claim_amount}}, `service_date` {{E1.type_service_date}}; `_rescued_data` added |
# MAGIC | E2 CSV | plain reader: {{E2.plain_rows}} rows, `{{E2.plain_columns}}`; `read_files` + header: {{E2.rf_rows}} rows, `npi` {{E2.rf_type_npi}} → with `schemaHints` {{E2.hint_type_npi}} |
# MAGIC | E3 `COPY INTO` | inserted {{E3.run1_inserted}} → {{E3.run2_inserted}} (rerun) → {{E3.run3_inserted}} (new file); table {{E3.after_run3}} rows; {{E3.copy_ops}} `COPY INTO` versions |
# MAGIC | E4 Auto Loader | added {{E4.run1_added}} → {{E4.run2_added}} → {{E4.run3_added}}; all columns strings (`claim_amount` {{E4.type_claim_amount}}); `cloud_files_state` {{E4.state_files}} files |
# MAGIC | E5 new column | A failed (`{{E5.runA.errors}}`), B failed (`{{E5.runB.errors}}`), C with writer `mergeSchema` +{{E5.runC_added}}; `_schemas` files {{E5.schema_files}} |
# MAGIC | E6 rescued data | {{E6.bad_amount_null}} NULL amounts with `_rescued_data`; rescue mode: `plan_type` column = {{E6.rescue_has_plan_type}}, {{E6.rescue_rows_with_rescued}} rescued rows |
# MAGIC | E7 mistakes | no `schemaLocation`: `{{E7.no_location_error}}`; CSV on JSON: `{{E7.wrong_format_error}}` |
# MAGIC | E8 Parquet + `_metadata` | added {{E8.run1_added}} → {{E8.run2_added}}; `result_value` {{E8.type_result_value}} without `inferColumnTypes`; {{E8.rows_per_file}} |
# MAGIC | E9 `_metadata` | in `SELECT *`: {{E9.star_has_metadata}}; fields `{{E9.fields}}`; `COPY INTO` with `source_file`: {{E9.copy_inserted}} rows |
# MAGIC | E10 `binaryFile` | {{E10.claim_files}} claim files ({{E10.claim_bytes}} bytes); landing: {{E10.landing_files}} |
# MAGIC | E11 SQL streaming | `STREAM read_files` without location: `{{E11.no_location_error}}`; with location {{E11.rows}} rows; streaming table: `{{E11.streaming_table}}` |
# MAGIC | Practice | P1 {{P1.rows}} rows · P2 {{P2.rows}} rows · P3 {{P3.rows}} rows, {{P3.null_amounts}} NULL amounts, {{P3.rescued}} rescued, {{P3.files}} source files · P4 {{P4.rows}} rows, rerun {{P4.second_inserted}} rows, {{P4.copy_ops}} `COPY INTO` version |
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **File notification / file events mode** — needs a cloud storage location with file events; this workspace has only volumes.
# MAGIC * **Sources `s3://`, `abfss://`, `gs://`, `wasbs://`** — no storage credential or external location of our own (checked with the CLI: only the Databricks-managed location exists).
# MAGIC * **Streaming tables (`CREATE OR REFRESH STREAMING TABLE … STREAM read_files`)** — the statement failed in this serverless notebook with `{{E11.streaming_table}}` (E11); streaming tables are built in Lakeflow Declarative Pipelines in L07. The `STREAM read_files` query itself was verified in a notebook (E11).
# MAGIC * **`addNewColumnsWithTypeWidening` and `failOnNewColumns`** — described from the docs only.
# MAGIC * **Avro, ORC, XML and text sources** — in the format lists of the docs, not loaded in this lesson.
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. `read_files` infers numbers, but kept `service_date` as `{{E1.type_service_date}}`, not `DATE` (E1).
# MAGIC 2. The docs say a restart continues with the new schema. That is true for the **source**: the restart with unchanged code still failed with `{{E5.runB.errors}}`, because the Delta **writer** needs `mergeSchema` (E5).
# MAGIC 3. A failing stream fails the notebook cell even inside `try/except` (`Some streams terminated before this command could finish!`, test run 771734934782354). This is why E5 has two cells that are expected to fail.
# MAGIC 4. `read_files` inferred the 10-digit NPI as `{{E2.rf_type_npi}}`: it fits, but an identifier should be a string (E2).
# MAGIC 5. A wrong `cloudFiles.format` did not give a format error: CSV on JSON files failed with `{{E7.wrong_format_error}}`, because pieces of JSON text became {{E7.csv_column_count}} column names (E7).
# MAGIC 6. `_metadata.file_path` starts with `dbfs:` (`{{E9.file_path}}`), while the `_file_path` inside `_rescued_data` does not (`{{E6.rescued_example}}`) (E6, E9).
# MAGIC 7. `STREAM read_files` worked in a plain notebook when we gave `schemaLocation`, but `CREATE OR REFRESH STREAMING TABLE` was refused on this serverless notebook compute (`{{E11.streaming_table}}`, E11).
# MAGIC 8. A `COPY INTO` rerun that loads nothing writes **no** table version: {{E3.copy_ops}} `COPY INTO` versions for three runs in E3, one for two runs in P4.
