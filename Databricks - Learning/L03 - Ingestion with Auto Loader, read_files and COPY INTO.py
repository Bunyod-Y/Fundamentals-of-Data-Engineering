# Databricks notebook source
# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L03 — Ingestion with Auto Loader, read_files and COPY INTO
# MAGIC
# MAGIC **Exam objectives** (quoted from the official exam guides):
# MAGIC
# MAGIC * Associate (May 4, 2026), Section 2 *Development and Ingestion*: "Classify valid Auto Loader sources and use cases", "Demonstrate knowledge of Auto Loader syntax", "Use Databricks' built-in debugging tools to troubleshoot a given issue".
# MAGIC * Associate, Section 3: "Identify DDL (Data Definition Language)/DML features" — here: `COPY INTO`.
# MAGIC * Professional (Oct 9, 2026), Section 2: "Develop data ingestion pipelines to ingest a variety of data formats … from diverse sources such as … cloud storage."
# MAGIC * Professional, Section 1: "Create production-ready data pipelines for streaming data using Lakeflow Pipelines and Auto Loader." This lesson covers the Auto Loader part. Lakeflow Declarative Pipelines (formerly Delta Live Tables, DLT) come in a later lesson.
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** `read_files` to query JSON and CSV files in a Unity Catalog volume, with schema inference, `schemaHints` and the `_metadata` column.
# MAGIC 2. **Explain** how Auto Loader remembers which files it has already loaded (checkpoint) and where it keeps the inferred schema (`cloudFiles.schemaLocation`).
# MAGIC 3. **Compare** `read_files`, `COPY INTO` and Auto Loader: batch vs incremental, idempotency, scale, schema evolution.
# MAGIC 4. **Implement** incremental ingestion with Auto Loader (`format("cloudFiles")`) and `trigger(availableNow=True)` into a Delta table, including schema evolution and rescued data.
# MAGIC 5. **Diagnose** ingestion problems with `cloud_files_state`, the `_schemas` folder, the error class of a stopped stream, and the `_rescued_data` column.
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

# DBTITLE 1,Lesson state (do not edit)
# METRICS holds the key numbers that the Explained cells quote.
# The validation cells record here which warm-up and practice tasks passed.
METRICS = {}
WARMUP_PASSED = {}
PRACTICE_PASSED = {}

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
# MAGIC ### W1 — Schema evolution on append
# MAGIC Create the table `main.dbacademy_l03.wu1_claims (claim_id INT, amount DOUBLE)` with the rows `(1, 100.0)` and `(2, 200.0)`.
# MAGIC Then append a DataFrame with the columns `claim_id, amount, payer` and the rows `(3, 300.0, 'Medicare')` and `(4, 400.0, 'Aetna')`, so that the table **gets the new column** `payer`.

# COMMAND ----------

# DBTITLE 1,W1 Code
# Your code here

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    t = spark.table("main.dbacademy_l03.wu1_claims")
    cols, rows = t.columns, t.count()
    null_payers = t.filter("payer IS NULL").count() if "payer" in cols else None
    WARMUP_PASSED["W1"] = cols == ["claim_id", "amount", "payer"] and rows == 4 and null_payers == 2
    print("✅ Passed" if WARMUP_PASSED["W1"] else
          f"Hint: want columns [claim_id, amount, payer], 4 rows, 2 NULL payers; got {cols}, {rows} rows, {null_payers} NULL payers. "
          "An append with a new column needs option('mergeSchema', 'true').")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create main.dbacademy_l03.wu1_claims first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — CHECK constraint
# MAGIC Create `main.dbacademy_l03.wu2_claims (claim_id INT, amount DOUBLE)`. Add a CHECK constraint named `positive_amount` so that `amount` must be greater than 0.
# MAGIC Insert `(1, 50.0)`. Then try to insert `(2, -5.0)` and catch the error, so the cell does not fail. Print the error class.

# COMMAND ----------

# DBTITLE 1,W2 Code
# Your code here

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    rows = spark.table("main.dbacademy_l03.wu2_claims").count()
    props = {r["key"]: r["value"] for r in spark.sql("SHOW TBLPROPERTIES main.dbacademy_l03.wu2_claims").collect()}
    has_check = "delta.constraints.positive_amount" in props
    WARMUP_PASSED["W2"] = rows == 1 and has_check
    print("✅ Passed" if WARMUP_PASSED["W2"] else
          f"Hint: want 1 row and a constraint named positive_amount; got {rows} rows, constraint present = {has_check}.")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: create main.dbacademy_l03.wu2_claims first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — DEEP CLONE is independent
# MAGIC Create `main.dbacademy_l03.wu3_src` with the ids 1 to 5 (one column `id`). Make a **deep clone** called `main.dbacademy_l03.wu3_clone`.
# MAGIC Delete the ids 1 and 2 from the **clone** only. The source must still have 5 rows.

# COMMAND ----------

# DBTITLE 1,W3 Code
# Your code here

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    src = spark.table("main.dbacademy_l03.wu3_src").count()
    clone = spark.table("main.dbacademy_l03.wu3_clone").count()
    first_op = (spark.sql("DESCRIBE HISTORY main.dbacademy_l03.wu3_clone")
                .orderBy("version").first()["operation"])
    WARMUP_PASSED["W3"] = src == 5 and clone == 3 and first_op == "CLONE"
    print("✅ Passed" if WARMUP_PASSED["W3"] else
          f"Hint: want source 5 rows, clone 3 rows, first clone operation CLONE; got {src}, {clone}, {first_op}.")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create wu3_src and wu3_clone first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent (drop the schema, recreate it), fake healthcare data only (no PHI).
import json, random                                    # json builds the JSON lines; random makes fake values

CATALOG, SCHEMA = "main", "dbacademy_l03"
FQ = f"{CATALOG}.{SCHEMA}"                              # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"               # Unity Catalog volume for all files
CLAIMS_DIR = f"{VOL}/landing/claims_json"               # the clearinghouse drops claim files here
PROVIDERS_DIR = f"{VOL}/landing/providers_csv"          # and the provider file here
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

# Check the files and record the real counts
claim_files = [f.name for f in dbutils.fs.ls(CLAIMS_DIR)]
claim_lines = spark.read.text(CLAIMS_DIR).count()       # one JSON object per line
approved = spark.read.json(CLAIMS_DIR).filter("claim_status = 'APPROVED'").count()
METRICS["setup.claim_files"] = len(claim_files)
METRICS["setup.claim_rows"] = claim_lines
METRICS["setup.approved"] = approved
METRICS["setup.provider_rows"] = len(csv_lines) - 1
assert claim_files == ["claims_001.json", "claims_002.json", "claims_003.json"], claim_files
assert claim_lines == 300, claim_lines
print(f"claim files: {claim_files}, claim rows: {claim_lines}, APPROVED: {approved}, providers: {len(csv_lines) - 1}")
display(dbutils.fs.ls(CLAIMS_DIR))

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):** 3 JSON files with 300 claims in total (75 of them `APPROVED`), and one CSV file with 10 providers.
# MAGIC
# MAGIC The helper `land_claims(file_name, first_id, n, extra=None, bad_amount_ids=())` writes one more JSON file into the landing folder. The examples call it to simulate new deliveries:
# MAGIC * `extra={"payer": "Medicare"}` adds a **new column**;
# MAGIC * `bad_amount_ids=(…)` writes `"N/A"` into `claim_amount` for those claims (a **type mismatch**).

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
# MAGIC **Formats:** `JSON`, `CSV`, `XML`, `PARQUET`, `AVRO`, `ORC`, `TEXT`, `BINARYFILE`.
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
# MAGIC The hidden `_metadata` column gives file information, for example `_metadata.file_path` and `_metadata.file_name`.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *, _metadata.file_name
# MAGIC FROM read_files('/Volumes/main/dbacademy_l03/data/landing/claims_json', format => 'json');
# MAGIC ```
# MAGIC ```python
# MAGIC spark.read.format("json").load("/Volumes/main/dbacademy_l03/data/landing/claims_json")   # PySpark batch read
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
# MAGIC CREATE TABLE IF NOT EXISTS main.dbacademy_l03.claims_copy;     -- a table with no columns yet
# MAGIC COPY INTO main.dbacademy_l03.claims_copy
# MAGIC FROM '/Volumes/main/dbacademy_l03/data/landing/claims_json'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');                         -- let the first load create the columns
# MAGIC ```
# MAGIC Other clauses: `FILES = ('a.json', …)` or `PATTERN = '*.json'` (not both), `FORMAT_OPTIONS (…)` for reader options such as `'header' = 'true'`.
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
# MAGIC     .toTable("main.dbacademy_l03.claims_bronze"))
# MAGIC ```
# MAGIC SQL: inside a Lakeflow pipeline, `CREATE OR REFRESH STREAMING TABLE t AS SELECT * FROM STREAM read_files('…')` (*read_files* reference). That is a later lesson.

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
METRICS["E1.rows"] = e1.count()
METRICS["E1.columns"] = ", ".join(e1.columns)
METRICS["E1.type_claim_id"] = types["claim_id"]
METRICS["E1.type_claim_amount"] = types["claim_amount"]
METRICS["E1.type_service_date"] = types["service_date"]
METRICS["E1.files"] = per_file.count()
METRICS["E1.rows_per_file"] = ", ".join(str(r["row_count"]) for r in per_file.collect())
assert METRICS["E1.rows"] == 300 and METRICS["E1.files"] == 3

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `read_files` returned **300** rows from **3** files (100, 100, 100 rows per file, from `_metadata.file_name`).
# MAGIC * Columns: `claim_amount, claim_id, claim_status, patient_id, provider_id, service_date, _rescued_data`. They come in alphabetical order, not in the order of the JSON keys.
# MAGIC * The last column, `_rescued_data`, is not in the files. `read_files` adds a rescued data column by default (*read_files* reference).
# MAGIC * Inferred types: `claim_id` = `bigint`, `claim_amount` = `double`, `service_date` = `string`. `read_files` infers types (`inferColumnTypes` is `true` by default), but the date text `"2026-03-05"` stays a string. Use `schemaHints => 'service_date DATE'` or a cast if you need a date.
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
METRICS["E2.rf_rows"] = rf.count()
METRICS["E2.rf_type_provider_id"] = dict(rf.dtypes)["provider_id"]
METRICS["E2.rf_type_npi"] = dict(rf.dtypes)["npi"]

hinted = spark.sql(f"""
    SELECT * FROM read_files('{PROVIDERS_DIR}', format => 'csv', header => true,
                             schemaHints => 'npi STRING')      -- change one column, infer the rest
""")
display(hinted)
METRICS["E2.hint_type_npi"] = dict(hinted.dtypes)["npi"]
METRICS["E2.hint_type_provider_id"] = dict(hinted.dtypes)["provider_id"]
assert METRICS["E2.rf_rows"] == 10

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `spark.read.csv(path)` with no options: columns `_c0, _c1, _c2, _c3` and **11** rows. The header line became a normal data row, and the first column has type `string`. The plain CSV reader does not use the header and does not infer types unless you ask for it.
# MAGIC * `read_files(…, format => 'csv', header => true)`: **10** rows, real column names, and inferred types: `provider_id` = `int`, `npi` = `int`.
# MAGIC * `schemaHints => 'npi STRING'` changed only that column: `npi` = `string`, while `provider_id` was still inferred as `int`.
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
METRICS["E3.after_run1"] = spark.table(f"{FQ}.claims_copy").count()

r2 = spark.sql(copy_sql)                                       # run 2: same files again
display(r2)
METRICS["E3.run2_inserted"] = r2.first()["num_inserted_rows"]
METRICS["E3.after_run2"] = spark.table(f"{FQ}.claims_copy").count()

land_claims("claims_004.json", first_id=301, n=50)             # a new delivery arrives
r3 = spark.sql(copy_sql)                                       # run 3: only the new file
display(r3)
METRICS["E3.run3_inserted"] = r3.first()["num_inserted_rows"]
METRICS["E3.after_run3"] = spark.table(f"{FQ}.claims_copy").count()
METRICS["E3.type_claim_amount"] = dict(spark.table(f"{FQ}.claims_copy").dtypes)["claim_amount"]
display(spark.sql(f"DESCRIBE HISTORY {FQ}.claims_copy").select("version", "operation", "operationMetrics"))
assert METRICS["E3.after_run1"] == 300 and METRICS["E3.after_run2"] == 300 and METRICS["E3.after_run3"] == 350

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `COPY INTO` returns one row with the columns `num_affected_rows, num_inserted_rows, num_skipped_corrupt_files`.
# MAGIC * Run 1 loaded the table that had **no columns**: `num_inserted_rows` = **300**, table rows = 300. `mergeSchema` let the first load create the columns.
# MAGIC * Run 2 with the same files: `num_inserted_rows` = **0**, table rows = 300. The files were already loaded, so they were skipped. This is the idempotency from the docs.
# MAGIC * After a new 50-claim file arrived, run 3 inserted **50** rows (table rows = 350). Only the new file was loaded.
# MAGIC * `claim_amount` in the table is `double`: the JSON reader behind `COPY INTO` infers types. Compare with Auto Loader in E4.
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

METRICS["E4.run1_added"] = load_bronze()                       # first run: every file in the folder
METRICS["E4.run2_added"] = load_bronze()                       # second run: no new files
land_claims("claims_005.json", first_id=351, n=25)             # a new delivery
METRICS["E4.run3_added"] = load_bronze()                       # third run: only the new file
METRICS["E4.total"] = table_count(BRONZE)

bronze_types = dict(spark.table(BRONZE).dtypes)
METRICS["E4.columns"] = ", ".join(spark.table(BRONZE).columns)
METRICS["E4.type_claim_amount"] = bronze_types["claim_amount"]
METRICS["E4.type_claim_id"] = bronze_types["claim_id"]
display(spark.table(BRONZE).orderBy("claim_id").limit(5))

state = spark.sql(f"SELECT * FROM cloud_files_state('{BRONZE_CKPT}')")   # which files Auto Loader loaded
display(state)
METRICS["E4.state_files"] = state.count()
METRICS["E4.state_columns"] = ", ".join(state.columns)
assert METRICS["E4.run1_added"] == 350 and METRICS["E4.run2_added"] == 0 and METRICS["E4.run3_added"] == 25

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Run 1 added **350** rows: every file that was in the folder (the 3 setup files and the file from E3).
# MAGIC * Run 2 added **0** rows: no new files. The checkpoint remembers the loaded files, so nothing is read twice.
# MAGIC * After a new 25-claim file, run 3 added **25** rows. Total: 375 rows.
# MAGIC * Types without `cloudFiles.inferColumnTypes`: `claim_amount` = `string`, `claim_id` = `string`. As the docs say, Auto Loader infers **all columns as strings** for JSON, CSV and XML. `read_files` (E1) and `COPY INTO` (E3) inferred numbers from the same files.
# MAGIC * Columns: `claim_amount, claim_id, claim_status, patient_id, provider_id, service_date, _rescued_data`. Auto Loader added `_rescued_data` by itself.
# MAGIC * `cloud_files_state(checkpoint)` listed **5** files, with the columns `path, size, create_time, discovery_time, commit_time, archive_time, source_id, processed_time, ingestion_state, archive_mode, move_location`. This is the built-in way to see which files a stream has loaded.
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

# DBTITLE 1,E5 Code
INGEST_NB = "./L03 - Bronze Ingestion"                         # companion notebook in the same folder

def run_ingest_notebook(run_name, merge_schema):
    """Run the bronze ingestion notebook once, like one job run. Return (status, rows added, error classes)."""
    error_file = f"{VOL}/_errors/{run_name}.json"               # the child writes its error classes here
    params = {"source_dir": CLAIMS_DIR, "table": BRONZE, "checkpoint": BRONZE_CKPT,
              "merge_schema": str(merge_schema).lower(), "error_file": error_file}
    try:
        result = json.loads(dbutils.notebook.run(INGEST_NB, 600, params))   # 600 s timeout
        return "SUCCESS", result["rows_added"], ""
    except Exception:                                           # the child run failed
        err = json.loads(dbutils.fs.head(error_file))
        return "FAILED", 0, ", ".join(err["error_classes"])

land_claims("claims_006.json", first_id=376, n=20, extra={"payer": "Medicare"})   # new column payer

for run_name, merge in [("runA", False),                       # A: the stream sees the new column
                        ("runB", False),                       # B: restart with the same code
                        ("runC", True)]:                       # C: restart, writer allows new columns
    status, added, errors = run_ingest_notebook(run_name, merge)
    METRICS[f"E5.{run_name}_status"] = status
    METRICS[f"E5.{run_name}_added"] = added
    METRICS[f"E5.{run_name}_errors"] = errors or "none"
    print(f"{run_name}: merge_schema={merge} -> {status}, rows added {added}, errors: {errors or 'none'}")

METRICS["E5.total"] = table_count(BRONZE)
METRICS["E5.columns"] = ", ".join(spark.table(BRONZE).columns)
METRICS["E5.payer_filled"] = spark.table(BRONZE).filter("payer IS NOT NULL").count()
display(spark.table(BRONZE).filter("claim_id >= 370").orderBy("claim_id"))

schema_files = sorted(f.name for f in dbutils.fs.ls(f"{BRONZE_CKPT}/_schemas"))
METRICS["E5.schema_files"] = ", ".join(schema_files)
print("schema versions:", schema_files)
print(dbutils.fs.head(f"{BRONZE_CKPT}/_schemas/{schema_files[-1]}", 1000))   # the latest stored schema

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC | Run | Writer `mergeSchema` | Status | Rows added | Error classes |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | A | no | FAILED | 0 | `STREAM_FAILED, UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_RECORD_WITH_FILE_PATH` |
# MAGIC | B (restart, same code) | no | FAILED | 0 | `STREAM_FAILED, DELTA_METADATA_MISMATCH` |
# MAGIC | C (restart) | yes | SUCCESS | 20 | none |
# MAGIC
# MAGIC * **Run A** stopped as the docs describe: the new column `payer` raised `UNKNOWN_FIELD_EXCEPTION` (wrapped in `STREAM_FAILED`). Before it stopped, Auto Loader saved a new schema version. The `_schemas` folder now holds the files `0, 1`: version 0 from the first run and version 1 with `payer`.
# MAGIC * **Run B** shows something the docs page does not say directly: a restart with the same code fails again, now with `DELTA_METADATA_MISMATCH`. The **source** schema has the new column, but the **Delta table** does not, and the writer may not add columns.
# MAGIC * **Run C** added `.option("mergeSchema", "true")` on the **writer**. Now the table has the columns `claim_amount, claim_id, claim_status, patient_id, provider_id, service_date, _rescued_data, payer`. `payer` is filled for 20 rows (the new file only); older rows have NULL. Total rows: 395.
# MAGIC
# MAGIC **Why a separate notebook?** In a test run of this lesson, `try/except` around the failing stream was not enough: the notebook cell still failed with `Some streams terminated before this command could finish!`, and this stopped the whole job (run 771734934782354, see `_authoring/L03.md`). A failed stream fails the cell, even if Python catches the exception. So E5 runs the stream in the companion notebook **`L03 - Bronze Ingestion`** with `dbutils.notebook.run`, like one run of a scheduled job. The caller catches the failed child run and restarts it.
# MAGIC
# MAGIC > **Key takeaway:** for `addNewColumns`, plan the restart (a job with retries, or a caller that restarts) **and** set `mergeSchema` on the writer. Otherwise the restart fails too.

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
METRICS["E6.typed_run1_added"] = load_with(TYPED, TYPED_CKPT, inferColumnTypes="true")
METRICS["E6.typed_type_claim_amount"] = dict(spark.table(TYPED).dtypes)["claim_amount"]
land_claims("claims_007.json", first_id=396, n=10, bad_amount_ids=(396, 397, 398))   # 3 bad values
METRICS["E6.typed_run2_added"] = load_with(TYPED, TYPED_CKPT, inferColumnTypes="true")
bad = spark.table(TYPED).filter("claim_id IN (396, 397, 398)").orderBy("claim_id")
display(bad.select("claim_id", "claim_amount", "_rescued_data"))
METRICS["E6.bad_amount_null"] = bad.filter("claim_amount IS NULL").count()
METRICS["E6.bad_rescued"] = bad.filter("_rescued_data IS NOT NULL").count()
METRICS["E6.rescued_example"] = bad.first()["_rescued_data"]

# Part B: rescue mode, then a file with a new column
METRICS["E6.rescue_run1_added"] = load_with(RESCUE, RESCUE_CKPT, schemaEvolutionMode="rescue")
land_claims("claims_008.json", first_id=406, n=10, extra={"plan_type": "HMO"})      # new column plan_type
METRICS["E6.rescue_run2_added"] = load_with(RESCUE, RESCUE_CKPT, schemaEvolutionMode="rescue")
rescue_cols = spark.table(RESCUE).columns
METRICS["E6.rescue_has_plan_type"] = "plan_type" in rescue_cols
METRICS["E6.rescue_rows_with_rescued"] = spark.table(RESCUE).filter("_rescued_data IS NOT NULL").count()
METRICS["E6.rescue_example"] = (spark.table(RESCUE).filter("claim_id = 406").first()["_rescued_data"])
display(spark.table(RESCUE).filter("claim_id >= 406").select("claim_id", "claim_amount", "_rescued_data"))

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC **Part A — type mismatch.** With `cloudFiles.inferColumnTypes = true`, the first run added 395 rows and `claim_amount` became `double`. Then a file with 3 `"N/A"` amounts arrived:
# MAGIC * The stream did **not** fail. It added 10 rows.
# MAGIC * For the 3 bad claims, `claim_amount` is NULL in 3 rows, and `_rescued_data` is filled in 3 rows.
# MAGIC * Example `_rescued_data`: `{"claim_amount":"N/A","_file_path":"/Volumes/main/dbacademy_l03/data/landing/claims_json/claims_007.json"}`. It keeps the original value and the source file path, so you can fix the data later.
# MAGIC
# MAGIC **Part B — `schemaEvolutionMode = rescue`.** The first run added 405 rows. A file with the new column `plan_type` arrived, and the second run added 10 rows **without failing**.
# MAGIC * Does the table have a `plan_type` column? **False**.
# MAGIC * Rows with `_rescued_data` filled: 10. Example: `{"plan_type":"HMO","_file_path":"/Volumes/main/dbacademy_l03/data/landing/claims_json/claims_008.json"}`.
# MAGIC
# MAGIC > **Key takeaway:** rescued data means "never stop, never lose data". `addNewColumns` (E5) changes the table but stops the stream. `rescue` keeps the table schema stable and never stops for a new column.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "Auto Loader infers numbers from JSON by itself." | No: without `cloudFiles.inferColumnTypes` every JSON/CSV/XML column is a string (E4, Auto Loader schema docs). |
# MAGIC | 2 | "`read_files` and Auto Loader have the same type defaults." | `read_files` has `inferColumnTypes = true` by default; Auto Loader does not (E1 vs E4). |
# MAGIC | 3 | "A date text in JSON becomes `DATE` automatically." | `read_files` kept `service_date` as a string (E1). Use `schemaHints` or a cast. |
# MAGIC | 4 | "Running `COPY INTO` again duplicates the rows." | Loaded files are skipped; the second run inserted 0 rows (E3). |
# MAGIC | 5 | "`COPY INTO` needs a table with columns." | A schemaless `CREATE TABLE t` plus `COPY_OPTIONS ('mergeSchema' = 'true')` works (E3). |
# MAGIC | 6 | "`'force' = 'true'` only forces a schema update." | `force` turns idempotency off: files load again (*COPY INTO* reference). |
# MAGIC | 7 | "`addNewColumns` adds a new column without stopping." | The stream stops with `UNKNOWN_FIELD_EXCEPTION`; a restart is needed (E5). |
# MAGIC | 8 | "After the schema change, a restart with the same code works." | It failed with `DELTA_METADATA_MISMATCH` until the writer had `mergeSchema` (E5). |
# MAGIC | 9 | "`rescue` mode adds new columns to the table." | No: the table keeps its schema; the new column goes into `_rescued_data` (E6). |
# MAGIC | 10 | "A wrong type (`"N/A"` in a number column) fails the stream or drops the row." | The value becomes NULL and the original value is saved in `_rescued_data` with the file path (E6). |
# MAGIC | 11 | "The checkpoint folder alone is enough for schema inference." | Schema inference and evolution need `cloudFiles.schemaLocation` (Auto Loader schema docs). |
# MAGIC | 12 | "`spark.read.csv(path)` uses the header row." | Not without `header`: you get `_c0, _c1, …` and the header as a data row (E2). |
# MAGIC | 13 | "`try/except` around a failing stream keeps a notebook job alive." | The cell still failed with `Some streams terminated before this command could finish!` (test run in `_authoring/L03.md`). Restart through a job or a caller notebook. |
# MAGIC | 14 | "Auto Loader uses file notification mode by default." | The default is directory listing mode (*What is Auto Loader?*). |
# MAGIC | 15 | "`DESCRIBE HISTORY` of the target lists the loaded files." | Use `cloud_files_state('<checkpoint>')` (E4). |

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
# MAGIC | Source file name | `_metadata.file_name`, `_metadata.file_path` | `.select("*", "_metadata.file_name")` |
# MAGIC | Idempotent batch load | `COPY INTO t FROM '/Volumes/…' FILEFORMAT = JSON COPY_OPTIONS ('mergeSchema' = 'true')` | `spark.sql("COPY INTO …")` |
# MAGIC | Incremental load | `CREATE OR REFRESH STREAMING TABLE t AS SELECT * FROM STREAM read_files('…')` (pipelines only) | `spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", ckpt).load(dir)` |
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
# MAGIC Load the provider CSV files from `PROVIDERS_DIR` into `main.dbacademy_l03.p1_providers` with Auto Loader. Replace the three `<FILL_IN>` markers.

# COMMAND ----------

# DBTITLE 1,P1 Code
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

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    t = spark.table(f"{FQ}.p1_providers")
    rows, cols = t.count(), t.columns
    PRACTICE_PASSED["P1"] = rows == 10 and "provider_id" in cols
    print("✅ Passed" if PRACTICE_PASSED["P1"] else
          f"Hint: want 10 rows and a provider_id column; got {rows} rows and columns {cols}. "
          "Did the header option reach the reader?")
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: the table main.dbacademy_l03.p1_providers does not exist yet. Check the three blanks.", error_class(e))
METRICS["P1.rows"] = spark.table(f"{FQ}.p1_providers").count() if spark.catalog.tableExists(f"{FQ}.p1_providers") else 0

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs): a nightly COPY INTO job
# MAGIC The job loads the files `claims_001.json` to `claims_003.json` into `main.dbacademy_l03.p2_claims`. It runs twice in the cell below, like a job that is retried after an error.
# MAGIC After the two runs the table must have **exactly 300 rows** and the normal claim columns. The code has two bugs. Fix them in place.

# COMMAND ----------

# DBTITLE 1,P2 Code
spark.sql(f"DROP TABLE IF EXISTS {FQ}.p2_claims")
spark.sql(f"CREATE TABLE {FQ}.p2_claims")                     # schemaless target
p2_sql = f"""
    COPY INTO {FQ}.p2_claims
    FROM '{CLAIMS_DIR}'
    FILEFORMAT = CSV
    PATTERN = 'claims_00[1-3].json'
    COPY_OPTIONS ('mergeSchema' = 'true', 'force' = 'true')
"""
spark.sql(p2_sql)                                              # the nightly run
spark.sql(p2_sql)                                              # the retry

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    t = spark.table(f"{FQ}.p2_claims")
    rows, cols = t.count(), t.columns
    PRACTICE_PASSED["P2"] = rows == 300 and "claim_amount" in cols
    if PRACTICE_PASSED["P2"]:
        print("✅ Passed")
    elif "claim_amount" not in cols:
        print(f"Hint: the columns are {cols}. Which FILEFORMAT do the claim files have?")
    else:
        print(f"Hint: {rows} rows instead of 300. Which copy option makes COPY INTO load the same files again?")
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: run the P2 code cell first.", error_class(e))
METRICS["P2.rows"] = spark.table(f"{FQ}.p2_claims").count()

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): a typed bronze table
# MAGIC Load **all** claim files from `CLAIMS_DIR` into `main.dbacademy_l03.p3_claims` with Auto Loader, in one `availableNow` run. Requirements:
# MAGIC * `claim_amount` must be `DOUBLE` and `service_date` must be `DATE`;
# MAGIC * the 3 claims with `"N/A"` must not stop the load (their original value must be kept).
# MAGIC
# MAGIC **Hints:** checkpoint and schema location `f"{CHECKPOINTS}/p3"` · `cloudFiles.inferColumnTypes` · `cloudFiles.schemaHints` with two columns · the `"N/A"` values go to the rescued data column by themselves.

# COMMAND ----------

# DBTITLE 1,P3 Code
# Your code here

# COMMAND ----------

# DBTITLE 1,P3 Validation
try:
    t = spark.table(f"{FQ}.p3_claims")
    expected = spark.read.text(CLAIMS_DIR).count()                # one claim per line in all landed files
    rows, types = t.count(), dict(t.dtypes)
    null_amounts = t.filter("claim_amount IS NULL").count()
    PRACTICE_PASSED["P3"] = (rows == expected and types.get("claim_amount") == "double"
                             and types.get("service_date") == "date" and null_amounts == 3)
    print("✅ Passed" if PRACTICE_PASSED["P3"] else
          f"Hint: want {expected} rows, claim_amount double, service_date date, 3 NULL amounts; got {rows} rows, "
          f"claim_amount {types.get('claim_amount')}, service_date {types.get('service_date')}, {null_amounts} NULL amounts.")
    METRICS["P3.rows"], METRICS["P3.null_amounts"] = rows, null_amounts
except Exception as e:
    PRACTICE_PASSED["P3"] = False
    print("Hint: the table main.dbacademy_l03.p3_claims does not exist yet.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints)
# MAGIC 1. Load the provider CSV files from `PROVIDERS_DIR` into `main.dbacademy_l03.p4_providers` with Auto Loader, so that **a new column never stops the stream and never changes the table**. Use the folder `f"{CHECKPOINTS}/p4"`. Run it once.
# MAGIC 2. Write a second file `providers_002.csv` into `PROVIDERS_DIR` with the header `provider_id,provider_name,specialty,npi,network` and 5 rows: provider ids 11 to 15, name `Dr Provider <id>`, specialty `General`, npi `10000000<id>`, network `IN`.
# MAGIC 3. Run the stream again.
# MAGIC
# MAGIC Goal: 15 rows, no `network` column, and the 5 new rows keep `network` in the rescued data column.

# COMMAND ----------

# DBTITLE 1,P4 Code
# Your code here

# COMMAND ----------

# DBTITLE 1,P4 Validation
try:
    t = spark.table(f"{FQ}.p4_providers")
    rows, cols = t.count(), t.columns
    rescued = t.filter("_rescued_data LIKE '%network%'").count() if "_rescued_data" in cols else 0
    PRACTICE_PASSED["P4"] = rows == 15 and "network" not in cols and rescued == 5
    print("✅ Passed" if PRACTICE_PASSED["P4"] else
          f"Hint: want 15 rows, no network column, 5 rows with network in _rescued_data; "
          f"got {rows} rows, network column = {'network' in cols}, {rescued} rescued rows.")
    METRICS["P4.rows"], METRICS["P4.rescued"] = rows, rescued
except Exception as e:
    PRACTICE_PASSED["P4"] = False
    print("Hint: the table main.dbacademy_l03.p4_providers does not exist yet.", error_class(e))

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
# MAGIC **Q4.** Which statement about `cloudFiles.schemaLocation` is correct?
# MAGIC A. It stores the list of processed files. · B. It stores the inferred schema and its later versions in a `_schemas` folder. · C. It is needed only for CSV files. · D. It replaces `checkpointLocation`.
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
# MAGIC **Q8.** A provider roster CSV has a header row and 10 data rows. An engineer runs `spark.read.csv(path)` with no options. What is the result?
# MAGIC A. 10 rows with the header names and inferred types · B. 11 rows, columns `_c0, _c1, …`, all strings · C. an error, because the header is missing · D. 10 rows, columns `_c0, _c1, …`, inferred types

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — schema evolution on append**
# MAGIC ```python
# MAGIC spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu1_claims")
# MAGIC spark.sql("CREATE TABLE main.dbacademy_l03.wu1_claims (claim_id INT, amount DOUBLE)")
# MAGIC spark.sql("INSERT INTO main.dbacademy_l03.wu1_claims VALUES (1, 100.0), (2, 200.0)")
# MAGIC new_rows = spark.createDataFrame([(3, 300.0, "Medicare"), (4, 400.0, "Aetna")], "claim_id INT, amount DOUBLE, payer STRING")
# MAGIC new_rows.write.mode("append").option("mergeSchema", "true").saveAsTable("main.dbacademy_l03.wu1_claims")
# MAGIC ```
# MAGIC
# MAGIC **W2 — CHECK constraint**
# MAGIC ```python
# MAGIC spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu2_claims")
# MAGIC spark.sql("CREATE TABLE main.dbacademy_l03.wu2_claims (claim_id INT, amount DOUBLE)")
# MAGIC spark.sql("ALTER TABLE main.dbacademy_l03.wu2_claims ADD CONSTRAINT positive_amount CHECK (amount > 0)")
# MAGIC spark.sql("INSERT INTO main.dbacademy_l03.wu2_claims VALUES (1, 50.0)")
# MAGIC try:
# MAGIC     spark.sql("INSERT INTO main.dbacademy_l03.wu2_claims VALUES (2, -5.0)")
# MAGIC except Exception as e:
# MAGIC     print("rejected:", error_class(e))
# MAGIC ```
# MAGIC
# MAGIC **W3 — DEEP CLONE**
# MAGIC ```python
# MAGIC spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu3_clone")
# MAGIC spark.sql("DROP TABLE IF EXISTS main.dbacademy_l03.wu3_src")
# MAGIC spark.sql("CREATE TABLE main.dbacademy_l03.wu3_src AS SELECT CAST(id AS INT) AS id FROM range(1, 6)")
# MAGIC spark.sql("CREATE TABLE main.dbacademy_l03.wu3_clone DEEP CLONE main.dbacademy_l03.wu3_src")
# MAGIC spark.sql("DELETE FROM main.dbacademy_l03.wu3_clone WHERE id <= 2")
# MAGIC ```
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. The checkpoint is the memory of the stream: it lists the loaded files (and the schema, if `schemaLocation` points there). Without it, the stream is a new stream. It loads **every** file in the folder again, so the table gets duplicates (E4: run 2 added 0 rows only because the checkpoint existed).
# MAGIC 2. 200 files per month and SQL only: `COPY INTO`. It is idempotent SQL and fine for "files in the order of thousands". Millions of files per day: Auto Loader, recommended "for files in the order of millions or more" and with better schema inference and evolution (*Ingest data from cloud object storage*).
# MAGIC 3. `addNewColumns` adds the new column to the table, but the stream stops first and needs a restart with writer `mergeSchema` (E5): the dashboard gets the column, but data is late until the restart. `rescue` never stops and never changes the table: the dashboard always gets fresh data, but the new column is only inside `_rescued_data` until someone changes the schema (E6).
# MAGIC
# MAGIC ### P1
# MAGIC ```python
# MAGIC p1_query = (spark.readStream
# MAGIC     .format("cloudFiles")                                   # 1: the Auto Loader source
# MAGIC     .option("cloudFiles.format", "csv")                     # 2: the option that sets the file format
# MAGIC     .option("header", "true")                               # the CSV files have a header row
# MAGIC     .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/p1")
# MAGIC     .load(PROVIDERS_DIR)
# MAGIC     .writeStream
# MAGIC     .option("checkpointLocation", f"{CHECKPOINTS}/p1")
# MAGIC     .trigger(availableNow=True)                             # 3: load everything new, then stop
# MAGIC     .toTable(f"{FQ}.p1_providers"))
# MAGIC p1_query.awaitTermination()
# MAGIC ```
# MAGIC
# MAGIC ### P2 (fixes: `FILEFORMAT = JSON`; no `'force' = 'true'`)
# MAGIC ```python
# MAGIC p2_sql = f"""
# MAGIC     COPY INTO {FQ}.p2_claims
# MAGIC     FROM '{CLAIMS_DIR}'
# MAGIC     FILEFORMAT = JSON
# MAGIC     PATTERN = 'claims_00[1-3].json'
# MAGIC     COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC """
# MAGIC ```
# MAGIC
# MAGIC ### P3
# MAGIC ```python
# MAGIC p3_query = (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.inferColumnTypes", "true")                              # numbers instead of strings
# MAGIC     .option("cloudFiles.schemaHints", "claim_amount DOUBLE, service_date DATE") # fix two types
# MAGIC     .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/p3")
# MAGIC     .load(CLAIMS_DIR)
# MAGIC     .writeStream
# MAGIC     .option("checkpointLocation", f"{CHECKPOINTS}/p3")
# MAGIC     .trigger(availableNow=True)
# MAGIC     .toTable(f"{FQ}.p3_claims"))
# MAGIC p3_query.awaitTermination()
# MAGIC ```
# MAGIC
# MAGIC ### P4
# MAGIC ```python
# MAGIC def load_p4():
# MAGIC     q = (spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "csv")
# MAGIC         .option("header", "true")
# MAGIC         .option("cloudFiles.schemaEvolutionMode", "rescue")       # never stop, never change the table
# MAGIC         .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/p4")
# MAGIC         .load(PROVIDERS_DIR)
# MAGIC         .writeStream
# MAGIC         .option("checkpointLocation", f"{CHECKPOINTS}/p4")
# MAGIC         .trigger(availableNow=True)
# MAGIC         .toTable(f"{FQ}.p4_providers"))
# MAGIC     q.awaitTermination()
# MAGIC
# MAGIC load_p4()
# MAGIC p4_lines = ["provider_id,provider_name,specialty,npi,network"]
# MAGIC p4_lines += [f"{i},Dr Provider {i},General,10000000{i},IN" for i in range(11, 16)]
# MAGIC dbutils.fs.put(f"{PROVIDERS_DIR}/providers_002.csv", "\n".join(p4_lines) + "\n", overwrite=True)
# MAGIC load_p4()
# MAGIC ```
# MAGIC
# MAGIC ### Exam answers
# MAGIC
# MAGIC **Q1 → B.** Without `cloudFiles.inferColumnTypes`, Auto Loader infers all JSON columns as strings (E4: `claim_amount` = `string`). A wrong: needs `inferColumnTypes`. C wrong: no decimal inference here. D wrong: there is no "first file decides" rule.
# MAGIC
# MAGIC **Q2 → B.** `COPY INTO` skips files it already loaded (E3: second run inserted 0 rows). A wrong: that is the behaviour with `force`. C wrong: it does not fail. D wrong: skipping is the default; `force` turns it **off**.
# MAGIC
# MAGIC **Q3 → B.** The source schema was already updated; the Delta table also needs permission to add the column (E5: run B `STREAM_FAILED, DELTA_METADATA_MISMATCH`, run C with `mergeSchema` added 20 rows). A wrong: `none` ignores new columns, it does not fix the table. C wrong: deleting the checkpoint reloads every file (duplicates). D wrong: types are not the problem.
# MAGIC
# MAGIC **Q4 → B.** The schema location holds the `_schemas` folder (E5: files `0, 1`). A wrong: processed files are tracked in the checkpoint. C wrong: it is for every format without a given schema. D wrong: the writer still needs `checkpointLocation`.
# MAGIC
# MAGIC **Q5 → C.** E6: 3 NULL values and the original in `_rescued_data`, for example `{"claim_amount":"N/A","_file_path":"/Volumes/main/dbacademy_l03/data/landing/claims_json/claims_007.json"}`. A/B/D wrong: the stream continued and added all 10 rows of the file.
# MAGIC
# MAGIC **Q6 → B.** Auto Loader for "files in the order of millions or more over time" (*Ingest data from cloud object storage*). A wrong: `COPY INTO` is for thousands of files. C wrong: `read_files` reads all files on every query. D wrong: a full reload every day.
# MAGIC
# MAGIC **Q7 → B.** `cloud_files_state` lists the files of the stream (E4: 5 files). A wrong: history shows operations, not file names. C wrong: lists the folder, not what was ingested. D wrong: unrelated.
# MAGIC
# MAGIC **Q8 → B.** E2: 11 rows and columns `_c0, _c1, _c2, _c3`, type `string`. A wrong: needs `header` and type inference. C wrong: no error. D wrong: no type inference without `inferSchema`.

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
# MAGIC | Setup | 3 JSON files, 300 claims (75 APPROVED), 10 providers |
# MAGIC | E1 `read_files` JSON | 300 rows from 3 files; `claim_amount` double, `service_date` string; `_rescued_data` added |
# MAGIC | E2 CSV | plain reader: 11 rows, `_c0, _c1, _c2, _c3`; `read_files` + header: 10 rows, `npi` int → with `schemaHints` string |
# MAGIC | E3 `COPY INTO` | inserted 300 → 0 (rerun) → 50 (new file); table 350 rows |
# MAGIC | E4 Auto Loader | added 350 → 0 → 25; all columns strings (`claim_amount` string); `cloud_files_state` 5 files |
# MAGIC | E5 new column | A FAILED (`STREAM_FAILED, UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_RECORD_WITH_FILE_PATH`), B FAILED (`STREAM_FAILED, DELTA_METADATA_MISMATCH`), C SUCCESS (+20); `_schemas` files 0, 1 |
# MAGIC | E6 rescued data | 3 NULL amounts with `_rescued_data`; rescue mode: `plan_type` column = False, 10 rescued rows |
# MAGIC | Practice | P1 10 rows · P2 300 rows · P3 415 rows, 3 NULL amounts · P4 15 rows, 5 rescued |
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **File notification / file events mode** — needs a cloud storage location with file events; this workspace has only volumes.
# MAGIC * **Sources `s3://`, `abfss://`, `gs://`, `wasbs://`** — no storage credential or external location of our own (checked with the CLI: only the Databricks-managed location exists).
# MAGIC * **`STREAM read_files` streaming tables in Lakeflow Declarative Pipelines** — covered in a later lesson.
# MAGIC * **`addNewColumnsWithTypeWidening` and `failOnNewColumns`** — described from the docs only.
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. `read_files` infers numbers, but kept `service_date` as `string`, not `DATE` (E1).
# MAGIC 2. The docs say a restart continues with the new schema. That is true for the **source**: the restart with unchanged code still failed with `STREAM_FAILED, DELTA_METADATA_MISMATCH`, because the Delta **writer** needs `mergeSchema` (E5).
# MAGIC 3. A failing stream fails the notebook cell even inside `try/except` (`Some streams terminated before this command could finish!`, test run 771734934782354). This is why E5 uses a companion notebook.
# MAGIC 4. `read_files` inferred the 10-digit NPI as `int`: it fits, but an identifier should be a string (E2).
