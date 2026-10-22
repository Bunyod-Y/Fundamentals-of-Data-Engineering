# Databricks notebook source
# LESSON_TITLE: Semi-structured Data - JSON, Nested Types and VARIANT

# COMMAND ----------

# DBTITLE 1,1 — Title
# MAGIC %md
# MAGIC # L05 — Semi-structured data: JSON, nested types, VARIANT
# MAGIC
# MAGIC **Exam objectives**
# MAGIC
# MAGIC * Professional (Oct 9, 2026), Section 3 *Data Transformation, Cleansing, and Quality* (12% of the exam): parse and flatten nested JSON, work with the `VARIANT` type, and handle malformed records.
# MAGIC * Professional Section 2 *Data Ingestion & Acquisition* (12%): keep raw JSON safely in bronze (as a string or as `VARIANT`), choose a schema strategy, read single-line and multi-line JSON files.
# MAGIC
# MAGIC **5 learning objectives**
# MAGIC
# MAGIC 1. **Use** dot notation on `STRUCT`/`ARRAY` columns, colon notation on JSON strings, and `from_json`, `schema_of_json`, `to_json`.
# MAGIC 2. **Explain** the `VARIANT` type: `parse_json` vs `try_parse_json`, colon paths and `::` casts, `variant_get` vs `try_variant_get`, `schema_of_variant`, `variant_explode`, and its limits.
# MAGIC 3. **Compare** three ways to keep JSON in a table — `STRING`, `STRUCT` (fixed schema) and `VARIANT` — and the case rules of each.
# MAGIC 4. **Implement** a flattening step from a bronze FHIR-like JSON table to silver `patients` and `observations` tables, with `explode` / `explode_outer`.
# MAGIC 5. **Diagnose** lost rows (`explode` on empty arrays), silent NULLs (wrong-case paths), out-of-bounds array errors and malformed records.
# MAGIC
# MAGIC > **Evidence-only rule:** every number and error class in this notebook comes from a real run of this notebook (the build fills them in), or from an official Databricks doc page named in the text. Things that cannot be tested on this workspace are marked **NOT VERIFIED — concept only (reason)**.
# MAGIC
# MAGIC > **Data:** FHIR-*like* JSON (FHIR is the HL7 standard for healthcare data exchange). All patients, names and numbers are fake; the resources are simplified and are not valid FHIR.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up (intro)
# MAGIC %md
# MAGIC ## 2 Warm-up — recall L04 PySpark DataFrame API, Transforms and UDFs (no hints)
# MAGIC
# MAGIC Write each task from memory. Run the validation cell after each one. Solutions are in section 11.
# MAGIC
# MAGIC > The warm-up uses its own small table. Run the next cell first: it creates the lesson schema, the table `wu_vitals` and a helper.

# COMMAND ----------

# DBTITLE 1,2 — Warm-up prep (run first)
spark.sql("CREATE SCHEMA IF NOT EXISTS main.dbacademy_l05")   # the warm-up table lives here; setup recreates the schema later

import re
from pyspark.sql import Window, functions as F, types as T

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
    found = re.search(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", str(e))   # e.g. [INVALID_VARIANT_CAST]
    return found.group(1) if found else type(e).__name__

spark.sql("CREATE OR REPLACE TABLE main.dbacademy_l05.wu_vitals (patient_id INT, temp_c DOUBLE, measured_at TIMESTAMP)")
spark.sql("""INSERT INTO main.dbacademy_l05.wu_vitals VALUES
    (1, 36.8, TIMESTAMP'2025-03-01 08:00:00'), (1, 38.4, TIMESTAMP'2025-03-02 08:00:00'),
    (2, 37.1, TIMESTAMP'2025-03-01 09:00:00'), (2, NULL, TIMESTAMP'2025-03-03 09:00:00'),
    (3, 39.0, TIMESTAMP'2025-03-01 10:00:00'), (3, 37.0, TIMESTAMP'2025-03-04 10:00:00'),
    (4, NULL, TIMESTAMP'2025-03-02 07:00:00')""")
print("Warm-up table: main.dbacademy_l05.wu_vitals")

# COMMAND ----------

# DBTITLE 1,W1 Task
# MAGIC %md
# MAGIC ### W1 — Newest reading per patient, in PySpark
# MAGIC `main.dbacademy_l05.wu_vitals` has several temperature readings per patient (`patient_id`, `temp_c`, `measured_at`).
# MAGIC With the **DataFrame API** (no `spark.sql`), create the table `main.dbacademy_l05.wu1_latest` with **one row per patient**: the row with the newest `measured_at`. Keep the three columns (and only them).

# COMMAND ----------

# DBTITLE 1,W1 Code
# >>> BLANK W1
# Your code here
# >>> SOLUTION W1
w = Window.partitionBy("patient_id").orderBy(F.desc("measured_at"))
(spark.table("main.dbacademy_l05.wu_vitals")
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
    .write.mode("overwrite").saveAsTable("main.dbacademy_l05.wu1_latest"))
# <<< END W1

# COMMAND ----------

# DBTITLE 1,W1 Validation
try:
    t = spark.table("main.dbacademy_l05.wu1_latest")
    got = {r["patient_id"]: r["temp_c"] for r in t.collect()}
    ok = got == {1: 38.4, 2: None, 3: 37.0, 4: None} and t.columns == ["patient_id", "temp_c", "measured_at"]
    WARMUP_PASSED["W1"] = ok
    print("✅ Passed" if ok else
          f"Hint: want {{1: 38.4, 2: None, 3: 37.0, 4: None}} and the columns patient_id, temp_c, measured_at; got {got}, {t.columns}.")
except Exception as e:
    WARMUP_PASSED["W1"] = False
    print("Hint: create main.dbacademy_l05.wu1_latest first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W2 Task
# MAGIC %md
# MAGIC ### W2 — A Unity Catalog SQL function
# MAGIC Create the SQL function `main.dbacademy_l05.wu_fever(temp_c DOUBLE)` that returns `'UNKNOWN'` when `temp_c` is NULL, `'FEVER'` when it is 38.0 or more, and `'NORMAL'` otherwise.

# COMMAND ----------

# DBTITLE 1,W2 Code
# >>> BLANK W2
# Your code here
# >>> SOLUTION W2
spark.sql("""
    CREATE OR REPLACE FUNCTION main.dbacademy_l05.wu_fever(temp_c DOUBLE)
    RETURNS STRING
    RETURN CASE WHEN temp_c IS NULL THEN 'UNKNOWN' WHEN temp_c >= 38.0 THEN 'FEVER' ELSE 'NORMAL' END
""")
# <<< END W2

# COMMAND ----------

# DBTITLE 1,W2 Validation
try:
    r = spark.sql("""SELECT main.dbacademy_l05.wu_fever(NULL) AS a, main.dbacademy_l05.wu_fever(38.0) AS b,
                            main.dbacademy_l05.wu_fever(37.9) AS c""").first()
    got = (r["a"], r["b"], r["c"])
    ok = got == ("UNKNOWN", "FEVER", "NORMAL")
    WARMUP_PASSED["W2"] = ok
    print("✅ Passed" if ok else f"Hint: for NULL, 38.0, 37.9 want ('UNKNOWN', 'FEVER', 'NORMAL'), got {got}.")
except Exception as e:
    WARMUP_PASSED["W2"] = False
    print("Hint: create main.dbacademy_l05.wu_fever first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,W3 Task
# MAGIC %md
# MAGIC ### W3 — Reusable steps with `transform()`
# MAGIC Write two functions that take and return a DataFrame:
# MAGIC * `drop_missing_temp(df)` — removes rows where `temp_c` is NULL;
# MAGIC * `add_temp_f(df)` — adds `temp_f` = `temp_c * 9 / 5 + 32`, rounded to 1 decimal.
# MAGIC
# MAGIC Chain them with `DataFrame.transform` on `wu_vitals` and save `main.dbacademy_l05.wu3_vitals`.

# COMMAND ----------

# DBTITLE 1,W3 Code
# >>> BLANK W3
# Your code here
# >>> SOLUTION W3
def drop_missing_temp(df):
    return df.filter(F.col("temp_c").isNotNull())

def add_temp_f(df):
    return df.withColumn("temp_f", F.round(F.col("temp_c") * 9 / 5 + 32, 1))

(spark.table("main.dbacademy_l05.wu_vitals")
    .transform(drop_missing_temp)
    .transform(add_temp_f)
    .write.mode("overwrite").saveAsTable("main.dbacademy_l05.wu3_vitals"))
# <<< END W3

# COMMAND ----------

# DBTITLE 1,W3 Validation
try:
    t = spark.table("main.dbacademy_l05.wu3_vitals")
    expected = spark.sql("""SELECT patient_id, temp_c, measured_at, round(temp_c * 9 / 5 + 32, 1) AS temp_f
                            FROM main.dbacademy_l05.wu_vitals WHERE temp_c IS NOT NULL""")
    wrong = t.select("patient_id", "temp_c", "measured_at", "temp_f").exceptAll(expected).count() + \
            expected.exceptAll(t.select("patient_id", "temp_c", "measured_at", "temp_f")).count()
    missing = [n for n in ("drop_missing_temp", "add_temp_f") if not callable(globals().get(n))]
    ok = wrong == 0 and not missing
    WARMUP_PASSED["W3"] = ok
    print("✅ Passed" if ok else f"Hint: {wrong} rows differ from the expected 5 rows; missing functions: {missing}.")
except Exception as e:
    WARMUP_PASSED["W3"] = False
    print("Hint: create main.dbacademy_l05.wu3_vitals first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,3 — Classroom Setup
# Classroom setup: idempotent (drop the schema, recreate it), fake FHIR-like JSON only (no PHI).
import json, random                                             # isolated generator below, never random.seed()
from datetime import datetime, timedelta

CATALOG, SCHEMA = "main", "dbacademy_l05"
FQ = f"{CATALOG}.{SCHEMA}"                                      # fully qualified schema name
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/data"                       # the lesson volume
FHIR = f"{VOL}/fhir"                                            # landing folder for the JSON files

spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")                # removes the warm-up objects too
spark.sql(f"CREATE SCHEMA {FQ}")
spark.sql(f"CREATE VOLUME {FQ}.data")

rng = random.Random(42)                                         # one isolated, seeded generator
FAMILY = ["Testerson", "Sampleton", "Demo", "Placeholder", "Fictive", "Mockley", "Synthia", "Exampleton"]
GIVEN = ["Ann", "Ben", "Cara", "Dev", "Ema", "Finn", "Gia", "Hal", "Ines", "Joe"]
CITIES = [("Springfield", "IL"), ("Riverton", "WY"), ("Fairview", "TX"), ("Lakeside", "CA"), ("Georgetown", "NY")]

# 40 Patient resources: optional gender, nickname as a 2nd name, telecom missing / empty / 1-2 entries, JSON null in "active"
patients = []
for i in range(1, 41):
    p = {"resourceType": "Patient", "id": f"pat-{i:03d}",
         "identifier": [{"system": "urn:fake:mrn", "value": f"MRN{i:05d}"}],
         "active": None if i % 17 == 0 else True,
         "name": [{"use": "official", "family": rng.choice(FAMILY), "given": rng.sample(GIVEN, rng.randint(1, 2))}],
         "birthDate": f"{rng.randint(1940, 2010)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"}
    if i % 13 != 0:
        p["gender"] = rng.choice(["female", "male"])
    if i % 10 == 0:
        p["name"].append({"use": "nickname", "given": [rng.choice(GIVEN)]})
    if i % 8 == 4:
        p["telecom"] = []                                       # an empty array
    elif i % 8 != 0:                                            # i % 8 == 0: no telecom key at all
        p["telecom"] = [{"system": "phone", "value": f"555-01{i:02d}", "use": rng.choice(["home", "mobile"])}]
        if i % 3 == 0:
            p["telecom"].append({"system": "email", "value": f"patient{i:03d}@example.org", "use": "home"})
    city, state = rng.choice(CITIES)
    p["address"] = [{"city": city, "state": state, "postalCode": f"{rng.randint(10000, 99999)}"}]
    if i % 11 == 0:
        del p["address"][0]["postalCode"]
    if i % 9 == 0:
        p["deceasedBoolean"] = False
    patients.append(p)

# 200 Observation resources in 5 shapes: 3 labs with valueQuantity, a blood pressure panel with 2 components, a test with valueString
KINDS = [("4548-4", "Hemoglobin A1c", "%"), ("2089-1", "LDL cholesterol", "mg/dL"), ("2345-7", "Glucose", "mg/dL"),
         ("85354-9", "Blood pressure panel", "mmHg"), ("94500-6", "SARS-CoV-2 RNA", None)]
observations = []
for j in range(1, 201):
    code, text, unit = KINDS[j % 5]
    o = {"resourceType": "Observation", "id": f"obs-{j:04d}", "status": "final" if j % 10 else "preliminary",
         "code": {"coding": [{"system": "http://loinc.org", "code": code, "display": text}]},
         "subject": {"reference": f"Patient/pat-{rng.randint(1, 40):03d}"},
         "effectiveDateTime": f"2025-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}T{rng.randint(6, 20):02d}:{rng.choice([0, 15, 30, 45]):02d}:00Z"}
    if j % 6 != 0:
        o["category"] = [{"coding": [{"code": "vital-signs" if code == "85354-9" else "laboratory"}]}]
    if code == "85354-9":
        o["component"] = [
            {"code": {"coding": [{"system": "http://loinc.org", "code": "8480-6", "display": "Systolic blood pressure"}]},
             "valueQuantity": {"value": rng.randint(95, 180), "unit": "mmHg"}},
            {"code": {"coding": [{"system": "http://loinc.org", "code": "8462-4", "display": "Diastolic blood pressure"}]},
             "valueQuantity": {"value": rng.randint(55, 110), "unit": "mmHg"}}]
    elif code == "94500-6":
        o["valueString"] = rng.choice(["Negative", "Negative", "Positive"])
    else:
        value = {"4548-4": round(rng.uniform(4.5, 11.0), 1), "2089-1": rng.randint(60, 200), "2345-7": rng.randint(70, 250)}[code]
        o["valueQuantity"] = {"value": "pending" if j == 77 else value, "unit": unit, "system": "http://unitsofmeasure.org"}
    observations.append(o)

# 3 malformed lines at the end of the 2nd observation file: cut off, single quotes, not JSON at all
BAD_LINES = ['{"resourceType": "Observation", "id": "obs-0201", "status": "final", "code": {"coding": [{"code": "2345-7"',
             "{'resourceType': 'Observation', 'id': 'obs-0202', 'status': 'final'}",
             "### export interrupted at line 104 ###"]

# 60 Encounter resources for the practice: optional period.end, diagnosis missing / empty / 1-3 entries
DX = [("E11.9", "Type 2 diabetes mellitus"), ("I10", "Essential hypertension"), ("J06.9", "Acute upper respiratory infection"),
      ("E78.5", "Hyperlipidemia"), ("U07.1", "COVID-19"), ("M54.5", "Low back pain")]
encounters = []
for k in range(1, 61):
    start = datetime(2025, 1, 1, 8) + timedelta(days=rng.randint(0, 360), hours=rng.randint(0, 10))
    e = {"resourceType": "Encounter", "id": f"enc-{k:03d}", "status": "finished",
         "class": {"code": rng.choice(["AMB", "AMB", "EMER", "IMP"])},
         "subject": {"reference": f"Patient/pat-{rng.randint(1, 40):03d}"},
         "period": {"start": start.strftime("%Y-%m-%dT%H:%M:%SZ")}}
    if k % 10 != 0:
        e["period"]["end"] = (start + timedelta(hours=rng.randint(1, 72))).strftime("%Y-%m-%dT%H:%M:%SZ")
    if k % 12 == 6:
        e["diagnosis"] = []
    elif k % 12 != 0:
        e["diagnosis"] = [{"rank": r + 1, "condition": {"code": c, "display": d}}
                          for r, (c, d) in enumerate(rng.sample(DX, rng.randint(1, 3)))]
    encounters.append(e)

# one pretty-printed (multi-line) Bundle file with 3 Patient entries
bundle = {"resourceType": "Bundle", "id": "bundle-001", "type": "collection",
          "entry": [{"resource": {"resourceType": "Patient", "id": f"pat-9{n:02d}", "gender": g,
                                  "name": [{"family": "Bundleton", "given": [gv]}]}}
                    for n, (g, gv) in enumerate([("female", "Ann"), ("male", "Ben"), ("female", "Cara")], start=1)]}

def ndjson(rows):
    """One JSON document per line (newline-delimited JSON, the default for spark.read.json)."""
    return "\n".join(json.dumps(r) for r in rows) + "\n"

dbutils.fs.put(f"{FHIR}/patients/patients_01.json", ndjson(patients[:20]), overwrite=True)
dbutils.fs.put(f"{FHIR}/patients/patients_02.json", ndjson(patients[20:]), overwrite=True)
dbutils.fs.put(f"{FHIR}/observations/observations_01.json", ndjson(observations[:100]), overwrite=True)
dbutils.fs.put(f"{FHIR}/observations/observations_02.json", ndjson(observations[100:]) + "\n".join(BAD_LINES) + "\n", overwrite=True)
dbutils.fs.put(f"{FHIR}/encounters/encounters_01.json", ndjson(encounters), overwrite=True)
dbutils.fs.put(f"{FHIR}/bundle/bundle_001.json", json.dumps(bundle, indent=2) + "\n", overwrite=True)

# bronze: every line of every file as a raw STRING, plus the file name; nothing is parsed, so nothing can be lost
(spark.read.text([f"{FHIR}/patients", f"{FHIR}/observations"])
    .select(F.col("value").alias("raw"), F.col("_metadata.file_name").alias("source_file"))
    .write.saveAsTable(f"{FQ}.bronze_fhir"))
(spark.read.text(f"{FHIR}/encounters")
    .select(F.col("value").alias("raw"), F.col("_metadata.file_name").alias("source_file"))
    .write.saveAsTable(f"{FQ}.bronze_encounters"))

# expected values, computed in Python from the generated data (used by asserts and validation cells)
EXP = {"patients": len(patients), "observations": len(observations), "bad_lines": len(BAD_LINES), "encounters": len(encounters),
       "telecom_missing": sum("telecom" not in p for p in patients),
       "telecom_empty": sum(p.get("telecom") == [] for p in patients),
       "telecom_entries": sum(len(p.get("telecom", [])) for p in patients),
       "phones": sum(any(t["system"] == "phone" for t in p.get("telecom", [])) for p in patients),
       "emails": sum(any(t["system"] == "email" for t in p.get("telecom", [])) for p in patients),
       "with_gender": sum("gender" in p for p in patients),
       "bp_obs": sum("component" in o for o in observations),
       "value_quantity": sum("valueQuantity" in o for o in observations),
       "numeric_values": sum(isinstance(o.get("valueQuantity", {}).get("value"), (int, float)) for o in observations),
       "value_strings": sum("valueString" in o for o in observations),
       "diagnoses": sum(len(e.get("diagnosis", [])) for e in encounters),
       "enc_no_dx": sum(len(e.get("diagnosis", [])) == 0 for e in encounters),
       "enc_no_end": sum("end" not in e["period"] for e in encounters)}

files = [(f.path.replace(VOL, ""), f.size) for d in ("patients", "observations", "encounters", "bundle")
         for f in dbutils.fs.ls(f"{FHIR}/{d}")]
display(spark.createDataFrame(files, "file STRING, bytes LONG"))
by_file = spark.sql(f"""SELECT source_file, count(*) AS lines FROM {FQ}.bronze_fhir GROUP BY ALL
                        UNION ALL SELECT source_file, count(*) FROM {FQ}.bronze_encounters GROUP BY ALL ORDER BY source_file""")
display(by_file)
n_bronze, n_bronze_enc = spark.table(f"{FQ}.bronze_fhir").count(), spark.table(f"{FQ}.bronze_encounters").count()
bundle_lines = len(json.dumps(bundle, indent=2).splitlines())
print(EXP, "| bronze_fhir", n_bronze, "| bronze_encounters", n_bronze_enc, "| bundle lines", bundle_lines)
METRICS["setup.patients"], METRICS["setup.observations"], METRICS["setup.bad_lines"] = EXP["patients"], EXP["observations"], EXP["bad_lines"]
METRICS["setup.encounters"], METRICS["setup.bronze_rows"], METRICS["setup.bronze_enc"] = EXP["encounters"], n_bronze, n_bronze_enc
METRICS["setup.files"], METRICS["setup.bundle_lines"] = len(files), bundle_lines
METRICS["setup.telecom_missing"], METRICS["setup.telecom_empty"], METRICS["setup.telecom_entries"] = EXP["telecom_missing"], EXP["telecom_empty"], EXP["telecom_entries"]
METRICS["setup.with_gender"], METRICS["setup.bp_obs"], METRICS["setup.value_quantity"] = EXP["with_gender"], EXP["bp_obs"], EXP["value_quantity"]
METRICS["setup.value_strings"], METRICS["setup.diagnoses"], METRICS["setup.enc_no_dx"], METRICS["setup.enc_no_end"] = EXP["value_strings"], EXP["diagnoses"], EXP["enc_no_dx"], EXP["enc_no_end"]
assert (EXP["patients"], EXP["observations"], EXP["bad_lines"], EXP["encounters"]) == (40, 200, 3, 60)
assert n_bronze == 243 and n_bronze_enc == 60 and len(files) == 6
assert EXP["telecom_missing"] == 5 and EXP["telecom_empty"] == 5 and EXP["bp_obs"] == 40 and EXP["value_quantity"] == 120

# COMMAND ----------

# DBTITLE 1,3 — Setup explained
# MAGIC %md
# MAGIC **Setup result (real run):** {{setup.files}} JSON files in `/Volumes/main/dbacademy_l05/data/fhir/` and two bronze tables.
# MAGIC
# MAGIC | Object | Content |
# MAGIC | --- | --- |
# MAGIC | `fhir/patients/` (2 files) | {{setup.patients}} `Patient` resources, one per line. `gender` exists for {{setup.with_gender}} of them; `telecom` is missing for {{setup.telecom_missing}} patients and an empty array `[]` for {{setup.telecom_empty}}; the others have {{setup.telecom_entries}} telecom entries in total. Every 10th patient has a second `name` (a nickname without `family`); every 17th patient has `"active": null` (a JSON null) |
# MAGIC | `fhir/observations/` (2 files) | {{setup.observations}} `Observation` resources in 5 shapes: {{setup.value_quantity}} lab results with `valueQuantity` (one of them has the text `"pending"` instead of a number), {{setup.bp_obs}} blood pressure panels with a `component` array and no `valueQuantity`, {{setup.value_strings}} test results with `valueString` — plus {{setup.bad_lines}} malformed lines at the end of the 2nd file |
# MAGIC | `fhir/encounters/` (1 file) | {{setup.encounters}} `Encounter` resources for the practice: {{setup.diagnoses}} diagnoses in total, {{setup.enc_no_dx}} encounters without any diagnosis (missing or empty), {{setup.enc_no_end}} without `period.end` |
# MAGIC | `fhir/bundle/` (1 file) | one `Bundle` printed over {{setup.bundle_lines}} lines (multi-line JSON) with 3 patients |
# MAGIC | `bronze_fhir` | {{setup.bronze_rows}} rows: every line of the patient and observation files as a raw `STRING` (`raw`) plus `source_file` |
# MAGIC | `bronze_encounters` | {{setup.bronze_enc}} rows, the same for the encounter file |
# MAGIC
# MAGIC The bronze tables were made with `spark.read.text`: each line becomes one row and nothing is parsed, so even the malformed lines are kept. The data comes from one seeded generator (`random.Random(42)`), so every run creates the same files.
# MAGIC
# MAGIC The three malformed lines:
# MAGIC ```text
# MAGIC {"resourceType": "Observation", "id": "obs-0201", "status": "final", "code": {"coding": [{"code": "2345-7"
# MAGIC {'resourceType': 'Observation', 'id': 'obs-0202', 'status': 'final'}
# MAGIC ### export interrupted at line 104 ###
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.1 Concept — Nested types and dot notation
# MAGIC %md
# MAGIC ## 4 Concepts
# MAGIC
# MAGIC ### 4.1 Nested types: `STRUCT`, `ARRAY`, `MAP`
# MAGIC
# MAGIC JSON has objects, arrays and scalar values. Spark maps them to three complex types:
# MAGIC
# MAGIC | JSON | Spark type | Example type | Read one value (SQL) | Read one value (PySpark) |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | object with known keys | `STRUCT` | `STRUCT<family: STRING, given: ARRAY<STRING>>` | `name.family` | `F.col("name")["family"]` or `F.col("name.family")` |
# MAGIC | array | `ARRAY` | `ARRAY<STRUCT<system: STRING, value: STRING>>` | `telecom[0].value` (0-based) | `F.col("telecom")[0]["value"]` |
# MAGIC | object with free keys | `MAP` | `MAP<STRING, STRING>` | `tags['unit']` | `F.col("tags")["unit"]` |
# MAGIC
# MAGIC * **Dot notation** works on `STRUCT` columns: `subject.reference`, `code.coding[0].code`.
# MAGIC * A field of an **array of structs** gives an array: `telecom.value` → all contact values of a patient (E1).
# MAGIC * In PySpark, `df.select("telecom.value")` works, but a string with brackets like `df.select("name[0].given")` is not a path — use `F.expr("name[0].given")` or `F.col("name")[0]["given"]` (E1 shows the real error).
# MAGIC * Under ANSI mode (on by default on serverless), `arr[5]` on a shorter array is an **error**, not NULL. `get(arr, 5)` (0-based) and `try_element_at(arr, 6)` (1-based) return NULL instead (E9).
# MAGIC
# MAGIC `spark.read.json(path)` reads **one JSON document per line** by default; a pretty-printed file needs `multiLine` = `true` (*Read and write JSON files* page: "In single-line mode (the default), each line of the output contains one complete JSON object"). E1 shows both.

# COMMAND ----------

# DBTITLE 1,4.2 Concept — Three ways to keep JSON in a table
# MAGIC %md
# MAGIC ### 4.2 Three ways to keep JSON in a table: `STRING`, `STRUCT`, `VARIANT`
# MAGIC
# MAGIC | | JSON `STRING` | `STRUCT` / `ARRAY` (fixed schema) | `VARIANT` |
# MAGIC | --- | --- | --- | --- |
# MAGIC | How you create it | keep the raw text | `from_json(raw, schema)`, `spark.read.json`, a table schema | `parse_json(raw)`, `try_parse_json(raw)`, `singleVariantColumn` reader option |
# MAGIC | Read a value | `raw:a.b[0]` (result is always `STRING`) | `col.a.b[0]` (typed) | `v:a.b[0]` (result is `VARIANT`) + `::type` or `variant_get` |
# MAGIC | Path case rules | case-**in**sensitive outside brackets (*Query JSON strings*) | `from_json` field names are case-sensitive (*from_json* page) | case-**sensitive** (*How is variant different than JSON strings?*) |
# MAGIC | Schema change in the source | nothing breaks | you must change the schema; unknown fields are dropped | nothing breaks; new fields are kept |
# MAGIC | Malformed input | stays as text; paths give NULL | NULL fields (PERMISSIVE) or an error (FAILFAST) | `parse_json` fails, `try_parse_json` gives NULL |
# MAGIC | Good for | bronze "keep everything" | silver with a stable, known schema | bronze/silver with a changing or unknown schema |
# MAGIC
# MAGIC Docs: "Databricks recommends using `VARIANT` over JSON strings for semi-structured data" (*Query variant data*), and VARIANT gives "improved read and write performance" compared with JSON strings (*How is variant different than JSON strings?*). `VARIANT` needs Databricks Runtime 15.3+ for JSON, and "can only hold records up to 16 MB" (*Ingest data as semi-structured variant type*).
# MAGIC
# MAGIC **When to choose what.** Stable, well-known fields that analysts query every day → typed silver columns (`STRUCT` or flat columns). A changing or unknown shape, or very many optional fields → `VARIANT`. Raw text → only when you must keep the bytes exactly (for example, to re-parse later); otherwise `VARIANT` is the recommended raw format.

# COMMAND ----------

# DBTITLE 1,4.3 Concept — Colon notation on JSON strings
# MAGIC %md
# MAGIC ### 4.3 Colon notation on JSON strings
# MAGIC
# MAGIC From the *Query JSON strings* page (Databricks SQL / Runtime):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT raw:id,                          -- top-level field
# MAGIC        raw:code.coding[0].code,         -- nested: dots, 0-based array index
# MAGIC        raw:['resourceType'],            -- bracket form: CASE-SENSITIVE
# MAGIC        raw:`zip code`,                  -- backticks for spaces / special characters
# MAGIC        raw:telecom[*].value,            -- [*]: the field of every array element, as a JSON array string
# MAGIC        raw:valueQuantity.value::double  -- :: casts the extracted string
# MAGIC FROM main.dbacademy_l05.bronze_fhir
# MAGIC ```
# MAGIC
# MAGIC Rules from the page: "The returned results are strings"; names **outside** brackets are matched **case-insensitively**, names **inside** brackets case-sensitively; a JSON `null` becomes a SQL `NULL`; `[*]` is not supported on native `ARRAY` or `VARIANT` columns. For complex results, use `from_json`. The older function `get_json_object(raw, '$.a.b')` does the same as `raw:a.b` for one path.
# MAGIC
# MAGIC Colon notation works on a `STRING` (and on `VARIANT`, 4.5), **not** on a `STRUCT` column — E3 shows the real error.

# COMMAND ----------

# DBTITLE 1,4.4 Concept — from_json, schema_of_json, to_json
# MAGIC %md
# MAGIC ### 4.4 `from_json`, `schema_of_json`, `to_json`
# MAGIC
# MAGIC | Function | Does | Note (*from_json* / function reference pages) |
# MAGIC | --- | --- | --- |
# MAGIC | `from_json(jsonStr, schema [, options])` | JSON string → `STRUCT` (or `ARRAY`, `MAP`) | schema is a DDL string, or `schema_of_json(...)` on a **literal** sample |
# MAGIC | `schema_of_json(json)` | sample JSON → DDL schema string | the argument must be a constant: E4 shows what happens with a column |
# MAGIC | `to_json(expr)` | `STRUCT` / `ARRAY` / `MAP` / `VARIANT` → JSON string | the reverse direction |
# MAGIC
# MAGIC **Options.** `mode` is `PERMISSIVE` (default) or `FAILFAST`. In `PERMISSIVE` mode "the malformed string is placed in the `columnNameOfCorruptRecord` field" **only if the schema has that field** (default name `_corrupt_record`); otherwise the record is not kept. `FAILFAST` raises `MALFORMED_RECORD_IN_PARSING`. Field names are case-sensitive: schema `'A INT'` on `{"a": 1}` gives `{"A": null}` (page example).
# MAGIC
# MAGIC ```sql
# MAGIC SELECT from_json(raw, 'id STRING, valueQuantity STRUCT<value: DOUBLE, unit: STRING>') AS o
# MAGIC FROM main.dbacademy_l05.bronze_fhir
# MAGIC ```
# MAGIC ```python
# MAGIC schema = "id STRING, valueQuantity STRUCT<value: DOUBLE, unit: STRING>"   # or a T.StructType
# MAGIC df.select(F.from_json("raw", schema).alias("o")).select("o.id", "o.valueQuantity.value")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.5 Concept — explode and friends
# MAGIC %md
# MAGIC ### 4.5 Turning arrays into rows: `explode` and friends
# MAGIC
# MAGIC | Function | Rows for `[a, b]` | Rows for `[]` or `NULL` | Extra |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `explode(arr)` | 2 | **0 — the parent row disappears** | maps: `key`, `value` columns |
# MAGIC | `explode_outer(arr)` | 2 | 1 row with NULL | keeps every parent row |
# MAGIC | `posexplode(arr)` / `posexplode_outer` | 2, plus `pos` (0-based) | 0 / 1 | the position in the array |
# MAGIC | `inline(arr_of_structs)` / `inline_outer` | 2, one column per struct field | 0 / 1 | no `.field` needed afterwards |
# MAGIC
# MAGIC **SQL form.** The *explode_outer* page says that since Databricks Runtime 12.2 calling a generator "from LATERAL VIEW or the SELECT list is deprecated"; call it "as a table reference in the FROM clause":
# MAGIC
# MAGIC ```sql
# MAGIC SELECT p.id, t.tel.system, t.tel.value
# MAGIC FROM main.dbacademy_l05.patients_struct AS p,
# MAGIC      LATERAL explode_outer(p.telecom) AS t(tel)
# MAGIC ```
# MAGIC ```python
# MAGIC df.select("id", F.explode_outer("telecom").alias("tel")).select("id", "tel.system", "tel.value")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.6 Concept — VARIANT
# MAGIC %md
# MAGIC ### 4.6 The `VARIANT` type
# MAGIC
# MAGIC `VARIANT` stores any JSON value (object, array, string, number, boolean, null) in an efficient binary form, together with its own types. (*Query variant data*, function reference pages.)
# MAGIC
# MAGIC | Function / syntax | Does | If it cannot |
# MAGIC | --- | --- | --- |
# MAGIC | `parse_json(str)` | JSON string → `VARIANT` | error on invalid JSON |
# MAGIC | `try_parse_json(str)` | the same | `NULL` on invalid JSON (*try_parse_json* page, Runtime 15.3+) |
# MAGIC | `v:a.b[0]` | path → a `VARIANT` value | missing path → `NULL`; paths are **case-sensitive** |
# MAGIC | `v:a.b::double`, `cast(... AS DOUBLE)` | `VARIANT` → typed value | error if the value cannot be cast |
# MAGIC | `variant_get(v, '$.a.b', 'double')` | path + cast in one call | `INVALID_VARIANT_CAST` error (*variant_get* page); missing path → `NULL` |
# MAGIC | `try_variant_get(v, '$.a.b', 'double')` | the same | `NULL` instead of the cast error |
# MAGIC | `schema_of_variant(v)` / `schema_of_variant_agg(v)` | the schema of one value / of many values (an aggregate) | conflicting types become `VARIANT` |
# MAGIC | `variant_explode(v)` / `variant_explode_outer(v)` | table function: one row per array element or object key; columns `pos`, `key`, `value` | non-array/object or NULL → no row (`_outer`: one row of NULLs) |
# MAGIC | `is_variant_null(v)` | true for a JSON `null` stored in the variant | a SQL `NULL` is not a variant null |
# MAGIC
# MAGIC **Limits** (*Query variant data*): a `VARIANT` column cannot be a clustering, partition or Z-order key, and cannot be used in comparisons, grouping, ordering or set operations. Cast the value first: `GROUP BY v:gender::string`. The `[*]` path is not supported (*How is variant different than JSON strings?*). E6 tests each limit.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE main.dbacademy_l05.bronze_variant AS
# MAGIC SELECT try_parse_json(raw) AS v FROM main.dbacademy_l05.bronze_fhir;
# MAGIC
# MAGIC SELECT v:id::string, variant_get(v, '$.valueQuantity.value', 'double') AS value
# MAGIC FROM main.dbacademy_l05.bronze_variant
# MAGIC WHERE v:resourceType::string = 'Observation';
# MAGIC ```
# MAGIC ```python
# MAGIC # PySpark: the same SQL expressions through F.expr / selectExpr (works on every PySpark client version)
# MAGIC spark.table("main.dbacademy_l05.bronze_fhir").select(F.expr("try_parse_json(raw)").alias("v"))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,4.7 Concept — Bad records
# MAGIC %md
# MAGIC ### 4.7 Malformed (bad) records
# MAGIC
# MAGIC | Where | Option | Bad line becomes |
# MAGIC | --- | --- | --- |
# MAGIC | `spark.read.json` | `mode` = `PERMISSIVE` (default) | a row with all columns NULL; the text in `_corrupt_record` (name set by `columnNameOfCorruptRecord`) |
# MAGIC | `spark.read.json` | `mode` = `DROPMALFORMED` | dropped |
# MAGIC | `spark.read.json` | `mode` = `FAILFAST` | the read fails |
# MAGIC | `from_json` | `PERMISSIVE` / `FAILFAST` | NULL fields (+ the `_corrupt_record` field if it is in the schema) / an error |
# MAGIC | colon on a `STRING` | — | `NULL` for every path |
# MAGIC | `parse_json` / `try_parse_json` | — | an error / `NULL` |
# MAGIC | reader `singleVariantColumn` | `read_files(..., singleVariantColumn => 'v')`, `spark.read.option("singleVariantColumn", "v")` | the whole record in one `VARIANT` column; "Malformed records cannot be encoded using VARIANT type" (*Ingest data as semi-structured variant type*) |
# MAGIC
# MAGIC Two more options exist on file readers: `rescuedDataColumn` (L06: it rescues type mismatches and unknown fields, and "only corrupt records — that is, incomplete or malformed JSON — are dropped or throw errors") and `badRecordsPath` (NOT VERIFIED — concept only: not tested in this lesson). The safest bronze pattern is the one in this lesson's setup: **land every line as text (or `try_parse_json` into `VARIANT`) and parse in silver**, so no record is ever lost at ingestion. E8 compares all methods on the same three bad lines.

# COMMAND ----------

# DBTITLE 1,5 — Examples (intro)
# MAGIC %md
# MAGIC ## 5 Examples
# MAGIC
# MAGIC Each example: **Predict** (answer before you run) → **code** (every line commented) → **Explained** (real numbers from the run).
# MAGIC
# MAGIC E2 creates the table `patients_struct`, E5 creates `bronze_variant`, E9 creates `silver_patients` and `silver_observations`. Run the examples in order.

# COMMAND ----------

# DBTITLE 1,5 — Example helpers
import pyspark                                                 # to show the PySpark client version (E6)

def first_error(fn):
    """Run fn and return 'ok' or the error class."""
    try:
        fn()
        return "ok"
    except Exception as e:
        return error_class(e)

def diff_rows(a, b):
    """Rows in a but not in b plus rows in b but not in a (0 = same result)."""
    return a.exceptAll(b).count() + b.exceptAll(a).count()

B = f"{FQ}.bronze_fhir"                                        # raw JSON lines (STRING)
V = f"{FQ}.bronze_variant"                                     # created in E5 (VARIANT)

# COMMAND ----------

# DBTITLE 1,E1 Predict
# MAGIC %md
# MAGIC ### E1 — Nested JSON files into structs: `spark.read.json`, dot notation, multi-line files
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `birthDate` is `"1971-04-05"` in the file. Which type does schema inference give it?
# MAGIC 2. Most lab values are numbers, and one is the text `"pending"`. Which type does inference give `valueQuantity.value`?
# MAGIC 3. The Bundle file is one JSON document printed over many lines. What does `spark.read.json` give without `multiLine`?

# COMMAND ----------

# DBTITLE 1,E1 Code
pat_df = spark.read.json(f"{FHIR}/patients")                    # schema inferred from all lines of both files
obs_df = spark.read.json(f"{FHIR}/observations")                # includes the 3 malformed lines
print(pat_df.schema.simpleString())                             # the inferred nested schema
display(pat_df.select("id",
                      F.col("name")[0]["family"].alias("family"),          # array element 0, then a struct field
                      F.expr("name[0].given[0]").alias("first_given"),     # the same kind of path as SQL text
                      F.col("address")[0]["city"].alias("city"),
                      F.col("telecom.value").alias("contact_values"),      # a field of every element -> an array
                      "birthDate")
              .orderBy("id").limit(5))

pat_cols = pat_df.columns                                       # top-level columns (sorted by name by inference)
birth_type = pat_df.schema["birthDate"].dataType.simpleString() # JSON has no date type
contacts_type = pat_df.select(F.col("telecom.value")).schema[0].dataType.simpleString()
str_path = first_error(lambda: pat_df.select("name[0].given").columns)   # brackets inside a column-name string
obs_value_type = obs_df.schema["valueQuantity"].dataType["value"].dataType.simpleString()   # labs: numbers + "pending"
comp_value_type = (obs_df.schema["component"].dataType.elementType["valueQuantity"]
                   .dataType["value"].dataType.simpleString())  # blood pressure: only whole numbers
obs_has_corrupt = "_corrupt_record" in obs_df.columns           # inference adds this column for bad lines
n_pat, n_obs = pat_df.count(), obs_df.count()

single = spark.read.json(f"{FHIR}/bundle")                      # default: one JSON document per LINE
multi = spark.read.option("multiLine", "true").json(f"{FHIR}/bundle")   # one JSON document per FILE
single_cols, single_count, n_multi = single.columns, single.count(), multi.count()
resources = multi.select(F.explode("entry").alias("e")).select("e.resource.*")   # Bundle -> one row per resource
display(resources)
n_resources = resources.count()
print(f"patients {n_pat}, observations {n_obs}; birthDate {birth_type}; value {obs_value_type}; component value {comp_value_type}; "
      f"'name[0].given' -> {str_path}; bundle single-line columns {single_cols} count {single_count}; multiLine rows {n_multi}, resources {n_resources}")
METRICS["E1.pat_rows"], METRICS["E1.obs_rows"], METRICS["E1.pat_cols"] = n_pat, n_obs, ", ".join(pat_cols)
METRICS["E1.birth_type"], METRICS["E1.contacts_type"], METRICS["E1.str_path"] = birth_type, contacts_type, str_path
METRICS["E1.obs_value_type"], METRICS["E1.comp_value_type"], METRICS["E1.obs_has_corrupt"] = obs_value_type, comp_value_type, obs_has_corrupt
METRICS["E1.single_cols"], METRICS["E1.single_count"], METRICS["E1.multi_rows"], METRICS["E1.resources"] = ", ".join(single_cols), single_count, n_multi, n_resources
assert n_pat == 40 and n_obs == 203 and n_multi == 1 and n_resources == 3

# COMMAND ----------

# DBTITLE 1,E1 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `spark.read.json` read {{E1.pat_rows}} patients and {{E1.obs_rows}} observation rows (one row per line, the malformed lines included). Top-level columns: `{{E1.pat_cols}}`.
# MAGIC * `birthDate` was inferred as **`{{E1.birth_type}}`**: JSON has no date type, so you cast it yourself (`to_date`, or `DATE` in a `from_json` schema, E9).
# MAGIC * `valueQuantity.value` became **`{{E1.obs_value_type}}`** because one record has `"pending"`: inference picks a type that fits every value. The blood-pressure `component` values (whole numbers only) became `{{E1.comp_value_type}}`. One unexpected value changes the type of the whole column — a reason to use an explicit schema in production.
# MAGIC * The observations got a `_corrupt_record` column: {{E1.obs_has_corrupt}} (details in E8).
# MAGIC * `telecom.value` on an array of structs returns `{{E1.contacts_type}}` — one array per patient. `df.select("name[0].given")` failed with `{{E1.str_path}}`: a column-name string is not a SQL expression; use `F.expr("name[0].given")` or `F.col("name")[0]["given"]`.
# MAGIC * **Multi-line file.** Without `multiLine` the Bundle file gave only the column(s) `{{E1.single_cols}}` (and `count()` returned {{E1.single_count}} for a file with {{setup.bundle_lines}} lines). With `multiLine` = `true`: {{E1.multi_rows}} row, and `explode("entry")` + `e.resource.*` gave {{E1.resources}} patient rows.
# MAGIC
# MAGIC > **Key takeaway:** schema inference is fine for exploring, but it guesses types from the data you have today. Dots and `[n]` walk into structs and arrays; `multiLine` is needed when one JSON document spans many lines.

# COMMAND ----------

# DBTITLE 1,E2 Predict
# MAGIC %md
# MAGIC ### E2 — `explode` vs `explode_outer`, `posexplode`, `inline`, `size`
# MAGIC
# MAGIC **Predict:** {{setup.patients}} patients; `telecom` is missing for {{setup.telecom_missing}}, an empty array for {{setup.telecom_empty}}, and the others have 1 or 2 entries.
# MAGIC 1. How many rows does `explode("telecom")` give? How many patients disappear?
# MAGIC 2. How many rows does `explode_outer("telecom")` give?
# MAGIC 3. What does `size(telecom)` return for a missing `telecom`?

# COMMAND ----------

# DBTITLE 1,E2 Code
tel = pat_df.select("id", "telecom")                                     # one row per patient
exploded = tel.select("id", F.explode("telecom").alias("t"))             # one row per telecom entry
n_explode, ids_explode = exploded.count(), exploded.select("id").distinct().count()
n_outer = tel.select("id", F.explode_outer("telecom").alias("t")).count()   # keeps patients without entries
pos_cols = tel.select("id", F.posexplode("telecom")).columns             # default column names: pos, col
inline_cols = tel.select("id", F.inline("telecom")).columns              # one column per struct field
n_inline_outer = tel.select("id", F.inline_outer("telecom")).count()
sizes = {r["id"]: r["s"] for r in tel.select("id", F.size("telecom").alias("s"))
                                      .filter(F.col("id").isin("pat-001", "pat-003", "pat-004", "pat-008")).collect()}

pat_df.write.saveAsTable(f"{FQ}.patients_struct")                        # the parsed structs as a table, for SQL
n_sql = spark.sql(f"""SELECT count(*) AS n FROM {FQ}.patients_struct p,
                      LATERAL explode(p.telecom) AS t(tel)""").first()["n"]           # generator in FROM (current form)
n_sql_outer = spark.sql(f"""SELECT count(*) AS n FROM {FQ}.patients_struct p,
                            LATERAL explode_outer(p.telecom) AS t(tel)""").first()["n"]
display(spark.sql(f"""SELECT p.id, size(p.telecom) AS size, t.tel.system, t.tel.value
                      FROM {FQ}.patients_struct p, LATERAL explode_outer(p.telecom) AS t(tel)
                      WHERE p.id IN ('pat-001', 'pat-003', 'pat-004', 'pat-008') ORDER BY p.id"""))
print(f"explode {n_explode} rows ({ids_explode} patients), explode_outer {n_outer}, inline_outer {n_inline_outer}; "
      f"posexplode {pos_cols}; inline {inline_cols}; size {sizes}; SQL {n_sql}/{n_sql_outer}")
METRICS["E2.explode"], METRICS["E2.ids_explode"], METRICS["E2.lost"], METRICS["E2.outer"] = n_explode, ids_explode, n_pat - ids_explode, n_outer
METRICS["E2.pos_cols"], METRICS["E2.inline_cols"], METRICS["E2.inline_outer"] = ", ".join(pos_cols), ", ".join(inline_cols), n_inline_outer
METRICS["E2.size_two"], METRICS["E2.size_empty"], METRICS["E2.size_missing"] = sizes["pat-003"], sizes["pat-004"], sizes["pat-008"]
METRICS["E2.sql"], METRICS["E2.sql_outer"] = n_sql, n_sql_outer
assert n_explode == EXP["telecom_entries"] and n_outer == EXP["telecom_entries"] + EXP["telecom_missing"] + EXP["telecom_empty"]
assert n_sql == n_explode and n_sql_outer == n_outer and n_inline_outer == n_outer

# COMMAND ----------

# DBTITLE 1,E2 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `explode("telecom")` gave **{{E2.explode}}** rows from only {{E2.ids_explode}} patients: **{{E2.lost}} patients disappeared** — the ones with a missing or empty `telecom`.
# MAGIC * `explode_outer("telecom")` gave **{{E2.outer}}** rows: every patient is kept; a patient without entries gets one row with `t` = NULL. `inline_outer` gave the same {{E2.inline_outer}} rows.
# MAGIC * `posexplode` adds the position: columns `{{E2.pos_cols}}`. `inline` turns an array of structs into one column per field: `{{E2.inline_cols}}`.
# MAGIC * `size(telecom)`: 2 entries → {{E2.size_two}}, `[]` → {{E2.size_empty}}, missing → **{{E2.size_missing}}** (NULL under ANSI mode). So "no telecom" can be NULL *or* 0 — use `coalesce(size(x), 0)` when you need a count.
# MAGIC * The SQL form `FROM t, LATERAL explode(t.arr) AS x(col)` gave the same counts ({{E2.sql}} / {{E2.sql_outer}}).
# MAGIC
# MAGIC > **Key takeaway:** `explode` silently drops parent rows whose array is NULL or empty. When the parent row matters (every patient must stay in silver), use `explode_outer` / `inline_outer`.

# COMMAND ----------

# DBTITLE 1,E3 Predict
# MAGIC %md
# MAGIC ### E3 — Colon notation on the raw JSON `STRING` in bronze
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `raw:valueQuantity.value` — which data type does it return?
# MAGIC 2. `raw:RESOURCETYPE` (wrong case) and `raw:['RESOURCETYPE']`: values or NULL?
# MAGIC 3. `raw:valueQuantity.value::double` over the whole table — does it work?

# COMMAND ----------

# DBTITLE 1,E3 Code
display(spark.sql(f"""
    SELECT raw:id                      AS id,           -- top-level field
           raw:resourceType            AS type,
           raw:code.coding[0].code     AS loinc,        -- nested path with a 0-based array index
           raw:valueQuantity.value     AS value,
           raw:valueQuantity           AS value_quantity_json,   -- an object comes back as JSON text
           raw:telecom[*].value        AS contact_values         -- [*]: every element, as a JSON array string
    FROM {B}
    WHERE raw:id IN ('obs-0001', 'obs-0003', 'obs-0077', 'pat-003', 'pat-004', 'pat-008')
    ORDER BY id"""))

c3 = spark.sql(f"""
    SELECT count(*)                                       AS n_rows,
           count(raw:resourceType)                        AS exact_case,
           count(raw:RESOURCETYPE)                        AS upper_case,      -- outside brackets
           count(raw:['resourceType'])                    AS bracket_exact,
           count(raw:['RESOURCETYPE'])                    AS bracket_upper,   -- inside brackets
           count(raw:no_such.path)                        AS missing_path,
           count_if(raw:id IS NULL)                       AS no_id,           -- the malformed lines?
           count(raw:valueQuantity.value)                 AS with_value,
           count(try_cast(raw:valueQuantity.value AS DOUBLE)) AS numeric_values   -- NULL instead of an error
    FROM {B}""").first()
value_type = spark.sql(f"SELECT typeof(raw:valueQuantity.value) AS t FROM {B} WHERE raw:id = 'obs-0001'").first()["t"]
cast_error = first_error(lambda: spark.sql(f"SELECT count(raw:valueQuantity.value::double) FROM {B}").collect())   # "pending"
nulls = {r["id"]: (r["active"], r["telecom"]) for r in spark.sql(f"""
    SELECT raw:id AS id, raw:active AS active, raw:telecom AS telecom FROM {B}
    WHERE raw:id IN ('pat-004', 'pat-008', 'pat-017')""").collect()}
gjo = spark.sql(f"SELECT get_json_object(raw, '$.code.coding[0].code') AS c FROM {B} WHERE raw:id = 'obs-0001'").first()["c"]
struct_colon = first_error(lambda: spark.sql("SELECT s:a FROM (SELECT named_struct('a', 1) AS s)").collect())   # colon on a STRUCT
print(c3.asDict(), "| typeof", value_type, "| ::double", cast_error, "| nulls", nulls, "| get_json_object", gjo, "| STRUCT:", struct_colon)
METRICS["E3.rows"], METRICS["E3.exact_case"], METRICS["E3.upper_case"] = c3["n_rows"], c3["exact_case"], c3["upper_case"]
METRICS["E3.bracket_exact"], METRICS["E3.bracket_upper"], METRICS["E3.missing_path"] = c3["bracket_exact"], c3["bracket_upper"], c3["missing_path"]
METRICS["E3.no_id"], METRICS["E3.with_value"], METRICS["E3.numeric"] = c3["no_id"], c3["with_value"], c3["numeric_values"]
METRICS["E3.value_type"], METRICS["E3.cast_error"], METRICS["E3.gjo"], METRICS["E3.struct_colon"] = value_type, cast_error, gjo, struct_colon
METRICS["E3.empty_telecom"], METRICS["E3.missing_telecom"], METRICS["E3.null_active"] = nulls["pat-004"][1], nulls["pat-008"][1], nulls["pat-017"][0]
assert c3["n_rows"] == 243 and c3["upper_case"] == c3["exact_case"] and c3["bracket_upper"] == 0 and c3["missing_path"] == 0
assert c3["numeric_values"] == EXP["numeric_values"] and c3["with_value"] == EXP["value_quantity"]

# COMMAND ----------

# DBTITLE 1,E3 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * Every colon result is a string: `typeof(raw:valueQuantity.value)` = **`{{E3.value_type}}`**. An object (`raw:valueQuantity`) comes back as JSON text, and `raw:telecom[*].value` as a JSON array string.
# MAGIC * **Case rules** on {{E3.rows}} bronze rows: `raw:resourceType` → {{E3.exact_case}} values, `raw:RESOURCETYPE` → **{{E3.upper_case}}** (outside brackets: case-insensitive); `raw:['resourceType']` → {{E3.bracket_exact}}, `raw:['RESOURCETYPE']` → **{{E3.bracket_upper}}** (inside brackets: case-sensitive). This matches the *Query JSON strings* page.
# MAGIC * A path that does not exist gives NULL, never an error: {{E3.missing_path}} values for `raw:no_such.path`. Rows where even `raw:id` is NULL: {{E3.no_id}} — the lines that are not valid JSON (E8 explains why the single-quoted line still worked).
# MAGIC * NULL vs empty: `raw:telecom` is `{{E3.empty_telecom}}` (text) for an empty array and {{E3.missing_telecom}} (SQL NULL) for a missing key; a JSON `null` (`"active": null`) also gives {{E3.null_active}}.
# MAGIC * `raw:valueQuantity.value::double` over the whole table failed with **`{{E3.cast_error}}`** because of the one `"pending"` value (ANSI mode). `try_cast(... AS DOUBLE)` returned {{E3.numeric}} numbers of {{E3.with_value}} values, NULL for the rest.
# MAGIC * `get_json_object(raw, '$.code.coding[0].code')` = `{{E3.gjo}}`, the same as `raw:code.coding[0].code`. Colon notation on a `STRUCT` column failed with `{{E3.struct_colon}}`: it is for JSON strings (and `VARIANT`), not for structs.
# MAGIC
# MAGIC > **Key takeaway:** colon notation is quick for exploring raw JSON text: forgiving with case, NULL for missing paths, but every value is a string, so cast carefully (`try_cast` when one bad value must not stop the query).

# COMMAND ----------

# DBTITLE 1,E4 Predict
# MAGIC %md
# MAGIC ### E4 — `schema_of_json`, `from_json` with a schema, `to_json`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. Can `from_json(raw, schema_of_json(raw))` infer a schema per row?
# MAGIC 2. `from_json` on the line `### export interrupted ###` — is the result NULL?
# MAGIC 3. The schema says `value DOUBLE`, the record has `"pending"`. Is the whole record lost, or only the value?

# COMMAND ----------

# DBTITLE 1,E4 Code
sample = json.dumps(observations[0])                                     # one lab Observation as a JSON string
sample_schema = spark.range(1).select(F.schema_of_json(F.lit(sample)).alias("s")).first()["s"]   # DDL from a sample
print(sample_schema)
per_row = first_error(lambda: spark.sql(f"SELECT from_json(raw, schema_of_json(raw)) FROM {B}").collect())   # not a literal

OBS_MINI = "id STRING, resourceType STRING, status STRING, valueQuantity STRUCT<value: DOUBLE, unit: STRING>"   # DDL string
parsed = spark.table(B).select("raw", F.from_json("raw", OBS_MINI).alias("o"))                  # PERMISSIVE (default)
display(parsed.filter(F.col("raw").startswith("###") | F.col("o.id").isin("obs-0001", "obs-0077", "obs-0202"))
              .select("o.*", F.substring("raw", 1, 40).alias("raw_start")))              # the first 40 characters
p = parsed.selectExpr("count(*) AS n", "count_if(o IS NULL) AS null_structs", "count_if(o.id IS NULL) AS null_ids",
                      "count(o.valueQuantity.value) AS values").first()
pending = parsed.filter("o.id = 'obs-0077'").select("o.valueQuantity.value", "o.valueQuantity.unit").first()

with_corrupt = spark.table(B).select("raw", F.from_json("raw", OBS_MINI + ", _corrupt_record STRING").alias("o"))   # keep the bad text
corrupt_starts = sorted(r["s"] for r in with_corrupt.filter("o._corrupt_record IS NOT NULL")
                        .select(F.substring("o._corrupt_record", 1, 50).alias("s")).collect())   # which rows?
by_sample = spark.table(B).select(F.from_json("raw", sample_schema).alias("o"))                  # schema from ONE sample
n_from_sample = by_sample.filter("o.valueQuantity.value IS NOT NULL").count()
n_sample_a1c = by_sample.filter("o.code.coding[0].code = '4548-4' AND o.valueQuantity.value IS NOT NULL").count()   # decimals
n_wrong_case = spark.table(B).select(F.from_json("raw", "ID STRING, ResourceType STRING").alias("o")).filter("o.ID IS NOT NULL").count()
failfast_bad = first_error(lambda: spark.table(B).filter(F.col("raw").startswith("###"))
                           .select(F.from_json("raw", OBS_MINI, {"mode": "FAILFAST"})).collect())   # invalid JSON
failfast_type = first_error(lambda: spark.table(B).filter(F.expr("raw:id = 'obs-0077'"))
                            .select(F.from_json("raw", OBS_MINI, {"mode": "FAILFAST"})).collect())  # valid JSON, wrong type
back = parsed.filter("o.id = 'obs-0001'").select(F.to_json("o.valueQuantity").alias("j")).first()["j"]   # STRUCT -> JSON text
print(p.asDict(), "| pending", tuple(pending), "| corrupt", corrupt_starts, "| sample schema values", n_from_sample, n_sample_a1c,
      "| wrong case", n_wrong_case, "| FAILFAST", failfast_bad, failfast_type, "| to_json", back, "| per-row schema", per_row)
METRICS["E4.sample_schema"], METRICS["E4.per_row"] = sample_schema, per_row
METRICS["E4.rows"], METRICS["E4.null_structs"], METRICS["E4.null_ids"], METRICS["E4.values"] = p["n"], p["null_structs"], p["null_ids"], p["values"]
METRICS["E4.pending_value"], METRICS["E4.pending_unit"] = pending[0], pending[1]
METRICS["E4.corrupt"], METRICS["E4.corrupt_starts"], METRICS["E4.wrong_case"] = len(corrupt_starts), " | ".join(corrupt_starts), n_wrong_case
METRICS["E4.from_sample"], METRICS["E4.sample_a1c"] = n_from_sample, n_sample_a1c
METRICS["E4.failfast_bad"], METRICS["E4.failfast_type"], METRICS["E4.to_json"] = failfast_bad, failfast_type, back
assert p["n"] == 243 and p["values"] == EXP["numeric_values"] and n_wrong_case == 0 and pending[0] is None

# COMMAND ----------

# DBTITLE 1,E4 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `schema_of_json` on one sample observation returned the DDL string `{{E4.sample_schema}}`. You can copy it into code, or pass `schema_of_json('<literal>')` to `from_json`. A per-row schema, `from_json(raw, schema_of_json(raw))`, failed with **`{{E4.per_row}}`**: the schema must be a constant.
# MAGIC * `from_json(raw, OBS_MINI)` (PERMISSIVE) on {{E4.rows}} rows: structs that are NULL: **{{E4.null_structs}}**; structs with `id` NULL: **{{E4.null_ids}}**. A malformed line does **not** give a NULL struct — it gives a struct whose fields are all NULL. Test a key field (`o.id IS NULL`), not `o IS NULL`.
# MAGIC * Type mismatch: for `obs-0077` (`"pending"`) the value became {{E4.pending_value}} while `unit` = `{{E4.pending_unit}}` was kept — only the bad field is lost. {{E4.values}} values parsed as `DOUBLE`.
# MAGIC * With `_corrupt_record STRING` in the schema, {{E4.corrupt}} rows kept their original text in that field. They start with: `{{E4.corrupt_starts}}` — the invalid lines **and** the `"pending"` record (`obs-0077`): a type mismatch also fills `_corrupt_record`, even though the other fields of that record were parsed.
# MAGIC * **One sample is not enough.** The sample record (`obs-0001`, an LDL result) had a whole-number value, so `schema_of_json` said `value: BIGINT` (see the schema above). With that schema only {{E4.from_sample}} values survived, and {{E4.sample_a1c}} of the A1C results (decimal values; E5 and E7 show how they are stored). `schema_of_variant_agg` (E7) looks at all rows instead.
# MAGIC * A schema with the wrong case (`ID`, `ResourceType`) parsed {{E4.wrong_case}} values: `from_json` field names are case-sensitive.
# MAGIC * `FAILFAST`: on the invalid line → `{{E4.failfast_bad}}`; on the valid line with `"pending"` → `{{E4.failfast_type}}`. FAILFAST stops on type mismatches too, not only on broken JSON.
# MAGIC * `to_json(o.valueQuantity)` → `{{E4.to_json}}`: struct back to JSON text.
# MAGIC
# MAGIC > **Key takeaway:** `from_json` + an explicit schema gives typed columns and keeps unknown fields out. PERMISSIVE never fails, so check a key field for NULL (or add `_corrupt_record`) to find bad records.

# COMMAND ----------

# DBTITLE 1,E5 Predict
# MAGIC %md
# MAGIC ### E5 — `VARIANT`: `parse_json`, `try_parse_json`, colon paths, `variant_get`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `parse_json(raw)` over the whole bronze table, which has malformed lines — result?
# MAGIC 2. `v:RESOURCETYPE` on a `VARIANT` — values or NULL? (Compare E3.)
# MAGIC 3. `"active": null` in JSON: is `v:active IS NULL` true?

# COMMAND ----------

# DBTITLE 1,E5 Code
parse_error = first_error(lambda: spark.sql(f"SELECT count(parse_json(raw)) FROM {B}").collect())   # strict parser
spark.sql(f"""CREATE OR REPLACE TABLE {V} AS
              SELECT try_parse_json(raw) AS v, raw, source_file      -- NULL instead of an error for bad lines
              FROM {B}""")
v_type = spark.sql(f"DESCRIBE TABLE {V}").filter("col_name = 'v'").first()["data_type"]
bad = [r["s"] for r in spark.sql(f"SELECT left(raw, 30) AS s FROM {V} WHERE v IS NULL ORDER BY s").collect()]

display(spark.sql(f"""
    SELECT v:id::string                                        AS id,      -- path + cast to STRING
           v:code.coding[0].code::string                       AS loinc,
           typeof(v:valueQuantity.value)                       AS path_type,   -- what a path returns
           schema_of_variant(v:valueQuantity.value)            AS value_schema,   -- the type stored inside
           try_variant_get(v, '$.valueQuantity.value', 'double') AS value   -- NULL if it cannot be cast
    FROM {V}
    WHERE v:id::string IN ('obs-0001', 'obs-0005', 'obs-0077', 'pat-001')
    ORDER BY id"""))
c5 = spark.sql(f"""
    SELECT count(v:resourceType)                                       AS exact_case,
           count(v:RESOURCETYPE)                                       AS upper_case,
           count(variant_get(v, '$.no_such', 'string'))                AS missing_path,
           count(try_variant_get(v, '$.valueQuantity.value', 'double')) AS try_values,
           count(try_cast(v:valueQuantity.value AS DOUBLE))            AS try_cast_values
    FROM {V}""").first()
path_type = spark.sql(f"SELECT typeof(v:valueQuantity.value) AS t FROM {V} WHERE v:id::string = 'obs-0001'").first()["t"]
get_error = first_error(lambda: spark.sql(f"SELECT count(variant_get(v, '$.valueQuantity.value', 'double')) FROM {V}").collect())
cast_error = first_error(lambda: spark.sql(f"SELECT count(v:valueQuantity.value::double) FROM {V}").collect())
n_null = {r["id"]: (r["sql_null"], r["variant_null"]) for r in spark.sql(f"""
    SELECT v:id::string AS id,
           CASE WHEN v:id::string = 'pat-017' THEN v:active IS NULL ELSE v:telecom IS NULL END AS sql_null,
           CASE WHEN v:id::string = 'pat-017' THEN is_variant_null(v:active) ELSE is_variant_null(v:telecom) END AS variant_null
    FROM {V} WHERE v:id::string IN ('pat-017', 'pat-008')""").collect()}
print("parse_json:", parse_error, "| type", v_type, "| NULL v:", bad, "|", c5.asDict(), "| path type", path_type,
      "| variant_get", get_error, "| ::double", cast_error, "| nulls", n_null)
METRICS["E5.parse_error"], METRICS["E5.v_type"], METRICS["E5.null_v"], METRICS["E5.null_v_lines"] = parse_error, v_type, len(bad), " | ".join(bad)
METRICS["E5.exact_case"], METRICS["E5.upper_case"], METRICS["E5.missing_path"] = c5["exact_case"], c5["upper_case"], c5["missing_path"]
METRICS["E5.try_values"], METRICS["E5.try_cast_values"], METRICS["E5.path_type"] = c5["try_values"], c5["try_cast_values"], path_type
METRICS["E5.get_error"], METRICS["E5.cast_error"] = get_error, cast_error
METRICS["E5.active_sql_null"], METRICS["E5.active_variant_null"] = n_null["pat-017"][0], n_null["pat-017"][1]
METRICS["E5.telecom_sql_null"], METRICS["E5.telecom_variant_null"] = n_null["pat-008"][0], n_null["pat-008"][1]
assert parse_error != "ok" and c5["upper_case"] == 0 and c5["try_values"] == EXP["numeric_values"] and get_error != "ok"

# COMMAND ----------

# DBTITLE 1,E5 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `parse_json(raw)` over the bronze table failed with **`{{E5.parse_error}}`** — one bad line stops the whole query. `try_parse_json` worked and created `bronze_variant` with `v` of type `{{E5.v_type}}`; {{E5.null_v}} rows got `v` = NULL: `{{E5.null_v_lines}}`. Note: the single-quoted line is one of them, although colon notation (E3) and `from_json` (E4) accepted it (E8).
# MAGIC * A path on a variant returns a variant: `typeof(v:valueQuantity.value)` = `{{E5.path_type}}`. Cast it (`::string`, `::double`) or use `variant_get`. The table above shows the type stored inside each value (`schema_of_variant`).
# MAGIC * **Case:** `v:resourceType` → {{E5.exact_case}} values, `v:RESOURCETYPE` → **{{E5.upper_case}}**. Variant paths are case-sensitive — the opposite of colon paths on a JSON string (E3).
# MAGIC * Missing path: `variant_get(v, '$.no_such', 'string')` → {{E5.missing_path}} values, no error.
# MAGIC * The `"pending"` value: `variant_get(..., 'double')` failed with **`{{E5.get_error}}`**, and `v:valueQuantity.value::double` with `{{E5.cast_error}}`. `try_variant_get` returned {{E5.try_values}} numbers and NULL for the rest; `try_cast(v:... AS DOUBLE)` returned {{E5.try_cast_values}}.
# MAGIC * **Two kinds of NULL.** `"active": null` (pat-017): `v:active IS NULL` = {{E5.active_sql_null}}, `is_variant_null(v:active)` = {{E5.active_variant_null}} — the value exists and is a JSON null. Missing `telecom` (pat-008): `IS NULL` = {{E5.telecom_sql_null}}, `is_variant_null` = {{E5.telecom_variant_null}} — there is no value at all.
# MAGIC
# MAGIC > **Key takeaway:** land raw JSON with `try_parse_json` (or `parse_json` when bad input must stop the load), read values with exact-case paths plus `::type`, and use `try_variant_get` when one bad value must not fail the query.

# COMMAND ----------

# DBTITLE 1,E6 Predict
# MAGIC %md
# MAGIC ### E6 — `VARIANT` limits: grouping, comparing, sorting, set operations, clustering
# MAGIC
# MAGIC **Predict:** which of these work on a `VARIANT` value without a cast?
# MAGIC 1. `GROUP BY v:gender` · 2. `WHERE v:resourceType = 'Patient'` · 3. `ORDER BY v:id` · 4. `SELECT DISTINCT v:status` · 5. `CLUSTER BY (v)` · 6. `count(DISTINCT v:status)`

# COMMAND ----------

# DBTITLE 1,E6 Code
checks = {
    "GROUP BY v:gender": f"SELECT v:gender::string AS g, count(*) FROM {V} GROUP BY v:gender",
    "WHERE v:resourceType = 'Patient'": f"SELECT count(*) FROM {V} WHERE v:resourceType = 'Patient'",
    "ORDER BY v:id": f"SELECT v:id::string FROM {V} ORDER BY v:id LIMIT 1",
    "SELECT DISTINCT v:status": f"SELECT DISTINCT v:status FROM {V}",
    "UNION of v:status": f"SELECT v:status FROM {V} UNION SELECT v:status FROM {V}",
    "count(DISTINCT v:status)": f"SELECT count(DISTINCT v:status) FROM {V}",
    "path with [*]": f"SELECT v:telecom[*].value::string FROM {V}",
    "CLUSTER BY (v)": f"CREATE OR REPLACE TABLE {FQ}.variant_clustered CLUSTER BY (v) AS SELECT v FROM {V}",
}
results = {name: first_error(lambda s=sql: spark.sql(s).collect()) for name, sql in checks.items()}   # run each, keep ok / error
display(spark.createDataFrame(list(results.items()), "statement STRING, result STRING"))
n_status = spark.sql(f"SELECT count(DISTINCT v:status) AS n FROM {V}").first()["n"] if results["count(DISTINCT v:status)"] == "ok" else -1
display(spark.sql(f"""SELECT v:gender::string AS gender, count(*) AS patients    -- the fix: cast, then group
                      FROM {V} WHERE v:resourceType::string = 'Patient'
                      GROUP BY ALL ORDER BY gender"""))
g = {r["gender"]: r["patients"] for r in spark.sql(f"""SELECT v:gender::string AS gender, count(*) AS patients FROM {V}
                                                     WHERE v:resourceType::string = 'Patient' GROUP BY ALL""").collect()}

# the PySpark client in this notebook and VARIANT values
client = pyspark.__version__
has_parse_json = hasattr(F, "parse_json")                                # F.parse_json exists only in newer clients
collect_variant = first_error(lambda: spark.table(V).select("v").limit(1).collect())   # bring a VARIANT value to Python
via_expr = spark.table(B).select(F.expr("try_parse_json(raw)").alias("v")).select(F.expr("v:id::string").alias("id"))
n_via_expr = via_expr.filter("id IS NOT NULL").count()                   # SQL expressions work from any client
print(results, "| distinct statuses", n_status, "| genders", g, "| client", client, "| F.parse_json", has_parse_json,
      "| collect VARIANT", collect_variant, "| via F.expr", n_via_expr)
METRICS["E6.group"], METRICS["E6.compare"], METRICS["E6.order"] = results["GROUP BY v:gender"], results["WHERE v:resourceType = 'Patient'"], results["ORDER BY v:id"]
METRICS["E6.distinct"], METRICS["E6.union"], METRICS["E6.count_distinct"] = results["SELECT DISTINCT v:status"], results["UNION of v:status"], results["count(DISTINCT v:status)"]
METRICS["E6.star"], METRICS["E6.cluster"], METRICS["E6.n_status"] = results["path with [*]"], results["CLUSTER BY (v)"], n_status
METRICS["E6.no_gender"], METRICS["E6.client"], METRICS["E6.has_parse_json"] = g.get(None, 0), client, has_parse_json
METRICS["E6.collect_variant"], METRICS["E6.via_expr"] = collect_variant, n_via_expr
assert results["GROUP BY v:gender"] != "ok" and results["CLUSTER BY (v)"] != "ok" and sum(g.values()) == 40

# COMMAND ----------

# DBTITLE 1,E6 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC | Statement | Result |
# MAGIC | --- | --- |
# MAGIC | `GROUP BY v:gender` | `{{E6.group}}` |
# MAGIC | `WHERE v:resourceType = 'Patient'` | `{{E6.compare}}` |
# MAGIC | `ORDER BY v:id` | `{{E6.order}}` |
# MAGIC | `SELECT DISTINCT v:status` | `{{E6.distinct}}` |
# MAGIC | `UNION` of `v:status` | `{{E6.union}}` |
# MAGIC | `count(DISTINCT v:status)` | `{{E6.count_distinct}}` ({{E6.n_status}} distinct values) |
# MAGIC | path with `[*]` | `{{E6.star}}` |
# MAGIC | `CLUSTER BY (v)` | `{{E6.cluster}}` |
# MAGIC
# MAGIC * This matches the *Query variant data* limits (no comparisons, grouping, ordering, set operations, clustering keys) and the `[*]` note on *How is variant different than JSON strings?*. The fix is always the same: **cast first** — `GROUP BY v:gender::string` worked (the table above; {{E6.no_gender}} patients have no gender).
# MAGIC * Surprise: `count(DISTINCT v:status)` → {{E6.count_distinct}}, although `SELECT DISTINCT` → `{{E6.distinct}}`. Do not rely on it; cast anyway.
# MAGIC * **PySpark client.** This notebook runs PySpark {{E6.client}}. `F.parse_json` exists in this client: {{E6.has_parse_json}}. Collecting a `VARIANT` value into Python: `{{E6.collect_variant}}` (it arrives as a `VariantVal` object). SQL expressions through `F.expr(...)` work on any client: {{E6.via_expr}} ids were read with `F.expr("v:id::string")`. This lesson casts variant values before `display`, so the output is readable text.
# MAGIC
# MAGIC > **Key takeaway:** a `VARIANT` value is for storing and extracting. Extract and cast before you filter, join, group, sort or cluster.

# COMMAND ----------

# DBTITLE 1,E7 Predict
# MAGIC %md
# MAGIC ### E7 — `schema_of_variant`, `schema_of_variant_agg`, `variant_explode`
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. A1C values like `6.1` are stored in a variant. Which type does `schema_of_variant` show: `DOUBLE`, `DECIMAL` or `STRING`?
# MAGIC 2. `schema_of_variant_agg` over all observations: which type will `valueQuantity.value` get, if some values are whole numbers, some decimals and one is `"pending"`?
# MAGIC 3. `variant_explode(v:component)` — how many rows for {{setup.bp_obs}} blood pressure panels with 2 components each? And for observations without `component`?

# COMMAND ----------

# DBTITLE 1,E7 Code
display(spark.sql(f"""SELECT v:id::string AS id, schema_of_variant(v) AS schema       -- the schema of ONE value
                      FROM {V} WHERE v:id::string IN ('obs-0001', 'obs-0003', 'obs-0077') ORDER BY id"""))
agg = {r["type"]: r["schema"] for r in spark.sql(f"""
    SELECT v:resourceType::string AS type, schema_of_variant_agg(v) AS schema   -- merged schema of MANY values
    FROM {V} WHERE v IS NOT NULL GROUP BY ALL""").collect()}
display(spark.createDataFrame(sorted(agg.items()), "resource_type STRING, merged_schema STRING"))
a1c_type = spark.sql(f"SELECT schema_of_variant(v:valueQuantity.value) AS t FROM {V} WHERE v:id::string = 'obs-0005'").first()["t"]
value_in_agg = re.search(r"valueQuantity: OBJECT<system: STRING, unit: STRING, value: (\w+)>", agg["Observation"]).group(1)   # top-level value

display(spark.sql(f"""
    SELECT v:id::string                                AS obs_id,
           c.pos,                                                       -- position in the array
           c.value:code.coding[0].code::string         AS component_code,  -- c.value is a VARIANT
           c.value:valueQuantity.value::int            AS mmhg
    FROM {V}, LATERAL variant_explode(v:component) AS c                 -- table function in FROM
    ORDER BY obs_id, c.pos LIMIT 6"""))
cnt = spark.sql(f"""
    SELECT (SELECT count(*) FROM {V}, LATERAL variant_explode(v:component) AS c)                      AS comp_rows,
           (SELECT count(*) FROM {V}, LATERAL variant_explode_outer(v:component) AS c
             WHERE v:resourceType::string = 'Observation')                                             AS comp_outer_rows,
           (SELECT count(*) FROM {V}, LATERAL variant_explode(v) AS kv WHERE v:id::string = 'pat-001') AS pat001_keys,
           (SELECT count(*) FROM {V}, LATERAL variant_explode(v:id) AS s)                              AS scalar_rows
""").first()
keys = [r["key"] for r in spark.sql(f"""SELECT kv.key FROM {V}, LATERAL variant_explode(v) AS kv
                                        WHERE v:id::string = 'pat-001' ORDER BY kv.pos""").collect()]
n_obs_v = spark.sql(f"SELECT count(*) AS n FROM {V} WHERE v:resourceType::string = 'Observation'").first()["n"]
print(agg, "| A1C stored as", a1c_type, "| merged value type", value_in_agg, "|", cnt.asDict(), keys, "| observations", n_obs_v)
METRICS["E7.patient_schema"], METRICS["E7.a1c_type"], METRICS["E7.value_in_agg"] = agg["Patient"], a1c_type, value_in_agg
METRICS["E7.comp_rows"], METRICS["E7.comp_outer_rows"], METRICS["E7.obs_v"] = cnt["comp_rows"], cnt["comp_outer_rows"], n_obs_v
METRICS["E7.pat001_keys"], METRICS["E7.keys"], METRICS["E7.scalar_rows"] = cnt["pat001_keys"], ", ".join(keys), cnt["scalar_rows"]
assert cnt["comp_rows"] == 2 * EXP["bp_obs"] and cnt["scalar_rows"] == 0

# COMMAND ----------

# DBTITLE 1,E7 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `schema_of_variant(v)` describes one value; each observation shape has its own schema (table above).
# MAGIC * An A1C value is stored as **`{{E7.a1c_type}}`**, not `DOUBLE`: `parse_json` keeps exact decimal numbers.
# MAGIC * `schema_of_variant_agg(v)` merges the schemas of all rows per resource type. For observations, `valueQuantity.value` became **`{{E7.value_in_agg}}`** — whole numbers, decimals and the `"pending"` text do not fit one type ("conflicting types" → `VARIANT`, *schema_of_variant* page). The merged Patient schema: `{{E7.patient_schema}}`. Use the merged schema to design typed silver columns.
# MAGIC * `variant_explode(v:component)` gave **{{E7.comp_rows}}** rows (2 per blood pressure panel); observations without `component` produced no row. `variant_explode_outer` kept them: {{E7.comp_outer_rows}} rows for {{E7.obs_v}} observations.
# MAGIC * On an object, `variant_explode` returns one row per key: pat-001 has {{E7.pat001_keys}} keys (`{{E7.keys}}`). On a scalar (`v:id`) it returned {{E7.scalar_rows}} rows.
# MAGIC * The result columns are `pos`, `key` (NULL for arrays) and `value` (a `VARIANT`, so `c.value:path::type` reads inside it).
# MAGIC
# MAGIC > **Key takeaway:** `schema_of_variant_agg` tells you what is really in the data; `variant_explode` is the `VARIANT` version of `explode`, and `variant_explode_outer` the version that keeps parent rows.

# COMMAND ----------

# DBTITLE 1,E8 Predict
# MAGIC %md
# MAGIC ### E8 — Malformed records: one input, nine methods
# MAGIC
# MAGIC The observation files contain {{setup.bad_lines}} bad lines: a cut-off object, an object with **single quotes**, and plain text.
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. How many rows does `spark.read.json` keep with `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`?
# MAGIC 2. Is a JSON object with single quotes a bad record for `spark.read.json`? For `parse_json`?
# MAGIC 3. Can you count the rows where `_corrupt_record IS NOT NULL` straight from the files?

# COMMAND ----------

# DBTITLE 1,E8 Code
OBS_PATH = f"{FHIR}/observations"
perm = spark.read.json(OBS_PATH)                                           # PERMISSIVE is the default mode
only_corrupt = first_error(lambda: perm.filter("_corrupt_record IS NOT NULL").count())   # only the corrupt column is used
corrupt_rows = perm.filter("_corrupt_record IS NOT NULL").select("id", "_corrupt_record").collect()   # with another column
n_drop = spark.read.option("mode", "DROPMALFORMED").json(OBS_PATH).count() # bad lines removed
failfast = first_error(lambda: spark.read.option("mode", "FAILFAST").json(OBS_PATH).count())
fixed = spark.read.schema(OBS_MINI).json(OBS_PATH)                          # explicit schema WITHOUT _corrupt_record
n_fixed, n_fixed_null = fixed.count(), fixed.filter("id IS NULL").count()
renamed_cols = [c for c in spark.read.option("columnNameOfCorruptRecord", "bad_line").json(OBS_PATH).columns
                if c in ("bad_line", "_corrupt_record")]                    # the corrupt column can be renamed
svc = spark.read.option("singleVariantColumn", "v").json(OBS_PATH)          # the whole record as one VARIANT column
n_svc, n_svc_null = svc.count(), svc.filter("v IS NULL").count()
rf = spark.sql(f"""SELECT count(*) AS n, count_if(v IS NULL) AS null_v
                   FROM read_files('{OBS_PATH}', format => 'json', singleVariantColumn => 'v')""").first()
single_quote = spark.sql(f"""SELECT raw:id AS colon_id, from_json(raw, '{OBS_MINI}').id AS from_json_id,
                                    try_parse_json(raw) IS NULL AS variant_is_null
                             FROM {B} WHERE raw LIKE "{{'resourceType'%" """).first()   # the single-quoted line

summary = [                                                                # (method, rows kept, what the bad lines become)
    ("spark.read.json PERMISSIVE (default)", n_obs, f"{len(corrupt_rows)} rows with _corrupt_record, all other columns NULL"),
    ("  count() using only _corrupt_record", None, only_corrupt),
    ("spark.read.json DROPMALFORMED", n_drop, "dropped"),
    ("spark.read.json FAILFAST", None, failfast),
    ("spark.read.schema(...) without _corrupt_record", n_fixed, f"{n_fixed_null} rows with every column NULL"),
    ("colon on the bronze STRING (E3)", c3["n_rows"], f"{c3['no_id']} rows where raw:id is NULL"),
    ("from_json PERMISSIVE (E4)", p["n"], f"{p['null_ids']} structs with all fields NULL"),
    ("parse_json (E5)", None, parse_error),
    ("try_parse_json (E5)", c3["n_rows"], f"{len(bad)} NULL values"),
    ("spark.read singleVariantColumn", n_svc, f"{n_svc_null} rows with v NULL"),
    ("read_files singleVariantColumn", rf["n"], f"{rf['null_v']} rows with v NULL"),
]
display(spark.createDataFrame(summary, "method STRING, rows_kept LONG, bad_lines_become STRING"))
print(summary, "| single-quoted line:", single_quote.asDict(), "| renamed:", renamed_cols)
METRICS["E8.permissive_rows"], METRICS["E8.corrupt_rows"], METRICS["E8.only_corrupt"] = n_obs, len(corrupt_rows), only_corrupt
METRICS["E8.drop_rows"], METRICS["E8.failfast"], METRICS["E8.fixed_rows"], METRICS["E8.fixed_null"] = n_drop, failfast, n_fixed, n_fixed_null
METRICS["E8.renamed"], METRICS["E8.svc_rows"], METRICS["E8.svc_null"] = ", ".join(renamed_cols), n_svc, n_svc_null
METRICS["E8.rf_rows"], METRICS["E8.rf_null"] = rf["n"], rf["null_v"]
METRICS["E8.sq_colon"], METRICS["E8.sq_from_json"], METRICS["E8.sq_variant_null"] = single_quote["colon_id"], single_quote["from_json_id"], single_quote["variant_is_null"]
assert n_drop + len(corrupt_rows) == n_obs and only_corrupt != "ok" and failfast != "ok"

# COMMAND ----------

# DBTITLE 1,E8 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run)** — the same {{setup.bad_lines}} bad lines in the {{E1.obs_rows}}-line observation files:
# MAGIC
# MAGIC | Method | Rows kept | Bad lines become |
# MAGIC | --- | --- | --- |
# MAGIC | `spark.read.json` (PERMISSIVE, default) | {{E8.permissive_rows}} | {{E8.corrupt_rows}} rows with text in `_corrupt_record` and all other columns NULL |
# MAGIC | … counting only `_corrupt_record` from the files | — | **`{{E8.only_corrupt}}`** |
# MAGIC | `spark.read.json` DROPMALFORMED | {{E8.drop_rows}} | dropped |
# MAGIC | `spark.read.json` FAILFAST | — | `{{E8.failfast}}` |
# MAGIC | `spark.read.schema(...)` without `_corrupt_record` | {{E8.fixed_rows}} | {{E8.fixed_null}} rows with every column NULL — no trace of the cause |
# MAGIC | colon on the bronze `STRING` (E3) | {{E3.rows}} (bronze) | {{E3.no_id}} rows where every path is NULL |
# MAGIC | `from_json` PERMISSIVE (E4) | {{E4.rows}} (bronze) | {{E4.null_ids}} structs with NULL fields |
# MAGIC | `parse_json` (E5) | — | `{{E5.parse_error}}` |
# MAGIC | `try_parse_json` (E5) | {{E3.rows}} (bronze) | {{E5.null_v}} NULL values |
# MAGIC | `spark.read` `singleVariantColumn` | {{E8.svc_rows}} | {{E8.svc_null}} rows with `v` NULL |
# MAGIC | `read_files(..., singleVariantColumn => 'v')` | {{E8.rf_rows}} | {{E8.rf_null}} rows with `v` NULL |
# MAGIC
# MAGIC * **Single quotes.** The line `{'resourceType': 'Observation', ...}` was **not** a bad record for the JSON reader, colon notation (`raw:id` = `{{E8.sq_colon}}`) or `from_json` (`id` = `{{E8.sq_from_json}}`): the JSON options default `allowSingleQuotes` = true (*from_json* page). `try_parse_json` returned NULL for it ({{E8.sq_variant_null}}): the variant parser is strict JSON. That is why the reader found {{E8.corrupt_rows}} corrupt rows but `try_parse_json` {{E5.null_v}} NULLs. The file readers with `singleVariantColumn` also had only {{E8.svc_null}} / {{E8.rf_null}} NULL rows: they accepted the single-quoted line as well.
# MAGIC * Counting `_corrupt_record` straight from the files failed with `{{E8.only_corrupt}}`; selecting it together with another column (`id`) worked. On classic compute the usual fix is to cache the DataFrame first; on serverless `cache()` is not supported (L04), so save to a table, or select another column too.
# MAGIC * The corrupt column can be renamed: with `columnNameOfCorruptRecord` = `bad_line` the column list contained `{{E8.renamed}}`.
# MAGIC * An explicit schema **without** a corrupt-record column hides the problem: {{E8.fixed_null}} all-NULL rows and no text to debug.
# MAGIC
# MAGIC > **Key takeaway:** decide on purpose what happens to bad records: keep the raw line (bronze text or `_corrupt_record`), drop it (DROPMALFORMED), or stop (FAILFAST / `parse_json`). The bronze-as-text pattern of this lesson keeps all {{setup.bronze_rows}} lines and lets silver decide.

# COMMAND ----------

# DBTITLE 1,E9 Predict
# MAGIC %md
# MAGIC ### E9 — Flatten bronze JSON to silver `patients` and `observations`
# MAGIC
# MAGIC Goal: `silver_patients` (one row per patient, typed columns) and `silver_observations` (one row per measured value: a blood pressure panel gives 2 rows, one per component).
# MAGIC
# MAGIC **Predict:**
# MAGIC 1. `filter(telecom, t -> t.system = 'phone')[0].value` — what happens for a patient with `"telecom": []`?
# MAGIC 2. How many silver observation rows do {{setup.observations}} observations give, if {{setup.bp_obs}} are panels with 2 components?
# MAGIC 3. What happens to the single-quoted line that has no `subject` and no `code`?

# COMMAND ----------

# DBTITLE 1,E9 Code
PATIENT_SCHEMA = ("id STRING, gender STRING, birthDate DATE, active BOOLEAN, "                 # DATE: parsed from yyyy-MM-dd
                  "name ARRAY<STRUCT<use: STRING, family: STRING, given: ARRAY<STRING>>>, "
                  "telecom ARRAY<STRUCT<system: STRING, value: STRING, use: STRING>>, "
                  "address ARRAY<STRUCT<city: STRING, state: STRING, postalCode: STRING>>, "
                  "identifier ARRAY<STRUCT<system: STRING, value: STRING>>")

# 1) the [n] trap: an index on an empty array is an error under ANSI mode
index_error = first_error(lambda: spark.sql(f"""
    SELECT filter(p.telecom, t -> t.system = 'phone')[0].value
    FROM (SELECT from_json(raw, '{PATIENT_SCHEMA}') AS p FROM {B} WHERE raw:resourceType = 'Patient')""").collect())

# 2) silver_patients in SQL: from_json + get() (NULL instead of an error) + higher-order filter()
spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.silver_patients AS
WITH parsed AS (
  SELECT from_json(raw, '{PATIENT_SCHEMA}') AS p                    -- typed struct
  FROM {B} WHERE raw:resourceType = 'Patient')                      -- colon path to choose the resource type
SELECT p.id                                                        AS patient_id,
       p.identifier[0].value                                       AS mrn,
       get(filter(p.name, n -> n.use = 'official'), 0).family      AS family_name,   -- get(): NULL if no element
       get(filter(p.name, n -> n.use = 'official'), 0).given[0]    AS given_name,
       p.gender,
       p.birthDate                                                 AS birth_date,
       p.active,
       p.address[0].city                                           AS city,
       p.address[0].state                                          AS state,
       p.address[0].postalCode                                     AS postal_code,
       get(filter(p.telecom, t -> t.system = 'phone'), 0).value    AS phone,
       get(filter(p.telecom, t -> t.system = 'email'), 0).value    AS email,
       coalesce(size(p.telecom), 0)                                AS telecom_count  -- missing and [] -> 0
FROM parsed""")

# 3) silver_observations in PySpark: from_json + explode_outer(component) + coalesce(component, top level)
OBS_SCHEMA = ("id STRING, status STRING, effectiveDateTime TIMESTAMP, "
              "code STRUCT<coding: ARRAY<STRUCT<system: STRING, code: STRING, display: STRING>>>, "
              "subject STRUCT<reference: STRING>, valueQuantity STRUCT<value: DOUBLE, unit: STRING>, valueString STRING, "
              "component ARRAY<STRUCT<code: STRUCT<coding: ARRAY<STRUCT<system: STRING, code: STRING, display: STRING>>>, "
              "valueQuantity: STRUCT<value: DOUBLE, unit: STRING>>>")
obs_all = (spark.table(B)
           .filter(F.expr("raw:resourceType = 'Observation'"))                 # observations only (bad lines give NULL)
           .select(F.from_json("raw", OBS_SCHEMA).alias("o")).select("o.*"))    # struct -> top-level columns
obs_ok = obs_all.filter(F.col("subject.reference").isNotNull() & F.col("code").isNotNull())   # quality rule
rows = obs_ok.select("*", F.explode_outer("component").alias("c"))             # panels: 1 row per component; others: 1 row
silver_obs = rows.select(
    F.col("id").alias("obs_id"),
    F.regexp_extract("subject.reference", r"^Patient/(.+)$", 1).alias("patient_id"),   # "Patient/pat-015" -> "pat-015"
    F.coalesce(F.col("c.code.coding")[0]["code"], F.col("code.coding")[0]["code"]).alias("loinc_code"),
    F.coalesce(F.col("c.code.coding")[0]["display"], F.col("code.coding")[0]["display"]).alias("loinc_display"),
    "status",
    F.col("effectiveDateTime").alias("effective_ts"),
    F.coalesce(F.col("c.valueQuantity.value"), F.col("valueQuantity.value")).alias("value_num"),
    F.col("valueString").alias("value_text"),
    F.coalesce(F.col("c.valueQuantity.unit"), F.col("valueQuantity.unit")).alias("unit"))
silver_obs.write.mode("overwrite").saveAsTable(f"{FQ}.silver_observations")

display(spark.table(f"{FQ}.silver_patients").orderBy("patient_id").limit(5))
display(spark.sql(f"""SELECT loinc_code, loinc_display, count(*) AS rows_, count(value_num) AS numeric, count(value_text) AS text
                      FROM {FQ}.silver_observations GROUP BY ALL ORDER BY loinc_code"""))
sp = spark.sql(f"""SELECT count(*) AS n, count(phone) AS phones, count(email) AS emails, count_if(telecom_count = 0) AS no_telecom,
                          count(gender) AS genders, count(family_name) AS families FROM {FQ}.silver_patients""").first()
so = spark.sql(f"""SELECT count(*) AS n, count(DISTINCT obs_id) AS obs, count(value_num) AS num, count(value_text) AS txt,
                          count_if(value_num IS NULL AND value_text IS NULL) AS no_value FROM {FQ}.silver_observations""").first()
types = dict(spark.table(f"{FQ}.silver_patients").selectExpr("typeof(birth_date) AS b").first().asDict())
n_obs_all, n_obs_ok = obs_all.count(), obs_ok.count()
print("index:", index_error, "| patients", sp.asDict(), "| observations", so.asDict(), "| parsed", n_obs_all, "kept", n_obs_ok, types)
METRICS["E9.index_error"], METRICS["E9.patients"], METRICS["E9.phones"], METRICS["E9.emails"] = index_error, sp["n"], sp["phones"], sp["emails"]
METRICS["E9.no_telecom"], METRICS["E9.genders"], METRICS["E9.families"], METRICS["E9.birth_type"] = sp["no_telecom"], sp["genders"], sp["families"], types["b"]
METRICS["E9.obs_rows"], METRICS["E9.obs_ids"], METRICS["E9.value_num"], METRICS["E9.value_text"] = so["n"], so["obs"], so["num"], so["txt"]
METRICS["E9.no_value"], METRICS["E9.obs_parsed"], METRICS["E9.obs_kept"] = so["no_value"], n_obs_all, n_obs_ok
assert sp["n"] == 40 and sp["phones"] == EXP["phones"] and sp["emails"] == EXP["emails"] and sp["genders"] == EXP["with_gender"]
assert so["n"] == EXP["observations"] + EXP["bp_obs"] and so["obs"] == EXP["observations"] and so["num"] == EXP["numeric_values"] + 2 * EXP["bp_obs"]

# COMMAND ----------

# DBTITLE 1,E9 Explained
# MAGIC %md
# MAGIC **Explained (real output from this run):**
# MAGIC
# MAGIC * `filter(telecom, …)[0]` failed with **`{{E9.index_error}}`** as soon as one patient had an empty array: ANSI mode turns an out-of-bounds index into an error. `get(array, 0)` returns NULL instead (and `try_element_at(array, 1)` does the same, 1-based).
# MAGIC * **`silver_patients`**: {{E9.patients}} rows, one per patient. `birth_date` has type `{{E9.birth_type}}` (the `from_json` schema said `DATE`). Phone {{E9.phones}}, email {{E9.emails}}, gender {{E9.genders}}, official family name {{E9.families}}; {{E9.no_telecom}} patients have `telecom_count` = 0 (missing and empty arrays both, thanks to `coalesce(size(...), 0)`).
# MAGIC * **`silver_observations`**: the resource-type filter + `from_json` gave {{E9.obs_parsed}} observations; the quality rule (a `subject` and a `code` must exist) kept {{E9.obs_kept}} — it removed the single-quoted line, which parsed but has no patient and no code. `explode_outer("component")` then gave **{{E9.obs_rows}}** rows for {{E9.obs_ids}} observations: one per lab or test result, two per blood pressure panel.
# MAGIC * Values: {{E9.value_num}} numeric, {{E9.value_text}} text results, and {{E9.no_value}} row with neither — `obs-0077` with `"pending"`, which the `DOUBLE` schema turned into NULL (E4). In a real pipeline you would send it to a quarantine table (L07).
# MAGIC * `coalesce(component value, top-level value)` puts both shapes into the same columns, so analysts query one simple table.
# MAGIC
# MAGIC > **Key takeaway:** silver = explicit schema + one row per business fact. Choose `explode_outer` when the parent must stay, `get()` / `try_element_at` when an array may be empty, and a quality rule for records that parsed but are incomplete.

# COMMAND ----------

# DBTITLE 1,6 — Exam Traps
# MAGIC %md
# MAGIC ## 6 Exam Traps
# MAGIC
# MAGIC | # | Trap (wrong belief) | Truth |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | "`explode` keeps every parent row." | {{E2.lost}} patients disappeared with `explode` (only {{E2.ids_explode}} of {{E1.pat_rows}} left); `explode_outer` kept all ({{E2.outer}} rows) (E2). |
# MAGIC | 2 | "`size()` of a missing array is 0." | It was {{E2.size_missing}} (NULL) under ANSI mode; only `[]` gave {{E2.size_empty}}. Use `coalesce(size(x), 0)` (E2). |
# MAGIC | 3 | "Colon paths on a JSON string are case-sensitive." | Outside brackets they are not: `raw:RESOURCETYPE` → {{E3.upper_case}} values; inside brackets they are: `raw:['RESOURCETYPE']` → {{E3.bracket_upper}} (E3, *Query JSON strings*). |
# MAGIC | 4 | "`VARIANT` paths follow the same case rules as JSON strings." | `v:RESOURCETYPE` → {{E5.upper_case}} values: variant paths are case-sensitive (E5, *How is variant different than JSON strings?*). |
# MAGIC | 5 | "`raw:a.b` returns a typed value." | Always a string (`{{E3.value_type}}`); on a variant, a path returns `{{E5.path_type}}`. Cast with `::type` (E3, E5). |
# MAGIC | 6 | "`raw:x::double` gives NULL for a value that is not a number." | It failed with `{{E3.cast_error}}` under ANSI mode; `try_cast` gave NULL (E3). |
# MAGIC | 7 | "`variant_get` returns NULL when the cast fails." | Only for a missing path. A failed cast → `{{E5.get_error}}`; `try_variant_get` gives NULL (E5, *variant_get* page). |
# MAGIC | 8 | "`from_json` returns a NULL struct for a malformed line." | NULL structs: {{E4.null_structs}}; structs with all fields NULL: {{E4.null_ids}}. Check a key field (E4). |
# MAGIC | 9 | "`from_json(raw, schema_of_json(raw))` infers a schema per row." | `{{E4.per_row}}`: the schema must be a constant (E4). |
# MAGIC | 10 | "A schema from one sample record is safe." | The sample said `BIGINT`; the A1C decimals were lost ({{E4.sample_a1c}} survived) (E4). |
# MAGIC | 11 | "FAILFAST stops only on broken JSON." | A valid record with `"pending"` for a `DOUBLE` also failed: `{{E4.failfast_type}}` (E4). |
# MAGIC | 12 | "`parse_json` skips bad lines." | One bad line failed the query: `{{E5.parse_error}}`; `try_parse_json` returns NULL (E5). |
# MAGIC | 13 | "`v:active IS NULL` finds JSON nulls." | For `"active": null` it was {{E5.active_sql_null}}; `is_variant_null(v:active)` was {{E5.active_variant_null}} (E5). |
# MAGIC | 14 | "You can `GROUP BY`, compare and `ORDER BY` a variant value." | `{{E6.group}}`, `{{E6.compare}}`, `{{E6.order}}`. Cast first: `GROUP BY v:gender::string` (E6, *Query variant data*). |
# MAGIC | 15 | "A `VARIANT` column can be a liquid clustering key." | `CLUSTER BY (v)` → `{{E6.cluster}}` (E6). |
# MAGIC | 16 | "`[*]` works on variant paths too." | `{{E6.star}}` — `[*]` is for JSON strings only (E6). |
# MAGIC | 17 | "`arr[0]` on an empty array returns NULL." | `{{E9.index_error}}` under ANSI mode; `get(arr, 0)` / `try_element_at(arr, 1)` return NULL (E9). |
# MAGIC | 18 | "Single-quoted JSON is a bad record everywhere." | The JSON reader, colon and `from_json` accepted it (`id` = `{{E8.sq_from_json}}`); `try_parse_json` returned NULL ({{E8.sq_variant_null}}) (E8). |
# MAGIC | 19 | "`spark.read.json` reads a pretty-printed file." | Without `multiLine` the only column was `{{E1.single_cols}}` (E1). |
# MAGIC | 20 | "I can count `_corrupt_record` rows straight from the files." | `{{E8.only_corrupt}}`; select another column too, or save to a table first (E8). |

# COMMAND ----------

# DBTITLE 1,7 — Syntax Cheat Sheet
# MAGIC %md
# MAGIC ## 7 Syntax Cheat Sheet
# MAGIC
# MAGIC | Goal | SQL | PySpark (`from pyspark.sql import functions as F`) |
# MAGIC | --- | --- | --- |
# MAGIC | Read JSON lines / one document per file | `SELECT * FROM read_files('/Volumes/…', format => 'json' [, multiLine => true])` | `spark.read.json(path)`, `spark.read.option("multiLine", "true").json(path)` |
# MAGIC | Keep raw lines as text | `SELECT value AS raw FROM read_files(path, format => 'text')` | `spark.read.text(path).select(F.col("value").alias("raw"), "_metadata.file_name")` |
# MAGIC | Struct field / array element | `name[0].family`, `subject.reference` | `F.col("name")[0]["family"]`, `F.col("subject.reference")`, `F.expr("name[0].family")` |
# MAGIC | Safe array element | `get(arr, 0)` (0-based), `try_element_at(arr, 1)` (1-based) | `F.expr("get(arr, 0)")`, `F.try_element_at("arr", F.lit(1))` |
# MAGIC | JSON string path | `raw:code.coding[0].code`, `raw:['Key']`, `raw:arr[*].x` | `F.expr("raw:code.coding[0].code")` |
# MAGIC | Cast a path | `raw:a.b::double`, `try_cast(raw:a.b AS DOUBLE)` | `F.expr("try_cast(raw:a.b AS DOUBLE)")` |
# MAGIC | Parse with a schema | `from_json(raw, 'id STRING, v STRUCT<x: DOUBLE>')` | `F.from_json("raw", "id STRING, v STRUCT<x: DOUBLE>")` |
# MAGIC | Keep bad text | add `_corrupt_record STRING` to the schema | the same; reader: `.option("columnNameOfCorruptRecord", "bad")` |
# MAGIC | Strict parsing | `from_json(raw, schema, map('mode', 'FAILFAST'))` | `F.from_json("raw", schema, {"mode": "FAILFAST"})`; reader `.option("mode", "FAILFAST" / "DROPMALFORMED")` |
# MAGIC | Schema from a sample | `schema_of_json('{"a": 1}')` (a literal) | `F.schema_of_json(F.lit(sample))` |
# MAGIC | Back to JSON text | `to_json(struct_or_variant)` | `F.to_json("col")` |
# MAGIC | Array → rows | `FROM t, LATERAL explode_outer(t.arr) AS x(item)` | `df.select("id", F.explode_outer("arr").alias("item"))` |
# MAGIC | With position / struct fields as columns | `LATERAL posexplode(t.arr) AS x(pos, item)`, `LATERAL inline_outer(t.arr)` | `F.posexplode("arr")`, `F.inline_outer("arr")` |
# MAGIC | String → VARIANT | `parse_json(raw)` (fails on bad JSON), `try_parse_json(raw)` (NULL) | `F.expr("try_parse_json(raw)")` |
# MAGIC | Files → VARIANT | `read_files(path, format => 'json', singleVariantColumn => 'v')` | `spark.read.option("singleVariantColumn", "v").json(path)` |
# MAGIC | Variant path + cast | `v:code.coding[0].code::string` (case-sensitive) | `F.expr("v:code.coding[0].code::string")` |
# MAGIC | Variant get with type | `variant_get(v, '$.a.b', 'double')`, `try_variant_get(...)` | `F.expr("try_variant_get(v, '$.a.b', 'double')")` |
# MAGIC | Variant schema | `schema_of_variant(v)`, `schema_of_variant_agg(v)` (aggregate) | `F.expr("schema_of_variant(v)")` |
# MAGIC | Variant array/object → rows | `FROM t, LATERAL variant_explode_outer(t.v:arr) AS x` → `x.pos, x.key, x.value` | through `spark.sql` |
# MAGIC | JSON null vs SQL NULL | `is_variant_null(v:a)` vs `v:a IS NULL` | `F.expr("is_variant_null(v:a)")` |
# MAGIC | Group / filter / sort by a variant value | `GROUP BY v:a::string`, `WHERE v:a::string = 'x'` | `df.groupBy(F.expr("v:a::string"))` |
# MAGIC | Count elements | `coalesce(size(arr), 0)`; variant: `array_size(v:arr::array<variant>)` | `F.coalesce(F.size("arr"), F.lit(0))` |
# MAGIC
# MAGIC Newer PySpark clients also have `F.parse_json`, `F.try_parse_json`, `F.variant_get` (E6 shows what this notebook's client has); `F.expr("…")` works everywhere.

# COMMAND ----------

# DBTITLE 1,8 — Check Your Understanding
# MAGIC %md
# MAGIC ## 8 Check Your Understanding
# MAGIC
# MAGIC Answer in your own words. Answers are in section 11.
# MAGIC
# MAGIC 1. A hospital receives FHIR JSON from five vendors; new optional fields appear every month and some files contain broken lines. Describe a bronze and a silver design. Which column type do you choose for bronze, and how do you make sure no record is lost and no load fails because of one bad line?
# MAGIC 2. After a release, the silver patient-contacts table has fewer patients than the patients table, and nobody saw an error. Name two code mistakes from this lesson that cause it silently, and how you would detect each one with a query.
# MAGIC 3. The same query `SELECT x:Status FROM t` returns values when `x` is a `STRING` column but only NULLs when `x` is a `VARIANT` column. Explain why, and how you would write the path so it works for both.

# COMMAND ----------

# DBTITLE 1,9 — Practice (intro)
# MAGIC %md
# MAGIC ## 9 Practice
# MAGIC
# MAGIC Do these yourself, without AI help. Each task has a validation cell that prints ✅ Passed or a hint. Solutions are in section 11.
# MAGIC
# MAGIC > The practice uses the setup objects only (`bronze_fhir`, `bronze_encounters` and the files in `/Volumes/main/dbacademy_l05/data/fhir/`). Run section 3 and the next cell first.

# COMMAND ----------

# DBTITLE 1,9 — Practice helpers (run first)
def diff_rows(a, b):
    """Rows in a but not in b plus rows in b but not in a (0 = same result)."""
    return a.exceptAll(b).count() + b.exceptAll(a).count()

# the expected Encounter results, built in Python from the generated data (setup)
exp_enc = spark.createDataFrame(
    [(e["id"], e["subject"]["reference"].split("/")[1], e["class"]["code"], e["period"]["start"], e["period"].get("end"),
      len(e.get("diagnosis", []))) for e in encounters],
    "encounter_id STRING, patient_id STRING, class_code STRING, start_s STRING, end_s STRING, diagnosis_count INT")
exp_dx = spark.createDataFrame(
    [(e["id"], d["rank"], d["condition"]["code"], d["condition"]["display"]) for e in encounters for d in e.get("diagnosis", [])],
    "encounter_id STRING, rank INT, icd10_code STRING, display STRING")

def check_encounter_tables(enc_name, dx_name):
    """Compare the two practice tables with the expected rows; return a list of problems (empty = passed)."""
    problems = []
    try:
        t = spark.table(enc_name)
        types = dict(t.dtypes)
        if types.get("start_ts") != "timestamp" or types.get("end_ts") != "timestamp":
            problems.append(f"{enc_name}: start_ts / end_ts must be TIMESTAMP, got {types.get('start_ts')} / {types.get('end_ts')}")
        got = t.select("encounter_id", "patient_id", "class_code",
                       F.date_format("start_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start_s"),
                       F.date_format("end_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("end_s"),
                       F.col("diagnosis_count").cast("int").alias("diagnosis_count"))
        d = diff_rows(got, exp_enc)
        if d:
            problems.append(f"{enc_name}: {d} rows differ from the expected {len(encounters)} rows "
                            "(patient_id without 'Patient/'? diagnosis_count 0 for missing AND empty arrays?)")
    except Exception as e:
        problems.append(f"{enc_name}: {error_class(e)}")
    try:
        t = spark.table(dx_name)
        got = t.select("encounter_id", F.col("rank").cast("int").alias("rank"), "icd10_code", "display")
        d = diff_rows(got, exp_dx)
        if d:
            problems.append(f"{dx_name}: {d} rows differ from the expected {exp_dx.count()} diagnosis rows (one row per diagnosis)")
    except Exception as e:
        problems.append(f"{dx_name}: {error_class(e)}")
    return problems

# COMMAND ----------

# DBTITLE 1,P1 Task
# MAGIC %md
# MAGIC ### P1 — Fill in the blanks (3 blanks): colon notation and `explode_outer`
# MAGIC 1. `main.dbacademy_l05.p1_obs_values` from `bronze_fhir`: Observation rows only, with `obs_id`, `loinc_code` (the `code` of the **first** `coding` entry) and `value_num` (`valueQuantity.value` as `DOUBLE`; NULL when it is not a number — the query must not fail).
# MAGIC 2. `main.dbacademy_l05.p1_patient_contacts` from the patient files: one row per patient per `telecom` entry (`patient_id`, `system`, `value`); patients **without** telecom entries must stay, with NULLs.
# MAGIC
# MAGIC Replace the three `<FILL_IN>` markers.

# COMMAND ----------

# DBTITLE 1,P1 Code
# >>> BLANK P1
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p1_obs_values AS
    SELECT raw:id                                          AS obs_id,
           <FILL_IN>                                       AS loinc_code,   -- 1: colon path to the first coding code
           try_cast(raw:valueQuantity.value AS <FILL_IN>)  AS value_num     -- 2: the numeric type
    FROM {FQ}.bronze_fhir
    WHERE raw:resourceType = 'Observation'
""")
(spark.read.json(f"{FHIR}/patients")
    .select("id", F.<FILL_IN>("telecom").alias("tel"))                                  # 3: keep patients without entries
    .select(F.col("id").alias("patient_id"), F.col("tel.system").alias("system"), F.col("tel.value").alias("value"))
    .write.mode("overwrite").saveAsTable(f"{FQ}.p1_patient_contacts"))
# >>> SOLUTION P1
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p1_obs_values AS
    SELECT raw:id                                          AS obs_id,
           raw:code.coding[0].code                         AS loinc_code,   -- 1: colon path to the first coding code
           try_cast(raw:valueQuantity.value AS DOUBLE)     AS value_num     -- 2: the numeric type
    FROM {FQ}.bronze_fhir
    WHERE raw:resourceType = 'Observation'
""")
(spark.read.json(f"{FHIR}/patients")
    .select("id", F.explode_outer("telecom").alias("tel"))                              # 3: keep patients without entries
    .select(F.col("id").alias("patient_id"), F.col("tel.system").alias("system"), F.col("tel.value").alias("value"))
    .write.mode("overwrite").saveAsTable(f"{FQ}.p1_patient_contacts"))
# <<< END P1

# COMMAND ----------

# DBTITLE 1,P1 Validation
try:
    o = spark.sql(f"""SELECT count(loinc_code) AS codes, count(value_num) AS values, typeof(max(value_num)) AS t,
                             count_if(loinc_code = '4548-4') AS a1c FROM {FQ}.p1_obs_values""").first()
    c = spark.sql(f"SELECT count(*) AS n, count(DISTINCT patient_id) AS p, count(value) AS v FROM {FQ}.p1_patient_contacts").first()
    want_rows = EXP["telecom_entries"] + EXP["telecom_missing"] + EXP["telecom_empty"]
    ok_o = o["codes"] == EXP["observations"] and o["values"] == EXP["numeric_values"] and o["t"] == "double"
    ok_c = c["n"] == want_rows and c["p"] == EXP["patients"] and c["v"] == EXP["telecom_entries"]
    ok = ok_o and ok_c
    PRACTICE_PASSED["P1"] = ok
    if ok:
        print("✅ Passed")
    if not ok_o:
        print(f"Hint (p1_obs_values): {o['codes']} codes (want {EXP['observations']}), {o['values']} numeric values "
              f"(want {EXP['numeric_values']}), type {o['t']} (want double). Path: code -> coding -> first element -> code.")
    if not ok_c:
        print(f"Hint (p1_patient_contacts): {c['n']} rows for {c['p']} patients (want {want_rows} rows, {EXP['patients']} patients). "
              "Which explode function keeps a row for a missing or empty array?")
    METRICS["P1.obs_codes"], METRICS["P1.a1c"], METRICS["P1.contact_rows"] = o["codes"], o["a1c"], c["n"]
except Exception as e:
    PRACTICE_PASSED["P1"] = False
    print("Hint: run the P1 code cell first (all three blanks filled).", error_class(e))

# COMMAND ----------

# DBTITLE 1,P2 Task
# MAGIC %md
# MAGIC ### P2 — Fix the bugs (2 bugs)
# MAGIC The registration team needs `main.dbacademy_l05.p2_patient_contacts` from the `VARIANT` form of `bronze_fhir`: one row per patient per contact (`patient_id`, `gender`, `system`, `contact`). Every patient must appear — a patient without contacts gets one row with NULL `system` and `contact` — and `gender` must be filled wherever the JSON has it.
# MAGIC
# MAGIC The cell runs without an error, but the result is wrong in two places. Fix them in place.

# COMMAND ----------

# DBTITLE 1,P2 Code
# >>> BLANK P2
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p2_patient_contacts AS
    SELECT b.v:id::string            AS patient_id,
           b.v:Gender::string        AS gender,
           t.value:system::string    AS system,
           t.value:value::string     AS contact
    FROM (SELECT try_parse_json(raw) AS v FROM {FQ}.bronze_fhir) AS b,
         LATERAL variant_explode(b.v:telecom) AS t
    WHERE b.v:resourceType::string = 'Patient'
""")
# >>> SOLUTION P2
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p2_patient_contacts AS
    SELECT b.v:id::string            AS patient_id,
           b.v:gender::string        AS gender,                     -- fix 1: variant paths are case-sensitive
           t.value:system::string    AS system,
           t.value:value::string     AS contact
    FROM (SELECT try_parse_json(raw) AS v FROM {FQ}.bronze_fhir) AS b,
         LATERAL variant_explode_outer(b.v:telecom) AS t             -- fix 2: keep patients without contacts
    WHERE b.v:resourceType::string = 'Patient'
""")
# <<< END P2

# COMMAND ----------

# DBTITLE 1,P2 Validation
try:
    c = spark.sql(f"""SELECT count(*) AS n, count(DISTINCT patient_id) AS p, count(gender) AS g, count(contact) AS contacts
                      FROM {FQ}.p2_patient_contacts""").first()
    want_rows = EXP["telecom_entries"] + EXP["telecom_missing"] + EXP["telecom_empty"]
    want_g = sum(max(len(p.get("telecom", [])), 1) for p in patients if "gender" in p)   # rows of patients with a gender
    ok_rows = c["n"] == want_rows and c["p"] == EXP["patients"]
    ok_g = c["g"] == want_g
    ok = ok_rows and ok_g
    PRACTICE_PASSED["P2"] = ok
    if ok:
        print("✅ Passed")
    if not ok_rows:
        print(f"Hint (rows): {c['n']} rows for {c['p']} patients, want {want_rows} rows for {EXP['patients']} patients. "
              "Which patients are missing? What does variant_explode do with a missing or empty array?")
    if not ok_g:
        print(f"Hint (gender): {c['g']} rows have a gender, want {want_g}. Compare the path with the key in the JSON, letter by letter.")
    METRICS["P2.rows"], METRICS["P2.gender_rows"] = c["n"], c["g"]
except Exception as e:
    PRACTICE_PASSED["P2"] = False
    print("Hint: run the P2 code cell first.", error_class(e))

# COMMAND ----------

# DBTITLE 1,P3 Task
# MAGIC %md
# MAGIC ### P3 — From scratch (with hints): FHIR Encounters → 2 silver tables (`from_json`)
# MAGIC `bronze_encounters.raw` holds one Encounter JSON per row. Create two tables:
# MAGIC 1. `main.dbacademy_l05.p3_encounters` — one row per encounter: `encounter_id`, `patient_id` (`pat-…`, without `Patient/`), `class_code`, `start_ts` and `end_ts` (`TIMESTAMP`; `end_ts` NULL when there is no end), `diagnosis_count` (0 when `diagnosis` is missing **or** empty).
# MAGIC 2. `main.dbacademy_l05.p3_encounter_diagnoses` — one row per diagnosis: `encounter_id`, `rank`, `icd10_code`, `display`.
# MAGIC
# MAGIC Use `from_json` with an explicit schema (no `VARIANT` here).
# MAGIC
# MAGIC **Hints:** look at one record first (`SELECT raw FROM … LIMIT 1`) · `period STRUCT<start: TIMESTAMP, \`end\`: TIMESTAMP>` (`end` and `rank` need backticks in the DDL and in SQL) · `regexp_extract(ref, '^Patient/(.+)$', 1)` · `coalesce(size(arr), 0)` · `FROM t, LATERAL explode(t.arr) AS x(d)` (here `explode` is right: an encounter without diagnoses has no diagnosis row).

# COMMAND ----------

# DBTITLE 1,P3 Code
# >>> BLANK P3
# Your code here
# >>> SOLUTION P3
ENC_SCHEMA = ("id STRING, class STRUCT<code: STRING>, subject STRUCT<reference: STRING>, "
              "period STRUCT<start: TIMESTAMP, `end`: TIMESTAMP>, "
              "diagnosis ARRAY<STRUCT<`rank`: INT, condition: STRUCT<code: STRING, display: STRING>>>")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p3_encounters AS
    SELECT e.id                                                   AS encounter_id,
           regexp_extract(e.subject.reference, '^Patient/(.+)$', 1) AS patient_id,
           e.class.code                                           AS class_code,
           e.period.start                                         AS start_ts,
           e.period.`end`                                         AS end_ts,
           coalesce(size(e.diagnosis), 0)                         AS diagnosis_count
    FROM (SELECT from_json(raw, '{ENC_SCHEMA}') AS e FROM {FQ}.bronze_encounters)
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p3_encounter_diagnoses AS
    SELECT e.id AS encounter_id, d.`rank`, d.condition.code AS icd10_code, d.condition.display AS display
    FROM (SELECT from_json(raw, '{ENC_SCHEMA}') AS e FROM {FQ}.bronze_encounters),
         LATERAL explode(e.diagnosis) AS x(d)
""")
# <<< END P3

# COMMAND ----------

# DBTITLE 1,P3 Validation
problems = check_encounter_tables(f"{FQ}.p3_encounters", f"{FQ}.p3_encounter_diagnoses")
PRACTICE_PASSED["P3"] = not problems
print("✅ Passed" if not problems else "Not yet:\n- " + "\n- ".join(problems))
if not problems:
    p3 = spark.sql(f"""SELECT (SELECT count(*) FROM {FQ}.p3_encounters) AS enc, (SELECT count(*) FROM {FQ}.p3_encounter_diagnoses) AS dx,
                              (SELECT count_if(diagnosis_count = 0) FROM {FQ}.p3_encounters) AS no_dx""").first()
    METRICS["P3.encounters"], METRICS["P3.diagnoses"], METRICS["P3.no_dx"] = p3["enc"], p3["dx"], p3["no_dx"]

# COMMAND ----------

# DBTITLE 1,P4 Task
# MAGIC %md
# MAGIC ### P4 — BLIND (no hints)
# MAGIC Do P3 again with `VARIANT`: create `main.dbacademy_l05.p4_bronze_encounters` with one `VARIANT` column `v` from `bronze_encounters.raw`, then `main.dbacademy_l05.p4_encounters` and `main.dbacademy_l05.p4_encounter_diagnoses` with the same columns and the same rows as in P3 — using only `VARIANT` functions on `v` (no `from_json`).

# COMMAND ----------

# DBTITLE 1,P4 Code
# >>> BLANK P4
# Your code here
# >>> SOLUTION P4
spark.sql(f"CREATE OR REPLACE TABLE {FQ}.p4_bronze_encounters AS SELECT parse_json(raw) AS v FROM {FQ}.bronze_encounters")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p4_encounters AS
    SELECT v:id::string                                                     AS encounter_id,
           regexp_extract(v:subject.reference::string, '^Patient/(.+)$', 1) AS patient_id,
           v:class.code::string                                             AS class_code,
           v:period.start::timestamp                                        AS start_ts,
           v:period.end::timestamp                                          AS end_ts,
           coalesce(array_size(v:diagnosis::array<variant>), 0)             AS diagnosis_count
    FROM {FQ}.p4_bronze_encounters
""")
spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.p4_encounter_diagnoses AS
    SELECT v:id::string                          AS encounter_id,
           d.value:rank::int                     AS `rank`,
           d.value:condition.code::string        AS icd10_code,
           d.value:condition.display::string     AS display
    FROM {FQ}.p4_bronze_encounters, LATERAL variant_explode(v:diagnosis) AS d
""")
# <<< END P4

# COMMAND ----------

# DBTITLE 1,P4 Validation
problems = check_encounter_tables(f"{FQ}.p4_encounters", f"{FQ}.p4_encounter_diagnoses")
try:
    v_type = dict(spark.table(f"{FQ}.p4_bronze_encounters").dtypes).get("v")
    if v_type != "variant":
        problems.append(f"p4_bronze_encounters.v must be VARIANT, got {v_type}")
except Exception as e:
    problems.append(f"p4_bronze_encounters: {error_class(e)}")
PRACTICE_PASSED["P4"] = not problems
print("✅ Passed" if not problems else "Not yet:\n- " + "\n- ".join(problems))
if not problems:
    p4 = spark.sql(f"SELECT count(*) AS n FROM {FQ}.p4_encounter_diagnoses").first()
    METRICS["P4.diagnoses"], METRICS["P4.v_type"] = p4["n"], v_type

# COMMAND ----------

# DBTITLE 1,10 — Exam-Style Questions
# MAGIC %md
# MAGIC ## 10 Exam-Style Questions
# MAGIC
# MAGIC Choose the **one best answer**. Answers and explanations are in section 11.
# MAGIC
# MAGIC **Q1.** A bronze table keeps FHIR Observation JSON in a `STRING` column `raw`. The key in the JSON is `status`. An analyst writes `SELECT raw:Status FROM bronze_obs`. What is returned?
# MAGIC A. NULL in every row · B. An error, because the path does not exist · C. The status values · D. The status values, but only if the column is cast to `VARIANT` first
# MAGIC
# MAGIC **Q2.** The same data is stored as `VARIANT` column `v`. The analyst writes `SELECT v:Status::string FROM bronze_obs_v`. What is returned?
# MAGIC A. The status values · B. NULL in every row · C. An `INVALID_VARIANT_CAST` error · D. An `UNRESOLVED_COLUMN` error
# MAGIC
# MAGIC **Q3.** A silver step turns the `telecom` array of each patient into one row per contact. Every patient must stay in the table, including patients whose `telecom` is missing or empty. Which function should the engineer use?
# MAGIC A. `explode` · B. `explode_outer` · C. `posexplode` · D. `flatten`
# MAGIC
# MAGIC **Q4.** A lab interface sends JSON lines; sometimes a line is cut off. The team wants a `VARIANT` column in bronze, the load must never fail because of one bad line, and the bad lines must be kept for investigation. What should the bronze statement be?
# MAGIC A. `SELECT parse_json(raw) AS v FROM landing` · B. `SELECT try_parse_json(raw) AS v, raw FROM landing` · C. `spark.read.option("mode", "FAILFAST").json(path)` · D. `spark.read.option("mode", "DROPMALFORMED").json(path)`
# MAGIC
# MAGIC **Q5.** Claims are stored as `VARIANT` column `v`. `SELECT v:payer, count(*) FROM claims_v GROUP BY v:payer` fails. What is the correct fix?
# MAGIC A. `GROUP BY v:payer::string` (and select `v:payer::string`) · B. `ALTER TABLE claims_v CLUSTER BY (v)` · C. `SELECT DISTINCT v:payer` first · D. Use `[*]` in the path
# MAGIC
# MAGIC **Q6.** A glucose feed stored as `VARIANT` sometimes has `"value": "pending"`. A dashboard needs the numeric values and NULL for the rest, and the query must not fail. Which expression?
# MAGIC A. `variant_get(v, '$.value', 'double')` · B. `v:value::double` · C. `try_variant_get(v, '$.value', 'double')` · D. `cast(v:value AS DOUBLE)`
# MAGIC
# MAGIC **Q7.** `from_json(raw, 'id STRING, valueQuantity STRUCT<value: DOUBLE>')` runs in the default mode on a row whose `raw` is `### export interrupted ###`. What does the expression return for that row?
# MAGIC A. NULL · B. A struct whose fields are all NULL · C. An error `MALFORMED_RECORD_IN_PARSING` · D. The row is removed
# MAGIC
# MAGIC **Q8.** A device vendor sends one pretty-printed JSON document per file. `spark.read.json(path)` returns a DataFrame with only the column `_corrupt_record`. What is the fix?
# MAGIC A. `.option("mode", "PERMISSIVE")` · B. `.option("multiLine", "true")` · C. `.option("columnNameOfCorruptRecord", "raw")` · D. `.option("inferSchema", "true")`

# COMMAND ----------

# DBTITLE 1,11 — Solutions
# MAGIC %md
# MAGIC ## 11 Solutions
# MAGIC
# MAGIC > Try the warm-up, practice and questions yourself first.
# MAGIC
# MAGIC ### Warm-up
# MAGIC **W1 — newest reading per patient (`Window` + `row_number` + `filter` + `drop`)**
# MAGIC {{solution:W1}}
# MAGIC
# MAGIC **W2 — a Unity Catalog SQL function (`CASE` with the NULL branch first)**
# MAGIC {{solution:W2}}
# MAGIC
# MAGIC **W3 — `DataFrame.transform` chain**
# MAGIC {{solution:W3}}
# MAGIC
# MAGIC ### Check Your Understanding
# MAGIC 1. **Bronze:** land every record without parsing risk — either the raw line as `STRING` (as in this setup, {{setup.bronze_rows}} of {{setup.bronze_rows}} lines kept) or `try_parse_json(raw)` into a `VARIANT` column **plus** the raw text, so the {{E5.null_v}} lines that are not valid JSON are still there to investigate (E5, E8). `VARIANT` is the recommended format for changing schemas (*Query variant data*): new vendor fields are kept without schema changes. Do **not** use `parse_json` or FAILFAST in bronze (one bad line fails the load, E5, E4). **Silver:** extract with exact-case paths and casts (`try_variant_get` / `try_cast` for unreliable values), or `from_json` with an explicit schema; use `schema_of_variant_agg` to design the columns (E7); add a quality rule and a quarantine for records that parsed but are incomplete (E9).
# MAGIC 2. (a) `explode` instead of `explode_outer` (or `variant_explode` instead of `variant_explode_outer`) drops patients with a missing or empty array — E2 lost {{E2.lost}} patients. Detect: `SELECT count(DISTINCT patient_id)` in silver vs the patients table, or an anti join. (b) A wrong-case `VARIANT` path (or a wrong-case `from_json` schema field) gives NULL silently — then a later `WHERE x IS NOT NULL` or inner join drops rows. Detect: `count(col)` vs `count(*)` per column, or `schema_of_variant_agg` to see the real key names. Also `arr[0]` with ANSI: that one is not silent — it fails with `{{E9.index_error}}`.
# MAGIC 3. On a `STRING`, a colon path outside brackets is matched case-insensitively (*Query JSON strings*; E3: `raw:RESOURCETYPE` → {{E3.upper_case}} values). On a `VARIANT` every path element is case-sensitive (*How is variant different than JSON strings?*; E5: {{E5.upper_case}} values). Write the exact key name (`x:status`) — it works for both. Brackets (`x:['status']`) are case-sensitive for both, which makes the intent explicit.
# MAGIC
# MAGIC ### P1
# MAGIC {{solution:P1}}
# MAGIC
# MAGIC ### P2 (fixes: `v:gender` with the exact case; `variant_explode_outer` keeps patients without contacts)
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
# MAGIC **Q1 → C.** On a JSON string, names outside brackets are matched case-insensitively (*Query JSON strings*; E3: `raw:RESOURCETYPE` returned {{E3.upper_case}} values). A wrong: that would be the `VARIANT` or bracket behaviour. B wrong: a missing path gives NULL, never an error (E3: {{E3.missing_path}} values, no error). D wrong: casting to `VARIANT` would make the path case-sensitive and return NULL.
# MAGIC
# MAGIC **Q2 → B.** Variant paths are case-sensitive, and a path that does not match gives NULL (E5: `v:RESOURCETYPE` → {{E5.upper_case}} values). A wrong: that is the JSON-string behaviour. C wrong: casting NULL to `STRING` is not a cast error; `INVALID_VARIANT_CAST` needs an existing value of the wrong type (E5: `{{E5.get_error}}` on `"pending"`). D wrong: `v` exists; the path is inside the value.
# MAGIC
# MAGIC **Q3 → B.** `explode_outer` returns one row with NULL for a NULL or empty array (E2: {{E2.outer}} rows, every patient kept). A wrong: `explode` dropped {{E2.lost}} patients. C wrong: `posexplode` adds a position but drops the same rows as `explode`. D wrong: `flatten` turns an array of arrays into one array; it does not create rows.
# MAGIC
# MAGIC **Q4 → B.** `try_parse_json` returns NULL for invalid JSON instead of failing (*try_parse_json* page; E5: {{E5.null_v}} NULLs), and the `raw` column keeps the original text. A wrong: `parse_json` failed the whole query with `{{E5.parse_error}}`. C wrong: FAILFAST fails the load (E8: `{{E8.failfast}}`). D wrong: DROPMALFORMED loses the bad lines (E8: {{E8.drop_rows}} of {{E8.permissive_rows}} rows kept) and produces structs, not `VARIANT`.
# MAGIC
# MAGIC **Q5 → A.** A variant value cannot be grouped, compared or sorted (*Query variant data*; E6: `{{E6.group}}`); cast it to a typed value first. B wrong: a `VARIANT` column cannot be a clustering key (E6: `{{E6.cluster}}`), and clustering does not fix grouping. C wrong: `SELECT DISTINCT` on a variant failed too (`{{E6.distinct}}`). D wrong: `[*]` is not supported on variant paths (`{{E6.star}}`).
# MAGIC
# MAGIC **Q6 → C.** `try_variant_get` returns NULL when the value cannot be cast (*variant_get* page; E5: {{E5.try_values}} numbers, no error). A wrong: `variant_get` failed with `{{E5.get_error}}`. B and D wrong: the cast failed with `{{E5.cast_error}}` (E5).
# MAGIC
# MAGIC **Q7 → B.** In PERMISSIVE mode the result was a struct with all fields NULL, not a NULL struct (E4: {{E4.null_structs}} NULL structs, {{E4.null_ids}} structs with `id` NULL). A wrong: see E4. C wrong: that is FAILFAST (E4: `{{E4.failfast_bad}}`). D wrong: an expression never removes rows.
# MAGIC
# MAGIC **Q8 → B.** By default `spark.read.json` expects one JSON document per line (*Read and write JSON files*); a pretty-printed document needs `multiLine` (E1: {{E1.multi_rows}} row with `multiLine`, only `{{E1.single_cols}}` without). A wrong: PERMISSIVE is already the default. C wrong: it only renames the corrupt-record column. D wrong: JSON schema inference is already on; `inferSchema` is a CSV option.

# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,12 — Cleanup
spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")        # drops all tables, the volume with its files, and the UC functions
print(f"Dropped {FQ}.")

# COMMAND ----------

# DBTITLE 1,13 — Final Summary
# MAGIC %md
# MAGIC ## 13 Final Summary (numbers from the real run)
# MAGIC
# MAGIC | Example | Key result |
# MAGIC | --- | --- |
# MAGIC | Setup | {{setup.files}} files: {{setup.patients}} patients, {{setup.observations}} observations + {{setup.bad_lines}} bad lines, {{setup.encounters}} encounters, 1 multi-line Bundle · `bronze_fhir` {{setup.bronze_rows}} raw lines |
# MAGIC | E1 structs | inferred `birthDate` `{{E1.birth_type}}`, `value` `{{E1.obs_value_type}}` (one `"pending"`) · Bundle without `multiLine`: `{{E1.single_cols}}`; with it: {{E1.resources}} resources |
# MAGIC | E2 explode | `explode` {{E2.explode}} rows ({{E2.lost}} patients lost) · `explode_outer` {{E2.outer}} · `size(missing)` = {{E2.size_missing}} |
# MAGIC | E3 colon on STRING | `raw:RESOURCETYPE` {{E3.upper_case}} values, `raw:['RESOURCETYPE']` {{E3.bracket_upper}} · type `{{E3.value_type}}` · `::double` → `{{E3.cast_error}}` |
# MAGIC | E4 from_json | malformed → {{E4.null_structs}} NULL structs, {{E4.null_ids}} all-NULL structs · per-row schema → `{{E4.per_row}}` · sample schema kept {{E4.sample_a1c}} A1C values |
# MAGIC | E5 VARIANT | `parse_json` → `{{E5.parse_error}}` · `try_parse_json` {{E5.null_v}} NULL · `v:RESOURCETYPE` {{E5.upper_case}} · `variant_get` → `{{E5.get_error}}`, `try_variant_get` {{E5.try_values}} values |
# MAGIC | E6 VARIANT limits | `GROUP BY` `{{E6.group}}` · `=` `{{E6.compare}}` · `CLUSTER BY` `{{E6.cluster}}` · client PySpark {{E6.client}}, collect VARIANT: `{{E6.collect_variant}}` |
# MAGIC | E7 schema / explode | A1C stored as `{{E7.a1c_type}}` · merged `value` type `{{E7.value_in_agg}}` · `variant_explode` {{E7.comp_rows}} component rows, `_outer` {{E7.comp_outer_rows}} |
# MAGIC | E8 bad records | PERMISSIVE {{E8.permissive_rows}} rows ({{E8.corrupt_rows}} corrupt) · DROPMALFORMED {{E8.drop_rows}} · FAILFAST `{{E8.failfast}}` · single-quoted line: reader yes, variant no |
# MAGIC | E9 silver | `[0]` on `[]` → `{{E9.index_error}}` · `silver_patients` {{E9.patients}} · `silver_observations` {{E9.obs_rows}} rows for {{E9.obs_ids}} observations |
# MAGIC | Practice | P1 {{P1.contact_rows}} contact rows · P2 {{P2.rows}} rows · P3 {{P3.encounters}} encounters / {{P3.diagnoses}} diagnoses · P4 {{P4.diagnoses}} diagnoses from `{{P4.v_type}}` |
# MAGIC
# MAGIC ### NOT VERIFIED — concept only
# MAGIC * **`badRecordsPath`** — a file-reader option named on *Read and write JSON files*; not tested in this lesson.
# MAGIC * **`rescuedDataColumn` on JSON reads** — tested in L06 (Auto Loader); not repeated here.
# MAGIC * **Auto Loader / Lakeflow Declarative Pipelines with `singleVariantColumn`, and `from_json` schema inference with `schemaLocationKey` in pipelines** — streaming and pipelines come in L06/L07 (*Ingest data as semi-structured variant type*, *from_json* page).
# MAGIC * **VARIANT performance** ("improved read and write performance") and the **16 MB** record limit — doc statements; this lesson does not measure them.
# MAGIC * **VARIANT for XML and CSV** (Runtime 16.4+) — doc statement only.
# MAGIC
# MAGIC ### Surprises compared with the first expectation
# MAGIC 1. The single-quoted line was accepted by `spark.read.json`, colon notation and `from_json` (default `allowSingleQuotes`), but rejected by `try_parse_json` / `parse_json` — so the "bad record" count depends on the method: {{E8.corrupt_rows}} vs {{E5.null_v}} (E8).
# MAGIC 2. `_corrupt_record` from `from_json` also caught the valid record with `"pending"` for a `DOUBLE` field ({{E4.corrupt}} rows), and FAILFAST failed on it with `{{E4.failfast_type}}` (E4).
# MAGIC 3. A schema from one `schema_of_json` sample typed the lab value as `BIGINT` and lost the A1C decimals ({{E4.sample_a1c}} kept) (E4).
# MAGIC 4. `count(DISTINCT v:status)` → `{{E6.count_distinct}}`, while `SELECT DISTINCT v:status` → `{{E6.distinct}}` (E6).
# MAGIC 5. Without `multiLine`, `count()` on the {{setup.bundle_lines}}-line Bundle file returned {{E1.single_count}} (E1).
# MAGIC 6. The job notebook runs PySpark {{E6.client}}, but `F.parse_json` existed ({{E6.has_parse_json}}) and collecting a `VARIANT` value worked (`{{E6.collect_variant}}`) (E6).
