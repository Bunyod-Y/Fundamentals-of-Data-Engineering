# Databricks notebook source
# LESSON_TITLE: L03 - Bronze Ingestion

# COMMAND ----------

# DBTITLE 1,About this notebook
# MAGIC %md
# MAGIC # L03 — Bronze ingestion (called by the L03 lesson)
# MAGIC
# MAGIC The lesson runs this notebook with `dbutils.notebook.run("./L03 - Bronze Ingestion", 600, {...})`.
# MAGIC It loads new claim files into the bronze table **once** (Auto Loader + `availableNow`), like one run of a scheduled ingestion job.
# MAGIC
# MAGIC * Success: it returns the number of rows it added with `dbutils.notebook.exit(...)`.
# MAGIC * Failure: it writes the error classes to `error_file`, then raises the error again, so the caller sees a failed run.
# MAGIC
# MAGIC Do not run it by hand: the widgets are empty until the lesson passes the values.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("source_dir", "")          # landing folder with the JSON files
dbutils.widgets.text("table", "")               # target Delta table
dbutils.widgets.text("checkpoint", "")          # checkpoint location, also used as schema location
dbutils.widgets.text("merge_schema", "false")   # "true": the writer may add new columns to the table
dbutils.widgets.text("error_file", "")          # where to write the error classes if the stream fails

source_dir = dbutils.widgets.get("source_dir")
table = dbutils.widgets.get("table")
checkpoint = dbutils.widgets.get("checkpoint")
merge_schema = dbutils.widgets.get("merge_schema") == "true"
error_file = dbutils.widgets.get("error_file")

# COMMAND ----------

# DBTITLE 1,Run Auto Loader once
import json, re

def table_count(name):
    """Row count of a table, or 0 if it does not exist yet."""
    return spark.table(name).count() if spark.catalog.tableExists(name) else 0

before = table_count(table)
try:
    writer = (spark.readStream
        .format("cloudFiles")                                  # Auto Loader source
        .option("cloudFiles.format", "json")                   # the files are JSON
        .option("cloudFiles.schemaLocation", checkpoint)       # keep the inferred schema here (_schemas)
        .load(source_dir)
        .writeStream
        .option("checkpointLocation", checkpoint)              # remember which files were loaded
        .trigger(availableNow=True))                           # load all new files, then stop
    if merge_schema:
        writer = writer.option("mergeSchema", "true")          # allow new columns in the Delta table
    writer.toTable(table).awaitTermination()
except Exception as e:
    # Error classes look like [UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_RECORD_WITH_FILE_PATH] in the message
    classes = list(dict.fromkeys(re.findall(r"\[([A-Z][A-Z0-9_]+(?:\.[A-Z0-9_]+)*)\]", str(e))))
    dbutils.fs.put(error_file, json.dumps({"error_classes": classes,
                                           "first_line": str(e).splitlines()[0][:500]}), overwrite=True)
    raise                                                      # the caller must see a failed run

dbutils.notebook.exit(json.dumps({"rows_added": table_count(table) - before}))
