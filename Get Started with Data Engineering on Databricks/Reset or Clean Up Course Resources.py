# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #D32F2F; color: white; border-radius: 8px; padding: 28px 32px; text-align: center; position: relative;">
# MAGIC   <div style="font-size: 14pt; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; opacity: 0.85; margin-bottom: 8px;">Cleanup</div>
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Reset or Remove Course Resources</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Use this notebook to start over or clean up after completing the course.</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="padding: 18px 24px; background: #FFF3E0; border: 3px solid #FF9800; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px; font-size: 16pt;">Before you run anything</div>
# MAGIC     <p>This notebook has <strong>two options</strong>. Read both and choose the one that fits your situation.</p>
# MAGIC     <ul style="padding-left: 20px; margin: 8px 0;">
# MAGIC       <li><strong>Option A: Mid-Course Reset</strong> — Drops all tables but keeps the schema and CSV files so you can restart from Lesson 1. Use this if you feel stuck and want a clean slate.</li>
# MAGIC       <li><strong>Option B: Full Cleanup</strong> — Removes <em>everything</em>: all tables, the volume, the CSV files, and the schema itself. Use this when you're completely finished with the course.</li>
# MAGIC     </ul>
# MAGIC     <p><strong>Run only the option you need.</strong> Do not run both.</p>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run this cell first — it connects to the course catalog and schema so the cleanup commands work correctly.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1

# COMMAND ----------

print(f"Connected to {my_catalog}.{my_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Option A: Mid-Course Reset
# MAGIC **Use this if you want to start over from Lesson 1.**
# MAGIC
# MAGIC This will:
# MAGIC - Drop all tables created during the course (lessons and practices)
# MAGIC - Drop any temporary views
# MAGIC - Keep the schema, volume, and CSV files intact
# MAGIC - After running this, go back to Lesson 1 and run the Setup cell. You'll be right back at the beginning

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="padding: 14px 20px; background: #FFF3E0; border-left: 4px solid #FF9800; border-radius: 6px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026;">
# MAGIC     <strong>Running the next cell will drop all tables.</strong> Your CSV source files will be preserved.
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# -----------------------------------------------
# OPTION A: Drop all tables, keep schema and files
# -----------------------------------------------

tables_to_drop = [
    # Lesson 01
    "employees",
    # Practice 01L
    "new_employees",
    # Lesson 04
    "current_employees_ctas",
    "current_employees_ui",
    # Practice 04L
    "new_hires_ctas",
    # Lesson 05
    "current_employees_copyinto",
    # Practice 05L
    "practice_copyinto",
    # Lesson 06
    "current_employees_bronze",
    "current_employees_silver",
    "total_roles_gold",
    # Practice 06L
    "practice_bronze",
    "practice_silver",
    "country_count_gold",
    # Lesson 07 / Job tasks
    "current_employees_bronze_job",
    "current_employees_silver_job",
    "total_roles_gold_job",
    # Lesson 08 / SDP pipeline
    "current_employees_bronze_sdp",
    "current_employees_silver_sdp",
    "total_roles_gold_sdp",
]

print("Dropping tables...\n")
for table in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS `{my_catalog}`.`{my_schema}`.`{table}`")
        print(f"  Dropped: {table}")
    except Exception as e:
        print(f"  Skipped: {table} ({e})")

# Drop temp views (only active in current session, but just in case)
for view in ["temp_total_roles", "temp_country_counts"]:
    try:
        spark.sql(f"DROP VIEW IF EXISTS {view}")
    except:
        pass

print("\n--- Reset complete ---")
print(f"All tables removed from {my_catalog}.{my_schema}.")
print("The schema, volume, and CSV files are still intact.")
print("You can now go back to Lesson 1 and start fresh.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Verify:** Run the cell below to confirm no tables remain.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Option B: Full Cleanup
# MAGIC **Use this when you are completely finished with the course.**
# MAGIC
# MAGIC This will remove **everything** created during the course:
# MAGIC - All tables
# MAGIC - The `myfiles` volume and its CSV files
# MAGIC - The `get_started_de` schema itself
# MAGIC
# MAGIC After running this, the course environment will be completely gone. To take the course again, you would need to start from scratch by running the Setup cell in Lesson 1.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="padding: 18px 24px; background: #FFEBEE; border: 3px solid #D32F2F; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px; font-size: 16pt;">This action is irreversible</div>
# MAGIC     <p>Running the next cell will permanently delete:</p>
# MAGIC     <ul style="padding-left: 20px; margin: 8px 0;">
# MAGIC       <li>All tables in <code>learn_get_started_de_YOUR_USERNAME.get_started_de</code></li>
# MAGIC       <li>The <code>myfiles</code> volume and its contents (<code>employees.csv</code>, <code>employees2.csv</code>)</li>
# MAGIC       <li>The <code>get_started_de</code> schema</li>
# MAGIC     </ul>
# MAGIC     <p>Only run this if you are done with the course and want to free up the resources.</p>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# -----------------------------------------------
# OPTION B: Remove everything — schema and all contents
# -----------------------------------------------

print("Starting full cleanup...\n")

# Drop all tables first (CASCADE on schema should handle this,
# but being explicit ensures clean error messages)
tables_to_drop = [
    "employees", "new_employees",
    "current_employees_ctas", "current_employees_ui", "new_hires_ctas",
    "current_employees_copyinto", "practice_copyinto",
    "current_employees_bronze", "current_employees_silver",
    "practice_bronze", "practice_silver",
    "total_roles_gold", "country_count_gold",
    "current_employees_bronze_job", "current_employees_silver_job", "total_roles_gold_job",
    "current_employees_bronze_sdp", "current_employees_silver_sdp", "total_roles_gold_sdp",
]

print("Dropping tables...")
for table in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS `{my_catalog}`.`{my_schema}`.`{table}`")
    except:
        pass

# Drop the volume (removes CSV files)
print("Dropping volume and files...")
try:
    spark.sql(f"DROP VOLUME IF EXISTS `{my_catalog}`.`{my_schema}`.myfiles")
    print("  Volume 'myfiles' dropped (employees.csv and employees2.csv removed)")
except Exception as e:
    print(f"  Could not drop volume: {e}")

# Drop the schema
print("Dropping schema...")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS `{my_catalog}`.`{my_schema}` CASCADE")
    print(f"  Schema '{my_schema}' dropped")
except Exception as e:
    print(f"  Could not drop schema: {e}")

print("\n--- Full cleanup complete ---")
print(f"The schema '{my_catalog}.{my_schema}' and all its contents have been removed.")
print("To retake the course, run the Setup cell in Lesson 1 to recreate everything.")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
