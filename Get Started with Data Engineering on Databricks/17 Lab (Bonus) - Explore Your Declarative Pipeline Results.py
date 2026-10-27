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
# MAGIC <div style="background: #02A36F; color: white; border-radius: 8px; padding: 28px 32px; text-align: center; position: relative;">
# MAGIC   <div style="font-size: 14pt; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; opacity: 0.85; margin-bottom: 8px;">Lab</div>
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Explore Your Declarative Pipeline Results</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Query the tables created by your SDP pipeline and compare them to the tables you built manually.</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED — SELECT A COMPUTE ENVIRONMENT
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select Serverless Compute</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Before running this notebook, confirm your compute environment at the top-right of the notebook.
# MAGIC
# MAGIC - Click the compute dropdown and select **Serverless** (the default option).
# MAGIC - If you do not see Serverless available, contact your workspace administrator.
# MAGIC
# MAGIC **Note:** This notebook was developed and tested on **Serverless compute**. Other compute options may work but are not guaranteed to behave the same.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup
# MAGIC Run the cell below to configure your environment.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC *Prerequisite: You must complete **Lesson 16** (create and run the ETL Pipeline) before starting this practice. The tables explored here are created by that pipeline.*

# COMMAND ----------

# Check that the SDP pipeline tables exist
_sdp_tables = [
    "current_employees_bronze_sdp",
    "current_employees_silver_sdp",
    "total_roles_gold_sdp"
]
_missing = []

for _t in _sdp_tables:
    try:
        spark.sql(f"DESCRIBE TABLE {_t}")
    except Exception as _e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(_e):
            _missing.append(_t)
        else:
            raise _e

if _missing:
    print("The following SDP tables were not found:")
    for _t in _missing:
        print(f"  - {_t}")
    print("\nThese tables are created by running the ETL Pipeline in Lesson 16.")
    print("Go back to Lesson 16, follow the steps to create and start the pipeline,")
    print("and return here once the pipeline shows 'Completed'.")
else:
    print("All 3 SDP tables found — ready to go!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Task 1: Query the SDP Bronze table
# MAGIC
# MAGIC Start by looking at the raw data that the streaming table ingested from the CSV files.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query the SDP Bronze table
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM current_employees_bronze_sdp;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **6 rows** — all employees from both `employees.csv` and `employees2.csv`. The streaming table ingested all CSV files from the volume in one pass, just like COPY INTO did in Lesson 12.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Task 2: Query the SDP Silver table
# MAGIC
# MAGIC Look at the cleaned and enriched data. Pay attention to the columns that were added by the transformation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query the SDP Silver table
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM current_employees_silver_sdp;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the `Role` column is uppercased and there are `processed_timestamp` and `processed_date` columns — the same transformations from Lesson 12, but defined declaratively.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Task 3: Query the SDP Gold table
# MAGIC
# MAGIC Check the aggregated results.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query the SDP Gold table
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM total_roles_gold_sdp;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC You should see the same role counts as `total_roles_gold` from Lesson 12 — the pipeline produced the same business-ready summary, just with less code and no manual orchestration.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Task 4: Compare SDP tables to your manual tables
# MAGIC
# MAGIC Let's see how the results compare. Run the query below to put the manual Gold table and the SDP Gold table side by side.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Manual (Lesson 12)' AS source, Role, TotalEmployees
# MAGIC FROM total_roles_gold
# MAGIC UNION ALL
# MAGIC SELECT 'SDP (Lesson 16)', Role, TotalEmployees
# MAGIC FROM total_roles_gold_sdp
# MAGIC ORDER BY Role, source;

# COMMAND ----------

# MAGIC %md
# MAGIC The data should match: both pipelines produced the same results from the same source files. The difference is *how* they got there: one was step-by-step imperative code across multiple notebooks, the other was three declarative SQL statements in a single notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Task 5: Explore data quality expectations
# MAGIC
# MAGIC In Lesson 16, you added two `CONSTRAINT` expectations to the Silver layer. Let's check if they were evaluated.
# MAGIC
# MAGIC 1. Open **Catalog Explorer** from the left sidebar
# MAGIC 2. Navigate to your catalog (`learn_get_started_de_YOUR_USERNAME`) → **get_started_de** → **current_employees_silver_sdp**
# MAGIC 3. Click the **Quality** tab
# MAGIC
# MAGIC You should see the two expectations listed:
# MAGIC - `valid_id` — EXPECT (ID IS NOT NULL)
# MAGIC - `valid_name` — EXPECT (FirstName IS NOT NULL)
# MAGIC
# MAGIC Both should show 100% pass rate since the employee data has no null values.

# COMMAND ----------

# MAGIC %md
# MAGIC You can also check the expectations from SQL. Run the query below to confirm there are no null IDs or names in the Silver table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(ID) AS non_null_ids,
# MAGIC   COUNT(FirstName) AS non_null_names,
# MAGIC   COUNT(*) - COUNT(ID) AS null_ids,
# MAGIC   COUNT(*) - COUNT(FirstName) AS null_names
# MAGIC FROM current_employees_silver_sdp;

# COMMAND ----------

# MAGIC %md
# MAGIC All rows should have non-null IDs and names, meaning both expectations passed. In a production pipeline with messier data, these expectations would help you catch quality issues early.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Task 6: Explore lineage
# MAGIC
# MAGIC One of the advantages of SDP is that lineage is tracked automatically.
# MAGIC
# MAGIC 1. In **Catalog Explorer**, navigate to **total_roles_gold_sdp**
# MAGIC 2. Click the **Lineage** tab
# MAGIC 3. You should see the full chain: CSV files → Bronze → Silver → Gold
# MAGIC
# MAGIC Compare this to the lineage for `total_roles_gold` (your manual Gold table from Lesson 12). Both show a similar chain, but the SDP lineage was established automatically from your SQL definitions. You didn't have to do anything extra.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="margin-top: 10px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;">Nice work! You just:</div>
# MAGIC     <ul style="padding-left: 20px; margin: 0;">
# MAGIC       <li>Queried all three layers of a declaratively-built pipeline</li>
# MAGIC       <li>Compared SDP results to your manual tables and confirmed they match</li>
# MAGIC       <li>Explored data quality expectations in Catalog Explorer</li>
# MAGIC       <li>Traced lineage from source files through Bronze, Silver, and Gold</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <!-- CHECKPOINT: Bonus lesson complete -->
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #1B5162; color: white; border-radius: 8px; padding: 24px 28px; text-align: center;">
# MAGIC   <div style="font-size: 14pt; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; opacity: 0.85; margin-bottom: 8px;">Bonus Complete</div>
# MAGIC   <div style="font-size: 20pt; font-weight: 700;">Declarative Pipelines — Done!</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 16px; padding: 20px 24px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7;">
# MAGIC     <p>In this bonus lesson, you rebuilt the same Bronze → Silver → Gold pipeline using a completely different approach:</p>
# MAGIC     <ul style="padding-left: 20px; margin: 8px 0;">
# MAGIC       <li>Defined the full pipeline in <strong>3 SQL statements</strong> instead of multiple notebooks and manual steps</li>
# MAGIC       <li>Used <strong>streaming tables</strong> for incremental ingestion and <strong>materialized views</strong> for transformations</li>
# MAGIC       <li>Added <strong>data quality expectations</strong> that are tracked automatically</li>
# MAGIC       <li>Let SDP handle orchestration, compute, and execution order</li>
# MAGIC     </ul>
# MAGIC     <p>In production, most Databricks data engineers use Spark Declarative Pipelines for exactly this reason: less code, automatic orchestration, and built-in data quality.</p>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 16px; padding: 16px 20px; background: rgba(0,169,114,0.08); border-left: 4px solid #00A972; border-radius: 6px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <strong>Ready to clean up?</strong> Use the <strong>Reset or Clean Up Course Resources</strong> notebook to remove all tables and assets created during this course.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
