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
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Automate a Pipeline Job</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Verify your automated pipeline and explore what the job produced.</div>
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
# MAGIC ### Instructions
# MAGIC
# MAGIC In Lesson 14, you created a Lakeflow Job that ran the Bronze → Silver → Gold pipeline automatically. Now let's verify what the job produced and compare it to the tables you built manually.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Compare manual vs. automated Bronze tables
# MAGIC
# MAGIC Query both the manual Bronze table (from Lesson 12) and the job-created Bronze table. Do they have the same data?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Manual (Lesson 12)' AS source, COUNT(*) AS rows FROM current_employees_bronze
# MAGIC UNION ALL
# MAGIC SELECT 'Job (Lesson 14)', COUNT(*) FROM current_employees_bronze_job;

# COMMAND ----------

# MAGIC %md
# MAGIC Both should show **6 rows**. The same data, loaded by the same COPY INTO logic, but one was run interactively and the other by the job.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Compare Gold table outputs
# MAGIC
# MAGIC Query both Gold tables and compare the aggregations.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query the manual Gold table from Lesson 12
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM total_roles_gold;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query the job-created Gold table from Lesson 14
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM total_roles_gold_job;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC The results should match. This confirms that the automated job produces the same output as your manual pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Explore the version history of the job tables
# MAGIC
# MAGIC Check the version history of `current_employees_bronze_job`. Who made the changes — you or the job?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Show version history of the job's Bronze table
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>DESCRIBE HISTORY current_employees_bronze_job;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the **userName** column. It shows the job executed the operations, not your interactive session. This is how you can distinguish manual changes from automated pipeline runs in your audit trail.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: List all tables in your schema
# MAGIC
# MAGIC Run `SHOW TABLES` to see everything you've built across all 8 lessons.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC You should see tables from every stage of the course: from your first `employees` table in Lesson 02 through the full automated pipeline tables. Each one represents a skill you've built.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="margin-top: 10px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;">Nice work! You just:</div>
# MAGIC     <ul style="padding-left: 20px; margin: 0;">
# MAGIC       <li>Verified the automated pipeline matches your manual pipeline</li>
# MAGIC       <li>Compared manual vs. job-created tables</li>
# MAGIC       <li>Used version history to distinguish interactive changes from automated runs</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <!-- FINAL SUMMARY: What You Built -->
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #1B5162; color: white; border-radius: 8px; padding: 24px 28px; text-align: center;">
# MAGIC   <div style="font-size: 14pt; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; opacity: 0.85; margin-bottom: 8px;">Course Complete</div>
# MAGIC   <div style="font-size: 20pt; font-weight: 700;">What You Built in This Course</div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 16px; padding: 20px 24px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7;">
# MAGIC     <p>Over 7 lessons you:</p>
# MAGIC     <ul style="padding-left: 20px; margin: 8px 0;">
# MAGIC       <li>Navigated Unity Catalog to find raw data files in volumes</li>
# MAGIC       <li>Created UC tables from CSV files and verified them in Catalog Explorer</li>
# MAGIC       <li>Modified data with INSERT, UPDATE, and DELETE</li>
# MAGIC       <li>Used DESCRIBE HISTORY and time travel to audit changes and query previous versions</li>
# MAGIC       <li>Ingested data three ways: CTAS (programmatic), Upload UI (ad-hoc), and COPY INTO (incremental)</li>
# MAGIC       <li>Proved COPY INTO's idempotency, safe to re-run, only loads new files</li>
# MAGIC       <li>Built a Medallion Architecture pipeline: Bronze (raw) → Silver (cleaned) → Gold (aggregated)</li>
# MAGIC       <li>Explored lineage, permissions, and insights in Catalog Explorer</li>
# MAGIC       <li>Automated the full pipeline as a Lakeflow Job with task dependencies and scheduling</li>
# MAGIC     </ul>
# MAGIC     <p><strong>You went from raw CSV files to an automated, governed data pipeline — the core workflow that data engineers use daily on Databricks.</strong></p>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="margin-top: 16px; padding: 16px 20px; background: rgba(0,169,114,0.08); border-left: 4px solid #00A972; border-radius: 6px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <strong>Want to go further?</strong> Lesson 16 is a bonus lesson where you rebuild this same pipeline using <strong>Spark Declarative Pipelines</strong>: a declarative approach that replaces all the manual orchestration with just three SQL statements. If you have time, give it a try!
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
