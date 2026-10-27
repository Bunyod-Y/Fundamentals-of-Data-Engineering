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
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Ingest Data with CTAS</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Practice creating a table from a file using CTAS with explicit format options and column selection.</div>
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
# MAGIC In Lesson 08, you created `current_employees_ctas` from `employees.csv` using explicit format options. Now you'll do the same thing with `employees2.csv`.
# MAGIC
# MAGIC Your goal: create a clean table called **`new_hires_ctas`** from `employees2.csv` with only the 4 data columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Preview the source file
# MAGIC
# MAGIC Before creating the table, preview the contents of `employees2.csv` using `read_files` with explicit format options.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Preview employees2.csv with format options (format, header, inferSchema)
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM read_files(
# MAGIC   '/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees2.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Create the table with column selection
# MAGIC
# MAGIC Use CTAS to create a table called **`new_hires_ctas`** from `employees2.csv`. Select only the 4 data columns (ID, FirstName, Country, Role) so the table doesn't include the `_rescued_data` column.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create new_hires_ctas from employees2.csv with format options and column selection
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>CREATE TABLE IF NOT EXISTS new_hires_ctas AS
# MAGIC SELECT ID, FirstName, Country, Role
# MAGIC FROM read_files(
# MAGIC   '/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees2.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Verify the table
# MAGIC
# MAGIC Query your new table. You should see 2 rows with exactly 4 columns (no `_rescued_data`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query new_hires_ctas
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM new_hires_ctas;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Compare column counts
# MAGIC
# MAGIC Run the query below to compare the number of columns in `new_hires_ctas` (created with explicit column selection) versus the `employees` table (created with `SELECT *` in Lesson 02). This shows the difference between selecting specific columns and including everything.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'new_hires_ctas' AS table_name, COUNT(*) AS column_count
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = my_schema AND table_name = 'new_hires_ctas'
# MAGIC UNION ALL
# MAGIC SELECT 'employees', COUNT(*)
# MAGIC FROM information_schema.columns
# MAGIC WHERE table_schema = my_schema AND table_name = 'employees';

# COMMAND ----------

# MAGIC %md
# MAGIC `new_hires_ctas` should have **4 columns** while `employees` has **5** (including `_rescued_data`). When you specify columns in your CTAS, you get exactly the schema you want.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="margin-top: 10px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;">Nice work! You just:</div>
# MAGIC     <ul style="padding-left: 20px; margin: 0;">
# MAGIC       <li>Previewed a file with explicit format options</li>
# MAGIC       <li>Created a clean table using CTAS with column selection</li>
# MAGIC       <li>Compared schemas to see the effect of selecting specific columns vs. <code>SELECT *</code></li>
# MAGIC     </ul>
# MAGIC     <div style="margin-top: 12px;">Lesson 10 explores COPY INTO, a method designed for loading data incrementally, where new files are picked up automatically and already-processed files are skipped.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
