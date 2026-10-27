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
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Find and Create Tables</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Apply what you learned in Lesson 02 to preview a new data file and create a table from it on your own.</div>
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
# MAGIC In Lesson 02, you found `employees.csv` in your volume and created a table from it. But there was a second file in that volume: **`employees2.csv`**.
# MAGIC
# MAGIC In this practice, you will:
# MAGIC 1. List the files in the volume to confirm `employees2.csv` is there
# MAGIC 2. Preview the contents of `employees2.csv`
# MAGIC 3. Create a new table called `new_employees` from that file
# MAGIC 4. Query your new table to verify it worked
# MAGIC
# MAGIC Replace the `<FILL_IN>` placeholders below with the correct SQL. If you get stuck, expand the hint under each task.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: List the files in the volume
# MAGIC
# MAGIC Confirm that `employees2.csv` exists alongside `employees.csv`.

# COMMAND ----------

# TODO: List the files in the myfiles volume
spark.sql(f"LIST '<FILL_IN>'").display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC The volume path is `/Volumes/{my_catalog}/{my_schema}/myfiles/`
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Preview the new CSV file
# MAGIC
# MAGIC Use `read_files` to peek at the contents of `employees2.csv` before creating a table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Preview the contents of employees2.csv
# MAGIC SELECT * FROM read_files('<FILL_IN>');

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC The full path to the file is `/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees2.csv`
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **2 rows** with the same columns as the original file: ID, FirstName, Country, and Role. These are new employees that haven't been added to a table yet.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Create a UC table from the new file
# MAGIC
# MAGIC Use `CREATE TABLE AS SELECT` to create a table called **`new_employees`** from `employees2.csv`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create a new UC table called new_employees from employees2.csv
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC The pattern is the same as Lesson 02:
# MAGIC <pre><code>CREATE TABLE IF NOT EXISTS new_employees
# MAGIC AS SELECT * FROM read_files('/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees2.csv');</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Verify your new table
# MAGIC
# MAGIC Query the `new_employees` table to confirm it has the expected data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query the new_employees table
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <details>
# MAGIC <summary>Hint</summary>
# MAGIC
# MAGIC <pre><code>SELECT * FROM new_employees;</code></pre>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Check your schema
# MAGIC
# MAGIC Run `SHOW TABLES` to see all the tables in your schema. You should now see both `employees` (from Lesson 02) and `new_employees` (from this practice).

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="margin-top: 10px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;">Nice work! You just:</div>
# MAGIC     <ul style="padding-left: 20px; margin: 0;">
# MAGIC       <li>Found a second data file in your volume</li>
# MAGIC       <li>Previewed it with <code>read_files</code> before committing to a table</li>
# MAGIC       <li>Created a new table using <code>CREATE TABLE AS SELECT</code></li>
# MAGIC       <li>Verified the table exists alongside your Lesson 02 table</li>
# MAGIC     </ul>
# MAGIC     <div style="margin-top: 12px;">You now have two tables in your schema, and both were created from raw CSV files using the same pattern.</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
