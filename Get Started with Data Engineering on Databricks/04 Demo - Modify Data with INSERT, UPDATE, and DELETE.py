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
# MAGIC <div style="background: #1B5162; color: white; border-radius: 8px; padding: 28px 32px; text-align: center; position: relative;">
# MAGIC   <div style="font-size: 14pt; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; opacity: 0.85; margin-bottom: 8px;">Lesson 04</div>
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Modify Data with INSERT, UPDATE, and DELETE</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Add, change, and remove rows in a table using standard SQL operations.</div>
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
# MAGIC **Where we left off:** In Lesson 02, we created an `employees` UC table from a CSV file. It has 4 rows from the original data in the file.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <!-- LEARN: DML Operations on UC Tables -->
# MAGIC <!-- Template: 3-card-centered-text -->
# MAGIC
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="font-size: 20pt; font-weight: 700; color: #0b2026; margin-bottom: 6px;">UC Tables Support Full SQL Modifications</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; margin-bottom: 24px;">Unlike plain data files, UC tables let you add, change, and remove individual rows just like a traditional database. Every change is recorded as a new version of the table.</div>
# MAGIC
# MAGIC <div style="display: flex; justify-content: center; align-items: stretch; gap: 40px; flex-wrap: wrap;">
# MAGIC
# MAGIC   <!-- Card 1: INSERT -->
# MAGIC   <div style="width: 300px; min-height: 200px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); display: flex; flex-direction: column; justify-content: center; gap: 10px; padding: 24px; text-align: center; position: relative; box-sizing: border-box;">
# MAGIC     <div style="position: absolute; top: 0; left: 0; width: 100%; height: 8px; background: #4299E0; border-radius: 8px 8px 0 0;"></div>
# MAGIC     <div style="font-size: 18pt; font-weight: 600; color: #0b2026;">INSERT</div>
# MAGIC     <div style="font-size: 14pt; color: #5E7077;">Add new rows to a table. The table grows, and a new version is created.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 2: UPDATE -->
# MAGIC   <div style="width: 300px; min-height: 200px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); display: flex; flex-direction: column; justify-content: center; gap: 10px; padding: 24px; text-align: center; position: relative; box-sizing: border-box;">
# MAGIC     <div style="position: absolute; top: 0; left: 0; width: 100%; height: 8px; background: #00A972; border-radius: 8px 8px 0 0;"></div>
# MAGIC     <div style="font-size: 18pt; font-weight: 600; color: #0b2026;">UPDATE</div>
# MAGIC     <div style="font-size: 14pt; color: #5E7077;">Change values in existing rows that match a condition. A new version is created.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Card 3: DELETE -->
# MAGIC   <div style="width: 300px; min-height: 200px; background: #F9F7F4; border-radius: 8px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); display: flex; flex-direction: column; justify-content: center; gap: 10px; padding: 24px; text-align: center; position: relative; box-sizing: border-box;">
# MAGIC     <div style="position: absolute; top: 0; left: 0; width: 100%; height: 8px; background: #FF5F46; border-radius: 8px 8px 0 0;"></div>
# MAGIC     <div style="font-size: 18pt; font-weight: 600; color: #0b2026;">DELETE</div>
# MAGIC     <div style="font-size: 14pt; color: #5E7077;">Remove rows that match a condition. The data is gone from the current version, but still recoverable.</div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- Key point callout -->
# MAGIC <div style="margin-top: 20px; padding: 16px 20px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <strong>Key concept:</strong> Every INSERT, UPDATE, and DELETE creates a new version of the table. Delta Lake tracks all of these versions automatically, which means you can always see what your data looked like before a change (see Lesson 06 to learn how to explore these versions). 
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC **Why this matters for data engineering**
# MAGIC
# MAGIC - Traditional data lakes built on plain files (CSV, Parquet) don't support row-level changes. If you need to update a record, you have to rewrite the entire file. Delta Lake solves this by adding a transaction log on top of your data files.
# MAGIC - Each DML operation (INSERT, UPDATE, DELETE) is **atomic**: it either fully succeeds or fully rolls back. You won't end up with half-written changes.
# MAGIC - These operations use standard SQL syntax. If you've worked with databases like PostgreSQL, MySQL, or SQL Server, the commands are the same.
# MAGIC - Under the hood, Delta Lake doesn't modify existing files. It writes new data files and updates the transaction log to point to the latest version. This is what makes versioning and time travel possible.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore: Insert new rows
# MAGIC
# MAGIC Let's add two new employees to the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees (ID, FirstName, Country, Role) VALUES
# MAGIC   (5, 'Maria', 'Brazil', 'Data Engineer'),
# MAGIC   (6, 'Aiden', 'Australia', 'Data Analyst');

# COMMAND ----------

# MAGIC %md
# MAGIC Query the table to confirm the new rows were added.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC You should now see **6 rows** — the original 4 plus the 2 you just inserted.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore: Update an existing row
# MAGIC
# MAGIC Maria just moved to a new role. Let's update her record.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees
# MAGIC SET Role = 'Senior Data Engineer'
# MAGIC WHERE FirstName = 'Maria';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM employees 
# MAGIC WHERE FirstName = 'Maria';

# COMMAND ----------

# MAGIC %md
# MAGIC Maria's role changed from `Data Engineer` to `Senior Data Engineer`. The original value isn't lost. Delta Lake stored it in a previous version of the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore: Delete a row
# MAGIC
# MAGIC Aiden has left the company. Let's remove his record.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees
# MAGIC WHERE FirstName = 'Aiden';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC The table is back to **5 rows**. Aiden's record is gone from the current version, but because Delta Lake tracks every change, it's still in the table's version history.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore: Check the version history
# MAGIC
# MAGIC Every operation you just ran created a new version of the table. Let's verify.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC You should see multiple versions:
# MAGIC - **Version 0** — `CREATE TABLE` (from Lesson 02)
# MAGIC - **Version 1** — `WRITE` (the INSERT)
# MAGIC - **Version 2** — `UPDATE`
# MAGIC - **Version 4** — `DELETE`
# MAGIC
# MAGIC You may also see versions identified as **OPTIMIZE** which are automatically added by the platform to ensure tables run efficiently.
# MAGIC
# MAGIC Each version records who made the change, when, and what type of operation it was. You'll learn how to query these older versions in Lesson 06.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <!-- Micro-win summary -->
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="margin-top: 10px; padding: 18px 24px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <div style="font-weight: 700; margin-bottom: 8px;">What you just did:</div>
# MAGIC     <ul style="padding-left: 20px; margin: 0;">
# MAGIC       <li>Added rows with <code>INSERT INTO</code></li>
# MAGIC       <li>Changed a value with <code>UPDATE ... SET ... WHERE</code></li>
# MAGIC       <li>Removed a row with <code>DELETE FROM ... WHERE</code></li>
# MAGIC       <li>Verified all changes are tracked with <code>DESCRIBE HISTORY</code></li>
# MAGIC     </ul>
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
