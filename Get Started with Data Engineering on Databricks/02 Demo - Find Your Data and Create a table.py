# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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
# MAGIC   <div style="font-size: 14pt; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; opacity: 0.85; margin-bottom: 8px;">Lesson 02</div>
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Find Your Data and Create a table</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Navigate the catalog hierarchy, locate a raw data file, preview its contents, and create a table from it.</div>
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
# MAGIC Run the cell below to configure your environment for this lesson. It will ensure you have the appropriate catalog, schema, and data set up to complete the following work. 

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <!-- LEARN: Unity Catalog Hierarchy -->
# MAGIC <!-- Template: vertical-layered-stack (adapted to 3 layers) -->
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="font-size: 20pt; font-weight: 700; color: #0b2026; margin-bottom: 6px;">How Databricks Organizes Your Data</div>
# MAGIC <div style="font-size: 14pt; color: #5A6F77; margin-bottom: 24px;">Everything in Databricks lives in a three-level hierarchy within Unity Catalog. Think of it like folders on your computer, except these folders also control who can see and use the data inside them.</div>
# MAGIC
# MAGIC <div style="display: flex; align-items: stretch; gap: 16px;">
# MAGIC
# MAGIC <!-- Left arrow label -->
# MAGIC <div style="
# MAGIC     writing-mode: vertical-lr;
# MAGIC     transform: rotate(180deg);
# MAGIC     text-align: center;
# MAGIC     font-weight: 700;
# MAGIC     font-size: 14pt;
# MAGIC     color: #618794;
# MAGIC     padding: 0 6px;
# MAGIC     display: flex;
# MAGIC     justify-content: flex-end;
# MAGIC ">
# MAGIC &larr; BROAD TO SPECIFIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- Stacked layers -->
# MAGIC <div style="flex: 1; display: flex; flex-direction: column; gap: 6px;">
# MAGIC
# MAGIC <!-- Catalog -->
# MAGIC <div style="background: #1C3037; color: white; border-radius: 8px 8px 4px 4px; padding: 22px 24px; text-align: center;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700;">Catalog</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 6px; opacity: 0.9;">The top-level container. Groups related schemas together.</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Schema -->
# MAGIC <div style="background: #2574B5; color: white; border-radius: 4px; padding: 18px 24px; text-align: center;">
# MAGIC   <div style="font-size: 16pt; font-weight: 700;">Schema</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 6px; opacity: 0.9;">A collection of related tables, views, and volumes. Organizes data by project or domain.</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Tables & Volumes -->
# MAGIC <div style="background: #02A36F; color: white; border-radius: 4px 4px 8px 8px; padding: 18px 24px; text-align: center;">
# MAGIC   <div style="font-size: 16pt; font-weight: 700;">Tables &amp; Volumes</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 6px; opacity: 0.9;">Tables hold structured data (rows and columns). Volumes hold raw files (CSVs, JSON, images).</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- Key point callout -->
# MAGIC <div style="margin-top: 20px; padding: 16px 20px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px;">
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.6;">
# MAGIC     <strong>Your first job as a data engineer:</strong> Find the raw data files in a volume, then turn them into reliable UC tables that everyone on your team can query.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC **Catalog → Schema → Tables/Volumes** is the path you'll use every time you work with data in Databricks. This is the three-level namespace that makes Unity Catalog stronger at governance and security.
# MAGIC
# MAGIC - **Catalogs** are the broadest container. In most organizations, you'll have separate catalogs for development, staging, and production, or organized by business unit. In this course, you're working in a pre-configured catalog.
# MAGIC - **Schemas** (also called databases) group related objects together. A schema might hold all the tables for a specific project, team, or data domain. In Databricks, Schemas hold more than just data tables; they also contain volumes, models, and functions.
# MAGIC - **Tables** store structured, queryable data using an open table format. Once data is in a table, anyone with permission can query it using SQL.
# MAGIC - **Volumes** store raw files before they become tables. Think of a volume as a managed folder where you land CSV files, JSON exports, or other data files that need to be processed.
# MAGIC
# MAGIC This hierarchy is managed by **Unity Catalog**, which also handles permissions. When you create a table inside a schema, Unity Catalog automatically tracks who created it, when, and who has access.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Explore: Find your data in the catalog
# MAGIC
# MAGIC Let's start by confirming where we are in the catalog hierarchy, then find the raw data file waiting for us.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1:** Run the cell below to confirm your current catalog and schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2:** Let's see what's in our schema. Run the cells below to check for any existing tables and volumes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3:** List the files inside the volume. You should see CSV files including `employees.csv` and `employees2.csv`.

# COMMAND ----------

spark.sql(f"LIST '/Volumes/{my_catalog}/{my_schema}/myfiles/'").display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 4:** Preview the CSV data without creating a table. The `read_files` function lets you peek at raw file contents directly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM read_files('/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC You should see 4 rows with columns: **ID**, **FirstName**, **Country**, and **Role**. This is raw file data, not stored as a table yet.
# MAGIC
# MAGIC You may also notice a column called **`_rescued_data`** with `null` values. Databricks automatically adds this column when reading files. If any rows don't conform to the inferred schema (wrong data types, extra fields, malformed records), those values are captured here instead of being lost or breaking the read. When all your data is clean, it stays `null`. This is one of the ways Databricks keeps your data safe during ingestion.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore: Create a table from the CSV
# MAGIC
# MAGIC Now let's turn that raw CSV into a proper table. The `CREATE TABLE AS SELECT` (CTAS) pattern reads the file and writes the result as a new table in one step. By default Databricks creates tables in Delta format.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS employees
# MAGIC AS 
# MAGIC SELECT * 
# MAGIC FROM read_files('/Volumes/' || my_catalog || '/' || my_schema || '/myfiles/employees.csv');

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5:** Verify the table was created. Run the cell below to query it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC Same 4 rows, but now they're stored as a **table** with a transaction log tracking every change. You can also verify this in **Catalog Explorer**: click **Catalog** in the left sidebar, expand your catalog and schema, and you should see the `employees` table listed under **Tables**. 
# MAGIC
# MAGIC The transaction log tracking every change is what enables features like time travel, schema enforcement, and ACID guarantees that you'll explore in later lessons.

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
# MAGIC       <li>Navigated the Unity Catalog hierarchy (catalog → schema → volume)</li>
# MAGIC       <li>Previewed raw CSV data using <code>read_files</code></li>
# MAGIC       <li>Created your first UC table using <code>CREATE TABLE AS SELECT</code></li>
# MAGIC       <li>Verified the table in both SQL and Catalog Explorer</li>
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