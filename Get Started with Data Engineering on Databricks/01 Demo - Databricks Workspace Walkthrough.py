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
# MAGIC   <div style="font-size: 14pt; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; opacity: 0.85; margin-bottom: 8px;">Lesson 01</div>
# MAGIC   <div style="font-size: 24pt; font-weight: 700; line-height: 1.3;">Databricks Workspace Walkthrough</div>
# MAGIC   <div style="font-size: 14pt; margin-top: 12px; opacity: 0.9;">Navigate the workspace.</div>
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
# MAGIC In this short walkthrough, you'll get oriented in your Databricks workspace and run your first cells. By the end, you'll be ready to dive into the lessons that follow.
# MAGIC
# MAGIC **What you'll do:**
# MAGIC 1. Confirm your compute is ready and run the classroom setup
# MAGIC 1. Run your first cell
# MAGIC 1. Tour the workspace sidebar
# MAGIC 1. Find data in the catalog
# MAGIC 1. Try notebook basics: cells, languages, and Genie Code
# MAGIC
# MAGIC > **Estimated time:** ~10 minutes
# MAGIC >
# MAGIC > **Note:** This is a follow-along walkthrough. Read the steps and try them in your own workspace as we go.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Setup
# MAGIC #### Confirm your compute is ready
# MAGIC
# MAGIC Look at the **top-right corner of this notebook** to confirm your compute is selected.
# MAGIC
# MAGIC - You should see **Serverless** selected by default.
# MAGIC - If a different cluster is selected, click the dropdown and choose **Serverless**.
# MAGIC
# MAGIC With Databricks serverless compute, you don't need to start or wait for a cluster, it attaches automatically when you run a cell.
# MAGIC
# MAGIC #### Run the classroom setup
# MAGIC
# MAGIC The cell below runs the classroom setup script. This sets up your personal catalog and schema for the course. Click the **Run** button at the top-left of the cell (or press **Shift + Enter** while the cell is selected).

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Confirm your environment
# MAGIC
# MAGIC Run the cell below to print your username, catalog, and schema. You'll use these throughout the course.

# COMMAND ----------

print(f"Username: {spark.sql('SELECT current_user()').first()[0]}")
print(f"Catalog:  {my_catalog}")
print(f"Schema:   {my_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Run your first cell
# MAGIC
# MAGIC Now run a SQL cell to confirm everything is working. The cell below greets you and shows the current date and time.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Hello Databricks Learner!' AS greeting,
# MAGIC   current_date() AS today,
# MAGIC   current_timestamp() AS right_now;

# COMMAND ----------

# MAGIC %md
# MAGIC You should see a one-row result with your greeting, today's date, and the current time. **You just ran your first SQL query in Databricks.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Tour the workspace sidebar
# MAGIC
# MAGIC The left sidebar is your home base in Databricks. Hover over the **hamburger icon** (☰) at the top-left to expand it.
# MAGIC
# MAGIC The items you'll use most often in this course:
# MAGIC
# MAGIC | Sidebar item | What it's for |
# MAGIC |---|---|
# MAGIC | **+ New** | Create a notebook, query, dashboard, job, or pipeline |
# MAGIC | **Workspace** | Browse and organize your own notebooks and folders |
# MAGIC | **Catalog** | Browse governed data — catalogs, schemas, tables, and volumes |
# MAGIC | **Compute** | View and configure compute resources |
# MAGIC | **Search** (top bar) | Find any notebook, table, or asset by name |
# MAGIC
# MAGIC > 💡 **Try it:** Click the **Databricks logo** in the top-left to go to the workspace homepage. You'll see Suggested, Favorites, and What's New tabs. Then click your browser's back button (or use Recents in the sidebar) to return to this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Find data in the catalog
# MAGIC
# MAGIC All governed data in Databricks lives in **Unity Catalog**, organized in a three-level hierarchy: **catalog → schema → table**. Let's find a sample table that's available in every Databricks workspace.
# MAGIC
# MAGIC #### Browse to the table
# MAGIC
# MAGIC 1. In the left sidebar, click **Catalog** to open Catalog Explorer.
# MAGIC 1. Find and expand the **`samples`** catalog.
# MAGIC 1. Expand the **`nyctaxi`** schema.
# MAGIC 1. Click the **`trips`** table.
# MAGIC 1. On the right panel, look at the **Overview** tab to see the column list, then click the **Sample Data** tab to preview a few rows.
# MAGIC
# MAGIC You're looking at NYC taxi trip data: pickup and dropoff times, fare amounts, distances, and zip codes.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query that same table from this notebook
# MAGIC
# MAGIC Run the cell below to query the table you just previewed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM samples.nyctaxi.trips
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC You're now seeing the same data you previewed in Catalog Explorer, just queried directly in the notebook. This is the loop you'll use throughout the course: **find data in the catalog, then work with it in a notebook.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Notebook basics: cells, languages, and Genie Code
# MAGIC Databricks notebooks support multiple languages in the same notebook. The cell below is Python. It counts the rows in the sample table and prints the result.

# COMMAND ----------

trip_count = spark.sql("SELECT COUNT(*) AS total FROM samples.nyctaxi.trips").collect()[0]["total"]
print(f"There are {trip_count:,} trips in the sample dataset.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing cell type
# MAGIC
# MAGIC Try adding a new cell to this notebook:
# MAGIC
# MAGIC 1. Hover between this cell and the next one. A **+ Code** button appears. Click it to add a new cell below.
# MAGIC 1. In the new cell, set the language to **SQL** using the dropdown at the top-right of the cell (or type `%sql` on the first line).
# MAGIC 1. Type: `SELECT MAX(fare_amount) AS biggest_fare FROM samples.nyctaxi.trips`
# MAGIC 1. Press **Shift + Enter** to run it.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Try Genie Code
# MAGIC
# MAGIC Genie Code can write, explain, and debug code for you. There are two ways to open it:
# MAGIC
# MAGIC - Click the **Genie icon** in the right sidebar to open the chat panel.
# MAGIC - Press **Cmd + I** (Mac) or **Ctrl + I** (Windows) inside a cell to invoke the in-cell Genie Code prompt.
# MAGIC
# MAGIC Try asking it: *"Write a SQL query that counts trips by pickup zip code from samples.nyctaxi.trips, sorted by trip count descending."*

# COMMAND ----------

# MAGIC %md
# MAGIC ### What's next
# MAGIC
# MAGIC You're now oriented in Databricks. You've:
# MAGIC
# MAGIC - Confirmed your compute is ready and run the classroom setup
# MAGIC - Run SQL and Python cells in a notebook
# MAGIC - Browsed the catalog and queried a sample table
# MAGIC - Practiced adding cells, switching languages, and using Genie Code

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>