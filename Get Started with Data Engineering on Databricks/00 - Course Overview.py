# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Started with Databricks for Data Engineering
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This course introduces the foundational skills needed to perform a basic data engineering workflow on the **Databricks Data Intelligence Platform**. You will explore the workspace, work with Unity Catalog, and learn the day-to-day building blocks a data engineer uses on Databricks.
# MAGIC
# MAGIC The course follows a hands-on path. You start by getting oriented in the workspace, then work through paired Demo and Lab notebooks for each topic. Demos walk you through a concept with the instructor or a guided notebook; Labs ask you to apply what you just saw on your own.
# MAGIC
# MAGIC The content covers:
# MAGIC
# MAGIC - The Databricks Data Intelligence Platform and how the Databricks workspace, Unity Catalog, and notebooks fit together.
# MAGIC - Creating and managing **Delta Lake** tables.
# MAGIC - Modifying data with `INSERT`, `UPDATE`, and `DELETE`.
# MAGIC - Exploring **version history** and **time travel** on UC tables.
# MAGIC - **Ingesting** data with **Lakeflow Connect** options: `CTAS`, the Upload UI, and `COPY INTO`.
# MAGIC - Building a **Medallion Architecture** pipeline that transforms data through Bronze, Silver, and Gold layers.
# MAGIC - **Automating** pipelines with **Lakeflow Jobs**.
# MAGIC - **(Bonus)** Building a declarative pipeline with **Apache Spark™ Declarative Pipelines**.
# MAGIC
# MAGIC By the end of the course, you will be comfortable creating Delta Lake tables, ingesting data into them, transforming that data through a Medallion pipeline, and automating the whole thing as a scheduled job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Terminal Objectives
# MAGIC
# MAGIC By the end of this course, you will be able to:
# MAGIC
# MAGIC - **Describe** how the Databricks Data Intelligence Platform unifies data and AI on a single governed foundation.
# MAGIC - **Navigate** the Databricks workspace, Unity Catalog, and notebooks to locate and work with data.
# MAGIC - **Create** and **modify** Delta Lake tables using SQL, and **query** previous versions using time travel.
# MAGIC - **Ingest** data into Delta Lake using multiple methods, including `CTAS`, the Upload UI, and `COPY INTO`.
# MAGIC - **Build** a Medallion Architecture pipeline that transforms data through Bronze, Silver, and Gold layers.
# MAGIC - **Automate** a data pipeline using a Lakeflow Job.
# MAGIC - **(Bonus) Build** a declarative pipeline with **Apache Spark™ Declarative Pipelines**.
# MAGIC
# MAGIC ##### This Academy Edition is based on course version 4.0.3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - **Basic familiarity with cloud data platforms.** You understand what a database, table, and query are at a conceptual level.
# MAGIC - **Working knowledge of SQL.** You can read and write `SELECT` statements with `WHERE`, `GROUP BY`, and aggregates, and you understand `INSERT`, `UPDATE`, and `DELETE`.
# MAGIC - **General awareness of data engineering concepts** like data ingestion, transformation, and pipelines is helpful but not required — the course covers the basics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Workspace Setup Information
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="color: #c62828; font-size: 1.1em;">Compute & Permissions</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">This content assumes you have:</p>
# MAGIC
# MAGIC - **CAN USE access on at least one Serverless compute resource.**
# MAGIC - **Unity Catalog enabled** in your workspace.
# MAGIC - **Permission to create tables** in your personal catalog or schema.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Your Course Catalog
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC
# MAGIC #### Catalog Information
# MAGIC
# MAGIC - The setup at the top of each Demo and Lab **creates a Unity Catalog catalog and schema for you** the first time it runs, then reuses them.
# MAGIC - The **Create Catalog** permission is required.
# MAGIC - The catalog name is derived from your Databricks username and follows this pattern: **`learn_get_started_de_YOUR_USERNAME`**, where `YOUR_USERNAME` is the part of your sign-in email before `@`, lowercased, with each run of characters other than letters and digits replaced by `_`, and limited to 19 characters.
# MAGIC - The course schema (**get_started_de**) and a **myfiles** volume holding the course CSV files are created inside that catalog by the setup script.
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #ff9800; background: #fff3e0; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">Troubleshooting Setup — Your Workspace Can't Create Catalogs</strong>
# MAGIC <details>
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC If you do not have permission to create a new catalog but already have one available, you can explicitly specify an existing catalog by setting the variable in the setup notebook:
# MAGIC
# MAGIC `my_catalog = build_user_catalog(catalog_forced='your_catalog_name')`
# MAGIC
# MAGIC The `build_user_catalog` function is defined in `./Includes/Classroom-Setup-Common`.
# MAGIC
# MAGIC </div>
# MAGIC </details>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Do Not Run in Production Environments</strong>
# MAGIC
# MAGIC - Only run this course in **development or sandbox workspaces**.
# MAGIC - Do not run in production environments. The setup scripts create catalogs, schemas, volumes, and tables in your workspace.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Course Structure
# MAGIC
# MAGIC The course is organized as paired **Demo** and **Lab** notebooks. Demos walk you through each concept; Labs ask you to apply it on your own.
# MAGIC
# MAGIC | Notebook | Type |
# MAGIC |---|---|
# MAGIC | Course Overview *(this notebook)* | Reference |
# MAGIC | Databricks Workspace Walkthrough | Demo |
# MAGIC | Find Your Data and Create a table | Demo |
# MAGIC | Find and Create Tables | Lab |
# MAGIC | Modify Data with INSERT, UPDATE, and DELETE | Demo |
# MAGIC | Modify Data in a UC Table | Lab |
# MAGIC | Explore Version History and Time Travel | Demo |
# MAGIC | Use Version History and Time Travel | Lab |
# MAGIC | Ingest Data with CTAS and the Upload UI | Demo |
# MAGIC | Ingest Data with CTAS | Lab |
# MAGIC | Load Data Incrementally with COPY INTO | Demo |
# MAGIC | Load Data Incrementally | Lab |
# MAGIC | Build a Medallion Architecture Pipeline | Demo |
# MAGIC | Build a Complete Medallion Pipeline | Lab |
# MAGIC | Automate Your Pipeline with a Lakeflow Job | Demo |
# MAGIC | Automate a Pipeline Job | Lab |
# MAGIC | Build a Declarative Pipeline with Spark Declarative Pipelines | Demo *(Bonus)* |
# MAGIC | Explore Your Declarative Pipeline Results | Lab *(Bonus)* |
# MAGIC | Summary and Next Steps | Reference |
# MAGIC
# MAGIC The `Resources` folder contains supporting notebooks for the Lakeflow Job lesson (Task 1 and Task 2). The `Includes` folder contains the shared setup scripts and the course data files. **You do not need to run anything in those folders directly** — the lesson notebooks reference them as needed.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
