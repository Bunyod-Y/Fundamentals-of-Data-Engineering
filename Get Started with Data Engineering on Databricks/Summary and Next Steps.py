# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Next Steps

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What You Built in This Course
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #1B5162; margin: 16px 0;">
# MAGIC <div style="font-size: 14pt; color: #0b2026; line-height: 1.7;">
# MAGIC
# MAGIC Over the course of these lessons, you went from an empty workspace to a fully automated data engineering pipeline. Specifically, you:
# MAGIC
# MAGIC - **Got oriented** in the Databricks workspace and Unity Catalog.
# MAGIC - **Created and queried** Unity Catalog tables using SQL.
# MAGIC - **Modified** table data with `INSERT`, `UPDATE`, and `DELETE`.
# MAGIC - **Used time travel** to query previous versions of a UC table.
# MAGIC - **Ingested data** with multiple methods: `CTAS`, the Upload UI, and `COPY INTO`.
# MAGIC - **Built a Medallion Architecture pipeline** that progressively refines data through Bronze, Silver, and Gold layers.
# MAGIC - **Automated a pipeline** with a Lakeflow Job that runs your transformation work end-to-end.
# MAGIC - **(Bonus)** Wrote a **Spark Declarative Pipeline** that lets the platform manage orchestration, dependencies, and quality for you.
# MAGIC
# MAGIC These are the same building blocks data engineers use in production Databricks workspaces every day.
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Continue Building With the Genie Code
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46; margin: 16px 0;">
# MAGIC <div style="font-size: 14pt; color: #0b2026; line-height: 1.7;">
# MAGIC
# MAGIC The **Genie Code** lives in every notebook and SQL editor. You can use it to write SQL or Python from natural language, explain code you don't understand, debug error messages, and get suggestions as you type.
# MAGIC
# MAGIC Two ways to invoke it:
# MAGIC
# MAGIC - Click the **Genie icon** in the right sidebar to open the chat panel.
# MAGIC - Press **Cmd + I** (Mac) or **Ctrl + I** (Windows) inside any cell to invoke the in-cell Genie Code prompt.
# MAGIC
# MAGIC Use it to keep building beyond this course — describe what you want to do in plain language and let Genie Code help draft the code.
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Additional Resources
# MAGIC
# MAGIC Explore these resources to keep learning and stay current with the Databricks Data Intelligence Platform. The links below come straight from the official Databricks documentation and product pages.
# MAGIC
# MAGIC #### Core Concepts
# MAGIC
# MAGIC - **Delta Lake** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/delta/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/delta/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/delta/)
# MAGIC
# MAGIC - **Unity Catalog** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/data-governance/unity-catalog/)
# MAGIC
# MAGIC #### Data Ingestion with Lakeflow Connect
# MAGIC
# MAGIC - **Lakeflow Connect** product page: [databricks.com/product/data-engineering/lakeflow-connect](https://www.databricks.com/product/data-engineering/lakeflow-connect)
# MAGIC
# MAGIC - **What is Lakeflow Connect?** (ingestion overview):
# MAGIC [AWS](https://docs.databricks.com/aws/en/ingestion/overview) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ingestion/overview)
# MAGIC
# MAGIC - **Auto Loader** for incremental cloud-storage file ingestion (part of Lakeflow Connect's standard connectors):
# MAGIC [AWS](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/)
# MAGIC
# MAGIC - **`COPY INTO`** SQL reference:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/delta-copy-into)
# MAGIC
# MAGIC #### Pipelines and Orchestration
# MAGIC
# MAGIC - **Lakeflow Jobs** (formerly Workflows) — orchestration for production workloads:
# MAGIC [AWS](https://docs.databricks.com/aws/en/jobs/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/jobs/)
# MAGIC
# MAGIC - **Apache Spark™ Declarative Pipelines** (formerly Delta Live Tables / DLT):
# MAGIC [AWS](https://docs.databricks.com/aws/en/ldp/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ldp/)
# MAGIC
# MAGIC - **What happened to DLT?** Migration guide for the rename:
# MAGIC [AWS](https://docs.databricks.com/aws/en/ldp/where-is-dlt) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/ldp/where-is-dlt) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/ldp/where-is-dlt)
# MAGIC
# MAGIC - **Medallion Architecture** overview:
# MAGIC [AWS](https://docs.databricks.com/aws/en/lakehouse/medallion) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/lakehouse/medallion)
# MAGIC
# MAGIC #### Productivity and AI Authoring
# MAGIC
# MAGIC - **Genie Code** — AI-assisted authoring inside notebooks and the SQL editor:
# MAGIC [AWS](https://docs.databricks.com/aws/en/genie-code/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/genie-code/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/genie-code/)
# MAGIC
# MAGIC - [Introducing Genie Code](https://www.databricks.com/blog/introducing-genie-code) (Databricks blog).
# MAGIC
# MAGIC #### Vision and Roadmap
# MAGIC
# MAGIC - [Introducing Databricks Lakeflow](https://www.databricks.com/blog/introducing-databricks-lakeflow) — the unified data engineering vision (blog).
# MAGIC
# MAGIC - [DAIS 2025: Lakeflow Connect — Smarter, Simpler File Ingestion With the Next Generation of Auto Loader](https://www.databricks.com/dataaisummit/session/lakeflow-connect-smarter-simpler-file-ingestion-next-generation-auto).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### B1. Continue Your Learning
# MAGIC
# MAGIC Recommended next courses to deepen your data engineering skills:
# MAGIC
# MAGIC - **[Data Ingestion with Lakeflow Connect](https://www.databricks.com/training/catalog/data-ingestion-with-lakeflow-connect-2968)** — Go deeper on connectors, streaming ingestion, and managed change data capture.
# MAGIC - **[Build Data Pipelines with Apache Spark™ Declarative Pipelines](https://www.databricks.com/training/catalog/build-data-pipelines-with-apache-spark-declarative-pipelines-1686)** — Build production-grade declarative pipelines with quality and orchestration handled for you.
# MAGIC - **[Deploy Workloads with Lakeflow Jobs](https://www.databricks.com/training/catalog/deploy-workloads-with-lakeflow-jobs-2978)** — Take Jobs from a single notebook task to multi-step, multi-environment production deployments.
# MAGIC
# MAGIC Browse the full catalog at **[Databricks Training and Certification](https://www.databricks.com/learn/training/home)**.
# MAGIC
# MAGIC ### B2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise with an official credential. The most common next step after this course is the **Databricks Certified Data Engineer Associate** exam.
# MAGIC
# MAGIC Visit **[Databricks Certification and Badging](https://www.databricks.com/learn/training/certification)** for current exam guides and registration.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
