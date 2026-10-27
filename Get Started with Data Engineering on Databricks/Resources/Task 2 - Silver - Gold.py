# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 2: Silver and Gold Layers
# MAGIC This notebook is used as a task in a LakeFlow Job. It transforms Bronze into Silver and aggregates into Gold.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1

# COMMAND ----------

# Create Silver table from Bronze
spark.sql("""
    CREATE OR REPLACE TABLE current_employees_silver_job AS
    SELECT
        ID,
        FirstName,
        Country,
        UPPER(Role) AS Role,
        current_timestamp() AS processed_timestamp,
        current_date() AS processed_date
    FROM current_employees_bronze_job
""")

print("Silver table created")
display(spark.sql("SELECT * FROM current_employees_silver_job"))

# COMMAND ----------

# Create Gold table from Silver
spark.sql("""
    CREATE TABLE IF NOT EXISTS total_roles_gold_job (
        Role STRING,
        TotalEmployees INT
    )
""")

spark.sql("""
    INSERT OVERWRITE total_roles_gold_job
    SELECT Role, COUNT(*) AS TotalEmployees
    FROM current_employees_silver_job
    GROUP BY Role
""")

print("Gold table created")
display(spark.sql("SELECT * FROM total_roles_gold_job"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
