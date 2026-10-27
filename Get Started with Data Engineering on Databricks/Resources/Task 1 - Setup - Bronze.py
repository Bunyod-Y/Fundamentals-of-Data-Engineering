# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 1: Setup and Bronze Layer
# MAGIC This notebook is used as a task in a LakeFlow Job. It sets up the catalog/schema and creates the Bronze table.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1

# COMMAND ----------

volume_path = f"/Volumes/{my_catalog}/{my_schema}/myfiles"

try:
    files = [f.name for f in dbutils.fs.ls(volume_path)]
except:
    files = []

if "employees.csv" not in files:
    csv_data = """ID,FirstName,Country,Role
1,Kristi,United States,Data Engineer
2,Sophia,Germany,Data Analyst
3,Peter,Canada,ML Engineer
4,Zebi,India,Data Scientist"""
    dbutils.fs.put(f"{volume_path}/employees.csv", csv_data, overwrite=True)

if "employees2.csv" not in files:
    csv_data2 = """ID,FirstName,Country,Role
5,Maria,Brazil,Data Engineer
6,Aiden,Australia,Data Analyst"""
    dbutils.fs.put(f"{volume_path}/employees2.csv", csv_data2, overwrite=True)

print("Environment ready")

# COMMAND ----------

# Create Bronze table and load data
spark.sql("""
    CREATE TABLE IF NOT EXISTS current_employees_bronze_job (
        ID INT,
        FirstName STRING,
        Country STRING,
        Role STRING
    )
""")

result = spark.sql(f"""
    COPY INTO current_employees_bronze_job
    FROM '/Volumes/{my_catalog}/{my_schema}/myfiles/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
""")
result.display()

print("Bronze table loaded")

# COMMAND ----------

# Verify
display(spark.sql("SELECT * FROM current_employees_bronze_job"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
