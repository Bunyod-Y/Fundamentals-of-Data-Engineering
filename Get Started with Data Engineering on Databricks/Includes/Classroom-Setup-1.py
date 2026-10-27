# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

#####################
# CREATE USER OR SET USER CATALOG
#####################
my_catalog = build_user_catalog(catalog_forced=None)

# COMMAND ----------

## Set the user's default catalog (learn_get_started_de_xxx)
_ = spark.sql(f'USE CATALOG {my_catalog}')

# COMMAND ----------

## Creates a SQL variable called my_catalog
_ = spark.sql(f'DECLARE OR REPLACE VARIABLE my_catalog STRING')
_ = spark.sql(f'SET VAR my_catalog = "{my_catalog}"')

# Make the user name safe if it's not in Vocareum
my_schema = 'get_started_de'

# COMMAND ----------

## Creates a SQL variable called my_schema
_ = spark.sql(f'DECLARE OR REPLACE VARIABLE my_schema STRING')
_ = spark.sql(f'SET VAR my_schema = "{my_schema}"')

# COMMAND ----------

create_schemas(my_catalog, [my_schema])

# COMMAND ----------

_ = spark.sql(f'USE SCHEMA {my_schema}')

# COMMAND ----------

create_volume = f'{my_catalog}.{my_schema}.myfiles'
spark.sql(f'CREATE VOLUME IF NOT EXISTS {create_volume}')

# COMMAND ----------

from pathlib import Path

def get_data_folder():
    p = Path.cwd()
    for parent in [p] + list(p.parents):
        data_path = parent / "Includes" / "data"
        if data_path.exists():
            return str(data_path)
    raise ValueError("Includes/data folder not found")

data_folder_path = get_data_folder()

# COMMAND ----------

copy_workspace_files_to_volume(data_folder_path, f'/Volumes/{my_catalog}/{my_schema}/myfiles', 2)

# COMMAND ----------

## (Optional - Display user's catalog, schema, etc)
display_config_values(
    [
        ('Your Catalog', my_catalog),
        ('Your Schema', my_schema),
    ]
)

# COMMAND ----------

setup_complete_msg()

# COMMAND ----------

# # -----------------------------------------------------------------------------
# # CHECK REQUIRED TABLES
# #
# # Stored procedure that verifies a given list of tables exists in the target
# # catalog.schema. If any tables are missing, raises an error with a helpful
# # message pointing the learner to the required setup notebook.
# #
# # Adapted from Peter Styliadis's enablement-medallion-labs/_starter-template
# # pattern. Reusable across courses — just change the required_tables list.
# #
# # Registered HERE (in Classroom-Setup-1) rather than Classroom-Setup-Common
# # because it must be created AFTER `USE CATALOG <my_catalog>` is set. The
# # procedure is registered in the user's catalog so they have permissions
# # to call it from any lesson.
# #
# # Usage from a SQL cell in any lesson notebook:
# #   CALL check_required_tables(
# #     target_catalog  => my_catalog,
# #     target_schema   => my_schema,
# #     required_tables => array('table_a', 'table_b', 'table_c')
# #   );
# # -----------------------------------------------------------------------------

# spark.sql("""
# CREATE OR REPLACE PROCEDURE check_required_tables(
#   IN target_catalog  STRING,
#   IN target_schema   STRING,
#   IN required_tables ARRAY<STRING>
# )
#   LANGUAGE SQL
#   SQL SECURITY INVOKER
#   COMMENT 'Verifies that the given array of tables exists in target_catalog.target_schema. Raises an error listing any that are missing.'
# AS
# BEGIN
#   DECLARE missing ARRAY<STRING>;

#   SET missing = array_except(
#     required_tables,
#     (SELECT collect_list(table_name)
#      FROM IDENTIFIER(target_catalog || '.information_schema.tables')
#      WHERE table_schema = target_schema)
#   );

#   IF size(missing) > 0 THEN
#     SELECT raise_error(CONCAT(
#       'Required tables missing in ',
#       target_catalog, '.', target_schema, ': ',
#       array_join(array_sort(missing), ', '),
#       '. Please run the Classroom-Setup-1 notebook in the Includes folder before continuing.'
#     ));
#   ELSE
#     SELECT CONCAT(
#       '✅ All required tables found in ',
#       target_catalog, '.', target_schema,
#       ' (', size(required_tables), ' tables).'
#     ) AS `Status`;
#   END IF;
# END;
# """)
# print('✅ Procedure check_required_tables() registered in', my_catalog)

