# Databricks notebook source
# MAGIC %md
# MAGIC #This notebook will perform delta operations : FSCK Repair, Describe History, Optemize, Vaccum

# COMMAND ----------

dbutils.widgets.text("delta_path", "", label="01. Enter a delta paths/table")
delta_path = dbutils.widgets.get("delta_path")

dbutils.widgets.dropdown("delta_type", "delta path",choices = ["delta path","delta table"], label="02. What you is provided")
delta_type = dbutils.widgets.get("delta_type")

dbutils.widgets.dropdown("operations", "YES",choices = ["YES","NO"], label="03. Perform dry run")
operations = dbutils.widgets.get("operations")

if operations == "YES":
    operation = "DRY RUN"
elif operations == "NO":
    operation = ""

dbutils.widgets.text("Retaintion", "168 hours", label="04. Retaintion hour in case of Vaccum")
Retaintion_Period = dbutils.widgets.get("Retaintion")

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------



if delta_type  == "delta path":
    FSCK_Query = f"""FSCK REPAIR TABLE delta.`{delta_path}` {operation} """
    vacuum_Query = f""" VACUUM delta.`{delta_path}` RETAIN {Retaintion_Period} {operation} """
    optimize_Query = f""" OPTIMIZE delta.`{delta_path}` """
    history_Query = f""" DESCRIBE HISTORY delta.`{delta_path}` """

elif delta_type  == "delta table":
    FSCK_Query = f"""FSCK REPAIR TABLE {delta_path} {operation} """
    vacuum_Query = f""" VACUUM {delta_path} RETAIN {Retaintion_Period} {operation} """
    optimize_Query = f""" OPTIMIZE {delta_path} """
    history_Query = f""" DESCRIBE HISTORY {delta_path} """
    

# COMMAND ----------

spark.sql(FSCK_Query).show()

# COMMAND ----------

spark.sql(vacuum_Query).show()

# COMMAND ----------

spark.sql(optimize_Query).show()

# COMMAND ----------

spark.sql(history_Query).show()
