# Databricks notebook source
# MAGIC %md
# MAGIC #This notebook will check whether given table path is delta table or not

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("delta_path", "", label="01. Enter a delta paths")
delta_path = dbutils.widgets.get("delta_path")

# Split the input string into a list of paths
delta_lists = delta_path.split(",")

print(delta_lists)

# dbutils.widgets.removeAll()

# COMMAND ----------

delta_paths = []
non_delta_paths = []

# COMMAND ----------

# Define the delta check function
def delta_check(path):
    if DeltaTable.isDeltaTable(spark, path):
        delta_paths.append(path)
        return path, "Delta table"
    else:
        non_delta_paths.append(path)
        return path, "Non-Delta table"               

# COMMAND ----------

# Print the results

try:
    results = list(map(delta_check, delta_lists))

    for path, label in results:
        print(f"{path} is a {label}")

except Exception as e:
    raise str(e)

# COMMAND ----------

print(f"All delta paths: {delta_paths}")

if len(non_delta_paths):
    print(f"All non-delta paths: {non_delta_paths}")

