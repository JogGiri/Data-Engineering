# Databricks notebook source
# dbutils.widgets.text('param','','param')
param = dbutils.widgets.get('param')
# param=1

# COMMAND ----------

print(f'hello{param}')

# COMMAND ----------

# dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson() 
# dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().toString()
# dbutils.notebook.getContext.currentRunId
# dbutils.notebook.exit(s"${notebook} #${dbutils.notebook.getContext.currentRunId} : success")

# import json 
# notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
# try: 
#     #The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...) 
#     jobId = notebook_info["tags"]["jobId"] 
# except: 
#     jobId = -1 
# dbutils.notebook.exit(jobId) 

# jobid = dbutils.notebook.run(...) 
# print(jobid)

# COMMAND ----------

# spark.conf.get("spark.databricks.io.cache.enabled")