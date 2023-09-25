# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

# COMMAND ----------

timeOut = 180000
notebook_path = './test-run'
arguments = ['1','2','3','4','5','6','7']
NumberOfParalellThreads = len(arguments)
param=tuple(arguments)
param

RunParallel = lambda a: dbutils.notebook.run(notebook_path, timeOut, {"param": a})

# COMMAND ----------

# pool = ThreadPoolExecutor(NumberOfParalellThreads)
results = []
# with ThreadPoolExecutor(NumberOfParalellThreads) as pool:
with ThreadPoolExecutor() as pool:
  results.extend(pool.map(RunParallel, param))

# COMMAND ----------

results