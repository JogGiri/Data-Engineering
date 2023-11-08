# Databricks notebook source
# MAGIC %md
# MAGIC * This Notebook will delete jobs (which jobis are provided)
# MAGIC * You/cluster must have Owner role or Admin role 
# MAGIC * Otherwise job can have job cluster with service account which have required access
# MAGIC

# COMMAND ----------

import os
import requests
import json
import pandas as pd
from pyspark.sql.functions import col,split,when

# COMMAND ----------

dbutils.widgets.text("jobIds","","01.Please enter job Ids:")
jobIds = dbutils.widgets.get("jobIds")
job_ids_list = jobIds.split(',')

print(job_ids_list)

# params = {'job_id': job_ids_list }
# print(params)

# COMMAND ----------

TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
env_url = os.environ.get("ENV_URL")[:-1]
api_url = env_url + '/api/2.1/'
print("Databricks Workspace Base URL:", env_url)
print(api_url)
delete_jobs_url = f'{api_url}jobs/delete'
HEADERS = {'Authorization': f'Bearer {TOKEN}'}

# COMMAND ----------

for job_id in job_ids_list:
  job_id_json = {"job_id": job_id}

  response = requests.post(delete_jobs_url, headers=HEADERS, json=job_id_json)

  # Check the response status code
  if response.status_code == 200:
    print(f"Job {job_id} deleted successfully")
  else:
    print(f"Error deleting job {job_id}:", response.status_code)

# COMMAND ----------

# params = {'job_id': job_ids_list }
# response = requests.post(f'{api_url}jobs/delete', headers=HEADERS,params=params)

# COMMAND ----------

# print(response)
# print(response.text)
