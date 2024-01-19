# From this code we can get job context

Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

SHardURL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
base_url = SHardURL.split('//')[-1]
run_env = base_url.split('.')[0]

notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  try:
      #The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...) 
      jobId = notebook_info["tags"]["jobId"] 
  except: 
      jobId = -1 
  jobUrl = env_url + str(jobId) + "/run/1"    
