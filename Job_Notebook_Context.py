# From this code we can get job context

Context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
