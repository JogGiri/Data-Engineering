# Databricks notebook source
# MAGIC %md
# MAGIC * This Notebook we can use to read files from http

# COMMAND ----------

import string
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark import SparkFiles

# COMMAND ----------

path = "https://gist.githubusercontent.com/aakashjainiitg/dbb668c58839d68d7903f508bf55043c/raw/1feec07802b4f53aceac450fa1aee5a87d9276e0/cities_data_bank.csv" #url github for csv
sc.addFile(path)

# COMMAND ----------

filepath = SparkFiles.get('cities_data_bank.csv')
print("rootdirectory: ",SparkFiles.getRootDirectory())
print("filepath: ",filepath)

# COMMAND ----------

options = {"header" : "true",
           "inferSchema" : "true"}
df = spark.read.options(**options).csv("file://"+filepath)
df.show()

# COMMAND ----------

def create_and_optimize_table(df : DataFrame, db_name : string, table_name : string, output_path : string):
    table_full_name = "{}.{}".format(db_name,table_name)
    df.withColumn("load_date",current_timestamp()).write.format("delta").mode('overwrite').option('overwriteSchema', 'true').save(output_path)
    spark.sql("create database if not exists {}".format(db_name))
    spark.sql("create table if not exists {}".format(table_name))
    spark.sql("optimize {}".format(table_name))

# COMMAND ----------

create_and_optimize_table(df,'world','cities','dbfs:/world/cities/')

# COMMAND ----------

spark.read.load('dbfs:/world/cities/').show()

