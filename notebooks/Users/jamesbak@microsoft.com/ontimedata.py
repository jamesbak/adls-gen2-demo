# Databricks notebook source
basePath = "/mnt/ontimedata/"
baseAbfsPath="abfss://ontimedata@adlsgen2demo2.dfs.core.windows.net/"
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "27142561-761b-4a54-89c1-cf56e577ca38",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "adlsgen2demo", key = "spn-secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/microsoft.com/oauth2/token"}

# Optionally, you can add <your-directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = baseAbfsPath,
  mount_point = basePath,
  extra_configs = configs)


# COMMAND ----------

for tableName in ["OnTimeFacts", "metar", "aircraft", "manufacturers"]:
  df = spark.read.format('csv').options(header='true', inferschema='true').load(basePath + "csv/" + tableName)
  df.toDF(*[col.strip().replace(' ','_').replace('-','_').replace('(','_').replace(')','_') for col in df.columns]).write.saveAsTable(tableName,
      format='parquet',
      mode='overwrite',
      path=baseAbfsPath + 'parquet/' + tableName)

# COMMAND ----------

for dimension in dbutils.fs.ls(basePath + "csv/dimensions"):
  df = spark.read.format('csv').options(header='true', inferschema='true').load(dimension.path)
  df.write.saveAsTable('Dim_' + dimension.name.strip('/'), format='parquet', mode='overwrite', path=baseAbfsPath + 'parquet/dimensions/' + dimension.name)


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from ontimefacts o inner join aircraft a
# MAGIC     on ltrim("N", o.tail_number) = a.n_number
# MAGIC   inner join 
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from OnTimeFacts
# MAGIC where NASDelay != 0
# MAGIC limit 20