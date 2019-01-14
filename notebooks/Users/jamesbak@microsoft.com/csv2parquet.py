# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "27142561-761b-4a54-89c1-cf56e577ca38",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "adlsgen2demo", key = "spn-secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/microsoft.com/oauth2/token"}

# Optionally, you can add <your-directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://ontimedata@adlsgen2demo2.dfs.core.windows.net/",
  mount_point = "/mnt/ontimedata",
  extra_configs = configs)

# COMMAND ----------

basePath = "/mnt/ontimedata/"
df = spark.read.format('csv').options(header='true', inferschema='true').load(basePath + "csv/OnTimeFacts")


# COMMAND ----------

df.write.mode("append").parquet(basePath + "parquet/OnTimeFacts")

# COMMAND ----------

for dimension in dbutils.fs.ls(basePath + "csv/dimensions"):
  df = spark.read.format('csv').options(header='true', inferschema='true').load(dimension.path)
  df.write.mode("append").parquet(basePath + "parquet/dimensions/" + dimension.name)

# COMMAND ----------

