# Databricks notebook source
# DBTITLE 1,dbutils
#dbutils--> databricks utilities--> it is a python library provided by databricks that allows users to interact with the databricks env and manages tasks like
    # accessing data in mounted storage
    # creating input widgets
    # running notebooks programatically
    # accessing secrets
    # working with job task values
    #1. file systems (dbutils.fs), secrets (dbutils.secrets), widgets (dbutils.widgets), jobs (dbutils.jobs), notebooks (dbutils.notebooks), clusters (dbutils.clusters)


# COMMAND ----------

account_name = "keystonejunesa"
container_name = "input-data"
account_key = "KJ4BdaOtiPwIwkEXzzR4sr+Q1bkEy8FxBrZ76stQoIUUaJHcTuKIrz1pvcRYm2eNZtODDST9/QtR+ASt8x6nTQ=="
mount_point = f"/mnt/{container_name}"

# Configuration key for wasbs
conf_key = f"fs.azure.account.key.{account_name}.blob.core.windows.net"

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{account_name}.blob.core.windows.net/",
  mount_point = mount_point,
  extra_configs = {conf_key: account_key}
)


# COMMAND ----------

df = spark.read.csv("/mnt/input-data/products_sample.csv")
df.display()

# COMMAND ----------

# DBTITLE 1,File system utilities
# list files/folders
# dbutils.fs.ls("mnt/input/")
#make a directory
# dbutils.fs.mkdirs("mnt/ouput1")
#copy files
# dbutils.fs.cp("mnt/input/employees.csv", "mnt/output/gopi_new/employees.csv")
#move files
# dbutils.fs.mv("mnt/input/employees.csv", "mnt/output/gopi_new1/employees.csv")
# remove file/folders
dbutils.fs.rm("mnt/output/gopi_new1/employees.csv")

# COMMAND ----------

help(dbutils.fs.cp)

# COMMAND ----------

# DBTITLE 1,widgets
help(dbutils.widgets)

# COMMAND ----------

dbutils.widgets.text("text_wid","hello world")

# COMMAND ----------

dbutils.widgets.dropdown("url","db1",["db1","db2","db3"])

# COMMAND ----------

database=dbutils.widgets.get("database")
print(database)

# COMMAND ----------

dbutils.widgets.combobox("city","Bangalore",["Mumbai","Bangalore","Delhi"])

# COMMAND ----------

dbutils.widgets.multiselect("city_new","Bangalore",["Mumbai","Bangalore","Delhi"])

# COMMAND ----------

