# Databricks notebook source
# MAGIC %sql
# MAGIC create database bronzelayerdb if not exists;

# COMMAND ----------



# COMMAND ----------

container_name = "incrementaldata"
account_name = "wetelco"
storage_account_key = "mG0nyBLqrK4T4SJnHOnc3ZBAf/Nkeu7f57Jro3o9ZJAz0ipXPtHkDlOCvaPefzadKSCl5UD97XiL+AStlZdB0Q=="
mount_point = "/mnt/wetelcodataincremental"
# Check if the storage is already mounted
mounts = dbutils.fs.mounts()
is_mounted = False
for mount in mounts:
  if mount.mountPoint == mount_point:
    is_mounted = True

# If not mounted, then mount it
if not is_mounted:
    dbutils.fs.mount(
      source='wasbs://{0}@{1}.blob.core.windows.net'.format(container_name,account_name),
      mount_point = mount_point,
      extra_configs  = {f"fs.azure.account.key.{account_name}.blob.core.windows.net" : storage_account_key}
    )
    print(f"Storage mounted at {mount_point}")
else:
    print(f"Storage is already mounted at {mount_point}")

# COMMAND ----------

files = dbutils.fs.ls("/mnt/wetelcodataincremental")

# COMMAND ----------

files[0].path

# COMMAND ----------

billing_partition_df_list = []
for file in files:
    billing_partition_df_list.append(spark.read.csv(file.path))

# COMMAND ----------

from delta.tables import *
def mergeDataInToDataBase(deltalake_database,dataframelist,unique_id):
    for df in dataframelist:
        deltalake_database.alias("Target")\
            .merge(
                    source = df.alias("Source"),
                    condition = f"Target.{unique_id} = Source.{unique_id}"
                )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

# COMMAND ----------

from delta.tables import *
billing_partition_delta = DeltaTable.forName(spark, "bronzelayerdb.billing_partition_df")

mergeDataInToDataBase(billing_partition_delta,billing_partition_df_list,"billing_id")
