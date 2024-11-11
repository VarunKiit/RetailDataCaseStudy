-- Databricks notebook source
SELECT * FROM `hive_metastore`.`default`.`retail_data` LIMIT 100;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("retail_data")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC row_count = df.count()
-- MAGIC row_count

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = df.dropna()
-- MAGIC row_count

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_dup = df

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_dup)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F, Window

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Generate new Transaction IDs for duplicate Transaction IDs 
-- MAGIC df_dup = df_dup.withColumn("new_transaction_id", F.monotonically_increasing_id())
-- MAGIC
-- MAGIC max_id = df_dup.agg({"Transaction_ID":"max"}).collect()[0][0]
-- MAGIC df_dup = df_dup.withColumn("new_transaction_id", F.col("new_transaction_id") + max_id)
-- MAGIC
-- MAGIC df_dup = df_dup.drop("Transaction_ID").withColumnRenamed("new_transaction_id", "Transaction_ID")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_dup.groupBy("Transaction_ID").agg(F.count("Customer_ID")).show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert date column 
-- MAGIC df_dup = df_dup.withColumn("Date", F.regexp_replace("Date", "/", "-"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_dup)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account_name = "snretail"
-- MAGIC container_name = "cnretail"
-- MAGIC sas_token = "F/NnTpdR+AkPBZLXMe4vtGQkFZ0Q6ApzzzR4ltNw4lUMZgme0b49eUdCXXCvYhScmpc4agL+gZeO+ASt/dEXBQ=="

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Check if the directory is already mounted
-- MAGIC if any(mount.mountPoint == '/mnt/mycontainer' for mount in dbutils.fs.mounts()):
-- MAGIC     dbutils.fs.unmount('/mnt/mycontainer')
-- MAGIC
-- MAGIC # Mount the directory
-- MAGIC dbutils.fs.mount(
-- MAGIC     source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
-- MAGIC     mount_point="/mnt/mycontainer",
-- MAGIC     extra_configs={f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/mycontainer")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC output_path = "dbfs:/mnt/mycontainer/final_data"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_dup)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_dup.write.csv("wasbs://cnretail@snretail.blob.core.windows.net/final_data.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_dup.coalesce(1) \
-- MAGIC     .write.option("header", "true") \
-- MAGIC     .csv("wasbs://cnretail@snretail.blob.core.windows.net/final_data.csv")
