# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog sagar_cat_project1;
# MAGIC create schema if not exists bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema bronze;

# COMMAND ----------

# Correct ADLS Gen2 config (dfs, not blob)
storage_account_name=''
storage_account_key=''
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS adls_ingested_iot")

adls_ingested = (
    spark.read.format("csv")
    .option("header", "true")
    .load("abfss://raw@sagarstorage0.dfs.core.windows.net/retail_iot_data.csv")
)

adls_ingested.write.mode("overwrite").saveAsTable("adls_ingested_iot")
display(adls_ingested)

# COMMAND ----------

#recieving data from Eventhub
event_hub_connection_string = (
   
)
eh_conf = {
    'eventhubs.connectionString': event_hub_connection_string
}

from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
    StructField("OrderID", StringType()),
    StructField("CustomerID", StringType()),
    StructField("Region", StringType()),
    StructField("StoreID", StringType()),
    StructField("Product", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", DoubleType()),
    StructField("SalesAmount", DoubleType()),
    StructField("OrderTime", StringType()),
    StructField("DeviceType", StringType()),
    StructField("Temperature", DoubleType()),
    StructField("DeviceStatus", StringType()),
    StructField("AnomalyFlag", IntegerType()),
    StructField("AnomalyType", StringType())
])

stream_df = (
    spark.readStream
    .format("eventhubs")
    .options(**eh_conf)
    .load()
)

telemetry_df = (
    stream_df
    .selectExpr("cast(body as string) as json")
    .select(from_json("json", schema).alias("data"))
    .select("data.*")
)
telemetry_df.write.mode('append').saveAsTable('iot_telemetry')
display(telemetry_df)

# COMMAND ----------

