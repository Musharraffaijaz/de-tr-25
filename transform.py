# Databricks notebook source
#data cleaning and deduplication
from pyspark.sql import functions as F

df_iot = spark.table("sagar_cat_project1.bronze.adls_ingested_iot")
df_sales = spark.table("sagar_cat_project1.bronze.iot_telemetry")

# Handle nulls
df_sales = df_sales.fillna({"Product": "Unknown"})
df_iot = df_iot.fillna({"DeviceStatus": "UNKNOWN"})

# Deduplicate
df_sales = df_sales.dropDuplicates(["OrderID"])
df_iot = df_iot.dropDuplicates(["OrderID", "DeviceType", "OrderTime"])



# COMMAND ----------

#Join IoT telemetry with Sales Transactions


# Convert OrderTime to timestamp
df_sales = df_sales.withColumn("OrderTime", F.to_timestamp("OrderTime"))
df_iot = df_iot.withColumn("OrderTime", F.to_timestamp("OrderTime"))

# Join: same store, same order timestamp
df_joined = df_sales.alias("s") \
    .join(df_iot.alias("i"),
          (F.col("s.StoreID") == F.col("i.StoreID")) &
          (F.col("s.OrderTime") == F.col("i.OrderTime")),
          "inner")



# COMMAND ----------

import pyspark.sql.functions as F
from itertools import chain


df_final = df_joined.withColumn(
    "AnomalyFlag_Sales",
    F.when(F.col("i.AnomalyFlag") == 1, 1).otherwise(0)
)

# Normalize store IDs (e.g., "Store-001" → 1)
df_final = df_final.withColumn(
    "StoreID_norm", 
    F.regexp_replace(F.col("s.StoreID"), "Store-", "").cast("int")
)

#  Normalize product categories
product_map = {
    "Mobile": "Electronics",
    "Laptop": "Electronics",
    "Tablet": "Electronics",
    "Printer": "Peripherals",
    "Smartwatch": "Wearables"
}


mapping_expr = F.create_map([F.lit(x) for x in chain(*product_map.items())])

df_final = df_final.withColumn(
    "ProductCategory", 
    mapping_expr[F.col("s.Product")]
)

df_final.select("s.StoreID", "StoreID_norm", "s.Product", "ProductCategory", "i.AnomalyFlag", "AnomalyFlag_Sales").show()

# COMMAND ----------

df_final.columns

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog sagar_cat_project1;
# MAGIC use schema silver;
# MAGIC drop table if exists iot_cleaned;

# COMMAND ----------

import pyspark.sql.functions as F

df_cleaned = df_final.select(
    F.col("s.OrderID").alias("OrderID"),
    F.col("s.CustomerID").alias("CustomerID"),
    "StoreID_norm", 
    "ProductCategory", 
    F.col("s.Region").alias("Region"),
    F.col("s.Quantity").alias("Quantity"),
    F.col("s.UnitPrice").alias("UnitPrice"),
    F.col("s.SalesAmount").alias("SalesAmount"),
    F.col("s.OrderTime").alias("OrderTime"),
    F.col("i.DeviceType").alias("DeviceType"),
    F.col("i.Temperature").alias("Temperature"),
    F.col("i.DeviceStatus").alias("DeviceStatus"),
    "AnomalyFlag_Sales", 
    F.col("i.AnomalyType").alias("AnomalyType")
)

df_cleaned.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "sagar_cat_project1.silver.iot_cleaned"
)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "sagar_cat_project1"
SCHEMA = "silver"
TABLE_NAME = "iot_cleaned"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"


df = spark.table(FULL_TABLE_NAME)

# Prepare for Partitioning
# Create a date column from the OrderTime timestamp to partition by date
if "order_date" not in df.columns:
    df = df.withColumn("order_date", F.to_date("OrderTime"))

# Standardize StoreID column if necessary
if "StoreID_norm" in df.columns and "StoreID" not in df.columns:
    df = df.withColumnRenamed("StoreID_norm", "StoreID")


(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .partitionBy("Region", "order_date")
   .saveAsTable('sagar_cat_project1.silver.iot_cleaned_zorder')
)

print("Table successfully rewritten with new partitions.")

# Apply Z-Ordering via the OPTIMIZE command
# This step physically reorganizes the data within the new partitions
print("Applying Z-Ordering on StoreID and ProductCategory...")


spark.sql(f"""
  OPTIMIZE {'sagar_cat_project1.silver.iot_cleaned_zorder'}
  ZORDER BY (StoreID, ProductCategory)
""")

print("Z-Ordering complete.")

# Verify the changes
print("\nVerifying table properties...")
display(spark.sql(f"DESCRIBE DETAIL {FULL_TABLE_NAME}"))

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "sagar_cat_project1"
SCHEMA = "silver"
TABLE_NAME = "iot_cleaned"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

# Load the existing Silver table
df = spark.table(FULL_TABLE_NAME)


(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .partitionBy("Region") # Apply partitioning by Region
   .saveAsTable('sagar_cat_project1.silver.iot_cleaned_partitionbyregion')
)

print("Table successfully rewritten and partitioned by Region.")

# Verify the partitioning
print("\nVerifying table properties...")
display(spark.sql(f"DESCRIBE DETAIL {FULL_TABLE_NAME}"))

# COMMAND ----------

df=spark.read.table('sagar_cat_project1.silver.iot_cleaned')
print(df.columns)