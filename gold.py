# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window


CATALOG = "sagar_cat_project1"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"
BASE_TABLE = f"{SILVER_SCHEMA}.iot_cleaned"

# Create the Gold schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# Load the base Silver table
base_df = spark.table(BASE_TABLE)



# Standardize column names to ensure consistency
# (Handles cases like 'sales_amount' vs 'SalesAmount' and 'StoreID_norm' vs 'StoreID')
if "sales_amount" in base_df.columns and "SalesAmount" not in base_df.columns:
    base_df = base_df.withColumnRenamed("sales_amount", "SalesAmount")
if "StoreID_norm" in base_df.columns and "StoreID" not in base_df.columns:
    base_df = base_df.withColumnRenamed("StoreID_norm", "StoreID")

# Ensure correct data types for calculations
base_df = (base_df
           .withColumn("OrderTime", F.to_timestamp("OrderTime"))
           .withColumn("SalesAmount", F.col("SalesAmount").cast("double"))
           .withColumn("Quantity", F.col("Quantity").cast("int"))
          )

# Derive common date and month grains for aggregation
base_df = (base_df
           .withColumn("order_date", F.to_date("OrderTime"))
           .withColumn("order_month", F.date_trunc("month", "OrderTime"))
          )

# -----------------------------
# 1) KPI: Daily and Monthly Sales per Region
# -----------------------------
print("Creating sales KPIs per region...")

# Daily aggregation
daily_sales = (base_df
    .groupBy("order_date", "Region")
    .agg(
        F.sum("SalesAmount").alias("total_sales"),
        F.sum("Quantity").alias("total_units_sold"),
        F.countDistinct("OrderID").alias("distinct_orders")
    )
)
(daily_sales.write.format("delta").mode("overwrite")
 .saveAsTable(f"{GOLD_SCHEMA}.daily_sales_by_region"))

# Monthly aggregation
monthly_sales = (base_df
    .groupBy("order_month", "Region")
    .agg(
        F.sum("SalesAmount").alias("total_sales"),
        F.sum("Quantity").alias("total_units_sold"),
        F.countDistinct("OrderID").alias("distinct_orders")
    )
)
(monthly_sales.write.format("delta").mode("overwrite")
 .saveAsTable(f"{GOLD_SCHEMA}.monthly_sales_by_region"))

# -----------------------------
# 2) KPI: Device Anomaly Trend per Store
# -----------------------------
print("Creating device anomaly trend KPI...")

anomaly_trend = (base_df
    .groupBy("order_date", "StoreID")
    .agg(
        F.sum("AnomalyFlag_Sales").alias("anomaly_count"),
        F.count("OrderID").alias("total_transactions")
    )
    .withColumn("anomaly_rate", F.col("anomaly_count") / F.col("total_transactions"))
)
(anomaly_trend.write.format("delta").mode("overwrite")
 .saveAsTable(f"{GOLD_SCHEMA}.daily_anomaly_trend_by_store"))

# -----------------------------
# 3) KPI: Conversion Rate Impact from Device Failures
# -----------------------------
print("Creating conversion rate impact KPI...")

# We define "conversion" as the ratio of successful orders to total orders at a store-day level
daily_impact = (base_df
    .groupBy("order_date", "StoreID")
    .agg(
        F.sum(F.when(F.col("AnomalyFlag_Sales") == 1, 1).otherwise(0)).alias("anomaly_orders"),
        F.count("OrderID").alias("total_orders")
    )
    .withColumn("successful_orders", F.col("total_orders") - F.col("anomaly_orders"))
    .withColumn("success_rate", F.col("successful_orders") / F.col("total_orders"))
    .withColumn("had_anomaly_day", F.when(F.col("anomaly_orders") > 0, 1).otherwise(0))
)
(daily_impact.write.format("delta").mode("overwrite")
 .saveAsTable(f"{GOLD_SCHEMA}.daily_conversion_impact_by_store"))

# -----------------------------
# 4) Enrichment: Tag Stores into Performance Tiers
# -----------------------------
print("Enriching stores with performance tiers...")

# Calculate total sales per store for each month
store_monthly_sales = base_df.groupBy("order_month", "StoreID").agg(F.sum("SalesAmount").alias("total_sales"))

# Use a window function to rank stores within each month
w_spec = Window.partitionBy("order_month").orderBy(F.col("total_sales").desc())
store_ranks = store_monthly_sales.withColumn("rank", F.percent_rank().over(w_spec))

# Assign tiers based on percentile rank
store_tiers = store_ranks.withColumn("performance_tier",
    F.when(F.col("rank") <= 0.2, "High")
     .when((F.col("rank") > 0.2) & (F.col("rank") <= 0.8), "Medium")
     .otherwise("Low")
)
(store_tiers.select("order_month", "StoreID", "total_sales", "performance_tier")
 .write.format("delta").mode("overwrite")
 .saveAsTable(f"{GOLD_SCHEMA}.monthly_store_performance_tiers"))

# -----------------------------
# 5) Enrichment: Compute Weighted Sales vs. Anomalies Score
# -----------------------------
print("Enriching stores with a weighted performance score...")

# Get monthly sales and anomaly counts per store
monthly_kpis = base_df.groupBy("order_month", "StoreID").agg(
    F.sum("SalesAmount").alias("sales"),
    F.sum("AnomalyFlag_Sales").alias("anomalies")
)

# Define a window to normalize scores across all stores within a month
w_monthly = Window.partitionBy("order_month")

# Calculate min-max normalized scores for sales and anomalies
normalized_kpis = (monthly_kpis
    .withColumn("max_sales", F.max("sales").over(w_monthly))
    .withColumn("min_sales", F.min("sales").over(w_monthly))
    .withColumn("max_anomalies", F.max("anomalies").over(w_monthly))
    .withColumn("min_anomalies", F.min("anomalies").over(w_monthly))
    .withColumn("sales_norm",
        F.when(F.col("max_sales") == F.col("min_sales"), 1.0)
         .otherwise((F.col("sales") - F.col("min_sales")) / (F.col("max_sales") - F.col("min_sales")))
    )
    .withColumn("anomalies_norm",
        F.when(F.col("max_anomalies") == F.col("min_anomalies"), 0.0)
         .otherwise((F.col("anomalies") - F.col("min_anomalies")) / (F.col("max_anomalies") - F.col("min_anomalies")))
    )
)

# Compute the final weighted score (e.g., 80% weight for sales, 20% for anomalies)
weighted_score = normalized_kpis.withColumn(
    "performance_score",
    (0.8 * F.col("sales_norm")) - (0.2 * F.col("anomalies_norm"))
)

(weighted_score.select("order_month", "StoreID", "sales", "anomalies", "performance_score")
 .write.format("delta").mode("overwrite")
 .saveAsTable(f"{GOLD_SCHEMA}.monthly_store_weighted_score"))

print("\n--- Gold Layer Generation Complete ---")

# COMMAND ----------

# Configuration
CATALOG = "sagar_cat_project1"
GOLD_SCHEMA = f"{CATALOG}.gold"

# List of all the Gold tables created
gold_tables = [
    "daily_sales_by_region",
    "monthly_sales_by_region",
    "daily_anomaly_trend_by_store",
    "daily_conversion_impact_by_store",
    "monthly_store_performance_tiers",
    "monthly_store_weighted_score"
]

# Loop through the list and display each table
for table_name in gold_tables:
    full_table_name = f"{GOLD_SCHEMA}.{table_name}"
    print(f"--- Displaying contents of: {full_table_name} ---")
    
    # Use display() for a rich, interactive table view in Databricks notebooks
    display(spark.table(full_table_name).head(5))

# COMMAND ----------

# 1. Configuration for the Silver layer
CATALOG = "sagar_cat_project1"
SILVER_SCHEMA = f"{CATALOG}.silver"

# 2. List of tables in your Silver schema based on the image
silver_tables = [
    "iot_cleaned",
    "iot_cleaned_partitionbyregion",
    "iot_cleaned_zorder"
]

# 3. Loop through the list and display each table
for table_name in silver_tables:
    full_table_name = f"{SILVER_SCHEMA}.{table_name}"
    print(f"--- Displaying top 5 rows of: {full_table_name} ---")
    
    # Load the table and display the first 5 rows
    display(spark.table(full_table_name).limit(10))