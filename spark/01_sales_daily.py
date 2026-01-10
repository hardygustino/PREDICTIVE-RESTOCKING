import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "olist")
    OUT_DIR = os.path.join(BASE_DIR, "output", "fact_sales_daily")

    orders_path = os.path.join(RAW_DIR, "olist_orders_dataset.csv")
    items_path  = os.path.join(RAW_DIR, "olist_order_items_dataset.csv")

    spark = (
        SparkSession.builder
        .appName("olist_fact_sales_daily")
        .getOrCreate()
    )

    # Read CSV
    orders = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(orders_path)
        .select(
            "order_id",
            "order_status",
            "order_purchase_timestamp"
        )
    )

    items = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(items_path)
        .select(
            "order_id",
            "product_id",
            "price",
            "freight_value"
        )
    )

    # Filter order yang valid (hindari cancelled/unavailable)
    valid_orders = orders.filter(
        F.col("order_status").isin(["delivered", "shipped", "invoiced", "processing", "approved"])
    )

    # Join + derive date
    joined = (
        items.join(valid_orders, on="order_id", how="inner")
        .withColumn("purchase_ts", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("purchase_date", F.to_date("purchase_ts"))
    )

    # Build fact_sales_daily
    fact_sales_daily = (
        joined.groupBy("purchase_date", "product_id")
        .agg(
            F.count("*").alias("qty_sold"),  # order_items satu baris = 1 unit pada dataset ini
            F.round(F.sum(F.col("price")), 2).alias("revenue"),
            F.round(F.sum(F.col("freight_value")), 2).alias("freight_total"),
        )
        .withColumnRenamed("purchase_date", "date")
        .orderBy("date", "product_id")
    )

    # Save output (CSV dulu biar gampang dicek)
    (
        fact_sales_daily.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(OUT_DIR)
    )

    print(f"Saved fact_sales_daily to: {OUT_DIR}")
    spark.stop()

if __name__ == "__main__":
    main()
