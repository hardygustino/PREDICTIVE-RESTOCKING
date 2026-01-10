import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROLLING_DAYS = 30
LEAD_TIME_DAYS = 7
SAFETY_FACTOR = 0.3
OVERSTOCK_THRESHOLD_DAYS = 60

def main():
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    SALES_DIR = os.path.join(BASE_DIR, "output", "fact_sales_daily")
    INV_DIR   = os.path.join(BASE_DIR, "output", "inventory_snapshot")
    OUT_DIR   = os.path.join(BASE_DIR, "output", "restock_recommendation")

    spark = (
        SparkSession.builder
        .appName("olist_restock_recommendation")
        .getOrCreate()
    )

    # Load tables
    sales = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(SALES_DIR)
        .withColumn("date", F.to_date("date"))
    )

    inv = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(INV_DIR)
        .withColumn("run_date", F.to_date("run_date"))
    )

    # Choose run_date = max run_date from inventory snapshot (usually today)
    run_date = inv.agg(F.max("run_date").alias("run_date")).collect()[0]["run_date"]

    # Filter last 30 days sales (including run_date)
    start_date_expr = F.date_sub(F.lit(run_date), ROLLING_DAYS - 1)

    sales_30d = sales.filter(
        (F.col("date") >= start_date_expr) & (F.col("date") <= F.lit(run_date))
    )

    # avg_daily_sales_30d = sum(qty_sold in 30d) / 30
    avg_sales = (
        sales_30d.groupBy("product_id")
        .agg(
            (F.sum("qty_sold") / F.lit(ROLLING_DAYS)).alias("avg_daily_sales_30d")
        )
    )

    # Join with inventory
    base = (
        inv.select("product_id", "current_stock", "starting_stock", "cumulative_qty_sold")
        .join(avg_sales, on="product_id", how="left")
        .withColumn("run_date", F.lit(run_date).cast("date"))
        .withColumn("lead_time_days", F.lit(LEAD_TIME_DAYS))
        .withColumn("safety_factor", F.lit(SAFETY_FACTOR))
        .withColumn("overstock_threshold_days", F.lit(OVERSTOCK_THRESHOLD_DAYS))
        .withColumn("avg_daily_sales_30d", F.coalesce(F.col("avg_daily_sales_30d"), F.lit(0.0)))
    )

    # stock_days_left
    base = base.withColumn(
        "stock_days_left",
        F.when(F.col("avg_daily_sales_30d") > 0, F.col("current_stock") / F.col("avg_daily_sales_30d"))
         .otherwise(F.lit(None).cast("double"))
    )

    # safety_stock = ceil(avg_daily_sales_30d * safety_factor)
    base = base.withColumn(
        "safety_stock",
        F.ceil(F.col("avg_daily_sales_30d") * F.col("safety_factor"))
    )

    # reorder_point = ceil(avg_daily_sales_30d * lead_time_days + safety_stock)
    base = base.withColumn(
        "reorder_point",
        F.ceil(F.col("avg_daily_sales_30d") * F.col("lead_time_days") + F.col("safety_stock"))
    )

    # recommended_order_qty = max(reorder_point - current_stock, 0)
    base = base.withColumn(
        "recommended_order_qty",
        F.greatest(F.col("reorder_point") - F.col("current_stock"), F.lit(0))
    )

    # status classification
    base = base.withColumn(
        "status",
        F.when(
            (F.col("avg_daily_sales_30d") > 0) & (F.col("stock_days_left") < F.col("lead_time_days")),
            F.lit("LOW_STOCK")
        ).when(
            (F.col("avg_daily_sales_30d") > 0) & (F.col("stock_days_left") > F.col("overstock_threshold_days")),
            F.lit("OVERSTOCK")
        ).otherwise(F.lit("OK"))
    )

    # Final columns
    restock = base.select(
        "run_date",
        "product_id",
        F.round("avg_daily_sales_30d", 4).alias("avg_daily_sales_30d"),
        "lead_time_days",
        F.round("safety_factor", 2).alias("safety_factor"),
        "safety_stock",
        "reorder_point",
        "current_stock",
        F.round("stock_days_left", 2).alias("stock_days_left"),
        "recommended_order_qty",
        "status",
        "starting_stock",
        "cumulative_qty_sold"
    ).orderBy(F.desc("recommended_order_qty"), F.desc("avg_daily_sales_30d"))

    # Save
    (
        restock.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(OUT_DIR)
    )

    print(f"Saved restock_recommendation to: {OUT_DIR}")
    spark.stop()

if __name__ == "__main__":
    main()
