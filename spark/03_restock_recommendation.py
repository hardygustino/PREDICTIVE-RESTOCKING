import os
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROLLING_DAYS = 30
LEAD_TIME_DAYS = 7
SAFETY_FACTOR = 0.3
OVERSTOCK_THRESHOLD_DAYS = 60


def parse_args():
    p = argparse.ArgumentParser()
    # ISO timestamp, contoh: "2026-01-17T03:10:00" atau "2026-01-17 03:10:00"
    p.add_argument("--run_ts", type=str, default=os.getenv("RUN_TS", ""))
    return p.parse_args()


def main():
    args = parse_args()

    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    SALES_DIR = os.path.join(BASE_DIR, "output", "fact_sales_daily")
    INV_DIR   = os.path.join(BASE_DIR, "output", "inventory_snapshot")
    OUT_DIR   = os.path.join(BASE_DIR, "output", "restock_recommendation")

    spark = (
        SparkSession.builder
        .appName("olist_restock_recommendation")
        .getOrCreate()
    )

    # =========================
    # Load tables
    # =========================
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

    # =========================
    # Run timestamp (snapshot time)
    # =========================
    if args.run_ts and args.run_ts.strip():
        run_ts_col = F.to_timestamp(F.lit(args.run_ts))
    else:
        run_ts_col = F.current_timestamp()

    # run_date_day (DATE) dari run_ts
    run_date_day_col = F.to_date(run_ts_col)

    # =========================
    # Anchor date untuk rolling window 30D
    # Dataset statis: kalau run_ts "masa depan", window bisa kosong,
    # jadi kita cap ke max(date) yang ada di data sales.
    # =========================
    max_sales_date = sales.agg(F.max("date").alias("max_date")).collect()[0]["max_date"]
    if max_sales_date is None:
        raise RuntimeError("fact_sales_daily kosong. Pastikan 01_sales_daily sudah menghasilkan data.")

    # calc_date_col = min(run_date_day, max_sales_date)
    calc_date_col = F.when(
        run_date_day_col > F.lit(max_sales_date),
        F.lit(max_sales_date)
    ).otherwise(run_date_day_col)

    # =========================
    # Filter last 30 days sales (based on calc_date_col)
    # =========================
    start_date_expr = F.date_sub(calc_date_col, ROLLING_DAYS - 1)

    sales_30d = sales.filter(
        (F.col("date") >= start_date_expr) & (F.col("date") <= calc_date_col)
    )

    # avg_daily_sales_30d = sum(qty_sold in 30d) / 30
    avg_sales = (
        sales_30d.groupBy("product_id")
        .agg((F.sum("qty_sold") / F.lit(ROLLING_DAYS)).alias("avg_daily_sales_30d"))
    )

    # =========================
    # Join with inventory
    # =========================
    base = (
        inv.select("product_id", "current_stock", "starting_stock", "cumulative_qty_sold")
        .join(avg_sales, on="product_id", how="left")
        .withColumn("run_date", run_ts_col)                  # TIMESTAMP snapshot
        .withColumn("run_date_day", run_date_day_col)        # DATE (optional)
        .withColumn("lead_time_days", F.lit(LEAD_TIME_DAYS))
        .withColumn("safety_factor", F.lit(SAFETY_FACTOR))
        .withColumn("overstock_threshold_days", F.lit(OVERSTOCK_THRESHOLD_DAYS))
        .withColumn("avg_daily_sales_30d", F.coalesce(F.col("avg_daily_sales_30d"), F.lit(0.0)))
    )

    # stock_days_left = current_stock / avg_daily_sales_30d (kalau avg 0 => null)
    base = base.withColumn(
        "stock_days_left",
        F.when(
            F.col("avg_daily_sales_30d") > 0,
            F.col("current_stock") / F.col("avg_daily_sales_30d")
        ).otherwise(F.lit(None).cast("double"))
    )

    # safety_stock = ceil(avg_daily_sales_30d * lead_time_days * safety_factor)
    base = base.withColumn(
        "safety_stock",
        F.ceil(F.col("avg_daily_sales_30d") * F.col("lead_time_days") * F.col("safety_factor"))
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

    # =========================
    # STATUS (rule-based)
    # NO_SALES  -> avg_daily_sales_30d = 0
    # LOW_STOCK -> current_stock <= reorder_point
    # OVERSTOCK -> stock_days_left > threshold (punya sales)
    # OK        -> lainnya
    # =========================
    base = base.withColumn(
        "status",
        F.when(F.col("avg_daily_sales_30d") <= 0, F.lit("NO_SALES"))
         .when(F.col("current_stock") <= F.col("reorder_point"), F.lit("LOW_STOCK"))
         .when(
            (F.col("stock_days_left").isNotNull()) &
            (F.col("stock_days_left") > F.col("overstock_threshold_days")),
            F.lit("OVERSTOCK")
         )
         .otherwise(F.lit("OK"))
    )

    restock = base.select(
        "run_date",          # TIMESTAMP snapshot
        "run_date_day",      # DATE (optional)
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

    # Save CSV output
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
