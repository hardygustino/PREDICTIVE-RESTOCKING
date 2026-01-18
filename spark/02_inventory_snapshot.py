import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

STARTING_STOCK = 100  # sesuai assumptions.md

def main():
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    SALES_DIR = os.path.join(BASE_DIR, "output", "fact_sales_daily")
    OUT_DIR = os.path.join(BASE_DIR, "output", "inventory_snapshot")

    spark = (
        SparkSession.builder
        .appName("olist_inventory_snapshot")
        .getOrCreate()
    )

    # Read fact_sales_daily + pastiin kolom date benar sebagai date
    sales = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(SALES_DIR)
        .withColumn("date", F.to_date("date"))
    )

    # run_date harus ngikut data, bukan current_date() (biar rolling window nyambung)
    run_date = sales.agg(F.max("date").alias("run_date")).collect()[0]["run_date"]

    # Hitung cumulative sales per product
    cumulative_sales = (
        sales.groupBy("product_id")
        .agg(F.sum("qty_sold").alias("cumulative_qty_sold"))
    )

    # Build inventory snapshot
    inventory_snapshot = (
        cumulative_sales
        .withColumn("starting_stock", F.lit(STARTING_STOCK))
        .withColumn(
            "current_stock",
            F.greatest(F.col("starting_stock") - F.col("cumulative_qty_sold"), F.lit(0))
        )
        .withColumn("run_date", F.lit(run_date).cast("date"))
        .select(
            "run_date",
            "product_id",
            "starting_stock",
            "cumulative_qty_sold",
            "current_stock"
        )
        .orderBy("product_id")
    )

    # Save output
    (
        inventory_snapshot.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(OUT_DIR)
    )

    print(f"Saved inventory_snapshot to: {OUT_DIR}")
    spark.stop()

if __name__ == "__main__":
    main()
