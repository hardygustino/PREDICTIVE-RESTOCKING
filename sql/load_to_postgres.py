import os
import glob
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

PG_HOST = os.getenv("PGHOST", "localhost")
PG_PORT = int(os.getenv("PGPORT", "5432"))
PG_DB   = os.getenv("PGDB", "restock_db")
PG_USER = os.getenv("PGUSER", "restock")
PG_PASS = os.getenv("PGPASS", "restock")


def read_single_csv(folder: str) -> pd.DataFrame:
    # Spark output biasanya part-*.csv
    files = glob.glob(os.path.join(folder, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {folder}")
    # ambil file csv pertama
    return pd.read_csv(files[0])

def upsert_fact_sales_daily(conn, df: pd.DataFrame):
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    records = df[["date","product_id","qty_sold","revenue","freight_total"]].to_records(index=False).tolist()

    sql = """
    INSERT INTO fact_sales_daily (date, product_id, qty_sold, revenue, freight_total)
    VALUES %s
    ON CONFLICT (date, product_id) DO UPDATE SET
      qty_sold = EXCLUDED.qty_sold,
      revenue = EXCLUDED.revenue,
      freight_total = EXCLUDED.freight_total;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=5000)

def upsert_inventory_snapshot(conn, df: pd.DataFrame):
    df = df.copy()
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    records = df[["run_date","product_id","starting_stock","cumulative_qty_sold","current_stock"]].to_records(index=False).tolist()

    sql = """
    INSERT INTO inventory_snapshot (run_date, product_id, starting_stock, cumulative_qty_sold, current_stock)
    VALUES %s
    ON CONFLICT (run_date, product_id) DO UPDATE SET
      starting_stock = EXCLUDED.starting_stock,
      cumulative_qty_sold = EXCLUDED.cumulative_qty_sold,
      current_stock = EXCLUDED.current_stock;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=5000)

def upsert_restock_recommendation(conn, df: pd.DataFrame):
    df = df.copy()
    df["run_date"] = pd.to_datetime(df["run_date"]).dt.date
    # stock_days_left bisa NaN
    records = df[[
        "run_date","product_id","avg_daily_sales_30d","lead_time_days","safety_factor",
        "safety_stock","reorder_point","current_stock","stock_days_left",
        "recommended_order_qty","status","starting_stock","cumulative_qty_sold"
    ]].to_records(index=False).tolist()

    sql = """
    INSERT INTO restock_recommendation (
      run_date, product_id, avg_daily_sales_30d, lead_time_days, safety_factor,
      safety_stock, reorder_point, current_stock, stock_days_left,
      recommended_order_qty, status, starting_stock, cumulative_qty_sold
    )
    VALUES %s
    ON CONFLICT (run_date, product_id) DO UPDATE SET
      avg_daily_sales_30d = EXCLUDED.avg_daily_sales_30d,
      lead_time_days = EXCLUDED.lead_time_days,
      safety_factor = EXCLUDED.safety_factor,
      safety_stock = EXCLUDED.safety_stock,
      reorder_point = EXCLUDED.reorder_point,
      current_stock = EXCLUDED.current_stock,
      stock_days_left = EXCLUDED.stock_days_left,
      recommended_order_qty = EXCLUDED.recommended_order_qty,
      status = EXCLUDED.status,
      starting_stock = EXCLUDED.starting_stock,
      cumulative_qty_sold = EXCLUDED.cumulative_qty_sold;
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=5000)

def main():
    fact_dir = os.path.join(BASE_DIR, "output", "fact_sales_daily")
    inv_dir  = os.path.join(BASE_DIR, "output", "inventory_snapshot")
    rec_dir  = os.path.join(BASE_DIR, "output", "restock_recommendation")

    df_fact = read_single_csv(fact_dir)
    df_inv  = read_single_csv(inv_dir)
    df_rec  = read_single_csv(rec_dir)

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False

    try:
        upsert_fact_sales_daily(conn, df_fact)
        upsert_inventory_snapshot(conn, df_inv)
        upsert_restock_recommendation(conn, df_rec)
        conn.commit()
        print("Load finished: fact_sales_daily, inventory_snapshot, restock_recommendation")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
