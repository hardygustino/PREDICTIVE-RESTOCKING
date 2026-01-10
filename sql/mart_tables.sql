-- MART TABLES

CREATE TABLE IF NOT EXISTS fact_sales_daily (
  date            DATE NOT NULL,
  product_id      TEXT NOT NULL,
  qty_sold        BIGINT NOT NULL,
  revenue         NUMERIC(18,2),
  freight_total   NUMERIC(18,2),
  PRIMARY KEY (date, product_id)
);

CREATE TABLE IF NOT EXISTS inventory_snapshot (
  run_date            DATE NOT NULL,
  product_id          TEXT NOT NULL,
  starting_stock      BIGINT NOT NULL,
  cumulative_qty_sold BIGINT NOT NULL,
  current_stock       BIGINT NOT NULL,
  PRIMARY KEY (run_date, product_id)
);

CREATE TABLE IF NOT EXISTS restock_recommendation (
  run_date                DATE NOT NULL,
  product_id              TEXT NOT NULL,
  avg_daily_sales_30d      NUMERIC(18,4) NOT NULL,
  lead_time_days           INT NOT NULL,
  safety_factor            NUMERIC(5,2) NOT NULL,
  safety_stock             BIGINT NOT NULL,
  reorder_point            BIGINT NOT NULL,
  current_stock            BIGINT NOT NULL,
  stock_days_left          NUMERIC(18,2),
  recommended_order_qty    BIGINT NOT NULL,
  status                  TEXT NOT NULL,
  starting_stock           BIGINT NOT NULL,
  cumulative_qty_sold      BIGINT NOT NULL,
  PRIMARY KEY (run_date, product_id)
);
