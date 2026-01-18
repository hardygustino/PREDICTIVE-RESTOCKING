# Assumptions & Parameters (Predictive Restocking)
## Tujuan
Pipeline menghasilkan rekomendasi restock produk berdasarkan pola penjualan historis. Karena dataset Olist tidak menyediakan data stok gudang secara eksplisit, nilai inventory dihitung secara estimasi dengan asumsi yang dijelaskan di bawah.

## Tipe Pipeline

* Mode: Batch (micro-batch)
* Jadwal: setiap 1 jam
* Orkestrasi: Apache Airflow
* Transformasi: PySpark
* Storage: PostgreSQL

## Sumber Data

* Dataset: Olist Brazilian E-Commerce (CSV multi-table)
* Tabel minimal yang digunakan:

  * olist_orders_dataset.csv
  * olist_order_items_dataset.csv
  * olist_products_dataset.csv

## Definisi Waktu (Time Column)

* Timestamp acuan untuk incremental window:

  * order_purchase_timestamp (dari tabel orders)
* Incremental processing menggunakan watermark:

  * start_ts = last_watermark_ts
  * end_ts = waktu eksekusi job (Airflow run time)
  * Filter data: order_purchase_timestamp > start_ts AND <= end_ts

## Parameter Utama

* Rolling window penjualan: 30 hari
* Lead time restock (default): 7 hari
* Safety factor (buffer): 0.3
* Overstock threshold: 60 hari stok
* Starting stock (estimasi awal): 100 unit per produk

## Definisi Metrik

1. Penjualan harian per produk

* qty_sold_daily = SUM(quantity) per product_id per tanggal

2. Rata-rata penjualan harian (rolling)

* avg_daily_sales_30d = SUM(qty_sold 30 hari terakhir) / 30

3. Estimasi stok saat ini

* current_stock = starting_stock - cumulative_qty_sold
* Jika hasil negatif, current_stock dipotong menjadi 0

4. Sisa hari stok

* stock_days_left = current_stock / NULLIF(avg_daily_sales_30d, 0)

5. Safety stock

* safety_stock = CEIL(avg_daily_sales_30d * safety_factor)

6. Reorder point

* reorder_point = CEIL(avg_daily_sales_30d * lead_time_days + safety_stock)

7. Rekomendasi jumlah restock

* recommended_order_qty = GREATEST(reorder_point - current_stock, 0)

## Klasifikasi Status Produk

* LOW_STOCK:

  * stock_days_left < lead_time_days
* OVERSTOCK:

  * stock_days_left > overstock_threshold_days
* OK:

  * selain kondisi di atas

## Catatan Keterbatasan

* Inventory bersifat estimasi karena keterbatasan dataset (tidak ada stok gudang asli).
* Rekomendasi restock bersifat analitis untuk tujuan simulasi dan pembelajaran pipeline data engineering.
* Parameter (starting_stock, lead_time, threshold) dapat disesuaikan saat evaluasi untuk melihat dampak pada rekomendasi.

