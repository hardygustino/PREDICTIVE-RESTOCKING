# Data Catalog — Predictive Restocking (Olist)

## A. Source Data (Raw CSV — Olist)

Sumber data berasal dari dataset Olist (Brazilian e-commerce). Dalam pipeline ini, tidak semua file dipakai. Yang dipakai inti untuk demand adalah Orders + Order Items.

### 1) `olist_orders_dataset.csv`

**Grain**: 1 baris = 1 order
**Primary Key**: `order_id`
**Kolom penting yang dipakai**:

* `order_id` (string): ID order unik
* `order_purchase_timestamp` (timestamp): waktu order dibuat
* `order_status` (string): status order (delivered, canceled, dll)

**Peran di pipeline**: menentukan tanggal transaksi dan filter order valid.

---

### 2) `olist_order_items_dataset.csv`

**Grain**: 1 baris = 1 item dalam 1 order
**Primary Key**: kombinasi (`order_id`, `order_item_id`)
**Kolom penting yang dipakai**:

* `order_id` (string): FK ke orders
* `product_id` (string): ID produk
* `order_item_id` (int): urutan item dalam order
* `price` (numeric): harga item (opsional jika mau revenue)

**Peran di pipeline**: menghitung quantity terjual per produk per hari.

---

(Opsional, kalau kamu mau enrich dashboard)

### 3) `olist_products_dataset.csv`

**Grain**: 1 baris = 1 produk
**Primary Key**: `product_id`
**Kolom penting**:

* `product_id`
* `product_category_name` (butuh translation mapping)

**Peran**: ngasih konteks kategori produk untuk dashboard.

### 4) `product_category_name_translation.csv`

**Grain**: mapping kategori Portuguese → English
**Key**: `product_category_name`

---

## B. Data Warehouse / Mart Tables (PostgreSQL, schema: `public`)

> Pipeline menghasilkan 3 tabel utama + 1 metadata.
> Yang paling penting untuk dashboard adalah **`restock_recommendation`**.

---

# 1) `fact_sales_daily` (Bronze / Fact)

**Tujuan**: agregasi penjualan harian per produk
**Grain**: 1 baris = 1 produk per tanggal
**Primary Key (logical)**: (`date`, `product_id`)

| Kolom        | Tipe | Deskripsi                                               |
| ------------ | ---- | ------------------------------------------------------- |
| `date`       | date | Tanggal penjualan (hasil dari order_purchase_timestamp) |
| `product_id` | text | ID produk                                               |
| `qty_sold`   | int  | Total unit terjual pada tanggal tersebut                |

**Sumber**: join `orders` + `order_items`
**Dipakai untuk**: menghitung rolling avg sales 30 hari.

---

# 2) `inventory_snapshot` (Silver)

**Tujuan**: simulasi stok produk berdasarkan asumsi stok awal
**Grain**: 1 baris = 1 produk per snapshot run
**Primary Key (logical)**: (`run_date`, `product_id`)

| Kolom                 | Tipe | Deskripsi                                                   |
| --------------------- | ---- | ----------------------------------------------------------- |
| `run_date`            | date | Tanggal snapshot dijalankan                                 |
| `product_id`          | text | ID produk                                                   |
| `starting_stock`      | int  | Asumsi stok awal (default: 100)                             |
| `cumulative_qty_sold` | int  | Total unit terjual kumulatif dari awal data                 |
| `current_stock`       | int  | Stok tersisa = max(starting_stock - cumulative_qty_sold, 0) |

**Catatan**: ini simulasi karena dataset tidak memiliki stok real.

---

# 3) `restock_recommendation` (Gold / Decision Table)

**Tujuan**: hasil akhir keputusan restock untuk operasional
**Grain**: 1 baris = 1 produk per run/snapshot
**Primary Key (logical)**: (`run_date`, `product_id`)

| Kolom                   | Tipe    |Deskripsi                                                                             |
| ----------------------- | ------- | ------------------------------------------------------------------------------------- |
| `run_date`              | date    | Tanggal rekomendasi dibuat (snapshot date)                                            |
| `product_id`            | text    | ID produk                                                                             |
| `avg_daily_sales_30d`   | numeric | Rata-rata penjualan harian 30 hari terakhir                                           |
| `lead_time_days`        | int     | Lead time pengadaan (default: 7 hari)                                                 |
| `safety_factor`         | numeric | Faktor safety stock (default: 0.3)                                                    |
| `safety_stock`          | int     | Buffer stok = ceil(avg_daily_sales_30d * lead_time_days * safety_factor)              |
| `reorder_point`         | int     | Ambang reorder = ceil(avg_daily_sales_30d * lead_time_days + safety_stock)            |
| `current_stock`         | int     | Stok tersisa dari inventory snapshot                                                  |
| `stock_days_left`       | numeric | Estimasi hari stok bertahan = current_stock / avg_daily_sales_30d (null jika sales=0) |
| `recommended_order_qty` | int     | Jumlah restock = max(reorder_point - current_stock, 0)                                |
| `status`                | text    | Label kondisi stok: `NO_SALES`, `LOW_STOCK`, `OVERSTOCK`, `OK`                        |
| `starting_stock`        | int     | Asumsi stok awal                                                                      |
| `cumulative_qty_sold`   | int     | Total unit terjual kumulatif                                                          |

---

### Definisi Status (penting untuk presentasi)

* `NO_SALES`: `avg_daily_sales_30d = 0`
* `LOW_STOCK`: `current_stock <= reorder_point`
* `OVERSTOCK`: `stock_days_left > 60` dan punya sales
* `OK`: selain kondisi di atas

---

# 4) `etl_metadata` (Operational Metadata)

**Tujuan**: tracking kapan pipeline jalan dan statusnya
**Grain**: 1 baris = 1 run ETL (tergantung desain kamu)

Contoh kolom (umum):

* `pipeline_name`
* `run_ts`
* `status`
* `row_count`
* `notes`

Kalau mentor nanya: ini untuk audit dan monitoring pipeline.

---

## C. Data Lineage (Singkat)

**Raw CSV** → Spark Transform
`orders + order_items` → `fact_sales_daily` → `inventory_snapshot` → `restock_recommendation` → PostgreSQL → Power BI Dashboard

---

## D. KPI Dashboard yang bisa dibuat dari tabel Gold

Dari `restock_recommendation`:

* Total produk LOW_STOCK
* Total `recommended_order_qty`
* Persentase produk yang perlu restock
* Top 10 produk dengan qty restock tertinggi
* Distribusi status (OK/LOW/OVER/NO_SALES)


