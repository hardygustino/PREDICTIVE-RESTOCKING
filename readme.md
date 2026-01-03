# Batch-Based Predictive Restocking Data Pipeline

**Case Study: Olist Brazilian E-Commerce Dataset**

## Ringkasan Proyek

Proyek ini bertujuan membangun **data pipeline batch** untuk menghasilkan **rekomendasi restock produk** berdasarkan data penjualan historis e-commerce.
Pipeline dirancang menggunakan pendekatan **end-to-end Data Engineering**, mulai dari ingestion data mentah, transformasi data, pemodelan data bisnis, hingga penyimpanan hasil ke database PostgreSQL.

Proyek ini berfokus pada **pemecahan masalah bisnis nyata**, yaitu bagaimana membantu pengambilan keputusan inventory agar lebih berbasis data, bukan intuisi semata.

---

## Latar Belakang Masalah

Dalam praktik retail dan e-commerce, keputusan restock sering kali dilakukan berdasarkan:

* intuisi pemilik atau staf gudang,
* laporan penjualan yang bersifat deskriptif,
* atau pengalaman masa lalu tanpa perhitungan kuantitatif.

Pendekatan tersebut menimbulkan dua masalah utama:

1. **Stockout**
   Produk dengan permintaan tinggi sering kehabisan stok sehingga menyebabkan kehilangan potensi pendapatan.
2. **Overstock**
   Produk dengan penjualan rendah menumpuk di gudang dan menyebabkan modal terjebak serta biaya penyimpanan meningkat.

Masalah ini membutuhkan solusi berupa **sistem data terstruktur** yang mampu memberikan rekomendasi restock secara kuantitatif dan konsisten.

---

## Tujuan Proyek

Tujuan utama proyek ini adalah:

1. Membangun pipeline batch yang terjadwal dan terotomasi.
2. Mengolah data transaksi e-commerce menjadi data yang siap digunakan untuk analisis bisnis.
3. Menghasilkan **rekomendasi restock detail per produk** berbasis metrik penjualan.
4. Menerapkan praktik Data Engineering seperti:

   * data layering (raw, staging, mart),
   * data modeling berbasis kebutuhan bisnis,
   * dokumentasi dan struktur repository yang rapi.

---

## Dataset

Proyek ini menggunakan **Olist Brazilian E-Commerce Dataset** yang tersedia di Kaggle.

Dataset terdiri dari beberapa tabel utama, antara lain:

* orders
* order_items
* products
* customers
* sellers
* payments

Dataset ini dipilih karena:

* memiliki struktur multi-table yang realistis,
* mendukung pemodelan data fact dan dimension,
* memiliki timestamp yang lengkap untuk analisis time-series.

Catatan: Dataset tidak menyediakan data stok secara eksplisit, sehingga **inventory dihitung secara estimasi** berdasarkan data penjualan historis. Asumsi ini dijelaskan secara eksplisit dalam dokumentasi proyek.

---

## Solusi dan Pendekatan

Pipeline batch dibangun dengan pendekatan berikut:

1. Menghitung **penjualan harian per produk**.
2. Menghitung **rata-rata penjualan harian (rolling window)**.
3. Mengestimasi **stok saat ini** berdasarkan stok awal dan akumulasi penjualan.
4. Menghitung metrik inventory, seperti:

   * jumlah hari stok tersisa,
   * reorder point,
   * jumlah rekomendasi pemesanan ulang.
5. Mengklasifikasikan status produk menjadi:

   * LOW_STOCK
   * OK
   * OVERSTOCK

Pendekatan ini memungkinkan sistem memberikan rekomendasi yang dapat langsung digunakan untuk pengambilan keputusan operasional.

---

## Output Utama

Output utama proyek adalah tabel **`restock_recommendation`** yang berisi rekomendasi restock per produk.

Contoh kolom utama:

* run_date
* product_id
* avg_daily_sales_30d
* current_stock
* stock_days_left
* reorder_point
* recommended_order_qty
* status

Tabel ini dirancang agar mudah digunakan oleh tim bisnis atau operasional tanpa perlu melakukan perhitungan tambahan.

---

## Arsitektur Data Pipeline

Pipeline dibangun menggunakan pendekatan batch dengan alur sebagai berikut:

1. Ingestion data mentah (CSV)
2. Penyimpanan data mentah dalam format Parquet
3. Transformasi data menggunakan PySpark
4. Penyimpanan data hasil transformasi ke PostgreSQL
5. Pemeriksaan kualitas data (data quality checks)

Pipeline dijadwalkan menggunakan Apache Airflow.

---

## Teknologi yang Digunakan

* Python
* Apache Airflow (Batch Orchestration)
* PySpark (Data Transformation)
* PostgreSQL (Data Storage)
* Parquet (Raw Data Storage)
* Docker dan Docker Compose
* Git (Version Control)

---

## Struktur Repository

```
final-project-restock-olist/
├── dags/
│   └── dag_restock_olist.py
├── spark/
│   ├── 01_sales_daily.py
│   ├── 02_inventory_snapshot.py
│   └── 03_restock_recommendation.py
├── sql/
│   ├── staging_tables.sql
│   └── mart_tables.sql
├── docker-compose.yml
├── README.md
└── docs/
    ├── architecture.png
    ├── data_model.png
    └── assumptions.md
```

---

## Catatan dan Asumsi

* Data inventory dihitung secara estimasi karena keterbatasan dataset.
* Proyek ini menggunakan pendekatan batch karena data bersifat historis dan tidak membutuhkan pemrosesan real-time.
* Fokus utama proyek adalah **kualitas pipeline dan logika data**, bukan visualisasi dashboard.

