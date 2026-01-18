Baik. Ini **README versi Bahasa Indonesia**, **formal**, **profesional**, dan **aman untuk mentor/dosen/reviewer industri**.
Tidak ada emoji, tidak ada bahasa santai.

Silakan **copy–paste langsung** ke `README.md`.

---

# Predictive Restocking – Micro-Batch Data Pipeline

## Gambaran Umum

Proyek ini membangun **pipeline data engineering end-to-end** untuk menghasilkan **rekomendasi restock produk** berdasarkan data penjualan historis e-commerce.

Pipeline dijalankan secara **micro-batch per jam**, menggunakan **Apache Spark** sebagai mesin pemrosesan data, **Apache Airflow** sebagai orchestrator, dan **PostgreSQL** sebagai penyimpanan data analitik.

---

## Latar Belakang Masalah

Dalam operasional retail online, keputusan restock sering kali tidak optimal karena:

* Kurangnya insight terstruktur dari data penjualan
* Risiko kehabisan stok (stock-out) atau kelebihan stok (overstock)
* Proses analisis yang masih manual dan tidak terjadwal

Proyek ini bertujuan untuk:
Menyediakan rekomendasi restock otomatis berdasarkan pola penjualan historis.

---

## Pendekatan Solusi

Solusi dirancang menggunakan pendekatan **micro-batch data pipeline**, di mana data diproses secara berkala (setiap satu jam) untuk menghasilkan output yang siap digunakan oleh bisnis.

Alasan pemilihan micro-batch:

* Update per jam sudah memadai untuk pengambilan keputusan restock
* Lebih stabil dan mudah dimonitor dibandingkan real-time streaming
* Dataset yang digunakan bersifat historis (batch)

---

## Arsitektur Sistem

```
Dataset CSV Olist
        ↓
Apache Spark (PySpark)
(Bronze → Silver → Gold)
        ↓
PostgreSQL (Data Mart)
        ↑
Apache Airflow (Orkestrasi Per Jam)
```

---

## Dataset

Proyek ini menggunakan **Olist Brazilian E-Commerce Dataset**.

### Dataset yang Digunakan Secara Aktif (Core Dataset)

1. olist_orders_dataset.csv
2. olist_order_items_dataset.csv
3. olist_products_dataset.csv
4. product_category_name_translation.csv

Dataset lainnya tersedia sebagai potensi enrichment, namun tidak digunakan secara langsung dalam pipeline inti.

---

## Desain ETL Pipeline (Bronze – Silver – Gold)

### Source Layer

* File CSV mentah berisi data transaksi historis

### Bronze Layer (Raw Ingest)

* Data di-ingest apa adanya menggunakan Apache Spark
* Parsing skema dan validasi dasar
* Belum ada logic bisnis pada tahap ini

### Silver Layer (Clean dan Transform)

* Join antar dataset
* Filtering data valid
* Normalisasi dan agregasi data penjualan

Output:

* fact_sales_daily
* inventory_snapshot

### Gold Layer (Output Bisnis)

* Penerapan logic bisnis untuk perhitungan restock
* Perbandingan antara stok saat ini dan rata-rata penjualan harian

Output utama:

* restock_recommendation

---

## Orkestrasi dengan Apache Airflow

Pipeline diorkestrasi menggunakan Apache Airflow dengan urutan task sebagai berikut:

```
spark_sales_daily
→ spark_inventory_snapshot
→ spark_restock_recommendation
→ load_to_postgres
```

Karakteristik orkestrasi:

* Dijadwalkan secara per jam
* Dependency antar task dikelola oleh Airflow
* Retry dan monitoring dijalankan secara otomatis
* Pipeline telah berhasil dijalankan secara end-to-end

---

## Penyimpanan Data

PostgreSQL digunakan sebagai **analytical data store** untuk:

* Menyimpan hasil transformasi
* Mendukung query berbasis SQL
* Validasi data menggunakan database client

Tabel yang dihasilkan:

* fact_sales_daily
* inventory_snapshot
* restock_recommendation
* etl_metadata

---

## Validasi

Validasi pipeline dilakukan melalui:

* Keberhasilan eksekusi seluruh task di Airflow
* Data tersedia dan dapat di-query di PostgreSQL
* Pemeriksaan langsung menggunakan DBeaver

---

## Teknologi yang Digunakan

* Apache Spark (PySpark) – Pemrosesan data
* Apache Airflow – Orkestrasi workflow
* PostgreSQL – Data mart analitik
* Docker dan Docker Compose – Manajemen environment
* Python – Implementasi ETL

---

## Asumsi dan Keterbatasan

* Dataset bersifat historis dan berbasis batch
* Inventory yang digunakan merupakan simulasi
* Belum menerapkan real-time event streaming

---

## Pengembangan Selanjutnya

* Integrasi dengan platform streaming (misalnya Kafka)
* Penerapan incremental data ingestion
* Integrasi dengan BI dashboard
* Deployment ke Spark cluster terdistribusi

---

## Kesimpulan

Proyek ini berhasil membangun **micro-batch data pipeline** yang stabil dan modular, serta merepresentasikan praktik data engineering yang baik mulai dari ingest data, transformasi, orkestrasi, hingga penyimpanan data analitik.

---

## Ringkasan Satu Kalimat

Pipeline data micro-batch per jam menggunakan Apache Spark dan Airflow untuk menghasilkan rekomendasi restock yang disimpan di PostgreSQL.

---

Jika sudah disimpan, balas: **“README done”**
Setelah itu kita lanjut ke **diagram arsitektur**.
