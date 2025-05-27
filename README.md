# Query Aggregator: Cassandra + MongoDB

Repository ini berisi aplikasi dashboard yang menggabungkan query dari dua database NoSQL berbeda: Cassandra dan MongoDB.  
Dashboard dibuat dengan Streamlit untuk visualisasi data dan query interaktif.

---

## Persyaratan

- Docker & Docker Compose (untuk menjalankan Cassandra dan MongoDB dengan container)
- Python 3.7+ (disarankan menggunakan python dibawah 3.13)
- Streamlit dan dependencies Python (`pandas`, `altair`, `cassandra-driver`, `pymongo`)

---

## Langkah-Langkah Setup

### 1. Clone repository dan masuk ke folder project
Setelah di clone, masuk ke docker dan change direktori (menggunakan cd <path ke folder kode>. 

### 2. docker-compose up -d
Perintah ini akan menjalankan seluruh container Docker sesuai konfigurasi di file docker-compose.yml.
Di sini, container Cassandra dan MongoDB akan otomatis berjalan di background (-d = detached mode).

### 3. docker exec -it cassandra-db1 cqlsh
Perintah ini mengakses terminal Cassandra (cqlsh) yang berjalan di dalam container cassandra-db1.
Query dengan mengetik perintah CQL (Cassandra Query Language) secara interaktif untuk mengelola database.

### 4. SOURCE '/cassandra-data/load_data1.cql'; 
Query ini akan melakukan load data cql kedalam database cassandra. pastikan sudah dalam terminal cassandra untuk menjalankan bagian ini, tapi apabila mau langsung dari terminal direktori awal, gunakan 'docker exec -it cassandra-db1 cqlsh -f /cassandra-data/load_data.cql'
untuk keluar dari terimnal cassandra, gunakan 'exit'

### 5. mongoimport --host localhost --db groceries --collection pegawai --file mongodb/load_data1.json --jsonArray
Sama seperti cassandra, query ini akan melakukan load data json yang sudah tertera dalam folder mongodb. Untuk mengakses terminal mongodb bisa menggunakan query 'docker exec -it mongodb-db1 mongosh'

### 6. streamlit run app.py
jalankan dashboard dengan menggunakan IDE sesuai selera dan masuk ke http://localhost:8501 untuk membuka dashboard

## Contoh Dashboard
![{7D6629BC-1DEF-45CE-8FDF-D5A5BD0A8F75}](https://github.com/user-attachments/assets/84c3b5c3-2750-41a5-8fee-a49110257531)
![{90756F52-B76C-4DAB-ACBC-5A3873D9B7FF}](https://github.com/user-attachments/assets/4a6ec023-6db6-4d1a-a543-ba547b56649f)


