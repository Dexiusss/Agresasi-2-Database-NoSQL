# Hanya digunakan untuk testing koneksi ke database

from cassandra.cluster import Cluster
from pymongo import MongoClient

# Koneksi ke Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('groceries')

# Koneksi ke MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['groceries']
pegawai = db['pegawai']

# Query 1: Total penjualan "kopi" di bulan Ramadhan (April)
query_cassandra = """
SELECT produk, SUM(jumlah) AS total_jumlah 
FROM transaksi 
WHERE produk='kopi' AND tanggal >= '2025-04-01' AND tanggal <= '2025-04-30' ALLOW FILTERING;
"""
rows = session.execute(query_cassandra)
for row in rows:
    print("Query 1 (Cassandra):", row.produk, row.total_jumlah)

# Query 2: Pegawai dengan jam kerja terbanyak di April
result_mongo = pegawai.find({"bulan": "April"}).sort("jam_kerja", -1).limit(1)
for doc in result_mongo:
    print("Query 2 (MongoDB):", doc)

# Query 3: Pegawai dengan pelanggan_dilayani terbanyak dan penjualan tertinggi (gabungan)
pegawai_top = pegawai.find().sort("pelanggan_dilayani", -1).limit(1)[0]
produk = 'kopi'
query_total = f"""
SELECT SUM(jumlah) AS total_jumlah 
FROM transaksi 
WHERE produk='{produk}' AND tanggal >= '2025-04-01' AND tanggal <= '2025-04-30' ALLOW FILTERING;
"""
result = session.execute(query_total)
print("Query 3 (Gabungan):")
print(f"Pegawai top: {pegawai_top['nama']} - {pegawai_top['pelanggan_dilayani']} pelanggan")
print(f"Total penjualan {produk} bulan April: {list(result)[0].total_jumlah}")
