import streamlit as st
from cassandra.cluster import Cluster
from pymongo import MongoClient
import ast
import pandas as pd
import altair as alt

# --- Koneksi ---
def connect_cassandra():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect('groceries')

def connect_mongo():
    client = MongoClient("mongodb://localhost:27017/")
    return client.groceries

# --- Header ---
st.title("🧠 Query Aggregator: Cassandra + MongoDB")
st.subheader("📊 Visualisasi Ringkasan Data")

# --- Visualisasi Cassandra: Total Penjualan per Produk ---
try:
    cass_session = connect_cassandra()
    query_cass_summary = """
        SELECT produk, SUM(jumlah) AS total_jumlah 
        FROM transaksi 
        GROUP BY produk ALLOW FILTERING;
    """
    result_cass = cass_session.execute(query_cass_summary)
    data_cass = [{"produk": row.produk, "total_jumlah": row.total_jumlah} for row in result_cass]
    df_cass = pd.DataFrame(data_cass)

    if not df_cass.empty:
        st.markdown("#### 📦 Total Penjualan per Produk (Cassandra)")
        chart_cass = alt.Chart(df_cass).mark_bar().encode(
            x=alt.X('produk:N', title='Produk'),
            y=alt.Y('total_jumlah:Q', title='Total Penjualan'),
            tooltip=['produk', 'total_jumlah']
        ).properties(height=300)
        st.altair_chart(chart_cass, use_container_width=True)
    else:
        st.info("Tidak ada data transaksi ditemukan di Cassandra.")
except Exception as e:
    st.error(f"❌ Gagal memuat data dari Cassandra: {e}")

# --- Visualisasi MongoDB: Total Jam Kerja Pegawai per Bulan ---
# --- Visualisasi MongoDB: Total Jam Kerja Pegawai per Bulan ---
try:
    mongo_db = connect_mongo()
    data_mongo = list(mongo_db.pegawai.find({}, {"_id": 0, "nama": 1, "jam_kerja": 1, "bulan": 1}))
    df_mongo = pd.DataFrame(data_mongo)

    if not df_mongo.empty:
        st.markdown("#### 👨‍💼 Total Jam Kerja Pegawai per Bulan (MongoDB)")

        urutan_bulan = [
            "Januari", "Februari", "Maret", "April", "Mei", "Juni",
            "Juli", "Agustus", "September", "Oktober", "November", "Desember"
        ]

        chart_mongo = alt.Chart(df_mongo).mark_bar().encode(
            x=alt.X('bulan:N', title='Bulan', sort=urutan_bulan),
            y=alt.Y('jam_kerja:Q', aggregate='sum', title='Total Jam Kerja'),
            color='bulan:N',
            tooltip=['bulan', 'jam_kerja']
        ).properties(height=300)
        st.altair_chart(chart_mongo, use_container_width=True)
    else:
        st.info("Tidak ada data pegawai ditemukan di MongoDB.")
except Exception as e:
    st.error(f"❌ Gagal memuat data dari MongoDB: {e}")


# --- Pemilihan Query ---
st.markdown("---")
opsi = st.radio("🔍 Pilih Jenis Query:", ["Query Cassandra (Custom)", "Query MongoDB (Custom)", "Query Gabungan (Custom)"])

# --- Cassandra Custom Query ---
if opsi == "Query Cassandra (Custom)":
    query = st.text_area("Masukkan CQL (Cassandra Query Language):", height=150)
    if st.button("Jalankan CQL"):
        try:
            session = connect_cassandra()
            result = session.execute(query)
            st.success("✅ Hasil Query:")
            for row in result:
                st.json(dict(row._asdict()))
        except Exception as e:
            st.error(f"❌ Error: {e}")

# --- MongoDB Custom Query ---
elif opsi == "Query MongoDB (Custom)":
    query = st.text_area("Masukkan Query MongoDB (JSON):", height=150, value='{"bulan": "April"}')
    if st.button("Jalankan MongoDB Query"):
        try:
            db = connect_mongo()
            q = ast.literal_eval(query)
            result = db.pegawai.find(q)
            st.success("✅ Hasil Query:")
            for doc in result:
                st.json(doc)
        except Exception as e:
            st.error(f"❌ Error: {e}")

# --- Gabungan Query ---
elif opsi == "Query Gabungan (Custom)":
    st.markdown("Masukkan kriteria untuk pencarian gabungan:")

    bulan = st.selectbox("Pilih Bulan Pegawai:", [
        "Januari", "Februari", "Maret", "April", "Mei", "Juni",
        "Juli", "Agustus", "September", "Oktober", "November", "Desember"
    ])

    produk = st.text_input("Masukkan Nama Produk:", value="kopi")
    tanggal_awal = st.date_input("Tanggal Awal Penjualan:")
    tanggal_akhir = st.date_input("Tanggal Akhir Penjualan:")

    if st.button("Jalankan Query"):
        try:
            # --- Query ke Cassandra ---
            session = connect_cassandra()
            query_cassandra = f"""
                SELECT tanggal, SUM(jumlah) AS total_jumlah 
                FROM transaksi 
                WHERE produk='{produk}'
            """
            if tanggal_awal and tanggal_akhir:
                query_cassandra += f" AND tanggal >= '{tanggal_awal}' AND tanggal <= '{tanggal_akhir}'"
            query_cassandra += " GROUP BY tanggal ALLOW FILTERING;"
            result_cassandra = session.execute(query_cassandra)

            st.subheader("📦 Total Penjualan Produk per Tanggal (Cassandra)")
            data_produk = [{"tanggal": row.tanggal, "total_jumlah": row.total_jumlah} for row in result_cassandra]
            df_produk = pd.DataFrame(data_produk)

            if not df_produk.empty:
                chart_produk = alt.Chart(df_produk).mark_bar().encode(
                    x='tanggal:T',
                    y='total_jumlah:Q'
                ).properties(title=f"Penjualan '{produk}' per Hari")
                st.altair_chart(chart_produk, use_container_width=True)

                # Tambahkan total penjualan produk pada tanggal yang dipilih
                total_penjualan = df_produk['total_jumlah'].sum()
                st.markdown(f"**Total {produk.capitalize()} pada Tanggal yang dipilih: {total_penjualan}**")

            else:
                st.warning("⚠️ Tidak ada data penjualan ditemukan.")

            # --- Query ke MongoDB ---
            db = connect_mongo()
            result_mongo = db.pegawai.find({"bulan": bulan}).sort("pelanggan_dilayani", -1).limit(1)

            st.subheader("👨‍💼 Pegawai Terbaik Bulan Ini (MongoDB)")
            data_pegawai = []
            for doc in result_mongo:
                st.json(doc)
                data_pegawai.append({
                    "nama": doc['nama'],
                    "pelanggan_dilayani": doc['pelanggan_dilayani']
                })

            if data_pegawai:
                df_pegawai = pd.DataFrame(data_pegawai)
                chart_pegawai = alt.Chart(df_pegawai).mark_bar().encode(
                    x='nama:N',
                    y='pelanggan_dilayani:Q'
                ).properties(title=f"Pegawai Terbaik - Bulan {bulan}")
                st.altair_chart(chart_pegawai, use_container_width=True)

        except Exception as e:
            st.error(f"❌ Error: {e}")
