import streamlit as st
from cassandra.cluster import Cluster
from pymongo import MongoClient
import json
import pandas as pd
import altair as alt
import time
import re

# --- Koneksi ---
def connect_cassandra():
    cluster = Cluster(['127.0.0.1'])
    return cluster.connect('groceries')

def connect_mongo():
    client = MongoClient("mongodb://localhost:27017/")
    return client.groceries

# --- Fungsi visualisasi umum ---
def show_shared_visualization():
    st.title("📊 Visualisasi Ringkasan Data & Perbandingan Kecepatan Query")

    dur_cass_non_opt = None
    dur_cass_opt = None
    dur_mongo_non_opt = None
    dur_mongo_opt = None

    
    # --- Cassandra Non-Optimized ---
    try:
        cass_session = connect_cassandra()
        table_non_opt = 'transaksi'
        start = time.perf_counter()
        query_non_opt = f"""
            SELECT produk, jumlah FROM {table_non_opt} ALLOW FILTERING;
        """
        result_non_opt = cass_session.execute(query_non_opt)
        dur_cass_non_opt = time.perf_counter() - start

        # Agregasi jumlah per produk manual
        df_non_opt = pd.DataFrame(result_non_opt, columns=["produk", "jumlah"])
        df_cass_non_opt = df_non_opt.groupby("produk", as_index=False).sum()

        if not df_cass_non_opt.empty:
            st.markdown("#### 📦 Total Penjualan per Produk (Cassandra Non-Optimized)")
            chart_cass_non_opt = alt.Chart(df_cass_non_opt).mark_bar().encode(
                x=alt.X('produk:N', title='Produk'),
                y=alt.Y('jumlah:Q', title='Total Penjualan'),
                tooltip=['produk', 'jumlah']
            ).properties(height=300)
            st.altair_chart(chart_cass_non_opt, use_container_width=True)
        else:
            st.info("Tidak ada data transaksi ditemukan di Cassandra Non-Optimized.")
    except Exception as e:
        st.error(f"❌ Cassandra Non-Optimized Error: {e}")

    # --- Cassandra Optimized ---
    try:
        cass_session = connect_cassandra()
        table_opt = 'transaksi_optimized'

        # Ambil semua data dari tabel
        start = time.perf_counter()
        query_opt = f"SELECT tanggal, produk, jumlah FROM {table_opt};"
        result_opt = cass_session.execute(query_opt)
        dur_cass_opt = time.perf_counter() - start

        df_opt = pd.DataFrame(result_opt, columns=["tanggal", "produk", "jumlah"])

        # Agregasi hanya berdasarkan kolom produk dan jumlah
        df_cass_opt = df_opt.groupby("produk", as_index=False)["jumlah"].sum()

        if not df_cass_opt.empty:
            st.markdown("#### ⚡ Total Penjualan per Produk (Cassandra Optimized)")
            chart_cass_opt = alt.Chart(df_cass_opt).mark_bar().encode(
                x='produk:N',
                y='jumlah:Q',
                tooltip=['produk', 'jumlah']
            ).properties(height=300)
            st.altair_chart(chart_cass_opt, use_container_width=True)
        else:
            st.info("Tidak ada data transaksi ditemukan di Cassandra Optimized.")
    except Exception as e:
        st.error(f"❌ Cassandra Optimized Error: {e}")
    # --- MongoDB Non-Optimized ---
    try:
        mongo_db = connect_mongo()
        coll_non_opt = "pegawai"
        start = time.perf_counter()
        data_mongo_non_opt = list(mongo_db[coll_non_opt].find({}, {"_id": 0, "nama": 1, "jam_kerja": 1, "bulan": 1}))
        dur_mongo_non_opt = time.perf_counter() - start

        df_mongo_non_opt = pd.DataFrame(data_mongo_non_opt)

        if not df_mongo_non_opt.empty:
            st.markdown("#### 👨‍💼 Total Jam Kerja Pegawai per Bulan (MongoDB Non-Optimized)")

            urutan_bulan = [
                "Januari", "Februari", "Maret", "April", "Mei", "Juni",
                "Juli", "Agustus", "September", "Oktober", "November", "Desember"
            ]

            chart_mongo_non_opt = alt.Chart(df_mongo_non_opt).mark_bar().encode(
                x=alt.X('bulan:N', title='Bulan', sort=urutan_bulan),
                y=alt.Y('jam_kerja:Q', aggregate='sum', title='Total Jam Kerja'),
                color='bulan:N',
                tooltip=['bulan', 'jam_kerja']
            ).properties(height=300)
            st.altair_chart(chart_mongo_non_opt, use_container_width=True)
        else:
            st.info("Tidak ada data pegawai ditemukan di MongoDB Non-Optimized.")
    except Exception as e:
        st.error(f"❌ MongoDB Non-Optimized Error: {e}")

    # --- MongoDB Optimized ---
    try:
        coll_opt = "pegawai_optimized"
        start = time.perf_counter()
        data_mongo_opt = list(mongo_db[coll_opt].find({}, {"_id": 0, "nama": 1, "jam_kerja": 1, "bulan": 1}))
        dur_mongo_opt = time.perf_counter() - start

        df_mongo_opt = pd.DataFrame(data_mongo_opt)

        if not df_mongo_opt.empty:
            st.markdown("#### 👨‍💼 Total Jam Kerja Pegawai per Bulan (MongoDB Optimized)")

            chart_mongo_opt = alt.Chart(df_mongo_opt).mark_bar().encode(
                x=alt.X('bulan:N', title='Bulan', sort=urutan_bulan),
                y=alt.Y('jam_kerja:Q', aggregate='sum', title='Total Jam Kerja'),
                color='bulan:N',
                tooltip=['bulan', 'jam_kerja']
            ).properties(height=300)
            st.altair_chart(chart_mongo_opt, use_container_width=True)
        else:
            st.info("Tidak ada data pegawai ditemukan di MongoDB Optimized.")
    except Exception as e:
        st.error(f"❌ MongoDB Optimized Error: {e}")

    # --- Visualisasi Perbandingan Kecepatan Query ---
    try:
        if None not in [dur_cass_non_opt, dur_cass_opt, dur_mongo_non_opt, dur_mongo_opt]:
            df_time = pd.DataFrame({
                'Database': ['Cassandra Non-Optimized', 'Cassandra Optimized', 'MongoDB Non-Optimized', 'MongoDB Optimized'],
                'Waktu (detik)': [dur_cass_non_opt, dur_cass_opt, dur_mongo_non_opt, dur_mongo_opt]
            })
            bar_time = alt.Chart(df_time).mark_bar().encode(
                x='Database',
                y='Waktu (detik)',
                color='Database'
            ).properties(title="Perbandingan Waktu Eksekusi Query")
            st.altair_chart(bar_time, use_container_width=True)
        else:
            st.info("⏳ Waktu eksekusi belum lengkap untuk semua query.")
    except Exception as e:
        st.error(f"❌ Error saat menampilkan perbandingan kecepatan: {e}")
        
def query_page(optimized=False):
    opsi = st.radio("🔍 Pilih Jenis Query:", [
        "Query Cassandra",
        "Query MongoDB",
        "Query Gabungan",
        "Query Gabungan (AGREGASI)"
    ])

    # Cassandra Custom Query
    if opsi == "Query Cassandra":
        default_query = 'SELECT produk, AVG(harga) AS rata_harga FROM transaksi GROUP BY produk ALLOW FILTERING;'
        if optimized:
            default_query = 'SELECT tanggal, produk, jumlah FROM transaksi_optimized WHERE tanggal = \'2025-01-01\';'
        query = st.text_area("Masukkan CQL (Cassandra Query Language):", height=150, value=default_query)

        if st.button("Jalankan CQL"):
            try:
                session = connect_cassandra()
                result = session.execute(query)
                st.success("✅ Hasil Query:")
                for row in result:
                    st.json(dict(row._asdict()))
            except Exception as e:
                st.error(f"❌ Error: {e}")

    # MongoDB Custom Query
    elif opsi == "Query MongoDB":
        query = st.text_area("Masukkan Query MongoDB (JSON):", height=150, value='{"jam_kerja": { "$gt": 150 }}')
        if st.button("Jalankan MongoDB Query"):
            try:
                db = connect_mongo()
                coll_name = "pegawai_optimized" if optimized else "pegawai"
                q = json.loads(query)
                result = db[coll_name].find(q)
                st.success("✅ Hasil Query:")
                for doc in result:
                    st.json(doc)

                if optimized:
                    stats = db[coll_name].find(q).explain("executionStats")
                    st.markdown("#### 🧪 Statistik Eksekusi (MongoDB Indexed)")
                    st.json(stats["executionStats"])
            except Exception as e:
                st.error(f"❌ Error: {e}")

    # Gabungan Query
    elif opsi == "Query Gabungan":
        st.markdown("Masukkan kriteria untuk pencarian gabungan:")

        bulan = st.selectbox("Pilih Bulan Pegawai:", [
            "Januari", "Februari", "Maret", "April", "Mei", "Juni",
            "Juli", "Agustus", "September", "Oktober", "November", "Desember"
        ])

        produk = st.text_input("Masukkan Nama Produk:", value="kopi")
        tanggal_awal = st.date_input("Tanggal Awal Penjualan:")
        tanggal_akhir = st.date_input("Tanggal Akhir Penjualan:")

        if st.button("Jalankan Query Gabungan"):
            try:
                session = connect_cassandra()
                table_name = 'transaksi_optimized' if optimized else 'transaksi'

                if optimized:
                    # Optimized: query per tanggal (partition key)
                    tanggal_list = pd.date_range(start=tanggal_awal, end=tanggal_akhir).to_pydatetime().tolist()
                    data_produk = []
                    for tanggal in tanggal_list:
                        tanggal_str = tanggal.strftime("%Y-%m-%d")
                        query_cassandra = f"""
                            SELECT produk, jumlah FROM {table_name}
                            WHERE tanggal = '{tanggal_str}'
                        """
                        result = session.execute(query_cassandra)
                        total = sum(row.jumlah for row in result if row.produk == produk)
                        data_produk.append({"tanggal": tanggal_str, "total_jumlah": total})
                else:
                    # Non-optimized: boleh pakai ALLOW FILTERING
                    query_cassandra = f"""
                        SELECT tanggal, SUM(jumlah) AS total_jumlah 
                        FROM {table_name} 
                        WHERE produk='{produk}' AND tanggal >= '{tanggal_awal}' AND tanggal <= '{tanggal_akhir}'
                        GROUP BY tanggal ALLOW FILTERING;
                    """
                    result = session.execute(query_cassandra)
                    data_produk = [{"tanggal": row.tanggal, "total_jumlah": row.total_jumlah} for row in result]

                # Visualisasi data Cassandra
                st.subheader("📦 Total Penjualan Produk per Tanggal (Cassandra)")
                df_produk = pd.DataFrame(data_produk)
                if not df_produk.empty:
                    chart_produk = alt.Chart(df_produk).mark_bar().encode(
                        x='tanggal:T',
                        y='total_jumlah:Q'
                    ).properties(title=f"Penjualan '{produk}' per Hari")
                    st.altair_chart(chart_produk, use_container_width=True)

                    total_penjualan = df_produk['total_jumlah'].sum()
                    st.markdown(f"**Total {produk.capitalize()} pada Tanggal yang dipilih: {total_penjualan}**")
                else:
                    st.warning("⚠️ Tidak ada data penjualan ditemukan.")

                # MongoDB Query
                db = connect_mongo()
                coll_name = "pegawai_optimized" if optimized else "pegawai"
                result_mongo = db[coll_name].find({"bulan": bulan}).sort("pelanggan_dilayani", -1).limit(1)

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

    # Gabungan Query via TextBox (AGREGASI)
    elif opsi == "Query Gabungan (AGREGASI)":
        example_query = (
            "SELECT tanggal, produk, jumlah FROM transaksi_optimized WHERE tanggal = '2025-04-01';\n"
            "AGREGASI\n"
            '{"jam_kerja": { "$gt": 150 }}'
        )
        query = st.text_area("Masukkan Query Gabungan (Gunakan pemisah 'AGREGASI')", height=250, value=example_query)

        if st.button("Jalankan Query Gabungan (AGREGASI)"):
            try:
                if "AGREGASI" not in query:
                    st.error("❌ Format salah. Harus mengandung kata 'AGREGASI' sebagai pemisah.")
                    return

                query_parts = query.split("AGREGASI")
                cql_query = query_parts[0].strip()
                mongo_query_str = query_parts[1].strip()

                # Cassandra Execution
                session = connect_cassandra()
                result_cql = session.execute(cql_query)
                st.subheader("📦 Hasil Query Cassandra")
                for row in result_cql:
                    st.json(dict(row._asdict()))

                # MongoDB Execution
                db = connect_mongo()
                coll_name = "pegawai_optimized" if optimized else "pegawai"
                mongo_query = json.loads(mongo_query_str)
                result_mongo = db[coll_name].find(mongo_query)
                st.subheader("👨‍💼 Hasil Query MongoDB")
                for doc in result_mongo:
                    st.json(doc)

                if optimized:
                    stats = db[coll_name].find(mongo_query).explain("executionStats")
                    st.markdown("#### 🧪 Statistik Eksekusi (MongoDB Indexed)")
                    st.json(stats["executionStats"])

            except Exception as e:
                st.error(f"❌ Error: {e}")

# --- Sidebar Navigasi Halaman ---
st.sidebar.title("Navigasi")
page = st.sidebar.radio("Pilih Halaman:", [
    "Page 1: Visualisasi Data",
    "Page 2: Query Non-Optimized",
    "Page 3: Query Optimized"
])

if page == "Page 1: Visualisasi Data":
    show_shared_visualization()

elif page == "Page 2: Query Non-Optimized":
    st.markdown("### 📌 Halaman 2: Query Cassandra Tanpa Optimisasi")
    query_page(optimized=False)

elif page == "Page 3: Query Optimized":
    st.markdown("### 📌 Halaman 3: Query Cassandra Teroptimisasi")
    st.markdown("#### Optimisasi berdasarkan tanggal dan produk")
    query_page(optimized=True)

