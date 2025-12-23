import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime, timedelta
import os

st.set_page_config(page_title="Monitor Baterai Cerdas", layout="wide", page_icon="⚡")

@st.cache_resource
def get_engine():
    db_url = os.getenv('DB_URL', 'postgresql://admin:admin@localhost:5435/battery_db')
    return create_engine(db_url)

engine = get_engine()

@st.cache_data(ttl=60)
def load_recommendations():
    # Fetch latest ETL output
    try:
        df = pd.read_sql("SELECT * FROM datawarehouse.final_recommendations", engine)
        return df
    except Exception as e:
        st.error(f"Gagal memuat rekomendasi: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_fact_usage():
    try:
        df = pd.read_sql("SELECT * FROM datawarehouse.fact_battery_usage", engine)
        return df
    except Exception as e:
        st.error(f"Gagal memuat data penggunaan: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_weather_forecast():
    try:
        df = pd.read_sql("SELECT date, avg_temp_c, min_temp_c, max_temp_c FROM datawarehouse.dim_weather_forecast ORDER BY date ASC", engine)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        st.error(f"Gagal memuat data cuaca: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_current_weather():
    try:
        df = pd.read_sql("SELECT * FROM datawarehouse.fact_weather_realtime ORDER BY timestamp DESC LIMIT 1", engine)
        return df
    except Exception as e:
        # Mungkin tabel belum dibuat jika DAG baru belum jalan
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_dim_device():
    try:
        df = pd.read_sql("SELECT device_id, device_name FROM datawarehouse.dim_device", engine)
        return df
    except Exception as e:
        st.error(f"Gagal memuat data device: {e}")
        return pd.DataFrame()

st.title("🔋 Sistem Rekomendasi Pengisian Daya Cerdas")

if st.button('🔄 Perbarui Data'):
    st.cache_data.clear()
    st.rerun()

with st.spinner('Memuat rekomendasi terbaru...'):
    df_rec = load_recommendations()
    df_fact = load_fact_usage()
    df_weather = load_weather_forecast()
    df_current_weather = load_current_weather()
    df_devices = load_dim_device()

# Tampilkan Cuaca Saat Ini
if not df_current_weather.empty:
    curr = df_current_weather.iloc[0]
    st.metric(
        label=f"Cuaca Saat Ini ({curr['condition_text']})", 
        value=f"{curr['temp_c']} °C", 
        delta=f"Terasa {curr['feelslike_c']} °C",
        delta_color="off"
    )

if not df_rec.empty and not df_devices.empty:
    # Buat mapping ID ke Nama Device dari Database
    device_map = dict(zip(df_devices['device_id'], df_devices['device_name']))
    
    # Ambil daftar ID device yang memiliki rekomendasi
    available_device_ids = df_rec['device_id'].unique()
    
    # Filter hanya device yang ada di mapping (untuk keamanan)
    valid_devices = [d_id for d_id in available_device_ids if d_id in device_map]
    
    if not valid_devices:
        st.warning("Data rekomendasi tersedia, namun tidak ada perangkat yang cocok di database.")
    else:
        # Dropdown menampilkan Nama, tapi mengembalikan ID
        selected_device_id = st.selectbox(
            "Pilih Perangkat:", 
            options=valid_devices,
            format_func=lambda x: device_map.get(x, f"Unknown ID: {x}")
        )

        # Ambil rekomendasi untuk ID yang dipilih
        device_rec = df_rec[df_rec['device_id'] == selected_device_id]

        if not device_rec.empty:
            latest_rec = device_rec.iloc[0]
            # Nama device untuk judul
            device_display_name = device_map.get(selected_device_id, selected_device_id)

            st.subheader(f"💡 Rekomendasi Pengisian Daya untuk {device_display_name}")
            
            rec_col1, rec_col2 = st.columns(2)
            with rec_col1:
                st.info(f"**Waktu Pengisian Direkomendasikan (Penggunaan):** `{latest_rec['recommended_charge_time_usage']}`")
                st.write(latest_rec['explanation_usage'])

            with rec_col2:
                st.success(f"**Waktu Pengisian Direkomendasikan (Suhu):** `{latest_rec['recommended_charge_time_temp']}`")
                st.write(latest_rec['explanation_temp'])

            st.divider()

            col_charge_time, col_pred_tomorrow = st.columns(2)
            with col_charge_time:
                st.subheader("⚡ Estimasi Waktu Pengisian (20% → 80%)")
                st.metric("Estimasi Durasi", f"{latest_rec['estimated_charging_time_minutes']} menit")
            
            with col_pred_tomorrow:
                st.subheader("🔮 Prediksi untuk Besok")
                st.info(f"**Prediksi Waktu Pengisian (Penggunaan):** `{latest_rec['prediction_tomorrow_usage']}`")
                st.success(f"**Prediksi Waktu Pengisian (Suhu):** `{latest_rec['prediction_tomorrow_temp']}`")
            
            st.divider()
            
            st.subheader("📈 Tren Penggunaan & Cuaca")
            
            # Visuals: Usage trends & Weather forecast
            chart_col1, chart_col2 = st.columns(2)

            with chart_col1:
                st.write("#### Penggunaan Baterai Mingguan")
                if not df_fact.empty:
                    df_device_fact = df_fact[df_fact['device_id'] == selected_device_id].copy()
                    if not df_device_fact.empty:
                        df_device_fact['full_date'] = pd.to_datetime(df_device_fact['date_id'], format='%Y%m%d')
                        weekly_usage = df_device_fact.groupby('full_date')['battery_usage_percent'].sum().reset_index()
                        weekly_usage.columns = ['Tanggal', 'Persentase Penggunaan Baterai']
                        
                        fig_usage = px.bar(weekly_usage, x='Tanggal', y='Persentase Penggunaan Baterai', title=f'Penggunaan Baterai Harian untuk {device_display_name}')
                        st.plotly_chart(fig_usage, use_container_width=True)
                    else:
                        st.info(f"Tidak ada data penggunaan baterai untuk {device_display_name}.")
                else:
                    st.info("Tidak ada data penggunaan baterai.")

            with chart_col2:
                st.write("#### Ramalan Suhu 7 Hari ke Depan")
                if not df_weather.empty:
                    fig_weather = go.Figure()
                    fig_weather.add_trace(go.Scatter(x=df_weather['date'], y=df_weather['avg_temp_c'], mode='lines+markers', name='Suhu Rata-rata'))
                    fig_weather.add_trace(go.Scatter(x=df_weather['date'], y=df_weather['min_temp_c'], mode='lines', name='Min Suhu', line=dict(dash='dash')))
                    fig_weather.add_trace(go.Scatter(x=df_weather['date'], y=df_weather['max_temp_c'], mode='lines', name='Max Suhu', line=dict(dash='dash')))
                    fig_weather.update_layout(title='Suhu Rata-rata, Min & Max Harian (7 Hari ke Depan)',
                                              xaxis_title='Tanggal',
                                              yaxis_title='Suhu (°C)')
                    st.plotly_chart(fig_weather, use_container_width=True)
                else:
                    st.info("Tidak ada data ramalan cuaca. Pastikan API key valid.")

        else:
            st.warning(f"Tidak ada rekomendasi untuk {selected_device_id}.")
else:
    st.warning("Tidak ada data rekomendasi ditemukan. Jalankan DAG 'battery_optimization_etl_with_weather'.")