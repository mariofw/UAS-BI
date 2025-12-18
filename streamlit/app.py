import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Baterai Cerdas", layout="wide", page_icon="⚡")

# Koneksi ke Database
@st.cache_resource
def get_engine():
    return create_engine('postgresql://admin:admin@postgres:5432/battery_db')

engine = get_engine()

@st.cache_data(ttl=60)
def load_recommendations():
    try:
        df = pd.read_sql("SELECT * FROM datawarehouse.final_recommendations", engine)
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_fact_usage():
    try:
        df = pd.read_sql("SELECT * FROM datawarehouse.fact_battery_usage", engine)
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_weather_forecast():
    try:
        df = pd.read_sql("SELECT date, avg_temp_c, min_temp_c, max_temp_c FROM datawarehouse.dim_weather_forecast ORDER BY date ASC", engine)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception:
        return pd.DataFrame()

# --- Dashboard Utama ---
st.title("🔋 Sistem Rekomendasi Pengisian Daya Cerdas")

if st.button('🔄 Perbarui Data'):
    st.cache_data.clear()
    st.rerun()

with st.spinner('Memuat rekomendasi terbaru...'):
    df_rec = load_recommendations()
    df_fact = load_fact_usage()
    df_weather = load_weather_forecast()

if not df_rec.empty:
    # Re-introduce device selection
    device_map = {"Samsung (D001)": "D001", "iPhone (D003)": "D003"}
    device_name_selection = st.selectbox("Pilih Perangkat untuk Analisis:", options=list(device_map.keys()))
    selected_device_id = device_map[device_name_selection]

    device_rec = df_rec[df_rec['device_id'] == selected_device_id]

    if not device_rec.empty:
        latest_rec = device_rec.iloc[0]

        st.subheader(f"💡 Rekomendasi Pengisian Daya untuk {device_name_selection}")
        
        rec_col1, rec_col2 = st.columns(2)
        with rec_col1:
            st.info(f"**Rekomendasi Berdasarkan Penggunaan:** `{latest_rec['recommended_charge_time_usage']}`")
            st.write(latest_rec['explanation_usage'])

        with rec_col2:
            st.success(f"**Rekomendasi Berdasarkan Suhu:** `{latest_rec['recommended_charge_time_temp']}`")
            st.write(latest_rec['explanation_temp'])

        st.divider()

        col_charge_time, col_pred_tomorrow = st.columns(2)
        with col_charge_time:
            st.subheader("⚡ Estimasi Waktu Pengisian")
            st.warning(f"**Waktu Pengisian dari 20% ke 80%:** `{latest_rec['estimated_charging_time_minutes']}` menit")
        
        with col_pred_tomorrow:
            st.subheader("🔮 Prediksi untuk Besok")
            st.info(f"**Waktu Pengisian Berbasis Penggunaan:** `{latest_rec['prediction_tomorrow_usage']}`")
            st.success(f"**Waktu Pengisian Berbasis Suhu:** `{latest_rec['prediction_tomorrow_temp']}`")
        
        st.divider()
        
        st.subheader("📈 Tren Penggunaan & Cuaca")
        
        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            st.write("#### Penggunaan Baterai Mingguan")
            if not df_fact.empty:
                df_device_fact = df_fact[df_fact['device_id'] == selected_device_id].copy()
                if not df_device_fact.empty:
                    df_device_fact['full_date'] = pd.to_datetime(df_device_fact['date_id'], format='%Y%m%d')
                    weekly_usage = df_device_fact.groupby('full_date')['battery_usage_percent'].sum().reset_index()
                    weekly_usage.columns = ['Tanggal', 'Persentase Penggunaan Baterai']
                    
                    fig_usage = px.bar(weekly_usage, x='Tanggal', y='Persentase Penggunaan Baterai', title=f'Penggunaan Baterai Harian untuk {device_name_selection}')
                    st.plotly_chart(fig_usage, use_container_width=True)
                else:
                    st.info(f"Tidak ada data penggunaan baterai untuk {device_name_selection}.")
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
        st.warning(f"Tidak ada rekomendasi untuk {device_name_selection}.")
else:
    st.warning("Tidak ada data rekomendasi ditemukan. Jalankan DAG 'battery_optimization_etl_with_weather'.")
