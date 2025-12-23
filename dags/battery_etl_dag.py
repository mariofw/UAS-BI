from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import requests
import boto3
import json
import logging

# --- KONFIGURASI ---
DB_URI = 'postgresql://admin:admin@postgres:5432/battery_db'
S3_CONFIG = {'endpoint_url': 'http://seaweedfs:8333', 'aws_access_key_id': 'any', 'aws_secret_access_key': 'any'}
BUCKET_NAME = 'battery-lake'
API_KEY = "8dc59c09bc2d4075aa4130715251812"
CITY = "banjarmasin"
logger = logging.getLogger("airflow.task")

# --- TASKS ---
def fetch_and_store_weather_forecast():
    """BRONZE & GOLD: API ke Lake & DWH (Forecast)"""
    # Ambil API
    url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={CITY}&days=7"
    data = requests.get(url).json()

    # Simpan ke Lake (Bronze)
    s3 = boto3.client('s3', **S3_CONFIG)
    try: s3.create_bucket(Bucket=BUCKET_NAME)
    except: pass
    s3.put_object(Body=json.dumps(data), Bucket=BUCKET_NAME, Key=f'raw/weather_forecast_{datetime.now().strftime("%Y%m%d")}.json')

    # Proses ke DWH (Gold)
    forecast = data['forecast']['forecastday']
    weather_data = [{
        'date': d['date'],
        'max_temp_c': d['day']['maxtemp_c'],
        'min_temp_c': d['day']['mintemp_c'],
        'avg_temp_c': d['day']['avgtemp_c'],
        'condition_text': d['day']['condition']['text']
    } for d in forecast]
    
    df = pd.DataFrame(weather_data)
    df['date'] = pd.to_datetime(df['date'])
    df['weather_id'] = df['date'].dt.strftime('%Y%m%d').astype(int)
    
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))
        ids = tuple(df['weather_id'].tolist())
        if ids:
            try:
                conn.execute(text(f"DELETE FROM datawarehouse.dim_weather_forecast WHERE weather_id IN {ids}"))
            except:
                pass # Table doesn't exist yet
        df.to_sql('dim_weather_forecast', engine, schema='datawarehouse', if_exists='append', index=False)

def fetch_current_weather():
    """BRONZE & GOLD: API ke Lake & DWH (Realtime)"""
    # Ambil API Current
    url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}&aqi=no"
    data = requests.get(url).json()

    # Simpan ke Lake (Bronze)
    s3 = boto3.client('s3', **S3_CONFIG)
    try: s3.create_bucket(Bucket=BUCKET_NAME)
    except: pass
    s3.put_object(Body=json.dumps(data), Bucket=BUCKET_NAME, Key=f'raw/weather_current_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')

    # Proses ke DWH (Gold)
    current = data['current']
    weather_data = [{
        'timestamp': data['location']['localtime'],
        'temp_c': current['temp_c'],
        'feelslike_c': current['feelslike_c'],
        'humidity': current['humidity'],
        'condition_text': current['condition']['text'],
        'uv': current['uv']
    }]
    
    df = pd.DataFrame(weather_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))
        # Kita append saja agar punya history per jam/waktu run DAG
        df.to_sql('fact_weather_realtime', engine, schema='datawarehouse', if_exists='append', index=False)

def generate_recommendations_with_new_charging_data():
    """GOLD: Hitung Rekomendasi"""
    engine = create_engine(DB_URI)
    
    # Ambil Data Usage dengan Kategori
    try:
        df_usage = pd.read_sql("SELECT device_id, date_id, battery_usage_percent, activity_category FROM datawarehouse.fact_battery_usage", engine)
    except:
        # Fallback jika kolom activity_category belum ada di fact (tergantung pipeline)
        df_usage = pd.read_sql("SELECT device_id, date_id, battery_usage_percent FROM datawarehouse.fact_battery_usage", engine)
        df_usage['activity_category'] = 'General'

    try:
        df_weather_forecast = pd.read_sql("SELECT date, avg_temp_c, min_temp_c, max_temp_c FROM datawarehouse.dim_weather_forecast", engine)
    except:
        # Fallback empty forecast
        df_weather_forecast = pd.DataFrame(columns=['date', 'avg_temp_c', 'min_temp_c', 'max_temp_c'])
    
    # Ambil Weather Current Terakhir
    try:
        df_weather_current = pd.read_sql("SELECT * FROM datawarehouse.fact_weather_realtime ORDER BY timestamp DESC LIMIT 1", engine)
        current_temp = df_weather_current.iloc[0]['temp_c'] if not df_weather_current.empty else None
    except:
        current_temp = None

    # Ambil Data Spek Smartphone langsung dari Dimensi (Star Schema)
    df_dim_dev = pd.read_sql("SELECT * FROM datawarehouse.dim_device", engine)
    
    # Ambil Charging Logs (Restore missing line)
    df_charging = pd.read_sql("SELECT device_id, plug_in_time, plug_out_time, start_level, end_level FROM public.charging_logs", engine)

    df_weather_forecast['date'] = pd.to_datetime(df_weather_forecast['date'])
    df_charging['plug_in_time'] = pd.to_datetime(df_charging['plug_in_time'])
    df_charging['dur_minutes'] = (pd.to_datetime(df_charging['plug_out_time']) - df_charging['plug_in_time']).dt.total_seconds() / 60
    
    # Hitung Kecepatan Charge (% per menit)
    df_charging['charged_percent'] = df_charging['end_level'] - df_charging['start_level']
    df_charging['charge_speed'] = df_charging['charged_percent'] / df_charging['dur_minutes']
    df_charging['charge_speed'] = df_charging['charge_speed'].replace([np.inf, -np.inf], 0)

    all_rec = []
    
    # Loop per device
    for dev_id, df_dev in df_usage.groupby('device_id'):
        # Ambil spek lengkap dari Dim Device
        dev_row = df_dim_dev[df_dim_dev['device_id'] == dev_id].iloc[0]
        dev_name = dev_row['device_name']
        
        # Gunakan data dari CSV yang sudah ada di dimensi
        cap_mah = float(dev_row['battery_capacity']) if pd.notnull(dev_row['battery_capacity']) else 5000
        watt = float(dev_row['fast_charging']) if pd.notnull(dev_row['fast_charging']) else 15
        
        # Manual Override hanya untuk penyesuaian khusus jika diperlukan
        overrides = {
            'Samsung Galaxy A54 5G': {'cap': 5000, 'watt': 25},
            'Apple iPhone 12 Pro (256GB)': {'cap': 2815, 'watt': 20}
        }
        
        if dev_name in overrides:
            cap_mah = overrides[dev_name]['cap']
            watt = overrides[dev_name]['watt']
        
        # Hitung Waktu Teoretis Hardware (20% ke 80% = 60% charge)
        theoretical_min = int(((cap_mah * 3.7 / 1000) * 0.6) / (watt * 0.85) * 60)

        # 1. Analisis Kebiasaan (Real Data dari Charging Logs)
        dev_charging = df_charging[df_charging['device_id'] == dev_id].copy()
        
        # Fast Charging (Quantile 90%) vs Normal Charging (Average)
        if not dev_charging.empty and dev_charging['charge_speed'].max() > 0:
            fast_speed = dev_charging['charge_speed'].quantile(0.9)
            normal_speed = dev_charging['charge_speed'].mean()
            
            if fast_speed == 0: fast_speed = dev_charging['charge_speed'].max()
            
            est_time_fast = int(60 / fast_speed) if fast_speed > 0 else 60
            est_time_normal = int(60 / normal_speed) if normal_speed > 0 else 90
        else:
            est_time_fast = theoretical_min # Fallback ke teori
            est_time_normal = int(theoretical_min * 1.5)

        # Analisis Kategori Penggunaan Terboros
        top_cat = "Umum"
        if 'activity_category' in df_dev.columns:
            cat_usage = df_dev.groupby('activity_category')['battery_usage_percent'].sum()
            if not cat_usage.empty:
                top_cat = cat_usage.idxmax()

        if not dev_charging.empty:
            dev_charging['hour'] = dev_charging['plug_in_time'].dt.hour
            if not dev_charging['hour'].mode().empty:
                usual_charge_hour = dev_charging['hour'].mode().iloc[0]
            else:
                usual_charge_hour = 20
            
            # Inferensi: Jika user charge jam X, berarti jam X-1 baterai sudah kritis/low.
            # Rekomendasi: Charge jam X-3 (Sore/Siang) agar aman sampai malam.
            critical_time = (usual_charge_hour - 1) % 24
            rec_h = (usual_charge_hour - 3) % 24 
            
            expl_usage = (
                f"**📊 Analisis Kebiasaan (Behavioral Analysis):**\n"
                f"- **Spesifikasi:** {dev_name} ({cap_mah}mAh, {watt}W Max).\n"
                f"- **Pola:** Anda sering mengisi daya sekitar pukul **{usual_charge_hour:02d}:00**.\n"
                f"- **Aktivitas Dominan:** **'{top_cat}'**.\n\n"
                f"**✅ Saran Optimalisasi:**\n"
                f"- **Strategi:** Untuk mencegah baterai drop di jam-jam krusial menjelang waktu charge biasa (sekitar pukul **{critical_time:02d}:00**), "
                f"kami sarankan Anda mengisi daya lebih awal pada pukul **{rec_h:02d}:00**.\n"
                f"- **Estimasi Durasi:** Sekitar **{est_time_normal} menit**."
            )
        else:
            rec_h = 20
            expl_usage = f"Data device {dev_name} ({cap_mah}mAh) terdeteksi. Silakan lakukan pengisian daya agar sistem bisa menganalisis kebiasaan Anda."

        # 2. Jam Dingin (Forecast - Besok)
        # Cari forecast untuk besok
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        w_tomorrow = df_weather_forecast[df_weather_forecast['date'] == tomorrow]
        
        if not w_tomorrow.empty:
            w = w_tomorrow.iloc[0]
            temp_curve = np.sin(np.linspace(0, np.pi, 24))
            hourly_temp = w['min_temp_c'] + (w['max_temp_c'] - w['min_temp_c']) * temp_curve
            cool_h_tomorrow = np.argmin(hourly_temp)
            pred_msg_temp = f"{cool_h_tomorrow:02d}:00"
        elif not df_weather_forecast.empty:
            # Fallback ke hari pertama data jika besok tidak ada
            w = df_weather_forecast.iloc[0] 
            temp_curve = np.sin(np.linspace(0, np.pi, 24))
            hourly_temp = w['min_temp_c'] + (w['max_temp_c'] - w['min_temp_c']) * temp_curve
            cool_h_tomorrow = np.argmin(hourly_temp)
            pred_msg_temp = f"{cool_h_tomorrow:02d}:00 (Data Lama)"
        else:
            pred_msg_temp = "Data ramalan tidak tersedia"

        # Rekomendasi "SAAT INI" (Current Weather)
        if current_temp is not None:
            if current_temp < 28:
                explanation_temp = f"Suhu lingkungan saat ini sangat mendukung ({current_temp}°C). Ini adalah waktu ideal untuk mengisi daya karena suhu rendah membantu menjaga kesehatan sel baterai dalam jangka panjang."
                rec_charge_time_temp = "SANGAT DISARANKAN"
            elif 28 <= current_temp < 33:
                explanation_temp = f"Suhu berada di level moderat ({current_temp}°C). Anda masih bisa mengisi daya, namun pastikan tidak menggunakan aplikasi berat (seperti game) secara bersamaan untuk mencegah panas berlebih (overheating)."
                rec_charge_time_temp = "BISA (HATI-HATI)"
            else:
                explanation_temp = f"Peringatan: Suhu lingkungan cukup tinggi ({current_temp}°C). Mengisi daya saat ini berisiko mempercepat degradasi baterai. Disarankan menunggu hingga suhu lingkungan turun atau pindah ke ruangan yang lebih sejuk."
                rec_charge_time_temp = "TUNDA JIKA MUNGKIN"
        else:
            explanation_temp = "Data cuaca real-time tidak tersedia untuk analisis saat ini."
            rec_charge_time_temp = "N/A"
        
        all_rec.append({
            'device_id': dev_id,
            'recommended_charge_time_usage': f"{rec_h:02d}:00",
            'explanation_usage': expl_usage,
            'recommended_charge_time_temp': rec_charge_time_temp,
            'explanation_temp': explanation_temp,
            'prediction_tomorrow_temp': pred_msg_temp,
            'prediction_tomorrow_usage': f"{rec_h:02d}:00",
            'estimated_charging_time_minutes': est_time_fast,
            'estimated_charging_time_normal': est_time_normal,
            'average_charging_duration_minutes': df_charging[df_charging['device_id'] == dev_id]['dur_minutes'].mean(),
            'last_updated': datetime.now()
        })

    if all_rec:
        pd.DataFrame(all_rec).to_sql('final_recommendations', engine, schema='datawarehouse', if_exists='replace', index=False)

# --- DAG ---
with DAG('battery_optimization_etl_with_weather', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    t1_forecast = PythonOperator(task_id='fetch_weather_forecast', python_callable=fetch_and_store_weather_forecast)
    t1_current = PythonOperator(task_id='fetch_weather_current', python_callable=fetch_current_weather)
    t2 = PythonOperator(task_id='gen_recommendation', python_callable=generate_recommendations_with_new_charging_data)
    
    [t1_forecast, t1_current] >> t2
