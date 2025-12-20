from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import os

# Konfigurasi Database
DB_CONN = 'postgresql://admin:admin@postgres:5432/battery_db'

def load_to_staging_layer():
    """
    Task 1: Membaca Raw Data (CSV & SQL) dan memuatnya ke skema 'staging'.
    Melakukan pembersihan dasar (casting tipe data).
    """
    engine = create_engine(DB_CONN)
    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        # Simulasikan source DB dari file SQL (Public Schema)
        # Jika file SQL berisi INSERT ke public table, kita jalankan dulu
        try:
            with open('/data_source/data charging.sql', 'r') as f:
                sql_script = f.read()
                for statement in sql_script.split(';'):
                    if statement.strip():
                        conn.execute(text(statement))
            print("✅ Source DB (Public Schema) populated from SQL file.")
        except Exception as e:
            print(f"⚠️ Warning processing SQL file: {e}")

    # 1. Load CSV: Log Aktivitas
    print("Membaca log_aktivitas_baterai_hp.csv...")
    df_activity = pd.read_csv('/data_source/log_aktivitas_baterai_hp.csv', sep=';')
    
    # Cleaning: Fix format angka (koma ke titik)
    if 'battery_usage_percent' in df_activity.columns:
        df_activity['battery_usage_percent'] = (
            df_activity['battery_usage_percent']
            .astype(str)
            .str.replace(',', '.')
            .astype(float)
        )
    # Standardize Date format to datetime object here
    if 'activity_date' in df_activity.columns:
        df_activity['activity_date'] = pd.to_datetime(df_activity['activity_date'], format='%d/%m/%Y')

    df_activity.to_sql('stg_activity', engine, schema='staging', if_exists='replace', index=False)

    # 2. Load CSV: Smartphones
    print("Membaca smartphones.csv...")
    df_specs = pd.read_csv('/data_source/smartphones.csv', sep=';')
    df_specs.to_sql('stg_smartphones', engine, schema='staging', if_exists='replace', index=False)

    # 3. Load dari Public Schema (Charging Logs) ke Staging
    # Ini mensimulasikan ekstraksi dari database operasional ke staging area
    try:
        df_charging = pd.read_sql("SELECT * FROM public.charging_logs", engine)
        df_charging.to_sql('stg_charging_logs', engine, schema='staging', if_exists='replace', index=False)
        print("✅ Charging logs loaded to staging.")
    except Exception as e:
        print(f"⚠️ Could not load charging logs to staging (maybe table empty): {e}")

    # 4. Load Weather (Optional / Current Data)
    # Mengambil data cuaca saat ini sebagai snapshot metadata
    try:
        api_url = "http://api.weatherapi.com/v1/forecast.json?key=30b681ca44f646db86b140433251512&q=banjarmasin"
        weather_res = requests.get(api_url, timeout=5).json()
        temp_c = weather_res['current']['temp_c']
        condition = weather_res['current']['condition']['text']
        df_weather = pd.DataFrame([{'city': 'Banjarmasin', 'temp': temp_c, 'condition': condition, 'ingested_at': datetime.now()}])
        df_weather.to_sql('stg_weather_snapshot', engine, schema='staging', if_exists='replace', index=False)
    except Exception as e:
        print(f"⚠️ Weather API fetch failed in staging (non-critical): {e}")

    print("✅ Staging Layer Populated Successfully!")


def transform_to_dwh_layer():
    """
    Task 2: Membaca tabel 'staging', melakukan transformasi, dan memuat ke 'datawarehouse'.
    """
    engine = create_engine(DB_CONN)
    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))

    # --- DIMENSION: DEVICE ---
    # Sumber: staging.stg_smartphones
    print("Building dim_device...")
    df_stg_phones = pd.read_sql("SELECT * FROM staging.stg_smartphones", engine)
    
    # Logic Filter Device ID D001 & D003 (Sesuai logic lama)
    # Kita generate ID manual jika belum ada, atau pakai logika lama
    df_stg_phones['device_id'] = [f'D{i+1:03}' for i in range(len(df_stg_phones))]
    df_dim_device = df_stg_phones[df_stg_phones['device_id'].isin(['D001', 'D003'])].copy()
    
    df_dim_device.to_sql('dim_device', engine, schema='datawarehouse', if_exists='replace', index=False)

    # --- DIMENSION: APPLICATION ---
    # Sumber: staging.stg_activity
    print("Building dim_application...")
    df_stg_activity = pd.read_sql("SELECT DISTINCT application_name, activity_category FROM staging.stg_activity", engine)
    df_stg_activity['application_id'] = df_stg_activity.index + 1
    df_stg_activity.to_sql('dim_application', engine, schema='datawarehouse', if_exists='replace', index=False)

    # --- DIMENSION: DATE ---
    # Sumber: staging.stg_activity (activity_date)
    print("Building dim_date...")
    # Baca tanggal yang unik saja
    query_dates = "SELECT DISTINCT activity_date FROM staging.stg_activity"
    df_dates = pd.read_sql(query_dates, engine)
    df_dates['activity_date'] = pd.to_datetime(df_dates['activity_date']) # Ensure datetime
    
    df_dim_date = pd.DataFrame({'full_date': df_dates['activity_date'].unique()})
    df_dim_date['date_id'] = df_dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.day_name()
    df_dim_date.to_sql('dim_date', engine, schema='datawarehouse', if_exists='replace', index=False)

    # --- FACT: BATTERY USAGE ---
    # Sumber: staging.stg_activity + Joins ke Dimensions
    print("Building fact_battery_usage...")
    df_activity_full = pd.read_sql("SELECT * FROM staging.stg_activity", engine)
    # Re-apply device ID logic to join
    # Karena di source tidak ada device_id yang konsisten (hanya asumsi logic lama), kita ikuti logic lama:
    # Logic lama: df_logs = df_logs[df_logs['device_id'].isin(['D001', 'D003'])]
    # Asumsi: CSV sudah punya kolom device_id? Cek file CSV sebelumnya.
    # Jika CSV punya kolom device_id, kita pakai.
    
    # Ambil dim_application untuk dapat ID
    df_dim_app = pd.read_sql("SELECT application_name, application_id FROM datawarehouse.dim_application", engine)
    
    # Ambil dim_date untuk dapat ID
    df_dim_date_ref = pd.read_sql("SELECT full_date, date_id FROM datawarehouse.dim_date", engine)
    df_dim_date_ref['full_date'] = pd.to_datetime(df_dim_date_ref['full_date'])

    # Join Process
    # 1. Join App ID
    df_fact = pd.merge(df_activity_full, df_dim_app, on='application_name', how='left')
    
    # 2. Join Date ID
    # Pastikan tipe data sama
    df_fact['activity_date'] = pd.to_datetime(df_fact['activity_date'])
    df_fact = pd.merge(df_fact, df_dim_date_ref, left_on='activity_date', right_on='full_date', how='left')
    
    # 3. Filter Device
    if 'device_id' in df_fact.columns:
        df_fact = df_fact[df_fact['device_id'].isin(['D001', 'D003'])]
    
    # Select Columns for Fact Table
    final_cols = ['device_id', 'application_id', 'date_id', 'screen_time_minutes', 'battery_usage_percent']
    # Pastikan kolom ada
    available_cols = [c for c in final_cols if c in df_fact.columns]
    df_fact_final = df_fact[available_cols]

    df_fact_final.to_sql('fact_battery_usage', engine, schema='datawarehouse', if_exists='replace', index=False)
    
    print("✅ DWH Transformation Complete.")


def upload_to_seaweedfs():
    """
    Task 3: Backup Raw Files ke Data Lake (SeaweedFS).
    """
    files_to_upload = [
        '/data_source/log_aktivitas_baterai_hp.csv',
        '/data_source/smartphones.csv',
        '/data_source/data charging.sql'
    ]
    
    seaweedfs_filer_url = 'http://seaweedfs:8333/data_source/'

    for file_path in files_to_upload:
        try:
            with open(file_path, 'rb') as f:
                file_name = file_path.split('/')[-1]
                response = requests.post(f"{seaweedfs_filer_url}{file_name}", files={'file': f})
                response.raise_for_status()
                print(f"✅ Successfully uploaded {file_name} to SeaweedFS.")
        except FileNotFoundError:
            print(f"⚠️ Warning: {file_path} not found, skipping upload.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error uploading {file_path} to SeaweedFS: {e}")


# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    'battery_data_pipeline_dimensional', 
    default_args=default_args,
    start_date=datetime(2025, 1, 1), 
    schedule_interval=None, 
    catchup=False,
    tags=['etl', 'staging', 'dwh']
) as dag:
    
    # 1. Ingest Data ke Staging
    task_staging = PythonOperator(
        task_id='ingest_to_staging_layer',
        python_callable=load_to_staging_layer
    )

    # 2. Upload Raw ke Data Lake (Bisa jalan paralel dengan staging)
    task_datalake = PythonOperator(
        task_id='upload_raw_to_datalake',
        python_callable=upload_to_seaweedfs
    )

    # 3. Transform Staging ke DWH (Menunggu Staging selesai)
    task_dwh = PythonOperator(
        task_id='transform_to_dwh_layer',
        python_callable=transform_to_dwh_layer
    )

    # Alur:
    # Staging & Datalake jalan dulu
    # DWH jalan setelah Staging selesai (Data Lake tidak memblokir DWH)
    task_staging >> task_dwh
    task_datalake
