from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import boto3
from io import StringIO
import logging
import os

# --- KONFIGURASI ---
DB_URI = 'postgresql://admin:admin@postgres:5432/battery_db'
S3_CONFIG = {'endpoint_url': 'http://seaweedfs:8333', 'aws_access_key_id': 'any', 'aws_secret_access_key': 'any'}
BUCKET_NAME = 'battery-lake'
logger = logging.getLogger("airflow.task")

# --- HELPER ---
def get_s3_client():
    """Koneksi S3/Lake"""
    return boto3.client('s3', **S3_CONFIG)

def get_db_engine():
    """Koneksi Database"""
    return create_engine(DB_URI)

def upload_file_to_s3(file_path, s3_key, s3_client=None):
    """Kirim file ke Lake"""
    if not s3_client: s3_client = get_s3_client()
    if os.path.exists(file_path):
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key)

def upload_df_to_s3(df, s3_key, s3_client=None):
    """Kirim tabel ke Lake"""
    if not s3_client: s3_client = get_s3_client()
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET_NAME, Key=s3_key)

# --- TASKS ---
def ingest_to_datalake():
    """BRONZE: Sumber ke Lake"""
    s3 = get_s3_client()
    try: s3.create_bucket(Bucket=BUCKET_NAME)
    except: pass
    
    upload_file_to_s3('/data_source/log_aktivitas_baterai_hp.csv', 'raw/log_aktivitas.csv', s3)
    upload_file_to_s3('/data_source/smartphones.csv', 'raw/smartphones.csv', s3)
    
    engine = get_db_engine()
    df_charging = pd.read_sql("SELECT * FROM public.charging_logs", engine)
    upload_df_to_s3(df_charging, 'raw/charging_logs.csv', s3)

def load_from_lake_to_staging():
    """SILVER: Lake ke Staging"""
    s3 = get_s3_client()
    engine = get_db_engine()
    with engine.begin() as conn: conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))

    def load_s3_csv(s3_key, table, sep=',', transform=None):
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        df = pd.read_csv(obj['Body'], sep=sep)
        if transform: df = transform(df)
        df.to_sql(table, engine, schema='staging', if_exists='replace', index=False)

    def clean_act(df):
        df['battery_usage_percent'] = df['battery_usage_percent'].astype(str).str.replace(',', '.').astype(float)
        df['activity_date'] = pd.to_datetime(df['activity_date'], dayfirst=True)
        return df

    load_s3_csv('raw/log_aktivitas.csv', 'stg_activity', sep=';', transform=clean_act)
    load_s3_csv('raw/smartphones.csv', 'stg_smartphones', sep=';')
    load_s3_csv('raw/charging_logs.csv', 'stg_charging_logs')

def transform_to_dwh_layer():
    """GOLD: Staging ke DWH dengan Star Schema & Relationships"""
    engine = get_db_engine()
    
    # 1. Bersihkan tabel lama agar tidak konflik constraint saat replace
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))
        conn.execute(text("DROP TABLE IF EXISTS datawarehouse.fact_battery_usage CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS datawarehouse.dim_device CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS datawarehouse.dim_application CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS datawarehouse.dim_date CASCADE;"))

    # --- DIMENSIONS ---
    
    # Dim Device: Ambil data LENGKAP dari stg_smartphones
    target_models = ['Samsung Galaxy A54 5G', 'Apple iPhone 12 Pro (256GB)']
    
    # Ambil semua kolom
    df_specs = pd.read_sql(f"SELECT * FROM staging.stg_smartphones WHERE model IN {tuple(target_models)}", engine)
    
    # Urutkan agar ID konsisten (Samsung=D001, iPhone=D003) -> Kita map manual
    # Buat dictionary map ID
    id_map = {
        'Samsung Galaxy A54 5G': 'D001',
        'Apple iPhone 12 Pro (256GB)': 'D003'
    }
    
    df_dim_dev = df_specs.copy()
    df_dim_dev['device_id'] = df_dim_dev['model'].map(id_map)
    df_dim_dev = df_dim_dev.rename(columns={'model': 'device_name'})
    
    # Pindahkan device_id ke depan (opsional, untuk kerapian)
    cols = ['device_id', 'device_name'] + [c for c in df_dim_dev.columns if c not in ['device_id', 'device_name']]
    df_dim_dev = df_dim_dev[cols]
    
    df_dim_dev.to_sql('dim_device', engine, schema='datawarehouse', if_exists='replace', index=False)

    # Dim App
    df_app = pd.read_sql("SELECT DISTINCT application_name, activity_category FROM staging.stg_activity", engine)
    df_app['application_id'] = range(1, len(df_app) + 1)
    df_app.to_sql('dim_application', engine, schema='datawarehouse', if_exists='replace', index=False)

    # Dim Date
    df_dates = pd.read_sql("SELECT DISTINCT activity_date FROM staging.stg_activity", engine)
    df_dates['activity_date'] = pd.to_datetime(df_dates['activity_date'])
    df_dim_date = pd.DataFrame({'full_date': df_dates['activity_date'].unique()})
    df_dim_date['date_id'] = df_dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.day_name()
    df_dim_date.to_sql('dim_date', engine, schema='datawarehouse', if_exists='replace', index=False)

    # --- FACTS ---
    
    # Fact Battery
    df_act = pd.read_sql("SELECT * FROM staging.stg_activity", engine)
    df_act['activity_date'] = pd.to_datetime(df_act['activity_date'])
    
    # Merge dengan Dim Date & App
    df_fact = df_act.merge(df_dim_date, left_on='activity_date', right_on='full_date', how='left')
    df_fact = df_fact.merge(df_app, on='application_name', how='left')
    
    # Simulasikan Device ID (Distribusi Random untuk contoh data)
    import numpy as np
    device_ids = df_dim_dev['device_id'].tolist()
    # Kita set seed agar konsisten, atau random full
    np.random.seed(42) 
    df_fact['device_id'] = np.random.choice(device_ids, size=len(df_fact))

    df_fact_final = df_fact.copy()
    df_fact_final['weather_id'] = df_fact_final['date_id']
    
    # Pastikan hanya kolom yang relevan masuk ke Fact Table
    # (Opsional, tapi baik untuk kebersihan schema)
    
    df_fact_final.to_sql('fact_battery_usage', engine, schema='datawarehouse', if_exists='replace', index=False)

    # --- ADD CONSTRAINTS (STAR SCHEMA ENFORCEMENT) ---
    # Pandas .to_sql tidak membuat Primary/Foreign Keys, jadi kita alter manual.
    with engine.begin() as conn:
        # 1. Set Primary Keys
        conn.execute(text("ALTER TABLE datawarehouse.dim_device ADD PRIMARY KEY (device_id);"))
        conn.execute(text("ALTER TABLE datawarehouse.dim_application ADD PRIMARY KEY (application_id);"))
        conn.execute(text("ALTER TABLE datawarehouse.dim_date ADD PRIMARY KEY (date_id);"))
        
        # 2. Set Foreign Keys (Relasi Antar Tabel)
        # FK Device
        conn.execute(text("""
            ALTER TABLE datawarehouse.fact_battery_usage 
            ADD CONSTRAINT fk_fact_device 
            FOREIGN KEY (device_id) REFERENCES datawarehouse.dim_device(device_id);
        """))
        
        # FK Application
        conn.execute(text("""
            ALTER TABLE datawarehouse.fact_battery_usage 
            ADD CONSTRAINT fk_fact_app 
            FOREIGN KEY (application_id) REFERENCES datawarehouse.dim_application(application_id);
        """))
        
        # FK Date
        conn.execute(text("""
            ALTER TABLE datawarehouse.fact_battery_usage 
            ADD CONSTRAINT fk_fact_date 
            FOREIGN KEY (date_id) REFERENCES datawarehouse.dim_date(date_id);
        """))

# --- DAG ---
with DAG('battery_data_pipeline_dimensional', start_date=datetime(2025, 1, 1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='ingest_to_lake', python_callable=ingest_to_datalake)
    t2 = PythonOperator(task_id='lake_to_staging', python_callable=load_from_lake_to_staging)
    t3 = PythonOperator(task_id='staging_to_dwh', python_callable=transform_to_dwh_layer)
    t1 >> t2 >> t3