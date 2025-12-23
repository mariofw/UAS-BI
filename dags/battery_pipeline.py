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
    """GOLD: Staging ke DWH"""
    engine = get_db_engine()
    with engine.begin() as conn: conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))

    # Dim Device
    df_dim_dev = pd.DataFrame([
        {'device_id': 'D001', 'device_name': 'Samsung Galaxy A56'},
        {'device_id': 'D003', 'device_name': 'iPhone 12 Pro Max'}
    ])
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

    # Fact Battery
    df_act = pd.read_sql("SELECT * FROM staging.stg_activity", engine)
    df_act['activity_date'] = pd.to_datetime(df_act['activity_date'])
    
    # Merge dengan Dim Date & App
    df_fact = df_act.merge(df_dim_date, left_on='activity_date', right_on='full_date', how='left')
    df_fact = df_fact.merge(df_app, on='application_name', how='left')
    
    # Clean up columns and ensure device_id is present
    # Hapus logika duplikasi hardcoded yang lama!
    df_fact_final = df_fact.copy()
    
    df_fact_final['weather_id'] = df_fact_final['date_id']
    df_fact_final.to_sql('fact_battery_usage', engine, schema='datawarehouse', if_exists='replace', index=False)

# --- DAG ---
with DAG('battery_data_pipeline_dimensional', start_date=datetime(2025, 1, 1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='ingest_to_lake', python_callable=ingest_to_datalake)
    t2 = PythonOperator(task_id='lake_to_staging', python_callable=load_from_lake_to_staging)
    t3 = PythonOperator(task_id='staging_to_dwh', python_callable=transform_to_dwh_layer)
    t1 >> t2 >> t3