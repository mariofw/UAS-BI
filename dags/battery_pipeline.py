from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import requests
import os

DB_CONN = 'postgresql://admin:admin@postgres:5432/battery_db'

def load_to_staging_layer():
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))

    df_activity = pd.read_csv('/data_source/log_aktivitas_baterai_hp.csv', sep=';')
    if 'battery_usage_percent' in df_activity.columns:
        df_activity['battery_usage_percent'] = df_activity['battery_usage_percent'].astype(str).str.replace(',', '.').astype(float)
    if 'activity_date' in df_activity.columns:
        df_activity['activity_date'] = pd.to_datetime(df_activity['activity_date'], format='%d/%m/%Y')
    df_activity.to_sql('stg_activity', engine, schema='staging', if_exists='replace', index=False)

    df_specs = pd.read_csv('/data_source/smartphones.csv', sep=';')
    df_specs.to_sql('stg_smartphones', engine, schema='staging', if_exists='replace', index=False)

    try:
        # Langsung extract dari tabel public.charging_logs di Postgres
        df_charging = pd.read_sql("SELECT * FROM public.charging_logs", engine)
        df_charging.to_sql('stg_charging_logs', engine, schema='staging', if_exists='replace', index=False)
        print("✅ Charging logs extracted directly from Postgres.")
    except Exception as e:
        print(f"⚠️ Warning: Could not extract from public.charging_logs: {e}")

    print("✅ Staging Layer Populated Successfully!")

def transform_to_dwh_layer():
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))

    df_stg_phones = pd.read_sql("SELECT * FROM staging.stg_smartphones", engine)
    df_stg_phones['device_id'] = [f'D{i+1:03}' for i in range(len(df_stg_phones))]
    
    df_dim_device = df_stg_phones[df_stg_phones['device_id'].isin(['D001', 'D003'])].copy()
    if 'model' in df_dim_device.columns:
        df_dim_device = df_dim_device.rename(columns={'model': 'device_name'})
    
    df_dim_device.to_sql('dim_device', engine, schema='datawarehouse', if_exists='replace', index=False)

    df_stg_activity = pd.read_sql("SELECT DISTINCT application_name, activity_category FROM staging.stg_activity", engine)
    df_stg_activity['application_id'] = df_stg_activity.index + 1
    df_stg_activity.to_sql('dim_application', engine, schema='datawarehouse', if_exists='replace', index=False)

    df_dates = pd.read_sql("SELECT DISTINCT activity_date FROM staging.stg_activity", engine)
    df_dates['activity_date'] = pd.to_datetime(df_dates['activity_date'])
    df_dim_date = pd.DataFrame({'full_date': df_dates['activity_date'].unique()})
    df_dim_date['date_id'] = df_dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.day_name()
    df_dim_date.to_sql('dim_date', engine, schema='datawarehouse', if_exists='replace', index=False)

    df_activity_full = pd.read_sql("SELECT * FROM staging.stg_activity", engine)
    df_dim_app = pd.read_sql("SELECT application_name, application_id FROM datawarehouse.dim_application", engine)
    df_dim_date_ref = pd.read_sql("SELECT full_date, date_id FROM datawarehouse.dim_date", engine)
    df_dim_date_ref['full_date'] = pd.to_datetime(df_dim_date_ref['full_date'])

    df_fact = pd.merge(df_activity_full, df_dim_app, on='application_name', how='left')
    df_fact['activity_date'] = pd.to_datetime(df_fact['activity_date'])
    df_fact = pd.merge(df_fact, df_dim_date_ref, left_on='activity_date', right_on='full_date', how='left')
    
    if 'device_id' not in df_fact.columns:
        df_fact_d1 = df_fact.copy(); df_fact_d1['device_id'] = 'D001'
        df_fact_d3 = df_fact.copy(); df_fact_d3['device_id'] = 'D003'
        df_fact = pd.concat([df_fact_d1, df_fact_d3], ignore_index=True)

    for col in ['screen_time_minutes', 'battery_usage_percent']:
        if col in df_fact.columns:
            df_fact[col] = df_fact[col].astype(str).str.replace(',', '.').astype(float)

    df_fact['weather_id'] = df_fact['date_id']
    df_fact.to_sql('fact_battery_usage', engine, schema='datawarehouse', if_exists='replace', index=False)
    print("✅ DWH Transformation Complete.")

def upload_to_seaweedfs():
    files_to_upload = ['/data_source/log_aktivitas_baterai_hp.csv', '/data_source/smartphones.csv', '/data_source/data charging.sql']
    seaweedfs_filer_url = 'http://seaweedfs:8333/data_source/'
    for file_path in files_to_upload:
        try:
            with open(file_path, 'rb') as f:
                file_name = file_path.split('/')[-1]
                requests.post(f"{seaweedfs_filer_url}{file_name}", files={'file': f})
        except:
            pass

default_args = {'owner': 'airflow', 'retries': 1}
with DAG('battery_data_pipeline_dimensional', default_args=default_args, start_date=datetime(2025, 1, 1), schedule_interval=None, catchup=False) as dag:
    task_staging = PythonOperator(task_id='ingest_to_staging_layer', python_callable=load_to_staging_layer)
    task_datalake = PythonOperator(task_id='upload_raw_to_datalake', python_callable=upload_to_seaweedfs)
    task_dwh = PythonOperator(task_id='transform_to_dwh_layer', python_callable=transform_to_dwh_layer)
    task_staging >> task_dwh
    task_datalake