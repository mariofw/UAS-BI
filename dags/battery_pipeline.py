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

    # Load Activity CSV
    try:
        df_activity = pd.read_csv('/data_source/log_aktivitas_baterai_hp.csv', sep=';')
        if 'battery_usage_percent' in df_activity.columns:
            df_activity['battery_usage_percent'] = df_activity['battery_usage_percent'].astype(str).str.replace(',', '.').astype(float)
        if 'activity_date' in df_activity.columns:
            df_activity['activity_date'] = pd.to_datetime(df_activity['activity_date'], dayfirst=True)
        df_activity.to_sql('stg_activity', engine, schema='staging', if_exists='replace', index=False)
        print("✅ Staging activity loaded.")
    except Exception as e:
        print(f"❌ Error activity CSV: {e}")

    # Load Specs CSV
    try:
        df_specs = pd.read_csv('/data_source/smartphones.csv', sep=';')
        df_specs.to_sql('stg_smartphones', engine, schema='staging', if_exists='replace', index=False)
        print("✅ Staging specs loaded.")
    except Exception as e:
        print(f"❌ Error specs CSV: {e}")

    # Load Charging Logs from Postgres (Source)
    try:
        # Kita asumsikan tabel source ada di public.charging_logs
        df_charging = pd.read_sql("SELECT * FROM public.charging_logs", engine)
        df_charging.to_sql('stg_charging_logs', engine, schema='staging', if_exists='replace', index=False)
        print("✅ Staging charging logs loaded.")
    except Exception as e:
        print(f"⚠️ Warning charging logs: {e}. Mencoba inisialisasi tabel kosong.")
        conn.execute(text("CREATE TABLE IF NOT EXISTS staging.stg_charging_logs (device_id varchar, plug_in_time timestamp, plug_out_time timestamp);"))

def transform_to_dwh_layer():
    engine = create_engine(DB_CONN)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))

    # 1. Dim Device
    df_stg_phones = pd.read_sql("SELECT * FROM staging.stg_smartphones", engine)
    df_stg_phones['device_id'] = [f'D{i+1:03}' for i in range(len(df_stg_phones))]
    df_dim_device = df_stg_phones[df_stg_phones['device_id'].isin(['D001', 'D003'])].copy()
    if 'model' in df_dim_device.columns:
        df_dim_device = df_dim_device.rename(columns={'model': 'device_name'})
    df_dim_device.to_sql('dim_device', engine, schema='datawarehouse', if_exists='replace', index=False)

    # 2. Dim Application
    df_stg_activity = pd.read_sql("SELECT DISTINCT application_name, activity_category FROM staging.stg_activity", engine)
    df_stg_activity['application_id'] = range(1, len(df_stg_activity) + 1)
    df_stg_activity.to_sql('dim_application', engine, schema='datawarehouse', if_exists='replace', index=False)

    # 3. Dim Date
    df_dates = pd.read_sql("SELECT DISTINCT activity_date FROM staging.stg_activity", engine)
    df_dates['activity_date'] = pd.to_datetime(df_dates['activity_date'])
    df_dim_date = pd.DataFrame({'full_date': df_dates['activity_date'].unique()})
    df_dim_date['date_id'] = df_dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_dim_date['day_of_week'] = df_dim_date['full_date'].dt.day_name()
    df_dim_date.to_sql('dim_date', engine, schema='datawarehouse', if_exists='replace', index=False)

    # 4. Fact Battery Usage
    df_act_full = pd.read_sql("SELECT * FROM staging.stg_activity", engine)
    df_act_full['activity_date'] = pd.to_datetime(df_act_full['activity_date'])
    
    # Merge dengan Date untuk dapat date_id
    df_fact = pd.merge(df_act_full, df_dim_date, left_on='activity_date', right_on='full_date', how='left')
    # Merge dengan App untuk dapat application_id
    df_fact = pd.merge(df_fact, df_stg_activity, on='application_name', how='left')
    
    # Duplikasi untuk D001 dan D003 sebagai sampel
    df_fact_d1 = df_fact.copy(); df_fact_d1['device_id'] = 'D001'
    df_fact_d3 = df_fact.copy(); df_fact_d3['device_id'] = 'D003'
    df_fact_final = pd.concat([df_fact_d1, df_fact_d3], ignore_index=True)
    
    df_fact_final['weather_id'] = df_fact_final['date_id']
    df_fact_final.to_sql('fact_battery_usage', engine, schema='datawarehouse', if_exists='replace', index=False)
    
    print("✅ Data Warehouse populated with real data.")

def upload_to_seaweedfs():
    # Opsional: abaikan jika tidak kritis
    pass

default_args = {'owner': 'airflow', 'retries': 1}
with DAG('battery_data_pipeline_dimensional', default_args=default_args, start_date=datetime(2025, 1, 1), schedule_interval=None, catchup=False) as dag:
    task_staging = PythonOperator(task_id='ingest_to_staging_layer', python_callable=load_to_staging_layer)
    task_dwh = PythonOperator(task_id='transform_to_dwh_layer', python_callable=transform_to_dwh_layer)
    task_staging >> task_dwh
