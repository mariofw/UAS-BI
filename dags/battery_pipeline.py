from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import re
import requests

def create_all_tables():
    engine = create_engine('postgresql://admin:admin@postgres:5432/battery_db')

    with engine.begin() as conn:
        # Create schemas
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))

        # Execute the SQL script to create and populate devices and charging_logs
        with open('/data_source/data charging.sql', 'r') as f:
            sql_script = f.read()
            # Split script into individual statements
            for statement in sql_script.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
    
    # --- Create Dimension Tables ---
    
    # dim_device from smartphones.csv
    df_smartphones = pd.read_csv('/data_source/smartphones.csv', sep=';')
    df_smartphones['device_id'] = [f'D{i+1:03}' for i in range(len(df_smartphones))]
    df_smartphones[df_smartphones['device_id'].isin(['D001', 'D003'])].to_sql('dim_device', engine, schema='datawarehouse', if_exists='replace', index=False)

    # dim_application from log_aktivitas_baterai_hp.csv
    df_logs = pd.read_csv('/data_source/log_aktivitas_baterai_hp.csv', sep=';')
    df_apps = df_logs[['application_name', 'activity_category']].drop_duplicates().reset_index(drop=True)
    df_apps['application_id'] = df_apps.index + 1
    df_apps.to_sql('dim_application', engine, schema='datawarehouse', if_exists='replace', index=False)

    # dim_date from log_aktivitas_baterai_hp.csv
    df_logs['activity_date'] = pd.to_datetime(df_logs['activity_date'], format='%d/%m/%Y')
    df_dates = pd.DataFrame({'full_date': df_logs['activity_date'].unique()})
    df_dates['date_id'] = df_dates['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_dates['day_of_week'] = df_dates['full_date'].dt.day_name()
    df_dates.to_sql('dim_date', engine, schema='datawarehouse', if_exists='replace', index=False)

    # --- Create Fact Table ---
    
    # fact_battery_usage
    df_logs = df_logs[df_logs['device_id'].isin(['D001', 'D003'])]
    df_logs['battery_usage_percent'] = df_logs['battery_usage_percent'].astype(str).str.replace(',', '.').astype(float)
    
    df_logs = pd.merge(df_logs, df_apps, on='application_name', how='left')
    df_logs = pd.merge(df_logs, df_dates, left_on='activity_date', right_on='full_date', how='left')

    df_fact = df_logs[['device_id', 'application_id', 'date_id', 'screen_time_minutes', 'battery_usage_percent']]
    df_fact.to_sql('fact_battery_usage', engine, schema='datawarehouse', if_exists='replace', index=False)
    
    print("✅ All tables created and populated successfully.")

def upload_to_seaweedfs():
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
            # Don't raise error, just log it
            
with DAG('battery_data_pipeline_dimensional', start_date=datetime(2025, 1, 1), schedule_interval=None, catchup=False) as dag:
    
    create_tables_task = PythonOperator(
        task_id='create_all_tables',
        python_callable=create_all_tables
    )

    upload_to_datalake_task = PythonOperator(
        task_id='upload_raw_data_to_seaweedfs',
        python_callable=upload_to_seaweedfs
    )

    create_tables_task >> upload_to_datalake_task