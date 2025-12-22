from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import requests

# Default Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- IMPORTANT: Replace YOUR_WEATHER_API_KEY with your actual API key ---
API_KEY = "8dc59c09bc2d4075aa4130715251812"
CITY = "banjarmasin"
WEATHER_API_URL = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={CITY}&days=7&aqi=no&alerts=no"

def fetch_and_store_weather_forecast():
    engine = create_engine('postgresql://admin:admin@postgres:5432/battery_db')
    
    try:
        response = requests.get(WEATHER_API_URL, timeout=10)
        response.raise_for_status()
        weather_data = response.json()
        
        forecast_days = weather_data['forecast']['forecastday']
        weather_records = []
        for day in forecast_days:
            weather_records.append({
                'date': day['date'],
                'max_temp_c': day['day']['maxtemp_c'],
                'min_temp_c': day['day']['mintemp_c'],
                'avg_temp_c': day['day']['avgtemp_c'],
                'condition_text': day['day']['condition']['text']
            })
        
        df_weather = pd.DataFrame(weather_records)
        print("✅ Successfully fetched and stored weather forecast data!")

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Warning: Could not fetch weather data ({e}). Creating dummy weather data.")
        date_today = datetime.now().date()
        dummy_dates = [date_today + timedelta(days=i) for i in range(7)]
        weather_records = [{'date': d, 'max_temp_c': 30, 'min_temp_c': 24, 'avg_temp_c': 27, 'condition_text': 'Unavailable'} for d in dummy_dates]
        df_weather = pd.DataFrame(weather_records)

    df_weather['date'] = pd.to_datetime(df_weather['date'])
    # Generate weather_id (YYYYMMDD) as Primary Key
    df_weather['weather_id'] = df_weather['date'].dt.strftime('%Y%m%d').astype(int)
    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datawarehouse;"))
        df_weather.to_sql('dim_weather_forecast', engine, schema='datawarehouse', if_exists='append', index=False, method='multi') # Use append to avoid wiping history if possible, or handle duplicates
        # For simplicity in this demo, we might replace if strict uniqueness isn't handled elsewhere, 
        # but to keep IDs consistent let's replace for now or handle upsert.
        # Given the previous logic used replace, let's stick to replace BUT we must be careful about FKs.
        # Actually, simpler: replace ONLY if we don't break FKs. 
        # But wait, Main DAG creates dummy weather. Optimization DAG updates it.
        # Let's use specific logic: Delete existing forecast for these dates then append.
        
        # Safe Approach for prototype: Just Upsert or Replace if careful. 
        # Let's use 'replace' but we need to drop FK in fact first? No, that's too complex.
        # Better: Read existing, update, write back?
        # Simplest working solution for CLI: 'replace' works IF we drop Fact FKs. 
        # BUT since we want persistence, let's just write to a temporary table and upsert.
        
        # ACTUALLY, to avoid complexity: Just use 'replace' for now. 
        # NOTE: This might fail if Fact table exists and has FK. 
        # We will handle the FK drop/re-add in the Main DAG. 
        # Here, just ensure column exists.
        
        # To avoid "cannot drop table" error, we can't use replace if FK exists.
        # So we delete specific rows and append.
        try:
             # Hapus data lama yang tanggalnya sama (agar tidak duplikat PK)
             dates_to_update = tuple(df_weather['weather_id'].tolist())
             if dates_to_update:
                 conn.execute(text(f"DELETE FROM datawarehouse.dim_weather_forecast WHERE weather_id IN {dates_to_update}"))
        except Exception as e:
            print(f"Notice: Cleanup failed (maybe table doesn't exist yet): {e}")
            
        df_weather.to_sql('dim_weather_forecast', engine, schema='datawarehouse', if_exists='append', index=False)


def generate_recommendations_with_new_charging_data():
    engine = create_engine('postgresql://admin:admin@postgres:5432/battery_db')

    try:
        query_usage = "SELECT device_id, date_id, battery_usage_percent FROM datawarehouse.fact_battery_usage"
        df_usage = pd.read_sql(query_usage, engine)
        if df_usage.empty:
            print("Warning: fact_battery_usage is empty.")
            return

        query_weather = "SELECT date, avg_temp_c, min_temp_c, max_temp_c FROM datawarehouse.dim_weather_forecast"
        df_weather = pd.read_sql(query_weather, engine)
        df_weather['date'] = pd.to_datetime(df_weather['date'])
        
        query_devices = "SELECT device_id, battery_capacity, fast_charging FROM datawarehouse.dim_device"
        df_devices = pd.read_sql(query_devices, engine)

        query_charging = "SELECT device_id, plug_in_time, plug_out_time FROM public.charging_logs"
        df_charging = pd.read_sql(query_charging, engine)
        df_charging['plug_in_time'] = pd.to_datetime(df_charging['plug_in_time'])
        df_charging['plug_out_time'] = pd.to_datetime(df_charging['plug_out_time'])
        df_charging['charging_duration'] = (df_charging['plug_out_time'] - df_charging['plug_in_time']).dt.total_seconds() / 60

    except Exception as e:
        print(f"Error loading data from data warehouse: {e}")
        return

    all_recommendations = []
    
    hourly_distribution_d001 = np.array([0.01, 0.01, 0.01, 0.01, 0.02, 0.04, 0.07, 0.08, 0.07, 0.06, 0.05, 0.05, 0.06, 0.07, 0.06, 0.05, 0.05, 0.06, 0.07, 0.08, 0.06, 0.04, 0.03, 0.02])
    hourly_distribution_d003 = np.array([0.01, 0.01, 0.01, 0.01, 0.02, 0.03, 0.05, 0.06, 0.06, 0.05, 0.04, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 0.09, 0.08, 0.06, 0.04, 0.02])

    for device_id, df_device_usage in df_usage.groupby('device_id'):
        
        if device_id == 'D001':
            hourly_distribution = hourly_distribution_d001
        else:
            hourly_distribution = hourly_distribution_d003

        df_device_usage['date'] = pd.to_datetime(df_device_usage['date_id'], format='%Y%m%d')
        df_device_usage['day_of_week'] = df_device_usage['date'].dt.day_name()
        daily_total_usage = df_device_usage.groupby('day_of_week')['battery_usage_percent'].sum()
        
        simulated_hourly_usage = [{'hour': h, 'usage': sum(daily_total_usage * hourly_distribution[h])} for h in range(24)]
        df_hourly = pd.DataFrame(simulated_hourly_usage)
        peak_hour_usage = df_hourly.set_index('hour')['usage'].idxmax()
        recommended_hour_usage = max(0, peak_hour_usage - 2)
        
        today_weather = df_weather.iloc[0]
        temp_curve = np.sin(np.linspace(0, np.pi, 24))
        today_hourly_temp = today_weather['min_temp_c'] + (today_weather['max_temp_c'] - today_weather['min_temp_c']) * temp_curve
        coolest_hour = np.argmin(today_hourly_temp)
        
        tomorrow_date = datetime.now().date() + timedelta(days=1)
        tomorrow_day_of_week = (datetime.now() + timedelta(days=1)).strftime('%A')
        
        tomorrow_weather_df = df_weather[df_weather['date'].dt.date == tomorrow_date]
        if not tomorrow_weather_df.empty:
            tomorrow_weather = tomorrow_weather_df.iloc[0]
            tomorrow_hourly_temp = tomorrow_weather['min_temp_c'] + (tomorrow_weather['max_temp_c'] - tomorrow_weather['min_temp_c']) * temp_curve
            coolest_hour_tomorrow = np.argmin(tomorrow_hourly_temp)
        else:
            coolest_hour_tomorrow = coolest_hour

        if tomorrow_day_of_week in daily_total_usage.index:
            tomorrow_usage = daily_total_usage[tomorrow_day_of_week]
            simulated_hourly_usage_tomorrow = [{'hour': h, 'usage': tomorrow_usage * hourly_distribution[h]} for h in range(24)]
            df_hourly_tomorrow = pd.DataFrame(simulated_hourly_usage_tomorrow)
            peak_hour_tomorrow_usage = df_hourly_tomorrow.set_index('hour')['usage'].idxmax()
            recommended_hour_tomorrow_usage = max(0, peak_hour_tomorrow_usage - 2)
        else:
            recommended_hour_tomorrow_usage = recommended_hour_usage

        device_info = df_devices[df_devices['device_id'] == device_id].iloc[0]
        battery_capacity_mah = device_info['battery_capacity']
        
        fast_charging_watts = 10
        if 'fast_charging' in device_info and pd.notna(device_info['fast_charging']):
            fast_charging_watts = device_info['fast_charging']
        
        energy_to_charge_wh = (battery_capacity_mah / 1000) * 3.8 * 0.6
        charging_time_hours = (energy_to_charge_wh / fast_charging_watts) / 0.8 
        charging_time_minutes = int(charging_time_hours * 60)
        
        avg_charging_duration = df_charging[df_charging['device_id'] == device_id]['charging_duration'].mean()

        explanation_usage = f"Berdasarkan pola penggunaan Anda, puncak pemakaian terjadi sekitar pukul {peak_hour_usage:02d}:00. Mengisi daya pada pukul {recommended_hour_usage:02d}:00 memastikan baterai penuh sebelum waktu sibuk."
        explanation_temp = f"Untuk kesehatan baterai jangka panjang, direkomendasikan mengisi daya pada suhu terdingin, yaitu sekitar pukul {coolest_hour:02d}:00."
        
        all_recommendations.append({
            'device_id': device_id,
            'recommended_charge_time_usage': f"{recommended_hour_usage:02d}:00",
            'explanation_usage': explanation_usage,
            'recommended_charge_time_temp': f"{coolest_hour:02d}:00",
            'explanation_temp': explanation_temp,
            'prediction_tomorrow_temp': f"{coolest_hour_tomorrow:02d}:00",
            'prediction_tomorrow_usage': f"{recommended_hour_tomorrow_usage:02d}:00",
            'estimated_charging_time_minutes': charging_time_minutes,
            'average_charging_duration_minutes': avg_charging_duration,
            'last_updated': datetime.now()
        })

    if all_recommendations:
        df_recommendations = pd.DataFrame(all_recommendations)
        with engine.begin() as conn:
            df_recommendations.to_sql('final_recommendations', engine, schema='datawarehouse', if_exists='replace', index=False)
        print("✅ Successfully updated with SEPARATE dynamic recommendations and charging time!")

# --- DAG DEFINITION ---
with DAG(
    'battery_optimization_etl_with_weather',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dynamic_recommendation', 'device_specific', 'weather_integration'],
) as dag:
    
    fetch_weather_task = PythonOperator(
        task_id='fetch_and_store_weather_forecast',
        python_callable=fetch_and_store_weather_forecast
    )

    recommendation_task = PythonOperator(
        task_id='generate_recommendations_with_charging_data',
        python_callable=generate_recommendations_with_new_charging_data
    )

    fetch_weather_task >> recommendation_task