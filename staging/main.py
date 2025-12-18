import pandas as pd
from sqlalchemy import create_engine
import requests

# Koneksi internal Docker ke Postgres
engine = create_engine('postgresql://admin:admin@postgres:5432/battery_db')

def run_staging():
    print("Membaca data CSV...")
    # 1. Baca data dari folder yang di-mount (sesuai docker-compose)
    # Gunakan delimiter ';' sesuai format file kamu
    try:
        df_activity = pd.read_csv('/data_source/log_aktivitas_baterai_hp.csv', sep=';')
        df_specs = pd.read_csv('/data_source/smartphones.csv', sep=';')
        
        # Bersihkan data: Ubah '15,6' menjadi 15.6 agar bisa dihitung
        if 'battery_usage_percent' in df_activity.columns:
            df_activity['battery_usage_percent'] = df_activity['battery_usage_percent'].str.replace(',', '.').astype(float)
        
        # 2. Ambil data Cuaca dari API kamu
        api_url = "http://api.weatherapi.com/v1/forecast.json?key=30b681ca44f646db86b140433251512&q=banjarmasin"
        weather_res = requests.get(api_url).json()
        temp_c = weather_res['current']['temp_c']
        condition = weather_res['current']['condition']['text']
        
        df_weather = pd.DataFrame([{'city': 'Banjarmasin', 'temp': temp_c, 'condition': condition}])

        # 3. Masukkan ke Postgres Skema Staging
        df_activity.to_sql('stg_activity', engine, schema='staging', if_exists='replace', index=False)
        df_specs.to_sql('stg_smartphones', engine, schema='staging', if_exists='replace', index=False)
        df_weather.to_sql('stg_weather', engine, schema='staging', if_exists='replace', index=False)
        
        print("✅ Data berhasil dipindah ke Staging!")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_staging()