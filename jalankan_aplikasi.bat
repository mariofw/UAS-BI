@echo off
echo ========================================================
echo      BATTERY AWET - AUTOMATIC STARTUP SCRIPT
echo ========================================================

echo [1/6] Mematikan container lama (jika ada)...
docker-compose down

echo.
echo [2/6] Membangun dan menyalakan server...
docker-compose up -d --build

echo.
echo [3/6] Menunggu sistem Airflow siap (60 detik)...
echo        Harap bersabar, sistem sedang inisialisasi database...
timeout /t 60 /nobreak >nul

echo.
echo [4/6] Menjalankan Pipeline Data (ETL Dimensi)...
docker exec battery_awet-main-airflow-scheduler-1 airflow dags unpause battery_data_pipeline_dimensional
docker exec battery_awet-main-airflow-scheduler-1 airflow dags trigger battery_data_pipeline_dimensional

echo.
echo [5/6] Menunggu data selesai diproses (30 detik)...
timeout /t 30 /nobreak >nul

echo.
echo [6/6] Menjalankan Analisis & Rekomendasi...
docker exec battery_awet-main-airflow-scheduler-1 airflow dags unpause battery_optimization_etl_with_weather
docker exec battery_awet-main-airflow-scheduler-1 airflow dags trigger battery_optimization_etl_with_weather

echo.
echo ========================================================
echo      SELESAI! APLIKASI SIAP DIGUNAKAN
echo ========================================================
echo.
echo Membuka Streamlit di browser...
start http://localhost:8501

pause