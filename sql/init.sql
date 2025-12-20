-- Buat database untuk Airflow
CREATE DATABASE airflow;
-- Buat Database tambahan (Jika script docker-compose env tidak jalan di OS tertentu)
CREATE DATABASE battery_db;

-- Pindah ke database battery_db
\c battery_db;

-- Buat Skema
CREATE SCHEMA staging;
CREATE SCHEMA datawarehouse;
2
-- Tabel awal untuk charging logs (Sesuai file .sql kamu)
CREATE TABLE staging.stg_charging_logs (
    charge_id SERIAL PRIMARY KEY,
    device_id VARCHAR(50),
    plug_in_time TIMESTAMP,
    plug_out_time TIMESTAMP,
    start_level INT,
    end_level INT,
    charge_temp DECIMAL(4,1)
);