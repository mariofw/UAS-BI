CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(50) PRIMARY KEY,
    device_type VARCHAR(30) NOT NULL,
    brand_name VARCHAR(50) NOT NULL,
    model VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS charging_logs (
    charge_id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    plug_in_time TIMESTAMP NOT NULL,
    plug_out_time TIMESTAMP NOT NULL,
    start_level INT NOT NULL,
    end_level INT NOT NULL,
    charge_temp DECIMAL(4,1) NOT NULL,
    CONSTRAINT fk_device
        FOREIGN KEY (device_id)
        REFERENCES devices(device_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

-- Clear existing data before inserting new data
DELETE FROM charging_logs;
DELETE FROM devices;

INSERT INTO devices (device_id, device_type, brand_name, model)
VALUES
('D001', 'Smartphone', 'Samsung', 'Galaxy A54'),
('D003', 'Smartphone', 'Apple', 'iPhone 13');

INSERT INTO charging_logs (device_id, plug_in_time, plug_out_time, start_level, end_level, charge_temp)
VALUES
('D001', '2025-12-10 10:00:00', '2025-12-10 12:00:00', 18, 92, 36.5),
('D003', '2025-12-10 16:00:00', '2025-12-10 18:00:00', 30, 90, 35.8),
('D001', '2025-12-11 10:00:00', '2025-12-11 12:00:00', 20, 90, 36.7),
('D003', '2025-12-11 16:00:00', '2025-12-11 18:00:00', 32, 91, 35.9),
('D001', '2025-12-12 10:00:00', '2025-12-12 12:00:00', 22, 92, 36.6),
('D003', '2025-12-12 16:00:00', '2025-12-12 18:00:00', 33, 93, 35.7),
('D001', '2025-12-13 10:00:00', '2025-12-13 12:00:00', 23, 94, 36.8),
('D003', '2025-12-13 16:00:00', '2025-12-13 18:00:00', 34, 94, 35.8),
('D001', '2025-12-14 10:00:00', '2025-12-14 12:00:00', 24, 95, 37.0),
('D003', '2025-12-14 16:00:00', '2025-12-14 18:00:00', 35, 95, 35.9),
('D001', '2025-12-15 10:00:00', '2025-12-15 12:00:00', 25, 96, 36.9),
('D003', '2025-12-15 16:00:00', '2025-12-15 18:00:00', 36, 96, 36.0),
('D001', '2025-12-16 10:00:00', '2025-12-16 12:00:00', 26, 97, 37.0),
('D003', '2025-12-16 16:00:00', '2025-12-16 18:00:00', 37, 97, 36.1),
('D001', '2025-12-17 10:00:00', '2025-12-17 12:00:00', 27, 98, 37.1),
('D003', '2025-12-17 16:00:00', '2025-12-17 18:00:00', 38, 98, 36.2);
