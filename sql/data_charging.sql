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
('D001', '2025-12-17 10:00:00', '2025-12-17 12:00:00', 18, 92, 36.5),
('D003', '2025-12-17 16:00:00', '2025-12-17 18:00:00', 30, 90, 35.8),
('D001', '2025-12-18 10:00:00', '2025-12-18 12:00:00', 20, 90, 36.7),
('D003', '2025-12-18 16:00:00', '2025-12-18 18:00:00', 32, 91, 35.9),
('D001', '2025-12-19 10:00:00', '2025-12-19 12:00:00', 22, 92, 36.6),
('D003', '2025-12-19 16:00:00', '2025-12-19 18:00:00', 33, 93, 35.7),
('D001', '2025-12-20 10:00:00', '2025-12-20 12:00:00', 23, 94, 36.8),
('D003', '2025-12-20 16:00:00', '2025-12-20 18:00:00', 34, 94, 35.8),
('D001', '2025-12-21 10:00:00', '2025-12-21 12:00:00', 24, 95, 37.0),
('D003', '2025-12-21 16:00:00', '2025-12-21 18:00:00', 35, 95, 35.9),
('D001', '2025-12-22 10:00:00', '2025-12-22 12:00:00', 25, 96, 36.9),
('D003', '2025-12-22 16:00:00', '2025-12-22 18:00:00', 37, 97, 36.1),
('D001', '2025-12-23 10:00:00', '2025-12-23 12:00:00', 27, 98, 37.1),
('D003', '2025-12-23 16:00:00', '2025-12-23 18:00:00', 38, 98, 36.2);
