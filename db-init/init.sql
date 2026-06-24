CREATE DATABASE IF NOT EXISTS service_logs;
USE service_logs;

CREATE TABLE IF NOT EXISTS logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    log_timestamp DATETIME,
    service_name VARCHAR(255),
    log_level VARCHAR(50),
    message TEXT,
    stack_trace TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
