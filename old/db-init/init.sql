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

CREATE TABLE IF NOT EXISTS failed_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,

    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    failure_reason VARCHAR(255) DEFAULT 'Exceeded retry count',
    retry_count INT,
    original_routing_key VARCHAR(255),
    raw_payload TEXT NOT NULL,
    log_timestamp DATETIME NULL,
    service_name VARCHAR(255) NULL,
    log_level VARCHAR(50) NULL,
    message TEXT NULL,
    stack_trace TEXT NULL
);
