# Distributed Log Pipeline

An event-driven, high-throughput microservice for centralized log ingestion and persistence. 

This service acts as an asynchronous sink for distributed microservices, allowing them to push logs via RabbitMQ without blocking their own execution. It uses a robust multi-stage buffering architecture with Redis to handle massive traffic spikes and applies backpressure to prevent system overload.

## Architecture & Data Flow

This pipeline handles logs in three main stages to guarantee high throughput and reliability:

1. **Ingestion (RabbitMQ & Redis ZSet)**
   * Microservices push logs to RabbitMQ topic queues (`log.<service>.*`).
   * The `LogConsumer` listens to these queues and immediately buffers the incoming logs into a Redis Sorted Set (`zsetlogs:<service>`), using the timestamp as the score.
   * *Backpressure:* If the Redis Sorted Set exceeds the configured upper bound, a `BackPressureController` emits an event to temporarily pause consumption from RabbitMQ, allowing the downstream processors to catch up.

2. **Draining & Archiving (Redis List & File System)**
   * A `RedisDrainer` constantly polls the Sorted Set. It pops logs in batches, writes them to a rotating local log file (using `FileHandler`), and then pushes them into a generic `logs` Redis List for database processing.
   * *Backpressure:* If the File stream becomes bottlenecked, the drainer waits for a `drain` event before popping more logs from Redis.

3. **Persistence (MySQL)**
   * A `DbWorker` polls the `logs` Redis List. It pops logs in large batches (e.g., 100 at a time), parses them, and performs a single bulk `INSERT` query into the MySQL `logs` table. This dramatically reduces database connection overhead.

## Key Features
* **Event-Driven:** Decouples log generation from log persistence using RabbitMQ.
* **Intelligent Backpressure:** Dynamically pauses queue consumption to prevent Out-Of-Memory (OOM) errors during high-traffic spikes.
* **Bulk Database Insertion:** Optimizes MySQL performance by batch-inserting logs.
* **Singleton Resource Management:** Uses the Singleton pattern for Redis, MySQL, and File connections to prevent connection leaks.

## Tech Stack
* **Runtime:** Node.js, Express.js
* **Message Broker:** RabbitMQ (`amqplib`)
* **Caching & Buffering:** Redis (`ioredis`)
* **Database:** MySQL (`mysql2` with Connection Pooling)

## Environment Variables

Create a `.env` file in the root directory:

```env
PORT=3000

# RabbitMQ Configuration
RABBITMQ_URL=amqp://localhost
RABBITMQ_USER=kbuser
RABBITMQ_PASSWORD=rabbitKB123r
LOG_QUEUE_NAME=service-logs

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_ZSET_UBOUND=1000
REDIS_ZSET_LBOUND=300

# MySQL Configuration
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=logger_db

# File System
LOG_FILE_PATH=./logs
LOG_FILE_RESET_INTERVAL=86400000 # 24 hours in milliseconds
```

## 🌐 API Endpoints

### `GET /api/logs`
Retrieve logs from the MySQL database with filtering and pagination.

**Query Parameters:**
* `startTime` (required): ISO date string or timestamp.
* `endTime` (required): ISO date string or timestamp.
* `service` (optional): Filter by service name.
* `level` (optional): Filter by log level (e.g., info, error).
* `limit` (optional): Number of records to return (default: 100).
* `offset` (optional): Pagination offset (default: 0).

**Example Request:**
```http
GET /api/logs?startTime=2023-10-01T00:00:00Z&endTime=2023-10-31T23:59:59Z&service=auth-service&level=error
```