const mysqlConfig = {
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "root",
  database: process.env.DB_NAME || "service_logs",
  port: process.env.DB_PORT || 3306,
};

module.exports = mysqlConfig;
