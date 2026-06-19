const DbConnector = require("../connectors/db-connector");
const RedisHandler = require("../utility/redis-handler");

class DBWorker {
  /**
   * @type {DBWorker|null}
   */
  static instance = null;

  static getInstance() {
    if (!DBWorker.instance) {
      DBWorker.instance = new DBWorker();
    }
    return DBWorker.instance;
  }

  constructor() {
    this.connector = DbConnector.Instance();
    this.redisHandler = RedisHandler.getInstance();
  }

  async processLogs() {
    const connection = await this.connector.getConnection();
    const logGenerator = this.logGenerator();

    for await (const logBatch of logGenerator) {
      try {
        const preprocessedLogs = this.preprocessLogs(logBatch);
        const query =
          "INSERT INTO logs (log_timestamp, service_name, log_level, message, stack_trace) VALUES ?";
        await connection.query(query, [preprocessedLogs]);
      } catch (error) {
        console.error(error);
      }
    }
  }

  async *logGenerator() {
    while (true) {
      const logCount = await this.redisHandler.getLogCacheCount();
      if (logCount == 0) {
        await new Promise((resolve) => setTimeout(resolve, 10000));
        continue;
      }

      const logs = await this.redisHandler.popFromLogCache(100);
      yield logs;
    }
  }

  /**
   *
   * @param {string[]} logs
   */
  preprocessLogs(logs) {
    return logs.map((log) => {
      const parsedLog = JSON.parse(log);
      const {
        timestamp,
        service,
        level,
        message = null,
        stack = null,
      } = parsedLog;
      return [new Date(timestamp), service, level, message, stack];
    });
  }
}

module.exports = DBWorker;
