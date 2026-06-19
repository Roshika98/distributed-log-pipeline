const mysql2 = require("mysql2/promise");
const mysql = require("mysql2");
const mysqlConfig = require("../configs/mysql-config");
const { instance } = require("../utility/redis-handler");

class DbConnector {
  /**
   * @type {DbConnector|null}
   */
  static instance = null;

  static Instance() {
    if (!DbConnector.instance) {
      DbConnector.instance = new DbConnector();
    }
    return DbConnector.instance;
  }

  constructor() {}

  async #initDBConnection() {
    this.pool = mysql2.createPool(mysqlConfig);
    // await this.pool.connect();
  }

  /**
   * Uses a connection from the pool to perform database operations.
   * @returns {Promise<mysql2.PoolConnection>} A connection from the pool.
   */
  async getConnection() {
    if (!this.pool) {
      await this.#initDBConnection();
    }
    return this.pool.getConnection();
  }

  /**
   * Releases a connection back to the pool.
   * @param {mysql2.PoolConnection} connection
   */
  async releaseConnection(connection) {
    if (this.pool) {
      this.pool.releaseConnection(connection);
    }
  }
}

module.exports = DbConnector;
