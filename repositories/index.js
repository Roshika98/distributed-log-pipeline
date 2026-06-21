const DbConnector = require("../connectors/db-connector");

/**
 * Retrieve logs
 * @param {Object} params
 * @param {Date|string} params.startTime
 * @param {Date|string} params.endTime
 * @param {string|null} params.service
 * @param {string|null} params.level
 * @param {number} params.limit
 * @param {number} params.offset
 */
const getLogs = async ({
  startTime,
  endTime,
  service = null,
  level = null,
  limit = 100,
  offset = 0,
}) => {
  const connector = DbConnector.Instance();
  const connection = await connector.getConnection();

  let query = `
      SELECT 
        log_timestamp,
        service_name,
        log_level,
        message,
        stack_trace
      FROM logs
      WHERE log_timestamp BETWEEN ? AND ?
    `;

  const params = [startTime, endTime];

  if (service) {
    query += ` AND service_name = ?`;
    params.push(service);
  }

  if (level) {
    query += ` AND log_level = ?`;
    params.push(level);
  }

  query += `
      ORDER BY log_timestamp DESC
      LIMIT ?
      OFFSET ?
    `;

  params.push(limit, offset);

  const [rows] = await connection.query(query, params);

  connector.releaseConnection(connection);

  return rows;
};

module.exports = {
  getLogs,
};
