const { Redis } = require("ioredis");
const BackPressureController = require("./backpressure-controller");

/**
 * @class RedisHandler
 * @author Roshika Perera
 * @description Singleton class to handle Redis operations
 */

class RedisHandler {
  /**
   * @type {RedisHandler|null}
   */
  static instance = null;

  static getInstance() {
    if (!RedisHandler.instance) {
      const redisHost = process.env.REDIS_HOST || "fx-redis";
      const redisPort = process.env.REDIS_PORT || 6379;
      RedisHandler.instance = new RedisHandler(redisPort, redisHost);
    }
    return RedisHandler.instance;
  }

  constructor(port, host) {
    this.redis = new Redis(port, host);
    this.zsetUpperLimit = process.env.REDIS_ZSET_UBOUND || 1000;
    this.zsetLowerLimit = process.env.REDIS_ZSET_LBOUND || 300;
    this.backPressureController = BackPressureController.getController();
  }

  /**
   * Pushes a log string to the end of a Redis cache list
   * @param {string[]} logStrings
   */
  async pushToLogCache(logStrings) {
    await this.redis.rpush(`logs`, ...logStrings);
  }

  /**
   * Pops a log string from the start of a Redis cache list for a specific service.
   * @param {number} count
   * @returns {Promise<string|null>} The popped log string or null if the list is empty.
   */
  async popFromLogCache(count) {
    // return await this.redis.lpop(`logs`, count);
    const result = await this.redis.blpop(`logs`, 0);
    if (!result) return [];

    const [key, firstLog] = result;
    const remainingLogs = await this.redis.lpop(`logs`, count - 1);
    if (remainingLogs === null) return [firstLog];
    return [firstLog, ...remainingLogs];
  }

  /**
   * Gets the length of the Redis cache list for a specific service.
   * @returns {Promise<number>} The length of the log list.
   */
  async getLogCacheCount() {
    return await this.redis.llen(`logs`);
  }

  /**
   * Inserts a log string into a Redis sorted set for a specific service with the given timestamp as the score.
   * @param {string} service
   * @param {number} timestamp
   * @param {string} logstring
   * @returns
   */
  async insertZSetByService(service, timestamp, logstring) {
    await this.redis.zadd(`zsetlogs:${service}`, timestamp, logstring);
    const currLen = await this.redis.zcard(`zsetlogs:${service}`);

    if (currLen >= this.zsetUpperLimit) {
      this.backPressureController.emitQueuePause(service);
      await this.redis.set(`zsetStatus:${service}`, "PAUSED");
    }
  }

  /**
   * Pops a log string from a Redis sorted set for a specific service.
   * @param {string} service
   * @param {number} minScore
   */
  async popZSetLog(service, minScore) {
    const result = await this.redis.bzpopmin(`zsetlogs:${service}`, 0);

		if (!result) return [];

		const [key, firstMember, firstScore] = result;

		const morelogs = await this.redis.zpopmin(`zsetlogs:${service}`, minScore - 1);

    const currLen = await this.redis.zcard(`zsetlogs:${service}`);
    if (currLen <= this.zsetLowerLimit) {
      const currZsetStatus = await this.redis.get(`zsetStatus:${service}`);
      if (currZsetStatus == "PAUSED") {
        await this.redis.set(`zsetStatus:${service}`, "RESUMED");
        this.backPressureController.emitQueueResume(service);
      }
    }
    return [firstMember, firstScore, ...morelogs];
  }

  /**
   * Gets the length of the Redis sorted set for a specific service.
   * @param {string} service
   * @returns {Promise<number>} The length of the sorted set.
   */
  async getZsetLength(service) {
    return await this.redis.zcard(`zsetlogs:${service}`);
  }

  /**
   * Gets the stream status for a specific service.
   * @param {string} service
   * @returns {Promise<string|null>} The stream status.
   */
  async getServiceStreamStatus(service) {
    return await this.redis.get(`stream_status:${service}`);
  }

  /**
   * Sets the stream status for a specific service.
   * @param {string} service
   * @param {string} status
   */
  async setServiceStreamStatus(service, status) {
    await this.redis.set(`stream_status:${service}`, status);
  }
}

module.exports = RedisHandler;
