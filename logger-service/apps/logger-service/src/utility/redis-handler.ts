import { Redis } from 'ioredis';
import BackPressureController from './backpressure-controller';

export enum RedisListStatus {
  RESUMED = 'RESUMED',
  PAUSED = 'PAUSED',
}

/**
 * @class RedisHandler
 * @author Roshika Perera
 * @description Singleton class to handle Redis operations
 */

class RedisHandler {
  /**
   * @type {RedisHandler | null}
   */
  private static instance: RedisHandler;

  /**
   * @returns {RedisHandler}
   */
  public static getInstance(): RedisHandler {
    if (!RedisHandler.instance) {
      const redisHost = process.env.REDIS_HOST || 'localhost';
      const redisPort = Number(process.env.REDIS_PORT) || 6379;
      RedisHandler.instance = new RedisHandler(redisPort, redisHost);
    }
    return RedisHandler.instance;
  }

  private redis: Redis;
  private zsetUpperLimit: number;
  private zsetLowerLimit: number;
  private backPressureController: BackPressureController;

  private constructor(port: number, host: string) {
    this.redis = new Redis(port, host);
    this.zsetUpperLimit = Number(process.env.REDIS_ZSET_UBOUND) || 1000;
    this.zsetLowerLimit = Number(process.env.REDIS_ZSET_LBOUND) || 300;
    this.backPressureController = BackPressureController.getInstance();
  }

  /**
   * Pushes a log string to the end of a Redis cache list
   * @param logStrings
   */
  async pushToLogCache(logStrings: string[]): Promise<void> {
    await this.redis.rpush(`logs`, ...logStrings);
  }

  /**
   * Pops a log string from the start of a Redis cache list for a specific service.
   * @param count
   * @returns Promise<string|null>
   */
  async popFromLogCache(count: number): Promise<string[]> {
    const result = await this.redis.blpop(`logs`, 0);
    if (!result) return [];

    const firstLog = result[1];
    const remainingLogs = await this.redis.lpop(`logs`, count - 1);
    if (remainingLogs === null) return [firstLog];
    return [firstLog, ...remainingLogs];
  }

  /**
   * Gets the length of the Redis cache list for a specific service.
   * @returns Promise<number>
   */
  async getLogCacheCount(): Promise<number> {
    return await this.redis.llen(`logs`);
  }

  /**
   * Inserts a log string into a Redis sorted set for a specific service with the given timestamp as the score.
   * @param service
   * @param timestamp
   * @param logstring
   * @returns
   */
  async insertZSetByService(
    service: string,
    timestamp: number,
    logstring: string,
  ): Promise<void> {
    await this.redis.zadd(`zsetlogs:${service}`, timestamp, logstring);
    const currLen = await this.redis.zcard(`zsetlogs:${service}`);

    if (currLen >= this.zsetUpperLimit) {
      this.backPressureController.emitQueuePause(service);
      await this.redis.set(`zsetStatus:${service}`, RedisListStatus.PAUSED);
    }
  }

  /**
   * Pops a log string from a Redis sorted set for a specific service.
   * @param service
   * @param minScore
   */
  async popZSetLog(
    service: string,
    minScore: number,
  ): Promise<[string, string, ...string[]] | []> {
    const result = await this.redis.bzpopmin(`zsetlogs:${service}`, 0);

    if (!result) return [];

    const [, firstMember, firstScore] = result;

    const morelogs = await this.redis.zpopmin(
      `zsetlogs:${service}`,
      minScore - 1,
    );

    const currLen = await this.redis.zcard(`zsetlogs:${service}`);
    if (currLen <= this.zsetLowerLimit) {
      const currZsetStatus = await this.redis.get(`zsetStatus:${service}`);
      if (currZsetStatus == RedisListStatus.PAUSED) {
        await this.redis.set(`zsetStatus:${service}`, RedisListStatus.RESUMED);
        this.backPressureController.emitQueueResume(service);
      }
    }
    return [firstMember, firstScore, ...morelogs];
  }

  /**
   * Gets the length of the Redis sorted set for a specific service.
   * @param service
   * @returns Promise<number>
   */
  async getZsetLength(service: string): Promise<number> {
    return await this.redis.zcard(`zsetlogs:${service}`);
  }

  /**
   * Gets the stream status for a specific service.
   * @param service
   * @returns Promise<string | null>
   */
  async getServiceStreamStatus(service: string): Promise<string | null> {
    return await this.redis.get(`stream_status:${service}`);
  }

  /**
   * Sets the stream status for a specific service.
   * @param service
   * @param status
   */
  async setServiceStreamStatus(service: string, status: string): Promise<void> {
    await this.redis.set(`stream_status:${service}`, status);
  }
}

export default RedisHandler;
