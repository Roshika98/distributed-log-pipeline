const BackPressureController = require("../utility/backpressure-controller");
const FileHandler = require("../utility/file-handler");
const RedisHandler = require("../utility/redis-handler");

/**
 * @class RedisDrainer
 * @author Roshika Perera
 * @description Class to drain logs from Redis sorted sets based on backpressure signals
 */
class RedisDrainer {
  /**
   * Constructor for RedisDrainer
   * @param {string} service
   */
  constructor(service) {
    this.backPressureController = BackPressureController.getController();
    this.filehandler = FileHandler.getInstance();
    this.redisHandler = RedisHandler.getInstance();
    this.service = service;
  }

  async initialize() {
    const drainGenerator = this.#drainLogs();

    console.log(`Initializing redis drainer for ${this.service} service`);
    

    for await (const logBatch of drainGenerator) {
      const writable = await this.filehandler.writeToLogFile(
        this.service,
        logBatch,
      );
      if (!writable) {
        console.log(
          `Backpressure detected for service: ${this.service}. Waiting for drain to complete.`,
        );
        await Promise.race([
          new Promise((resolve) => {
            setTimeout(resolve, 10000);
          }),
          this.filehandler.awaitDrainComplete(this.service),
        ]);
        console.log(`Resuming drain service for ${this.service}`);
      }
    }
  }

  async *#drainLogs() {
    while (true) {
      const currZsetLength = await this.redisHandler.getZsetLength(
        this.service,
      );
      if (currZsetLength === 0) {
        // If no logs to drain, wait for a while before checking again
        // console.log(
        //   `No items in cache for service: ${this.service}. Waiting...`,
        // );

        await new Promise((resolve) => setTimeout(resolve, 1000));
        continue;
      }

      const batch = await this.redisHandler.popZSetLog(this.service, 100);
      let finalString = "";
      const currBatch = [];
      for (let i = 0; i < batch.length; i += 2) {
        const logString = batch[i];
        currBatch.push(logString);
        finalString += logString + "\n";
      }
      await this.redisHandler.pushToLogCache(currBatch);
      yield finalString;
    }
  }
}

module.exports = RedisDrainer;
