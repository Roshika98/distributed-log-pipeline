const fs = require("fs");
const RedisHandler = require("./redis-handler");
const BackPressureController = require("./backpressure-controller");

/**
 * @class FileHandler
 * @author Roshika Perera
 * @description Singleton class to handle file operations for logging
 */
class FileHandler {
  /**
   * @type {FileHandler|null}
   */
  static instance = null;
  static getInstance() {
    if (!FileHandler.instance) {
      FileHandler.instance = new FileHandler();
    }
    return FileHandler.instance;
  }

  constructor() {
    this.basePath = process.env.LOG_FILE_PATH || "./logs";
    this.resetTimeInterval =
      parseInt(process.env.LOG_FILE_RESET_INTERVAL) || 24 * 60 * 60 * 1000;
    this.fileMaps = new Map();
    this.drainSubscribers = new Map();
    this.redishandler = RedisHandler.getInstance();
    this.backPressureController = BackPressureController.getController();
  }

  async #openLogFile(serviceName) {
    const filePath = `${this.basePath}/${serviceName}-${Date.now()}.log`;

    const writeStream = fs.createWriteStream(filePath, {
      flags: "a",
      highWaterMark: 1024,
    });

    const fileObj = {
			stream: writeStream,
			canWrite: true,
			filename: filePath,
			createdAt: Date.now(),
			bytesWritten: 0,
		};

		this.fileMaps.set(serviceName, fileObj);

    writeStream.on("drain", () => {
			fileObj.canWrite = true;
			if (fileObj._resolveDrain) {
				fileObj._resolveDrain();
				fileObj._resolveDrain = null;
				this.drainSubscribers.delete(serviceName);
			}
		});

    writeStream.on("error", (err) => {
      console.error(
        `Error writing to log file for service ${serviceName}:`,
        err,
      );
      fileObj.canWrite = false;
    });

    // writeStream.on("finish", () => {
    //   console.log("finished writing data");
    // });

    writeStream.on("close", () => {
      console.log(`closed write stream for service: ${serviceName}`);
      fileObj.canWrite = true;
			if (fileObj._resolveDrain) {
				fileObj._resolveDrain();
				fileObj._resolveDrain = null;
				this.drainSubscribers.delete(serviceName);
			}
    });

    return writeStream;
  }

  #closeLogFile(serviceName) {
    const fileObj = this.fileMaps.get(serviceName);
    if (fileObj) {
      this.fileMaps.delete(serviceName);
    }
  }

  /**
   * Function to write log details to a file based on service name
   * @param {string} serviceName
   * @param {string} logDetails
   */
  async writeToLogFile(serviceName, logDetails) {
    let fileObj = this.fileMaps.get(serviceName);
    if (!fileObj) {
      await this.#openLogFile(serviceName);
      fileObj = this.fileMaps.get(serviceName);
    }

    const tempBuffer = Buffer.from(logDetails, "utf-8");

    if (
      fileObj.createdAt &&
      Date.now() - fileObj.createdAt > this.resetTimeInterval
      // TODO: add size based rotation also
    ) {
			console.log("time difference exceeded");

			fileObj.stream.end(tempBuffer);
			this.#closeLogFile(serviceName);
			return false;
		}

    try {
      fileObj.bytesWritten += tempBuffer.length;
      const canWrite = fileObj.stream.write(tempBuffer);
      fileObj.canWrite = canWrite;

      return canWrite;
    } catch (err) {
      fileObj.canWrite = false;
      throw err;
    }
  }

  async awaitDrainComplete(serviceName) {
    const fileObj = this.fileMaps.get(serviceName);
    if (fileObj && fileObj.canWrite) return Promise.resolve();

    if (!this.drainSubscribers.has(serviceName)) {
      const drainPromise = new Promise((resolve) => {
        fileObj._resolveDrain = resolve;
      });
      this.drainSubscribers.set(serviceName, drainPromise);
    }

    return this.drainSubscribers.get(serviceName);
  }
}

module.exports = FileHandler;
