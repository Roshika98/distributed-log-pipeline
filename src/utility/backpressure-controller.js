const EventEmitter = require("events");

/**
 * @class BackPressureController
 * @author Roshika Perera
 * @description Class to manage backpressure for RabbitMQ consumers
 */
class BackPressureController extends EventEmitter {
  /**
   * @type {BackPressureController|null}
   */
  static controller = null;

  static getController() {
    if (!BackPressureController.controller) {
      BackPressureController.controller = new BackPressureController();
    }
    return BackPressureController.controller;
  }

  constructor(zsetlimit = 1000) {
    super();
    this.zsetlimit = zsetlimit;
  }

  /**
   * Emit a message queue pause event for a specific service
   * @param {string} serviceName
   */
  emitQueuePause(serviceName) {
    console.log(`emiting queue pause event for service ${serviceName}`);
    this.emit("pause", { service: serviceName });
  }

  emitQueueResume(serviceName) {
    this.emit("resume", { service: serviceName });
  }

  /**
   * Subscribe to message queue pause events
   * @param {function} callback
   */
  subscribeToQueuePause(callback) {
    this.on("pause", async (data) => await callback(data));
  }

  subscribeToQueueResume(callback) {
    this.on("resume", async (data) => await callback(data));
  }

  // emitResumeWriting(serviceName) {
  //   this.emit(`resume-write`, { service: serviceName });
  // }

  // subscribeOnceToResumeWriting(serviceName, callback) {
  //   this.once(`resume-write`, (data) => {
  //     if (data.service === serviceName) {
  //       callback();
  //     }
  //   });
  // }
}

module.exports = BackPressureController;
