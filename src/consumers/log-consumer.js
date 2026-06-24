const amqplib = require("amqplib");
const FileHandler = require("../utility/file-handler");
const RedisHandler = require("../utility/redis-handler");
const servicesList = require("../service_list.json");
const BackPressureController = require("../utility/backpressure-controller");

/**
 * @class LogConsumer
 * @author Roshika Perera
 * @description Singleton class to consume logs from RabbitMQ and write them to files and DB
 */
class LogConsumer {
  /**
   * @type {LogConsumer|null}
   */
  static instance = null;

  static getInstance() {
    if (!LogConsumer.instance) {
      const rabbitmqUrl = process.env.RABBITMQ_URL || "amqp://localhost";
      const queueName = process.env.LOG_QUEUE_NAME || "service-logs";
      LogConsumer.instance = new LogConsumer(rabbitmqUrl, queueName);
    }
    return LogConsumer.instance;
  }

  constructor(rabbitmqUrl, queueName) {
    this.rabbitmqUrl = rabbitmqUrl;
    this.queueName = queueName;
    this.filehandler = FileHandler.getInstance();
    this.redishandler = RedisHandler.getInstance();
    this.channelMap = new Map();
    this.backPressureController = BackPressureController.getController();
  }

  async intialize() {
    this.connection = await amqplib.connect(this.rabbitmqUrl, {
      credentials: amqplib.credentials.plain(
        process.env.RABBITMQ_USER || "kbuser",
        process.env.RABBITMQ_PASSWORD || "rabbitKB123r"
      ),
    });

    this.mainChannel = await this.connection.createChannel();

    // # declare the exchange
    await this.mainChannel.assertExchange("logs.topic", "topic", {
      durable: true,
    });

    await this.mainChannel.assertExchange("logs.retry.exchange", "topic", { durable: true });

		this.dlqChannel = await this.mainChannel.assertQueue("log-dlq", { durable: true });
		this.retryQueue = await this.mainChannel.assertQueue("log-retry", {
			durable: true,
			deadLetterExchange: "logs.topic",
			messageTtl: 5000,
		});

		await this.mainChannel.bindQueue(
			this.retryQueue.queue,
			"logs.retry.exchange",
			"#"
		);

    await this.#setupServiceChannels();
    this.#consumeLogs();

    console.log("Log Consumer is up and running, waiting for messages...");
  }

  /**
   * Setup channels for each service and bind queues
   */
  async #setupServiceChannels() {
    const services = servicesList.services;

    for (const element of services) {
      await this.mainChannel.assertQueue(element.queue, { durable: true });
      await this.mainChannel.bindQueue(
        element.queue,
        "logs.topic",
        `log.${element.queue}.*`,
      );

      const channel = await this.connection.createChannel();

      // this.backPressureController.on("pause", async (attr) => {
      //   if (attr.service === element.queue) {
      //     console.log(`pausing consumption for ${element.queue}`);
      //     await this.pauseConsuming(element.queue);
      //   }
      // });

      this.backPressureController.subscribeToQueuePause(async (attr) => {
        if (attr.service === element.queue) {
          console.log(`pausing consumption for ${element.queue}`);
          await this.pauseConsuming(element.queue);
        }
      });

      this.backPressureController.subscribeToQueueResume(async (attr) => {
        if (attr.service === element.queue) {
          console.log(`resuming consumption for ${element.queue}`);
          await this.resumeConsuming(element.queue);
        }
      });

      // this.backPressureController.on("resume", async (attr) => {
      //   if (attr.service === element.queue) {
      //     console.log(`resuming consumption for ${element.queue}`);
      //     await this.resumeConsuming(element.queue);
      //   }
      // });

      this.channelMap.set(element.queue, { channel, consumerTag: null });
    }
  }

  /**
   * Consume logs from all service queues
   */
  async #consumeLogs() {
    this.channelMap.forEach(async (value, queueName) => {
      await value.channel.prefetch(1);
      const tag = await this.#createConsumer(value.channel, queueName);
      value.consumerTag = tag;
    });
  }

  /**
   * create a consumer for the given channel and queue
   * @param {amqplib.Channel} channel
   * @param {string} queueName
   * @returns
   */
  async #createConsumer(channel, queueName) {
    const consumerTag = await channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        const headers = msg.properties.headers || {};
        const logString = msg.content.toString();
        const retryCount = msg.properties.headers["retry-count"] || 0;

        const service = headers["service-name"];

        try {
          await this.redishandler.insertZSetByService(
            service,
            Date.now(),
            logString,
          );

          channel.ack(msg);
        } catch (error) {
          console.error("Error processing log message:", error);
          if (retryCount < 3) {
						msg.properties.headers["retry-count"] = retryCount + 1;

						const originalRoutingKey = msg.fields.routingKey;

						this.mainChannel.publish(
							"logs.retry.exchange",
							originalRoutingKey,
							msg.content,
							msg.properties,
						);

						channel.ack(msg);
					} else {
						channel.sendToQueue(this.dlqChannel.queue, msg.content, msg.properties);
						channel.ack(msg);
					}
        }
      }
    });

    return consumerTag.consumerTag;
  }

  /**
   * Pause consuming messages from the given queue
   * @param {string} queueName
   */
  async pauseConsuming(queueName) {
    const value = this.channelMap.get(queueName);
    if (value) {
      await value.channel.cancel(value.consumerTag);
      value.consumerTag = null;
    }
  }

  /**
   * Resume consuming messages for the given queue
   * @param {string} queueName
   */
  async resumeConsuming(queueName) {
    const value = this.channelMap.get(queueName);
    if (value) {
      const tag = await this.#createConsumer(value.channel, queueName);
      value.consumerTag = tag;
    }
  }
}

module.exports = LogConsumer;


