const amqplib = require("amqplib");
const FileHandler = require("../utility/file-handler");
const RedisHandler = require("../utility/redis-handler");
const servicesList = require("../service_list.json");
const BackPressureController = require("../utility/backpressure-controller");
const { QueueManager, QUEUE_TYPE, QUEUE_EXCHANGE, QUEUE } = require("../managers/queue-manger");

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
			const queueManager = QueueManager.getInstance();

			LogConsumer.instance = new LogConsumer(queueManager);
		}
		return LogConsumer.instance;
	}

	/**
	 *
	 * @param {QueueManager} queueManager
	 */
	constructor(queueManager) {
		this.queueManager = queueManager;
		this.filehandler = FileHandler.getInstance();
		this.redishandler = RedisHandler.getInstance();
		this.channelMap = new Map();
		this.backPressureController = BackPressureController.getController();
		this.mainChannel = null;
	}

	async intialize() {
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
			this.mainChannel = this.queueManager.getChannel(QUEUE_TYPE.MAIN);
			await this.mainChannel.assertQueue(element.queue, { durable: true });
			await this.mainChannel.bindQueue(
				element.queue,
				QUEUE_EXCHANGE.MAIN,
				`log.${element.queue}.*`,
			);

			const channel = await this.queueManager.createChannel();

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
					const parsedLog = JSON.parse(logString);
					if (!parsedLog.timestamp || !parsedLog.service || !parsedLog.level) {
						const err = new Error("Missing required log fields (timestamp, service, level)");
						err.isUnrecoverable = true;
						throw err;
					}

					await this.redishandler.insertZSetByService(service, Date.now(), logString);

					channel.ack(msg);
				} catch (error) {
					console.error("Error processing log message:", error);
					const isUnrecoverable = error instanceof SyntaxError || error.isUnrecoverable;

					if (!isUnrecoverable && retryCount < 3) {
						msg.properties.headers["retry-count"] = retryCount + 1;

						const originalRoutingKey = msg.fields.routingKey;

						this.mainChannel.publish(
							QUEUE_EXCHANGE.RETRY,
							originalRoutingKey,
							msg.content,
							msg.properties,
						);

						channel.ack(msg);
					} else {
						channel.sendToQueue(QUEUE.DLQ, msg.content, {
							headers: {
								...msg.properties.headers,
								"error-reason": error.message,
								"is-unrecoverable": isUnrecoverable,
								"retry-count": retryCount,
								"service-name": service ?? "unknown",
							},
						});
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


