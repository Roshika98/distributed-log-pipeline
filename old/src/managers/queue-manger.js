const amqplib = require("amqplib");

const QUEUE_TYPE = {
	MAIN: "main",
	RETRY: "retry",
	DLQ: "dlq",
};

const QUEUE_EXCHANGE = {
	MAIN: "logs.topic",
	RETRY: "logs.retry.exchange",
};

const QUEUE = {
	MAIN: "service-logs",
	RETRY: "log-retry",
	DLQ: "log-dlq",
};

class QueueManager {
	/**
	 * @type {QueueManager}
	 */
	static instance = null;

	/**
	 * @returns {QueueManager}
	 */
	static getInstance() {
		if (!QueueManager.instance) {
			const rabbitmqUrl = process.env.RABBITMQ_URL || "amqp://localhost";
			const queueName = process.env.LOG_QUEUE_NAME || "service-logs";
			QueueManager.instance = new QueueManager(rabbitmqUrl, queueName);
		}
		return QueueManager.instance;
	}

	constructor(rabbitmqUrl, queueName) {
		this.rabbitmqUrl = rabbitmqUrl;
		this.queueName = queueName;
		this.connection = null;
		this.mainChannel = null;
		this.retryChannel = null;
		this.dlqChannel = null;
		this.retryQueue = null;
		this.dlqQueue = null;
	}

	async initialize() {
		this.connection = await amqplib.connect(this.rabbitmqUrl, {
			credentials: amqplib.credentials.plain(
				process.env.RABBITMQ_USER || "logger",
				process.env.RABBITMQ_PASSWORD || "abcd1234",
			),
		});
		this.mainChannel = await this.connection.createChannel();

		await this.mainChannel.assertExchange(QUEUE_EXCHANGE.MAIN, "topic", { durable: true });
		await this.mainChannel.assertExchange(QUEUE_EXCHANGE.RETRY, "topic", { durable: true });

		this.retryChannel = await this.connection.createChannel();
		this.retryQueue = await this.retryChannel.assertQueue(QUEUE.RETRY, {
			durable: true,
			deadLetterExchange: QUEUE_EXCHANGE.MAIN,
			messageTtl: 5000,
		});
		await this.retryChannel.bindQueue(QUEUE.RETRY, QUEUE_EXCHANGE.RETRY, "#");

		this.dlqChannel = await this.connection.createChannel();
		this.dlqQueue = await this.dlqChannel.assertQueue(QUEUE.DLQ, { durable: true });
	}

	/**
	 * Get the main channel
	 * @param {QUEUE_TYPE} type
	 * @returns {amqplib.Channel}
	 */
	getChannel(type) {
		switch (type) {
			case QUEUE_TYPE.MAIN:
				return this.mainChannel;
			case QUEUE_TYPE.RETRY:
				return this.retryChannel;
			case QUEUE_TYPE.DLQ:
				return this.dlqChannel;
			default:
				throw new Error("Invalid queue type");
		}
	}

	/**
	 * Create a channel
	 * @returns {Promise<amqplib.Channel>}
	 */
	async createChannel() {
		const channel = await this.connection.createChannel();
		return channel;
	}
}

module.exports = {
	QueueManager,
	QUEUE_TYPE,
	QUEUE_EXCHANGE,
	QUEUE,
};
