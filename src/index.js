require("dotenv").config();
const LogConsumer = require("./consumers/log-consumer");
const RedisDrainer = require("./consumers/redis-drainer");
const RedisHandler = require("./utility/redis-handler");
const DbWorker = require("./consumers/db-worker");
const { QueueManager } = require("./managers/queue-manger");
const servicesList = require("./service_list.json");
const express = require("express");
const amqp = require("amqplib");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

app.use("/api", require("./routes"));

app.get("/", (req, res) => {
  res.send("Log Consumer is running.");
});

async function waitForRabbitMQ(retries = 10, interval = 5000) {
  const rabbitMqUser = process.env.RABBITMQ_USER || "kbuser";
  const rabbitMqPassword = process.env.RABBITMQ_PASSWORD || "rabbitKB123r";

  for (let i = 0; i < retries; i++) {
    try {
      const connection = await amqp.connect("amqp://rabbitmq", {
        credentials: amqp.credentials.plain(rabbitMqUser, rabbitMqPassword),
      });
      await connection.close();
      console.log("RabbitMQ is ready");
      return true;
    } catch (err) {
      console.log(`Waiting for RabbitMQ... (${i + 1}/${retries})`);
      await new Promise((res) => setTimeout(res, interval));
    }
  }

  throw new Error("RabbitMQ not available after multiple attempts");
}

async function start() {
	const redisHandler = RedisHandler.getInstance();
	const queueManager = QueueManager.getInstance();
	await queueManager.initialize();
	// const consumer = LogConsumer.getInstance();
	const dbWorker = DbWorker.getInstance();
	dbWorker.processLogs();
	await consumer.intialize();
	setTimeout(() => {
		const services = servicesList.services;
		services.forEach((service) => {
			const drainer = new RedisDrainer(service.queue);
			drainer.initialize();
		});
	}, 1000);
}

async function startServer() {
  try {
    await waitForRabbitMQ();

    app.listen(PORT, () => {
      console.log(`Server is running on port ${PORT}`);

      start().catch((err) => {
        console.error("Failed to start log consumer:", err);
        process.exit(1);
      });
    });
  } catch (error) {
    console.error(err.message);
    process.exit(1); // Exit if RabbitMQ is not available
  }
}

startServer();


