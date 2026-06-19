const amqp = require("amqplib");
const { v4: uuidv4 } = require("uuid");

const RABBIT_URL = process.env.RABBIT_URL || "amqp://localhost";
const QUEUE_NAME = process.env.QUEUE_NAME || "service-logs";
const INTERVAL_MS = Number(
  process.env.LOG_INTERVAL_MS || process.argv[2] || 10,
);

let sequence = 0;

async function start() {
  const conn = await amqp.connect(RABBIT_URL, {
    credentials: amqp.credentials.plain(
      process.env.RABBITMQ_USER || "kbuser",
      process.env.RABBITMQ_PASSWORD || "rabbitKB123r"
    ),
  });
  const channel = await conn.createConfirmChannel();

  //   await channel.assertQueue(QUEUE_NAME, {
  //     durable: true,
  //   });

  console.log(`📤 Publishing logs every ${INTERVAL_MS} ms`);

  setInterval(() => {
    const log = {
      id: uuidv4(),
      service: "tms",
      // service: Math.random() > 0.5 ? "tms" : "lb",
      level: Math.random() > 0.7 ? "ERROR" : "WARN",
      message: "Mock log message",
      stack: "Error stack trace here",
      timestamp: new Date().toISOString(),
      sequence: ++sequence,
    };

    const payload = Buffer.from(JSON.stringify(log));

    channel.publish(
      "logs.topic",
      `log.${log.service}.error`,
      payload,
      { persistent: true, headers: { "service-name": log.service } },
      (err) => {
        if (err) {
          console.error("Failed to publish log", err);
        } else {
          console.log("Log sent", log.sequence);
        }
      }
    );

    // channel.sendToQueue(
    //   QUEUE_NAME,
    //   payload,
    //   { persistent: true, headers: { "service-name": log.service } },
    //   (err) => {
    //     if (err) {
    //       console.error("❌ Failed to publish log", err);
    //     } else {
    //       console.log("✅ Log sent", log.sequence);
    //     }
    //   }
    // );
  }, INTERVAL_MS);
}

start().catch(console.error);
