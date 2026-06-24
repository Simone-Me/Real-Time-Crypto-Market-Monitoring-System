import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

import cors from "cors";
import express from "express";
import { Kafka, logLevel } from "kafkajs";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT ?? 3000);
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const METRICS_TOPIC = process.env.METRICS_TOPIC ?? "crypto-metrics";
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? "crypto-api";
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID ?? "crypto-api-group";

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.NOTHING
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.resolve(__dirname, "../public");

let latestMetrics = {
  status: "waiting-for-data",
  message: "No metrics received yet."
};

function broadcastMetrics(snapshot) {
  const payload = JSON.stringify({
    type: "metrics",
    payload: snapshot
  });

  for (const client of wss.clients) {
    if (client.readyState === 1) {
      client.send(payload);
    }
  }
}

app.use(cors());
app.use(express.json());
app.use(express.static(publicDir));

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    kafkaBrokers: KAFKA_BROKERS,
    metricsTopic: METRICS_TOPIC
  });
});

app.get("/api/metrics", (_req, res) => {
  res.json(latestMetrics);
});

wss.on("connection", (socket) => {
  socket.send(
    JSON.stringify({
      type: "metrics",
      payload: latestMetrics
    })
  );
});

async function startKafkaConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topic: METRICS_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      latestMetrics = JSON.parse(message.value.toString());
      broadcastMetrics(latestMetrics);
      console.log(
        `[API] broadcast ${latestMetrics.symbol} last=${latestMetrics.lastPrice}`
      );
    }
  });
}

async function main() {
  await startKafkaConsumer();

  server.listen(PORT, () => {
    console.log(`API and dashboard available on http://localhost:${PORT}`);
  });
}

async function shutdown(signal) {
  console.log(`Received ${signal}. Closing API service...`);
  await consumer.disconnect().catch(() => undefined);
  wss.close();
  server.close(() => {
    process.exit(0);
  });
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

main().catch(async (error) => {
  console.error("API service failed to start:", error);
  await consumer.disconnect().catch(() => undefined);
  process.exit(1);
});
