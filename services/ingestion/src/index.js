import WebSocket from "ws";
import { Kafka, logLevel } from "kafkajs";

const BINANCE_STREAM_URL =
  process.env.BINANCE_STREAM_URL ??
  "wss://stream.binance.com:9443/ws/btcusdt@trade";
const TRADE_TOPIC = process.env.TRADE_TOPIC ?? "crypto-trades";
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? "crypto-ingestion";
const RECONNECT_DELAY_MS = 3_000;

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.NOTHING
});

const producer = kafka.producer();
let socket;
let reconnectTimer;
let shuttingDown = false;

function normalizeTradeMessage(rawMessage) {
  return {
    exchange: "binance",
    symbol: rawMessage.s,
    tradeId: rawMessage.t,
    price: Number(rawMessage.p),
    quantity: Number(rawMessage.q),
    notionalUsd: Number(rawMessage.p) * Number(rawMessage.q),
    side: rawMessage.m ? "sell" : "buy",
    eventTime: rawMessage.E,
    tradeTime: rawMessage.T,
    receivedAt: Date.now()
  };
}

async function publishTrade(trade) {
  await producer.send({
    topic: TRADE_TOPIC,
    messages: [
      {
        key: `${trade.symbol}`,
        value: JSON.stringify(trade)
      }
    ]
  });
}

function scheduleReconnect() {
  if (shuttingDown || reconnectTimer) {
    return;
  }

  reconnectTimer = setTimeout(() => {
    reconnectTimer = undefined;
    connectToBinance().catch((error) => {
      console.error("Reconnect failed:", error.message);
      scheduleReconnect();
    });
  }, RECONNECT_DELAY_MS);
}

async function connectToBinance() {
  console.log(`Connecting to Binance stream: ${BINANCE_STREAM_URL}`);
  socket = new WebSocket(BINANCE_STREAM_URL);

  socket.on("open", () => {
    console.log("Binance WebSocket connected.");
  });

  socket.on("message", async (buffer) => {
    try {
      const rawMessage = JSON.parse(buffer.toString());
      const trade = normalizeTradeMessage(rawMessage);
      await publishTrade(trade);
      console.log(
        `[INGESTION] ${trade.symbol} price=${trade.price} qty=${trade.quantity}`
      );
    } catch (error) {
      console.error("Failed to process trade message:", error.message);
    }
  });

  socket.on("close", () => {
    console.warn("Binance WebSocket closed. Scheduling reconnect.");
    scheduleReconnect();
  });

  socket.on("error", (error) => {
    console.error("Binance WebSocket error:", error.message);
    socket.close();
  });
}

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  console.log(`Received ${signal}. Closing ingestion service...`);

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
  }

  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.close();
  }

  await producer.disconnect();
  process.exit(0);
}

async function main() {
  await producer.connect();
  console.log(`Kafka producer connected on ${KAFKA_BROKERS.join(", ")}`);
  await connectToBinance();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

main().catch(async (error) => {
  console.error("Ingestion service failed to start:", error);
  await producer.disconnect().catch(() => undefined);
  process.exit(1);
});
