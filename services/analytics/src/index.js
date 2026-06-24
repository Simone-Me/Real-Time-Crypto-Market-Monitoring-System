import { Kafka, logLevel } from "kafkajs";

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const TRADE_TOPIC = process.env.TRADE_TOPIC ?? "crypto-trades";
const METRICS_TOPIC = process.env.METRICS_TOPIC ?? "crypto-metrics";
const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? "crypto-analytics";
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID ?? "crypto-analytics-group";
const WINDOW_MS = Number(process.env.WINDOW_MS ?? 60_000);
const LARGE_TRADE_USD = Number(process.env.LARGE_TRADE_USD ?? 50_000);
const MAX_LARGE_TRADES = 12;
const MAX_PRICE_POINTS = 120;

const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.NOTHING
});

const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });
const producer = kafka.producer();

const state = {
  totalTrades: 0,
  cumulativeVolume: 0,
  recentTrades: [],
  recentLargeTrades: [],
  priceSeries: [],
  latestSnapshot: null
};

function pruneState(now) {
  state.recentTrades = state.recentTrades.filter(
    (trade) => now - trade.tradeTime <= WINDOW_MS
  );
  state.priceSeries = state.priceSeries.filter(
    (point) => now - point.timestamp <= WINDOW_MS
  );
}

function buildSnapshot(lastTrade) {
  const windowTrades = state.recentTrades;
  const windowCount = windowTrades.length;
  const totalWindowPrice = windowTrades.reduce((sum, trade) => sum + trade.price, 0);
  const totalWindowQuantity = windowTrades.reduce(
    (sum, trade) => sum + trade.quantity,
    0
  );
  const totalWindowNotional = windowTrades.reduce(
    (sum, trade) => sum + trade.notionalUsd,
    0
  );
  const firstTrade = windowTrades[0];
  const priceChangePercent =
    firstTrade && firstTrade.price > 0
      ? ((lastTrade.price - firstTrade.price) / firstTrade.price) * 100
      : 0;

  return {
    symbol: lastTrade.symbol,
    exchange: lastTrade.exchange,
    updatedAt: Date.now(),
    windowMs: WINDOW_MS,
    totalTrades: state.totalTrades,
    lastPrice: lastTrade.price,
    averagePrice: windowCount > 0 ? totalWindowPrice / windowCount : 0,
    volumeWeightedAveragePrice:
      totalWindowQuantity > 0 ? totalWindowNotional / totalWindowQuantity : 0,
    windowVolume: totalWindowQuantity,
    cumulativeVolume: state.cumulativeVolume,
    priceChangePercent,
    recentLargeTrades: state.recentLargeTrades,
    priceSeries: state.priceSeries
  };
}

async function publishSnapshot(snapshot) {
  await producer.send({
    topic: METRICS_TOPIC,
    messages: [
      {
        key: snapshot.symbol,
        value: JSON.stringify(snapshot)
      }
    ]
  });
}

async function handleTrade(trade) {
  const now = Date.now();
  state.totalTrades += 1;
  state.cumulativeVolume += trade.quantity;
  state.recentTrades.push(trade);
  state.priceSeries.push({
    timestamp: trade.tradeTime,
    price: trade.price
  });

  if (trade.notionalUsd >= LARGE_TRADE_USD) {
    state.recentLargeTrades.unshift(trade);
    state.recentLargeTrades = state.recentLargeTrades.slice(0, MAX_LARGE_TRADES);
  }

  pruneState(now);

  if (state.priceSeries.length > MAX_PRICE_POINTS) {
    state.priceSeries = state.priceSeries.slice(-MAX_PRICE_POINTS);
  }

  const snapshot = buildSnapshot(trade);
  state.latestSnapshot = snapshot;
  await publishSnapshot(snapshot);

  console.log(
    `[ANALYTICS] last=${snapshot.lastPrice} avg=${snapshot.averagePrice.toFixed(
      2
    )} volume=${snapshot.windowVolume.toFixed(4)}`
  );
}

async function main() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TRADE_TOPIC, fromBeginning: false });

  console.log(`Analytics consumer listening on topic "${TRADE_TOPIC}"`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) {
        return;
      }

      const trade = JSON.parse(message.value.toString());
      await handleTrade(trade);
    }
  });
}

async function shutdown(signal) {
  console.log(`Received ${signal}. Closing analytics service...`);
  await consumer.disconnect().catch(() => undefined);
  await producer.disconnect().catch(() => undefined);
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

main().catch(async (error) => {
  console.error("Analytics service failed to start:", error);
  await consumer.disconnect().catch(() => undefined);
  await producer.disconnect().catch(() => undefined);
  process.exit(1);
});
