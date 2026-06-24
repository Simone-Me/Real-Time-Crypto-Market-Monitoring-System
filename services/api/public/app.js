const connectionStatus = document.getElementById("connection-status");
const connectionDot = document.getElementById("connection-dot");
const lastPrice = document.getElementById("last-price");
const avgPrice = document.getElementById("avg-price");
const windowVolume = document.getElementById("window-volume");
const priceChange = document.getElementById("price-change");
const exchange = document.getElementById("exchange");
const totalTrades = document.getElementById("total-trades");
const cumulativeVolume = document.getElementById("cumulative-volume");
const vwap = document.getElementById("vwap");
const symbolBadge = document.getElementById("symbol-badge");
const largeTradesList = document.getElementById("large-trades");

const chartContext = document.getElementById("price-chart").getContext("2d");

const chart = new Chart(chartContext, {
  type: "line",
  data: {
    labels: [],
    datasets: [
      {
        label: "BTC/USDT",
        data: [],
        borderColor: "#73e0a9",
        backgroundColor: "rgba(115, 224, 169, 0.14)",
        fill: true,
        tension: 0.28,
        pointRadius: 0,
        borderWidth: 2
      }
    ]
  },
  options: {
    animation: false,
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        labels: {
          color: "#dfe8fb"
        }
      }
    },
    scales: {
      x: {
        ticks: {
          color: "#8ba0c7",
          maxTicksLimit: 6
        },
        grid: {
          color: "rgba(139, 160, 199, 0.12)"
        }
      },
      y: {
        ticks: {
          color: "#8ba0c7"
        },
        grid: {
          color: "rgba(139, 160, 199, 0.12)"
        }
      }
    }
  }
});

function setConnectionState(statusText, stateClass) {
  connectionStatus.textContent = statusText;
  connectionDot.classList.remove("connected", "disconnected");

  if (stateClass) {
    connectionDot.classList.add(stateClass);
  }
}

function formatNumber(value, digits = 2) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }

  return value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits
  });
}

function formatPercent(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }

  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatTime(timestamp) {
  return new Date(timestamp).toLocaleTimeString();
}

function renderLargeTrades(trades = []) {
  if (!trades.length) {
    largeTradesList.innerHTML = "<li>No large trades detected yet.</li>";
    return;
  }

  largeTradesList.innerHTML = trades
    .map(
      (trade) => `
        <li>
          <strong>${trade.side.toUpperCase()}</strong>
          ${formatNumber(trade.quantity, 5)} BTC at $${formatNumber(trade.price, 2)}
          <small>${formatTime(trade.tradeTime)} | Notional: $${formatNumber(
            trade.notionalUsd,
            2
          )}</small>
        </li>
      `
    )
    .join("");
}

function renderMetrics(metrics) {
  if (!metrics || metrics.status === "waiting-for-data") {
    return;
  }

  lastPrice.textContent = `$${formatNumber(metrics.lastPrice, 2)}`;
  avgPrice.textContent = `$${formatNumber(metrics.averagePrice, 2)}`;
  windowVolume.textContent = `${formatNumber(metrics.windowVolume, 5)} BTC`;
  priceChange.textContent = formatPercent(metrics.priceChangePercent);
  exchange.textContent = metrics.exchange ?? "--";
  totalTrades.textContent = formatNumber(metrics.totalTrades, 0);
  cumulativeVolume.textContent = `${formatNumber(metrics.cumulativeVolume, 5)} BTC`;
  vwap.textContent = `$${formatNumber(metrics.volumeWeightedAveragePrice, 2)}`;
  symbolBadge.textContent = metrics.symbol ?? "Unknown";

  chart.data.labels = (metrics.priceSeries ?? []).map((point) =>
    formatTime(point.timestamp)
  );
  chart.data.datasets[0].data = (metrics.priceSeries ?? []).map(
    (point) => point.price
  );
  chart.data.datasets[0].label = metrics.symbol ?? "Live price";
  chart.update();

  renderLargeTrades(metrics.recentLargeTrades);
}

async function bootstrapFromRest() {
  try {
    const response = await fetch("/api/metrics");
    const metrics = await response.json();
    renderMetrics(metrics);
  } catch (error) {
    console.error("Failed to fetch initial metrics:", error);
  }
}

function connectWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);

  setConnectionState("Connecting...", null);

  socket.addEventListener("open", () => {
    setConnectionState("Connected", "connected");
  });

  socket.addEventListener("message", (event) => {
    const { payload } = JSON.parse(event.data);
    renderMetrics(payload);
  });

  socket.addEventListener("close", () => {
    setConnectionState("Disconnected", "disconnected");
    setTimeout(connectWebSocket, 3_000);
  });

  socket.addEventListener("error", () => {
    socket.close();
  });
}

bootstrapFromRest();
connectWebSocket();
