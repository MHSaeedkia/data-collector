const path = require("path");
const http = require("http");
const express = require("express");
const { WebSocketServer } = require("ws");
const { Kafka, logLevel } = require("kafkajs");

const PORT = process.env.PORT || 3000;
const KAFKA_BROKER = process.env.KAFKA_BROKER || "localhost:9092";

const app = express();
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// Latest consolidated book per topic. topic = `${pair}-${side}` (e.g. BTC-USDT-asks).
const latestByTopic = new Map();

function broadcast(payload) {
	const msg = JSON.stringify(payload);
	for (const client of wss.clients) {
		if (client.readyState === client.OPEN) client.send(msg);
	}
}

// New browser → send everything we have so it can render immediately.
wss.on("connection", (ws) => {
	ws.send(
		JSON.stringify({
			type: "snapshot",
			books: Array.from(latestByTopic.values()),
		}),
	);
});

const kafka = new Kafka({
	clientId: "orderbook-web",
	brokers: [KAFKA_BROKER],
	logLevel: logLevel.ERROR,
});

// Fresh group each start so we always replay current state from the beginning.
const consumer = kafka.consumer({ groupId: `orderbook-web-${Date.now()}` });

// Serve the UI immediately so the page loads even before (or without) Kafka.
server.listen(PORT, () => {
	console.log(`Order book UI:    http://localhost:${PORT}`);
	console.log(`Kafka broker:     ${KAFKA_BROKER}`);
});

async function consume() {
	await consumer.connect();
	// Output topics only: end in -asks/-bids. Input topics end in -<exchange>, so they don't match.
	await consumer.subscribe({
		topics: [/^.+-(asks|bids)$/],
		fromBeginning: true,
	});

	await consumer.run({
		eachMessage: async ({ topic, message }) => {
			if (!message.value) return;
			try {
				const book = JSON.parse(message.value.toString());
				book.pair = (book.pair + "").replace("-", "/");
				latestByTopic.set(topic, book);
				broadcast({ type: "update", book });
			} catch (err) {
				console.error(
					`Skipping bad message on ${topic}: ${err.message}`,
				);
			}
		},
	});
}

consume().catch((err) => {
	console.error(
		`Kafka consumer error (UI stays up; ensure broker at ${KAFKA_BROKER} is reachable): ${err.message}`,
	);
});
