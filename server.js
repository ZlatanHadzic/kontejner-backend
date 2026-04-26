const { WebSocketServer } = require("ws");

const PORT = 3001;
const wss  = new WebSocketServer({ port: PORT });

const dashboards     = new Set();
const devices        = new Map();
const sensorHistory  = {};   // id → zadnjih N mjerenja
const fullStatus     = {};   // id → trenutni full status (sa histerezom)

const HISTORY_SIZE    = 5;   // usrednji zadnjih 5 mjerenja
const THRESHOLD_PUNO  = 85;  // postaje pun na 85%
const THRESHOLD_FREE  = 75;  // postaje slobodan tek ispod 75%
const MAX_CM          = 50;  // prazan kontejner (prilagodi prema svom)
const MIN_CM          = 5;   // pun kontejner

function distToFill(dist) {
  const clamped = Math.min(MAX_CM, Math.max(MIN_CM, dist));
  return Math.round(((MAX_CM - clamped) / (MAX_CM - MIN_CM)) * 100);
}

function processReading(id, dist) {
  // Rolling average
  if (!sensorHistory[id]) sensorHistory[id] = [];
  sensorHistory[id].push(dist);
  if (sensorHistory[id].length > HISTORY_SIZE) sensorHistory[id].shift();

  const avgDist = Math.round(
    sensorHistory[id].reduce((a, b) => a + b, 0) / sensorHistory[id].length
  );
  const fill = distToFill(avgDist);

  // Dual-threshold hysteresis za full status
  const wasFull = fullStatus[id] || false;
  const nowFull = wasFull ? fill >= THRESHOLD_FREE : fill >= THRESHOLD_PUNO;
  fullStatus[id] = nowFull;

  return { dist: avgDist, fill, full: nowFull };
}

wss.on("connection", (socket, req) => {
  const ip   = req.socket.remoteAddress || "unknown";
  const path = req.url;
  console.log(`\n[KONEKCIJA] IP: ${ip} | Path: ${path}`);

  const pingInterval = setInterval(() => {
    if (socket.readyState === socket.OPEN) socket.ping();
  }, 10000);

  socket.on("pong", () => {
    console.log(`[PONG] ${ip} je živ`);
  });

  socket.on("message", (raw) => {
    const str = raw.toString().trim();

    try {
      const data = JSON.parse(str);

      // ESP32 senzor
      if (data.id && data.dist !== undefined) {
        const { dist, fill, full } = processReading(data.id, data.dist);

        console.log(`[SENZOR] ${data.id} → raw:${data.dist}cm avg:${dist}cm fill:${fill}% pun:${full}`);

        const smoothed = { id: data.id, dist, fill, full, ts: Date.now() };
        devices.set(data.id, smoothed);

        const msg = JSON.stringify({ type: "sensor", ...smoothed });
        let sent = 0;
        for (const dash of dashboards) {
          if (dash.readyState === dash.OPEN) { dash.send(msg); sent++; }
        }
        if (sent > 0) console.log(`[PROSLIJEĐENO] na ${sent} dashboard(a)`);
      }

      // Dashboard registracija
      if (data.type === "register") {
        dashboards.add(socket);
        console.log(`[DASHBOARD] registrovan. Ukupno: ${dashboards.size}`);
        socket.send(JSON.stringify({
          type: "state",
          devices: Object.fromEntries(devices)
        }));
      }

    } catch (e) {
      // Stari format — samo broj
      const num = parseInt(str);
      if (!isNaN(num) && num > 0) {
        const { dist, fill, full } = processReading("BIN-001", num);
        const smoothed = { id: "BIN-001", dist, fill, full, ts: Date.now() };
        devices.set("BIN-001", smoothed);
        const msg = JSON.stringify({ type: "sensor", ...smoothed });
        for (const dash of dashboards) {
          if (dash.readyState === dash.OPEN) dash.send(msg);
        }
      }
    }
  });

  socket.on("close", (code) => {
    dashboards.delete(socket);
    clearInterval(pingInterval);
    console.log(`[ZATVORENO] ${ip} | kod: ${code} | Dashboardova: ${dashboards.size}`);
  });

  socket.on("error", (err) => {
    console.log(`[GREŠKA] ${ip}: ${err.message}`);
  });
});

console.log(`✓ WebSocket server pokrenut na ws://localhost:${PORT}`);
console.log(`✓ ESP32 spoji na: ws://<IP_MACBOOKA>:${PORT}/ws`);