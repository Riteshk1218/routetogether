// RouteTogether — real-time group location sharing
// Trips live in memory: each trip has members (last known position) and an optional destination.
const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

 app.use(express.static(__dirname));
app.get("/health", (_req, res) => res.send("ok"));

/** trips: code -> { members: Map<id, member>, dest: {lat,lng,name}|null, touched } */
const trips = new Map();
const COLORS = ["#e2453c", "#8b45d6", "#e0a20e", "#13a36b", "#1677e5", "#d6457f", "#0f9fb0", "#7a5b2e"];

function getTrip(code) {
  if (!trips.has(code)) trips.set(code, { members: new Map(), dest: null, touched: Date.now() });
  const t = trips.get(code);
  t.touched = Date.now();
  return t;
}
const clean = (s, n) => String(s || "").replace(/[<>]/g, "").trim().slice(0, n);
const validCode = (c) => /^[A-Z0-9-]{3,16}$/.test(c);
const num = (v, lo, hi) => typeof v === "number" && isFinite(v) && v >= lo && v <= hi;

function snapshot(code) {
  const t = trips.get(code);
  if (!t) return { members: [], dest: null };
  return { members: [...t.members.values()], dest: t.dest };
}

io.on("connection", (socket) => {
  let code = null;
  let memberId = null;

  socket.on("join", (data, ack) => {
    const c = clean(data?.code, 16).toUpperCase();
    const name = clean(data?.name, 24);
    memberId = clean(data?.memberId, 40) || socket.id;
    if (!validCode(c) || !name) return ack?.({ error: "Enter your name and a trip code (3–16 letters or numbers)." });
    code = c;
    socket.join(code);
    const t = getTrip(code);
    const existing = t.members.get(memberId);
    const used = new Set([...t.members.values()].map((m) => m.color));
    const color = existing?.color || COLORS.find((x) => !used.has(x)) || COLORS[t.members.size % COLORS.length];
    t.members.set(memberId, {
      id: memberId, name, color,
      lat: existing?.lat ?? null, lng: existing?.lng ?? null, acc: existing?.acc ?? null,
      updated: existing?.updated ?? null, online: true, sharing: existing?.sharing ?? true,
    });
    ack?.({ ok: true, code, memberId, ...snapshot(code) });
    io.to(code).emit("state", snapshot(code));
  });

  socket.on("pos", (p) => {
    if (!code || !memberId) return;
    const t = trips.get(code); const m = t?.members.get(memberId);
    if (!m || !num(p?.lat, -90, 90) || !num(p?.lng, -180, 180)) return;
    Object.assign(m, { lat: p.lat, lng: p.lng, acc: num(p.acc, 0, 1e5) ? Math.round(p.acc) : null, updated: Date.now(), online: true, sharing: true });
    t.touched = Date.now();
    io.to(code).emit("member", m);
  });

  socket.on("sharing", (on) => {
    const m = trips.get(code)?.members.get(memberId);
    if (!m) return;
    m.sharing = !!on;
    if (!on) { m.lat = null; m.lng = null; }
    io.to(code).emit("member", m);
  });

  socket.on("dest", (d) => {
    const t = code && trips.get(code);
    if (!t) return;
    if (d === null) t.dest = null;
    else if (num(d?.lat, -90, 90) && num(d?.lng, -180, 180)) t.dest = { lat: d.lat, lng: d.lng, name: clean(d.name, 40) || "Meeting point" };
    else return;
    io.to(code).emit("state", snapshot(code));
  });

  socket.on("leave", () => {
    const t = trips.get(code);
    if (t && memberId) { t.members.delete(memberId); io.to(code).emit("state", snapshot(code)); }
    code = null;
  });

  socket.on("disconnect", () => {
    const m = trips.get(code)?.members.get(memberId);
    if (m) { m.online = false; io.to(code).emit("member", m); }
  });
});

// Forget trips nobody has touched for 24 hours
setInterval(() => {
  const cutoff = Date.now() - 24 * 3600 * 1000;
  for (const [c, t] of trips) if (t.touched < cutoff) trips.delete(c);
}, 3600 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`RouteTogether running on port ${PORT}`));
