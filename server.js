// Watch Sync : serveur de relais (protocole v2).
// Il ne voit passer que des commandes (lecture / pause / position / adresse de la page),
// jamais de vidéo. Compatible avec les anciennes versions de l'extension.
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const PROTOCOL = 2;
const MAX_PER_ROOM = 10;
const HOLD_TIMEOUT_MS = Number(process.env.HOLD_TIMEOUT_MS) || 20000; // attente max d'un lecteur qui charge
const RATE_PER_SEC = 20; // anti-flood : messages par seconde et par client
const RATE_BURST = 60;
const ACTIONS = new Set(['play', 'pause', 'seek']);

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Watch Sync server OK (v' + PROTOCOL + ')\n');
});

const wss = new WebSocketServer({ server });

// room = { clients:Set<ws>, last:null|{videoId,time,playing,ts}, hold:null|{wasPlaying,ids:Set,timer} }
const rooms = new Map();

const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 && n < 1e6 ? n : null;
};
const str = (v) => (typeof v === 'string' && v ? v.slice(0, 500) : null);
const send = (ws, msg) => {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
};
function toAll(room, msg, except) {
  const data = JSON.stringify(msg);
  for (const c of room.clients) if (c !== except && c.readyState === 1) c.send(data);
}

function broadcastPeers(room) {
  toAll(room, { type: 'peers', count: room.clients.size });
}

// ---------- attente groupée : un lecteur qui charge met les autres en pause ----------
function dropHold(room) {
  const h = room.hold;
  if (!h) return null;
  clearTimeout(h.timer);
  room.hold = null;
  return h;
}

// Fin de l'attente : tout le monde reprend à la position du dernier lecteur prêt.
function finishHold(room, time) {
  const h = dropHold(room);
  if (!h) return;
  if (h.wasPlaying && room.last) {
    const t = time != null ? time : room.last.time;
    room.last = { ...room.last, time: t, playing: true, ts: Date.now() };
    toAll(room, { type: 'resume', time: t, videoId: room.last.videoId });
  } else {
    toAll(room, { type: 'wait', waiting: 0 });
  }
}

// Une action volontaire (lecture, pause, saut, nouveau titre) annule l'attente.
function cancelHold(room) {
  const h = dropHold(room);
  if (!h) return;
  if (room.last) room.last = { ...room.last, playing: h.wasPlaying };
  toAll(room, { type: 'wait', waiting: 0 });
}

function leave(ws) {
  const code = ws.room;
  if (!code) return;
  ws.room = null;
  const room = rooms.get(code);
  if (!room) return;
  room.clients.delete(ws);
  if (room.clients.size === 0) {
    dropHold(room);
    rooms.delete(code);
    return;
  }
  if (room.hold && room.hold.ids.delete(ws.id)) {
    if (room.hold.ids.size === 0) finishHold(room, null);
    else toAll(room, { type: 'wait', waiting: room.hold.ids.size });
  }
  broadcastPeers(room);
}

wss.on('connection', (ws) => {
  ws.id = crypto.randomBytes(4).toString('hex');
  ws.isAlive = true;
  ws.room = null;
  ws.tokens = RATE_BURST;
  ws.refilled = Date.now();
  ws.on('pong', () => (ws.isAlive = true));
  send(ws, { type: 'welcome', v: PROTOCOL, id: ws.id });

  ws.on('message', (raw) => {
    if (raw.length > 4096) return;

    const now = Date.now();
    ws.tokens = Math.min(RATE_BURST, ws.tokens + ((now - ws.refilled) / 1000) * RATE_PER_SEC);
    ws.refilled = now;
    if (ws.tokens < 1) return;
    ws.tokens -= 1;

    let m;
    try {
      m = JSON.parse(raw);
    } catch (e) {
      return;
    }
    if (!m || typeof m !== 'object') return;

    if (m.type === 'ping') return send(ws, { type: 'pong', t: m.t });

    if (m.type === 'join') {
      const code = String(m.room || '').trim().toUpperCase().slice(0, 64);
      if (!code) return;
      leave(ws);
      let room = rooms.get(code);
      if (!room) {
        room = { clients: new Set(), last: null, hold: null };
        rooms.set(code, room);
      }
      if (room.clients.size >= MAX_PER_ROOM) {
        return send(ws, { type: 'error', message: 'Salle pleine' });
      }
      room.clients.add(ws);
      ws.room = code;
      broadcastPeers(room);
      return;
    }

    const room = rooms.get(ws.room);
    if (!room) return;

    if (m.type === 'event') {
      if (!ACTIONS.has(m.action)) return;
      const t = num(m.time);
      if (t === null) return;
      const vid = str(m.videoId);
      cancelHold(room);
      const prev = room.last;
      // On n'écrase l'état de la salle que si l'événement concerne le titre en cours.
      if (!prev || !vid || prev.videoId === vid) {
        room.last = {
          videoId: vid || (prev && prev.videoId) || null,
          time: t,
          playing: m.action === 'play' ? true : m.action === 'pause' ? false : prev ? prev.playing : false,
          ts: Date.now(),
        };
      }
      toAll(room, { type: 'event', action: m.action, time: t, videoId: vid, from: ws.id }, ws);
    } else if (m.type === 'navigate') {
      const vid = str(m.videoId);
      if (!vid) return;
      cancelHold(room);
      room.last = { videoId: vid, time: 0, playing: true, ts: Date.now() };
      toAll(room, { type: 'navigate', videoId: vid, from: ws.id }, ws);
    } else if (m.type === 'sync_request') {
      const last = room.last;
      send(ws, {
        type: 'state',
        state: last ? { ...last, age: Date.now() - last.ts, waiting: !!room.hold } : null,
      });
    } else if (m.type === 'buffering') {
      const last = room.last;
      if (!last) return;
      const vid = str(m.videoId);
      if (vid && vid !== last.videoId) return;
      const t = num(m.time);
      if (m.on) {
        if (!room.hold) {
          if (!last.playing) return; // salle en pause : personne n'a à attendre
          room.hold = { wasPlaying: true, ids: new Set(), timer: null };
          room.last = { ...last, time: t != null ? t : last.time, playing: false, ts: Date.now() };
        }
        room.hold.ids.add(ws.id);
        clearTimeout(room.hold.timer);
        room.hold.timer = setTimeout(() => finishHold(room, null), HOLD_TIMEOUT_MS);
        toAll(room, { type: 'wait', waiting: room.hold.ids.size });
      } else if (room.hold && room.hold.ids.delete(ws.id)) {
        if (room.hold.ids.size === 0) finishHold(room, t);
        else toAll(room, { type: 'wait', waiting: room.hold.ids.size });
      }
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});

// Coupe les connexions mortes.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log('Watch Sync server (v' + PROTOCOL + ') sur le port ' + PORT));
