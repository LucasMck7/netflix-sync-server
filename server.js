// Watch Sync : serveur de relais (protocole v3).
// Il ne voit passer que des commandes (lecture / pause / position / adresse de la page),
// jamais de vidéo. Compatible avec les anciennes versions de l'extension.
// v3 ajoute deux petites routes HTTP : /config (réglages distants, fichier config.json)
// et /diag (dépôt et lecture de diagnostics, gardés 30 min en mémoire).
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const PROTOCOL = 3;
const MAX_PER_ROOM = 10;
const HOLD_TIMEOUT_MS = Number(process.env.HOLD_TIMEOUT_MS) || 20000; // attente max d'un lecteur qui charge
const RATE_PER_SEC = 20; // anti-flood : messages par seconde et par client
const RATE_BURST = 60;
const ACTIONS = new Set(['play', 'pause', 'seek']);

// ---------- routes HTTP : /config et /diag ----------
const DIAG_TTL_MS = Number(process.env.DIAG_TTL_MS) || 30 * 60 * 1000;
const MAX_DIAG_BYTES = 64 * 1024;
const MAX_DIAG_ROOMS = 300;
const MAX_DIAG_PER_ROOM = 8;
const POSTS_PER_MIN = 12;
const diags = new Map(); // room -> Map(who -> { at, data })
const posts = new Map(); // ip -> [timestamps]

function loadConfig() {
  try {
    const text = fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8');
    if (text.length > 16 * 1024) return {};
    const o = JSON.parse(text);
    return o && typeof o === 'object' && !Array.isArray(o) ? o : {};
  } catch (e) {
    return {};
  }
}

const roomKey = (v) => String(v || '').trim().toUpperCase().replace(/[^A-Z0-9_-]/g, '').slice(0, 64);
const whoKey = (v) => String(v || 'x').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 24) || 'x';

function reply(res, code, obj) {
  res.writeHead(code, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, POST, OPTIONS',
    'access-control-allow-headers': 'content-type',
    'cache-control': 'no-store',
  });
  res.end(JSON.stringify(obj));
}

function tooManyPosts(req) {
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const now = Date.now();
  const list = (posts.get(ip) || []).filter((t) => now - t < 60000);
  list.push(now);
  posts.set(ip, list);
  return list.length > POSTS_PER_MIN;
}

function storeDiag(room, who, data) {
  let m = diags.get(room);
  if (!m) {
    if (diags.size >= MAX_DIAG_ROOMS) {
      const oldest = [...diags.entries()].sort((a, b) => maxAt(a[1]) - maxAt(b[1]))[0];
      if (oldest) diags.delete(oldest[0]);
    }
    m = new Map();
    diags.set(room, m);
  }
  m.set(who, { at: Date.now(), data });
  while (m.size > MAX_DIAG_PER_ROOM) m.delete([...m.entries()].sort((a, b) => a[1].at - b[1].at)[0][0]);
}
const maxAt = (m) => Math.max(0, ...[...m.values()].map((e) => e.at));

setInterval(() => {
  const now = Date.now();
  for (const [room, m] of diags) {
    for (const [who, e] of m) if (now - e.at > DIAG_TTL_MS) m.delete(who);
    if (!m.size) diags.delete(room);
  }
  for (const [ip, list] of posts) {
    const keep = list.filter((t) => now - t < 60000);
    if (keep.length) posts.set(ip, keep);
    else posts.delete(ip);
  }
}, 60000).unref();

const server = http.createServer((req, res) => {
  const url = new URL(req.url || '/', 'http://x');

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, POST, OPTIONS',
      'access-control-allow-headers': 'content-type',
    });
    return res.end();
  }

  if (req.method === 'GET' && url.pathname === '/config') {
    return reply(res, 200, { ok: true, serverV: PROTOCOL, config: loadConfig() });
  }

  if (req.method === 'POST' && url.pathname === '/diag') {
    const room = roomKey(url.searchParams.get('room'));
    if (!room) return reply(res, 400, { ok: false, error: 'room manquante' });
    if (tooManyPosts(req)) return reply(res, 429, { ok: false, error: 'trop de requêtes' });
    const who = whoKey(url.searchParams.get('who'));
    let size = 0;
    const chunks = [];
    let aborted = false;
    req.on('data', (c) => {
      if (aborted) return;
      size += c.length;
      if (size > MAX_DIAG_BYTES) {
        aborted = true;
        reply(res, 413, { ok: false, error: 'diagnostic trop gros' });
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (aborted) return;
      let data;
      try {
        data = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      } catch (e) {
        return reply(res, 400, { ok: false, error: 'JSON invalide' });
      }
      storeDiag(room, who, data);
      reply(res, 200, { ok: true, room, who, bytes: size });
    });
    return;
  }

  if (req.method === 'GET' && url.pathname.startsWith('/diag/')) {
    const room = roomKey(decodeURIComponent(url.pathname.slice(6)));
    const now = Date.now();
    const items = [...(diags.get(room) || new Map()).entries()]
      .filter(([, e]) => now - e.at <= DIAG_TTL_MS)
      .sort((a, b) => b[1].at - a[1].at)
      .map(([who, e]) => ({ who, ageSec: Math.round((now - e.at) / 1000), data: e.data }));
    return reply(res, 200, { ok: true, room, items });
  }

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
