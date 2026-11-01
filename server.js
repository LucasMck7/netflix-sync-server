// Netflix Sync : serveur de relais. Il ne voit passer que des commandes
// (lecture / pause / position / identifiant du titre), jamais de vidéo.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PER_ROOM = 10;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Netflix Sync server OK\n');
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // code -> { clients:Set<ws>, last:null|{videoId,time,playing,ts} }

function broadcastPeers(code) {
  const room = rooms.get(code);
  if (!room) return;
  const msg = JSON.stringify({ type: 'peers', count: room.clients.size });
  for (const c of room.clients) if (c.readyState === 1) c.send(msg);
}

function leave(ws) {
  const code = ws.room;
  if (!code) return;
  const room = rooms.get(code);
  ws.room = null;
  if (!room) return;
  room.clients.delete(ws);
  if (room.clients.size === 0) rooms.delete(code);
  else broadcastPeers(code);
}

function relay(ws, message) {
  const room = rooms.get(ws.room);
  if (!room) return;
  const data = JSON.stringify(message);
  for (const c of room.clients) if (c !== ws && c.readyState === 1) c.send(data);
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (raw) => {
    if (raw.length > 4096) return;
    let m;
    try {
      m = JSON.parse(raw);
    } catch (e) {
      return;
    }

    if (m.type === 'ping') return ws.send(JSON.stringify({ type: 'pong' }));

    if (m.type === 'join') {
      const code = String(m.room || '').trim().toUpperCase().slice(0, 64);
      if (!code) return;
      leave(ws);
      let room = rooms.get(code);
      if (!room) {
        room = { clients: new Set(), last: null };
        rooms.set(code, room);
      }
      if (room.clients.size >= MAX_PER_ROOM) {
        return ws.send(JSON.stringify({ type: 'error', message: 'Salle pleine' }));
      }
      room.clients.add(ws);
      ws.room = code;
      broadcastPeers(code);
      return;
    }

    const room = rooms.get(ws.room);
    if (!room) return;

    if (m.type === 'event') {
      const prev = room.last || { videoId: m.videoId, playing: false };
      room.last = {
        videoId: m.videoId || prev.videoId,
        time: Number(m.time) || 0,
        playing: m.action === 'play' ? true : m.action === 'pause' ? false : prev.playing,
        ts: Date.now(),
      };
      relay(ws, { type: 'event', action: m.action, time: Number(m.time) || 0, videoId: m.videoId });
    } else if (m.type === 'navigate') {
      room.last = { videoId: m.videoId, time: 0, playing: true, ts: Date.now() };
      relay(ws, { type: 'navigate', videoId: m.videoId });
    } else if (m.type === 'sync_request') {
      const last = room.last;
      ws.send(
        JSON.stringify({
          type: 'state',
          state: last ? { ...last, age: Date.now() - last.ts } : null,
        })
      );
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

server.listen(PORT, () => console.log('Netflix Sync server sur le port ' + PORT));
