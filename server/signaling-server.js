// Genesis signaling server — real, minimal, and honest about what it does.
//
// What this IS: a WebSocket relay for the three messages a WebRTC mesh call
// needs to set itself up — "someone joined", "here's my offer/answer", "here's
// an ICE candidate". That's it. Once two browsers have exchanged those, video
// and audio flow directly between them (peer-to-peer) — this server never
// sees a single video frame, and stops being involved at all once the call
// is connected.
//
// What this is NOT: a media server (SFU/MCU). Because it's a pure mesh, every
// participant sends their upload N-1 times (once per other participant). That
// is a real, honest limit — fine up to ~6 people on decent connections, not
// fine for 20. Scaling past that needs a real SFU (e.g. mediasoup, LiveKit),
// which is a different, bigger project, not a tweak to this file.
//
// Deploy: `npm install && npm start` on any Node 18+ host. Free options:
// Render.com free web service, Fly.io free tier, or your own machine/VM
// (same "you host it, zero dollars if you already have somewhere to run it"
// model as the existing docs/LIVE_RELAY_SETUP.md).

const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
const MAX_PEERS_PER_ROOM = 6; // mesh cost grows O(n^2); this is the honest ceiling

const wss = new WebSocketServer({ port: PORT });

// rooms: Map<roomCode, Map<peerId, ws>>
const rooms = new Map();

function roomOf(code) {
  if (!rooms.has(code)) rooms.set(code, new Map());
  return rooms.get(code);
}

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcast(room, fromId, msg) {
  for (const [peerId, peer] of room) {
    if (peerId !== fromId) send(peer.ws, msg);
  }
}

let nextPeerId = 1;

wss.on('connection', (ws) => {
  let joinedRoom = null;
  let peerId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      const room = roomOf(msg.room);
      if (room.size >= MAX_PEERS_PER_ROOM) {
        send(ws, { type: 'room-full', max: MAX_PEERS_PER_ROOM });
        return;
      }
      peerId = 'p' + (nextPeerId++);
      joinedRoom = msg.room;

      const existingPeers = [...room.entries()].map(([id, p]) => ({ id, name: p.name }));
      send(ws, { type: 'joined', peerId, existingPeers });

      broadcast(room, peerId, { type: 'peer-joined', peerId, name: msg.name });

      room.set(peerId, { ws, name: msg.name });
      return;
    }

    if (!joinedRoom) return;
    const room = rooms.get(joinedRoom);
    if (!room) return;
    const target = room.get(msg.to);
    if (target) send(target.ws, { ...msg, from: peerId });
  });

  ws.on('close', () => {
    if (!joinedRoom) return;
    const room = rooms.get(joinedRoom);
    if (!room) return;
    room.delete(peerId);
    broadcast(room, peerId, { type: 'peer-left', peerId });
    if (room.size === 0) rooms.delete(joinedRoom);
  });
});

console.log(`Genesis signaling server listening on ws://localhost:${PORT}`);
console.log(`Rooms cap at ${MAX_PEERS_PER_ROOM} peers each (pure mesh — see file header for why).`);