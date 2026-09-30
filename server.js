'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const OPKEY = process.env.OPERATOR_KEY || 'admin123';
const GL = 'ABCDEFGHIJKLMNOP';

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: 65536 });

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

const CHUNKS_DIR = path.join(__dirname, 'chunks');
if (fs.existsSync(CHUNKS_DIR)) fs.rmSync(CHUNKS_DIR, { recursive: true, force: true });
fs.mkdirSync(CHUNKS_DIR, { recursive: true });

/* ---------- Tournament & Match Data ---------- */
let midx = 0;
const mkMatch = (type, grp, home, away) => ({
  id: 'm' + (++midx),
  type,
  grp,
  home,
  away,
  hs: 0,
  as: 0,
  min: 0,
  st: 'up',
  streamer: null,
  chunks: [],
  chat: [],
  det: null,
  koSlot: null
});

const DEMO = { A: ['KEV', 'BRI', 'DAV', 'SOL'], B: ['SAM', 'TOM', 'EMMA', 'JOE'] };
let GROUPS = DEMO;
const T = {
  name: 'Community Cup',
  status: 'running',
  players: Object.values(DEMO).flat(),
  groups: DEMO
};

const M = [];
const UPG = {};
const VQ = [];
const feed = [];
const clients = new Set();
let KO = { sf1: null, sf2: null, final: null, seeded: false };
let CHAMP = null;

function pairs(arr) {
  const res = [];
  for (let i = 0; i < arr.length; i++) {
    for (let j = i + 1; j < arr.length; j++) res.push([arr[i], arr[j]]);
  }
  return res.sort(() => Math.random() - 0.5);
}

function buildWorld() {
  M.length = 0;
  VQ.length = 0;
  feed.length = 0;
  KO = { sf1: null, sf2: null, final: null, seeded: false };
  CHAMP = null;
  midx = 0;

  for (const g of Object.keys(GROUPS)) {
    const p = pairs(GROUPS[g]);
    if (!p.length) continue;
    UPG[g] = [];
    const f = mkMatch('fx', g, p[0][0], p[0][1]);
    f.st = 'live';
    M.push(f);
    p.slice(1).forEach(x => {
      const nxt = mkMatch('fx', g, x[0], x[1]);
      UPG[g].push(nxt);
      M.push(nxt);
    });
  }
}
buildWorld();

/* ---------- Auto Table Calculation ---------- */
function getTable(g) {
  const t = {};
  (GROUPS[g] || []).forEach(p => t[p] = { p, pl: 0, w: 0, d: 0, l: 0, gf: 0, ga: 0, pts: 0 });
  M.filter(m => m.grp === g && m.st === 'ft').forEach(m => {
    const H = t[m.home], A = t[m.away];
    if (!H || !A) return;
    H.pl++; A.pl++;
    H.gf += m.hs; H.ga += m.as;
    A.gf += m.as; A.ga += m.hs;
    if (m.hs > m.as) { H.w++; H.pts += 3; A.l++; }
    else if (m.as > m.hs) { A.w++; A.pts += 3; H.l++; }
    else { H.d++; A.d++; H.pts += 1; A.pts += 1; }
  });
  return Object.values(t).sort((a, b) => b.pts - a.pts || (b.gf - b.ga) - (a.gf - a.ga) || b.gf - a.gf);
}

function checkAndSeedKO() {
  if (KO.seeded) return;
  const gs = Object.keys(GROUPS);
  if (gs.some(k => M.some(x => x.grp === k && x.st !== 'ft'))) return;

  const rows = [];
  gs.forEach(k => getTable(k).forEach((r, i) => rows.push({ ...r, g: k, rank: i })));
  const cmp = (a, b) => b.pts - a.pts || (b.gf - b.ga) - (a.gf - a.ga) || b.gf - a.gf;
  let q = rows.filter(r => r.rank < 2).sort(cmp);
  if (q.length < 4) q = rows.sort(cmp).slice(0, 4);
  if (q.length < 4) return;

  KO.seeded = true;
  const s1 = mkMatch('fx', 'KO', q[0].p, q[3].p); s1.koSlot = 'sf1'; s1.st = 'live';
  const s2 = mkMatch('fx', 'KO', q[1].p, q[2].p); s2.koSlot = 'sf2'; s2.st = 'live';
  M.push(s1, s2);
  feed.unshift({ txt: `🏆 SEMIFINALS: ${s1.home} vs ${s1.away} & ${s2.home} vs ${s2.away}` });
  broadcast(state());
}

/* ---------- AI Scoreboard Confidence Engine ---------- */
function confOf(sigs) {
  const cv = sigs.filter(x => x === 'cv_screen' || x === 'cv_cam').length;
  const pl = sigs.includes('player');
  if (cv >= 2) return 98;
  if (cv >= 1 && pl) return 92;
  if (cv >= 1) return 85;
  if (pl) return 70;
  return 50;
}

function processDetection(m, side, src) {
  if (m.st !== 'live') return;
  if (!m.det) m.det = { side, sigs: [] };
  if (!m.det.sigs.includes(src)) m.det.sigs.push(src);
  m.det.conf = confOf(m.det.sigs);

  if (m.det.conf >= 85) {
    const goalSide = m.det.side;
    m.det = null;
    deQueue(m.id);
    publishGoal(m, goalSide, `AI Cam (${src})`);
  } else {
    if (!VQ.find(v => v.mid === m.id)) {
      VQ.push({ mid: m.id, side: m.det.side, conf: m.det.conf, ts: Date.now() });
    }
    broadcast(state());
  }
}

function publishGoal(m, side, how) {
  if (side === 'home') m.hs++;
  else m.as++;
  feed.unshift({ txt: `⚽ GOAL! [${how}] ${m.home} ${m.hs} - ${m.as} ${m.away} (${m.min}')` });
  if (feed.length > 25) feed.pop();
  broadcast(state());
}

const deQueue = mid => {
  const i = VQ.findIndex(v => v.mid === mid);
  if (i > -1) VQ.splice(i, 1);
};

function viewerCount(mid) {
  let c = 0;
  clients.forEach(ws => { if (ws.readyState === 1 && ws.watchingMid === mid) c++; });
  return c;
}

function state() {
  const tables = {};
  if (T.groups) Object.keys(T.groups).forEach(g => tables[g] = getTable(g));
  return {
    type: 'state',
    matches: M.map(m => ({
      ...m,
      viewers: viewerCount(m.id),
      chunkCount: m.chunks.length,
      chat: m.chat.slice(-30)
    })),
    t: { name: T.name, status: T.status, players: T.players, groups: T.groups, tables },
    vq: VQ,
    ko: KO,
    champ: CHAMP,
    feed
  };
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

/* ---------- HTTP Endpoints ---------- */
// Instant HTTP state backup endpoint so page populates immediately
app.get('/api/state', (req, res) => {
  res.json(state());
});

app.post('/api/matches/:id/chunk', express.raw({ type: '*/*', limit: '25mb' }), (req, res) => {
  const m = M.find(x => x.id === req.params.id);
  if (!m) return res.status(404).send('Match not found');
  if (!Buffer.isBuffer(req.body) || !req.body.length) return res.status(400).send('Empty buffer');

  const chunkIdx = m.chunks.length;
  const chunkFileName = `${m.id}_${chunkIdx}.webm`;
  fs.writeFileSync(path.join(CHUNKS_DIR, chunkFileName), req.body);
  m.chunks.push(chunkFileName);
  m.st = 'live';

  broadcast({ type: 'chunk_ready', mid: m.id, idx: chunkIdx, url: `/api/chunks/${chunkFileName}` });
  res.json({ ok: true, idx: chunkIdx });
});

app.get('/api/chunks/:file', (req, res) => {
  const p = path.join(CHUNKS_DIR, req.params.file);
  if (!fs.existsSync(p)) return res.sendStatus(404);
  res.setHeader('Content-Type', 'video/webm');
  res.sendFile(p);
});

app.get('/api/matches/:id/manifest', (req, res) => {
  const m = M.find(x => x.id === req.params.id);
  if (!m) return res.sendStatus(404);
  res.json({ id: m.id, chunks: m.chunks.map(f => `/api/chunks/${f}`) });
});

app.post('/api/cv/:id', (req, res) => {
  const m = M.find(x => x.id === req.params.id);
  if (!m || m.st !== 'live') return res.sendStatus(404);
  const { home, away, src } = req.body;
  const h = +home, a = +away;
  if (h > m.hs) processDetection(m, 'home', src || 'cv_cam');
  else if (a > m.as) processDetection(m, 'away', src || 'cv_cam');
  res.json({ ok: true });
});

/* ---------- WebSocket Handling ---------- */
wss.on('connection', ws => {
  clients.add(ws);
  ws.watchingMid = null;
  ws.op = false;
  ws.send(JSON.stringify(state()));

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    const m = M.find(x => x.id === msg.mid);

    switch (msg.act) {
      case 'auth':
        ws.op = (msg.key === OPKEY);
        ws.send(JSON.stringify({ type: 'auth', ok: ws.op }));
        break;

      case 'join_watch':
        ws.watchingMid = msg.mid;
        broadcast(state());
        break;

      case 'leave_watch':
        if (ws.watchingMid === msg.mid) {
          ws.watchingMid = null;
          broadcast(state());
        }
        break;

      case 'claim_stream':
        if (m) {
          m.streamer = msg.name || 'ANON';
          m.st = 'live';
          feed.unshift({ txt: `📱 ${m.streamer} is live on ${m.home} vs ${m.away}` });
          broadcast(state());
        }
        break;

      case 'chat':
        if (m && msg.txt && msg.txt.trim()) {
          const chatMsg = { u: String(msg.u || 'FAN').slice(0, 15), t: String(msg.txt).slice(0, 200) };
          m.chat.push(chatMsg);
          if (m.chat.length > 50) m.chat.shift();
          broadcast({ type: 'chat_msg', mid: m.id, msg: chatMsg });
        }
        break;

      case 'goal_claim':
        if (m && m.st === 'live') {
          processDetection(m, msg.side, 'player');
        }
        break;

      case 'confirm_q':
        if (ws.op && m) {
          const v = VQ.find(x => x.mid === m.id);
          if (v) {
            deQueue(m.id);
            publishGoal(m, v.side, 'Referee Confirmed');
          }
        }
        break;

      case 'reject_q':
        if (ws.op && m) {
          deQueue(m.id);
          m.det = null;
          broadcast(state());
        }
        break;

      case 'fulltime':
        if (m && m.st === 'live') {
          m.st = 'ft';
          feed.unshift({ txt: `🏁 FULL-TIME: ${m.home} ${m.hs} - ${m.as} ${m.away}` });
          if (m.koSlot === 'sf1') KO.sf1 = m.hs >= m.as ? m.home : m.away;
          if (m.koSlot === 'sf2') KO.sf2 = m.hs >= m.as ? m.home : m.away;
          if (KO.sf1 && KO.sf2 && !KO.final) {
            const fin = mkMatch('fx', 'KO', KO.sf1, KO.sf2);
            fin.koSlot = 'final'; fin.st = 'live';
            KO.final = 1;
            M.push(fin);
            feed.unshift({ txt: `🏆 FINAL IS LIVE: ${fin.home} vs ${fin.away}` });
          }
          if (m.koSlot === 'final' && !CHAMP) {
            CHAMP = m.hs >= m.as ? m.home : m.away;
            feed.unshift({ txt: `👑 CHAMPION: ${CHAMP}!` });
          }
          checkAndSeedKO();
          broadcast(state());
        }
        break;

      case 'tCreate':
        if (ws.op) {
          T.name = String(msg.name || 'Tournament').slice(0, 30);
          T.status = 'registration';
          T.players = [];
          T.groups = null;
          M.length = 0;
          broadcast(state());
        }
        break;

      case 'tRegister':
        if (T.status === 'registration' && msg.name) {
          const p = String(msg.name).toUpperCase().trim().slice(0, 10);
          if (!T.players.includes(p) && T.players.length < 32) {
            T.players.push(p);
            feed.unshift({ txt: `👤 ${p} joined ${T.name}` });
            broadcast(state());
          }
        }
        break;

      case 'tStart':
        if (ws.op && T.status === 'registration' && T.players.length >= 4) {
          const ps = [...T.players].sort(() => Math.random() - 0.5);
          const gc = Math.max(1, Math.round(ps.length / 4));
          const g = {};
          for (let i = 0; i < gc; i++) g[GL[i]] = [];
          ps.forEach((p, i) => g[GL[i % gc]].push(p));
          GROUPS = g;
          T.groups = g;
          T.status = 'running';
          buildWorld();
          feed.unshift({ txt: `⚡ ${T.name} groups drawn!` });
          broadcast(state());
        }
        break;
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    broadcast(state());
  });
});

setInterval(() => {
  M.forEach(m => {
    if (m.st === 'live' && m.min < 90) m.min++;
  });
  broadcast(state());
}, 2500);

server.listen(PORT, () => {
  console.log(`⚽ MatchDay Auto-Engine running on http://localhost:${PORT}`);
});
