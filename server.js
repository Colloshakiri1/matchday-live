/* MATCHDAY LIVE — backend (v2 with mobile network relay) */
'use strict';
const express = require('express'), http = require('http'), path = require('path'), fs = require('fs'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const SIM = process.env.SIM_MODE !== 'false', PORT = process.env.PORT || 3000;
const MATCH_MIN = +process.env.MATCH_MINUTES || 10, MAX_MB = +process.env.MAX_UPLOAD_MB || 400;
const OPKEY = process.env.OPERATOR_KEY || (SIM ? '' : crypto.randomBytes(5).toString('hex'));
const CVKEY = process.env.CV_KEY || '';
const DUR = SIM ? 135000 : MATCH_MIN * 60000, GL = 'ABCDEFGHIJKLMNOP';
const rid = () => crypto.randomBytes(12).toString('hex');
const clean = (s, n = 10) => String(s || '').replace(/[^\p{L}\p{N} _-]/gu, '').trim().toUpperCase().slice(0, n);
const txt = s => String(s || '').replace(/[\u0000-\u001f]/g, ' ').slice(0, 300);

const app = express(), server = http.createServer(app), wss = new WebSocketServer({ server, maxPayload: 16384 });
app.use(express.static(path.join(__dirname, 'public')));
const UPLOADS = path.join(__dirname, 'uploads'); fs.mkdirSync(UPLOADS, { recursive: true });
const upf = id => path.join(UPLOADS, id + '.webm');

/* ---------- data ---------- */
const DEMO = { A: ['KEV', 'BRI', 'DAV', 'SOL'], B: ['SAM', 'TOM', 'EMMA', 'JOE'], C: ['ZARA', 'MUSA', 'CHI', 'NANA'] };
let GROUPS = DEMO;
const T = { name: 'Fan Cup (demo)', status: 'running', players: Object.values(DEMO).flat(), groups: DEMO };
const M = [], UPG = {}, VQ = [], feed = [], clients = new Set();
let midx = 0, KO = { sf1: null, sf2: null, final: null }, CHAMP = null, QUALIFIED = [];

function mk(type, grp, home, away, hs, as, min, st) {
  const m = { id: 'm' + (++midx), type, grp, home, away, hs, as, min, st, vw: 20 + Math.floor(Math.random() * 90), sc: [], chat: [],
    seed: Math.floor(Math.random() * 90) + 3, det: null, cvA: '—', cvB: '—', log: [], last: Date.now(), gseq: 0, koSlot: null,
    real: false, open: false, by: { home: null, away: null }, tok: {}, feedBy: null, startedAt: null };
  M.push(m); return m;
}
function say(m, t) { m.chat.push({ u: 'sys', t }); if (m.chat.length > 40) m.chat.shift(); m.log.unshift(m.min + "' " + t); if (m.log.length > 10) m.log.pop(); }
function startM(m, note) { m.st = 'live'; m.min = 0; m.startedAt = Date.now(); m.last = Date.now(); if (note) say(m, note); }
const pairs = n => { const o = []; for (let i = 0; i < n.length; i++) for (let j = i + 1; j < n.length; j++) o.push([n[i], n[j]]); return o.sort(() => Math.random() - .5); };
function resetWorld() {
  M.length = 0; VQ.length = 0; feed.length = 0; Object.keys(UPG).forEach(k => delete UPG[k]);
  KO = { sf1: null, sf2: null, final: null }; CHAMP = null; QUALIFIED = []; midx = 0;
  fs.readdirSync(UPLOADS).forEach(f => fs.unlink(path.join(UPLOADS, f), () => {}));
}
function buildWorld() {
  resetWorld();
  for (const g of Object.keys(GROUPS)) {
    const p = pairs(GROUPS[g]); if (!p.length) continue; UPG[g] = [];
    const f = mk('fx', g, p[0][0], p[0][1], 0, 0, 0, 'up'); if (SIM) startM(f);
    p.slice(1).forEach(x => UPG[g].push(mk('fx', g, x[0], x[1], 0, 0, 0, 'up')));
  }
}

/* ---------- tournament lifecycle ---------- */
function tCreate(name) { T.name = clean(name, 30) || 'Tournament'; T.status = 'registration'; T.players = []; T.groups = null; resetWorld(); broadcast(); }
function tRegister(name) { const n = clean(name); if (!n || T.status !== 'registration' || T.players.includes(n) || T.players.length >= 64) return; T.players.push(n); broadcast(); }
function tStart() {
  if (T.status !== 'registration' || T.players.length < 4) return;
  const ps = [...T.players].sort(() => Math.random() - .5), gc = Math.max(1, Math.round(ps.length / 4)), g = {};
  for (let i = 0; i < gc; i++) g[GL[i]] = [];
  ps.forEach((p, i) => g[GL[i % gc]].push(p));
  GROUPS = g; T.groups = g; T.status = 'running'; buildWorld();
  M.filter(m => m.st === 'live').forEach(m => say(m, T.name + ' — fixture 1 auto-scheduled 🤖'));
  broadcast();
}

/* ---------- state + throttled broadcast ---------- */
const pub = m => ({ ...m, chat: m.chat.slice(-25), tok: undefined, feedBy: undefined, startedAt: undefined });
const state = () => ({ type: 'state', matches: M.map(pub), feed, vq: VQ, ko: KO, champ: CHAMP, qualified: QUALIFIED,
  t: { name: T.name, status: T.status, players: T.players, groups: T.groups }, sim: SIM, now: Date.now() });
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
let dirty = false;
function broadcast() { if (dirty) return; dirty = true; setTimeout(() => { dirty = false; const s = JSON.stringify(state()); clients.forEach(c => c.readyState === 1 && c.send(s)); }, 120); }

/* ---------- confidence fusion ---------- */
function confOf(s) {
  const cv = s.filter(x => x === 'cvA' || x === 'cvB').length, pl = s.includes('player'), rp = s.includes('reporter');
  if (cv >= 2) return 98; if (cv === 1 && pl) return 88; if (cv === 1 && rp) return 82; if (pl && rp) return 90;
  if (cv === 1) return 70; if (pl) return 66; if (rp) return 62; return 55;
}
function fireDetection(m, side, first) {
  if (m.det || m.st !== 'live') return;
  m.det = { side, sigs: first ? [first] : [], conf: first ? confOf([first]) : 0 };
  if (SIM) {
    setTimeout(() => m.st === 'live' && addSig(m, 'cvA'), 500 + Math.random() * 700);
    if (Math.random() < .9) setTimeout(() => m.st === 'live' && addSig(m, 'cvB'), 900 + Math.random() * 900);
    if (!first && Math.random() < .75) setTimeout(() => m.st === 'live' && addSig(m, 'player'), 600 + Math.random() * 800);
  }
  if (first) addSig(m, first, true); else broadcast();
}
function addSig(m, src, already) {
  if (!m.det || m.st !== 'live') return;
  if (!m.det.sigs.includes(src)) m.det.sigs.push(src);
  m.det.conf = confOf(m.det.sigs);
  if (m.det.conf >= 90) { const d = m.det; m.det = null; deQ(m.id); publishGoal(m, d.side, 'auto ' + d.conf + '%'); return; }
  if (!VQ.find(v => v.mid === m.id)) VQ.push({ mid: m.id, side: m.det.side, conf: m.det.conf, ts: Date.now() });
  else VQ.find(v => v.mid === m.id).conf = m.det.conf;
  broadcast();
}
const deQ = id => { const i = VQ.findIndex(v => v.mid === id); if (i > -1) VQ.splice(i, 1); };
function confirmQ(m) { const v = VQ.find(x => x.mid === m.id); if (!v) return; deQ(m.id); m.det = null; publishGoal(m, v.side, 'human ' + v.conf + '%'); }
function rejectQ(m) { deQ(m.id); say(m, 'Goal claim rejected ❌'); m.det = null; broadcast(); }
function publishGoal(m, side, how) {
  side === 'home' ? m.hs++ : m.as++;
  m.sc.push(m.min + "' " + (side === 'home' ? m.home : m.away)); m.last = Date.now(); m.gseq++;
  say(m, '⚽ GOAL · ' + how + ' → ' + m.home + ' ' + m.hs + '-' + m.as + ' ' + m.away);
  feed.unshift({ id: m.id, txt: m.min + "' ⚽ " + m.home + ' ' + m.hs + '-' + m.as + ' ' + m.away, how }); if (feed.length > 15) feed.pop();
  broadcast();
}

/* ---------- full-time + tournament automation ---------- */
function winner(m) {
  if (m.hs !== m.as) return m.hs > m.as ? m.home : m.away;
  const w = Math.random() < .5 ? m.home : m.away; say(m, w + ' win on penalties'); return w;
}
function fullTime(m, src) {
  if (m.st !== 'live') return; m.st = 'ft'; m.real = false; m.det = null; deQ(m.id);
  say(m, 'FULL-TIME (' + src + ') — ' + m.home + ' ' + m.hs + '-' + m.as + ' ' + m.away);
  feed.unshift({ id: m.id, txt: 'FT ' + m.home + ' ' + m.hs + '-' + m.as + ' ' + m.away, how: 'FT' });
  if (m.koSlot === 'sf1' || m.koSlot === 'sf2') KO[m.koSlot] = winner(m);
  if (m.koSlot === 'final' && !CHAMP) CHAMP = winner(m);
  if (m.grp && m.grp !== 'KO') {
    if (SIM && UPG[m.grp]) setTimeout(() => { const nx = UPG[m.grp].shift(); if (nx) { startM(nx, 'Auto-scheduled 🤖'); broadcast(); } }, 12000);
    seedKO();
  }
  if (KO.sf1 && KO.sf2 && !KO.final) {
    KO.final = 1;
    setTimeout(() => {
      const f = mk('fx', 'KO', KO.sf1, KO.sf2, 0, 0, 0, 'up'); f.koSlot = 'final'; say(f, '🏆 FINAL created');
      if (SIM) setTimeout(() => { startM(f, 'The FINAL is live! 🏆'); broadcast(); }, 12000);
      broadcast();
    }, SIM ? 12000 : 500);
  }
  broadcast();
}

function seedKO() {
  if (KO.seeded) return; const gs = Object.keys(GROUPS);
  if (gs.some(k => M.some(x => x.grp === k && x.st !== 'ft'))) return;
  const rows = []; gs.forEach(k => table(k).forEach((r, i) => rows.push({ ...r, g: k, rank: i })));
  const cmp = (a, b) => b.pts - a.pts || (b.gf - b.ga) - (a.gf - a.ga) || b.gf - a.gf;
  let q = rows.filter(r => r.rank < 2).sort(cmp); q = q.length < 4 ? rows.sort(cmp).slice(0, 4) : q.slice(0, 4);
  if (q.length < 4) return;
  KO.seeded = true; QUALIFIED = q.map(r => ({ g: r.g, team: r.p, pts: r.pts }));
  setTimeout(() => {
    const s1 = mk('fx', 'KO', q[0].p, q[3].p, 0, 0, 0, 'up'), s2 = mk('fx', 'KO', q[1].p, q[2].p, 0, 0, 0, 'up');
    s1.koSlot = 'sf1'; s2.koSlot = 'sf2'; say(s1, 'SEMIFINAL auto-paired 🤖'); say(s2, 'SEMIFINAL auto-paired 🤖');
    if (SIM) setTimeout(() => { startM(s1); startM(s2); broadcast(); }, 12000);
    broadcast();
  }, SIM ? 8000 : 500);
}

function table(g) {
  const t = {}; (GROUPS[g] || []).forEach(p => t[p] = { p, pl: 0, w: 0, d: 0, l: 0, gf: 0, ga: 0, pts: 0 });
  M.filter(m => m.grp === g && m.st === 'ft').forEach(m => {
    const H = t[m.home], A = t[m.away]; if (!H || !A) return;
    H.pl++; A.pl++; H.gf += m.hs; H.ga += m.as; A.gf += m.as; A.ga += m.hs;
    if (m.hs > m.as) { H.w++; H.pts += 3; A.l++; } else if (m.as > m.hs) { A.w++; A.pts += 3; H.l++; } else { H.d++; A.d++; H.pts++; A.pts++; }
  });
  return Object.values(t).sort((a, b) => b.pts - a.pts || (b.gf - b.ga) - (a.gf - a.ga) || b.gf - a.gf);
}

/* ---------- clock ---------- */
setInterval(() => {
  const now = Date.now();
  M.forEach(m => {
    if (m.st !== 'live') return;
    if (!m.startedAt) m.startedAt = now - m.min * DUR / 90;
    m.min = Math.min(90, Math.floor((now - m.startedAt) / DUR * 90));
    m.cvA = m.hs + '-' + m.as; m.cvB = Math.random() < .92 ? m.hs + '-' + m.as : '…';
    if (m.min >= 90) { fullTime(m, m.by.home || m.by.away ? 'timer' : 'AI cam'); return; }
    if (SIM) { m.vw = Math.max(12, m.vw + Math.floor(Math.random() * 9) - 3); if (!m.det && Math.random() < .01) fireDetection(m, Math.random() < .5 ? 'home' : 'away'); }
    else m.vw = [...clients].filter(c => c.watch === m.id).length;
  });
  for (let i = VQ.length - 1; i >= 0; i--) if (now - VQ[i].ts > 25000) { const v = VQ.splice(i, 1)[0], m = M.find(x => x.id === v.mid); if (m) { say(m, 'Claim expired — no confirmation'); m.det = null; } }
  broadcast();
}, 1500);

if (SIM) {
  const F = ['@ade', '@kofi', '@chi', '@musa', '@zara', '@tunde'], L = ['WHAT A GOAL 🔥🔥', 'the defending 💀💀', 'pass the ball!', 'better than EPL 😂', 'stream is clean ✅', 'who else is watching?', 'ref where??? 👀', 'MY GUY!!!', 'gg wp', 'that keeper 😭', 'TACKLE HIM', 'cornerrrr'];
  setInterval(() => { const l = M.filter(m => m.st === 'live'); if (!l.length) return; const m = l[Math.floor(Math.random() * l.length)]; m.chat.push({ u: F[Math.floor(Math.random() * F.length)], t: L[Math.floor(Math.random() * L.length)] }); if (m.chat.length > 40) m.chat.shift(); }, 4500);
}

/* ---------- video chunk receiver ---------- */
app.post('/api/matches/:id/chunk', express.raw({ type: () => true, limit: '8mb' }), (q, r) => {
  const m = M.find(x => x.id === q.params.id); if (!m) return r.sendStatus(404);
  const t = q.get('x-stream-token'); if (!t || !Object.values(m.tok).includes(t)) return r.sendStatus(403);
  if (m.feedBy && m.feedBy !== t) return r.sendStatus(409);
  if (!Buffer.isBuffer(q.body) || !q.body.length) return r.sendStatus(400);
  const f = upf(m.id); if (fs.existsSync(f) && fs.statSync(f).size > MAX_MB * 1048576) return r.sendStatus(413);
  m.feedBy = t; fs.appendFileSync(f, q.body); if (!m.real) { m.real = true; broadcast(); } r.json({ ok: true });
});
app.get('/live/:id.webm', (q, r) => { const m = M.find(x => x.id === q.params.id); if (!m || !fs.existsSync(upf(m.id))) return r.sendStatus(404); r.type('video/webm'); r.sendFile(upf(m.id)); });

/* CV endpoint */
app.post('/api/cv/:id', express.json({ limit: '2kb' }), (q, r) => {
  if (!CVKEY || q.get('x-cv-key') !== CVKEY) return r.sendStatus(403);
  const m = M.find(x => x.id === q.params.id); if (!m || m.st !== 'live') return r.sendStatus(404);
  const src = q.body.src === 'cvB' ? 'cvB' : 'cvA', h = +q.body.home, a = +q.body.away;
  const side = h > m.hs ? 'home' : a > m.as ? 'away' : null; if (!side) return r.json({ ok: true, change: false });
  m.det ? addSig(m, src) : fireDetection(m, side, src); r.json({ ok: true, change: true });
});

/* ---------- websocket ---------- */
const peers = mid => [...clients].filter(c => c.pl && c.pl.mid === mid);
const deny = (ws, msg) => send(ws, { type: 'err', msg });
function join(ws, m, side, name) {
  const tok = rid(); m.by[side] = name; m.tok[side] = tok; ws.pl = { mid: m.id, side, name };
  send(ws, { type: 'joined', mid: m.id, side, name, token: tok });
  peers(m.id).forEach(c => send(c, { type: 'pchat', u: 'sys', t: name + ' joined the channel' })); broadcast();
}

wss.on('connection', ws => {
  clients.add(ws); ws.n = 0; ws.t = Date.now(); ws.op = !OPKEY; send(ws, state());
  ws.on('message', d => {
    const now = Date.now(); if (now - ws.t > 10000) { ws.t = now; ws.n = 0; } if (++ws.n > 80) return;
    let a; try { a = JSON.parse(d); } catch (e) { return; } if (!a || typeof a !== 'object') return;
    const m = M.find(x => x.id === a.mid), isP = m && ws.pl && ws.pl.mid === m.id, needOp = () => ws.op || (deny(ws, 'Operator key required'), false);
    switch (a.act) {
      case 'auth': ws.op = !OPKEY || a.key === OPKEY; send(ws, { type: 'auth', ok: ws.op }); break;
      case 'watch': ws.watch = m ? m.id : null; break;
      case 'chat': if (m && now - (ws.lc || 0) > 800) { ws.lc = now; m.chat.push({ u: clean(a.u, 14) || 'FAN', t: txt(a.t) }); if (m.chat.length > 40) m.chat.shift(); broadcast(); } break;
      case 'claim': {
        if (!m || m.st !== 'live') return; const side = a.side === 'away' ? 'away' : 'home'; let src;
        if (isP) src = side === ws.pl.side ? 'player' : 'reporter'; else if (ws.op) src = 'reporter'; else return deny(ws, 'Only the players or an operator can report goals');
        m.det ? addSig(m, src) : fireDetection(m, side, src); break; }
      case 'confirm': if (m && needOp()) confirmQ(m); break;
      case 'reject': if (m && needOp()) rejectQ(m); break;
      case 'ft': if (m && (isP || needOp())) fullTime(m, isP ? 'player' : 'operator'); break;
      case 'kickoff': if (m && m.st === 'up' && needOp()) { startM(m, 'Kicked off by operator'); broadcast(); } break;
      case 'tCreate': if (needOp()) tCreate(a.name); break;
      case 'tStart': if (needOp()) tStart(); break;
      case 'tRegister': tRegister(a.name); break;
      case 'streamFixture': {
        if (!m || m.type !== 'fx' || m.st === 'ft') return; const side = a.side === 'away' ? 'away' : 'home', name = clean(a.name) || 'PLAYER';
        if (m.by[side] && m.by[side] !== name) return deny(ws, 'That side already has a streamer');
        m.st === 'up' ? startM(m, name + ' went live 📱') : say(m, name + ' went live 📱'); join(ws, m, side, name); break; }
      case 'streamFriendly': {
        const nm = clean(a.name) || 'PLAYER', open = M.find(x => x.type === 'fr' && x.st === 'live' && x.open && x.home !== nm);
        if (open) { open.away = nm; open.open = false; say(open, nm + ' joined — ' + open.home + ' vs ' + nm + ' 📱'); join(ws, open, 'away', nm); }
        else { const f = mk('fr', null, nm, 'RIVAL', 0, 0, 0, 'live'); f.open = true; startM(f, nm + ' is live — waiting for an opponent'); join(ws, f, 'home', nm); }
        break; }
      case 'pchat': if (isP && now - (ws.lp || 0) > 500) { ws.lp = now; peers(m ? m.id : ws.pl.mid).forEach(c => send(c, { type: 'pchat', u: ws.pl.name, t: txt(a.t) })); } break;
      case 'sig': if (ws.pl) peers(ws.pl.mid).filter(c => c !== ws).forEach(c => send(c, { type: 'sig', data: a.data })); break;
    }
  });
  ws.on('close', () => {
    clients.delete(ws);
    if (ws.pl) { const m = M.find(x => x.id === ws.pl.mid), left = peers(ws.pl.mid);
      left.forEach(c => send(c, { type: 'pchat', u: 'sys', t: ws.pl.name + ' left the channel' }));
      if (m && m.open && !left.length && m.st === 'live') fullTime(m, 'abandoned'); }
  });
});

buildWorld();
M.filter(m => m.st === 'live').forEach(m => { say(m, 'Stream live — engine monitoring'); m.chat.push({ u: '@ade', t: 'lets gooo 🔥' }); });
server.listen(PORT, () => {
  console.log('⚽ MatchDay Live on port ' + PORT + (SIM ? ' (SIM_MODE)' : ' (PRODUCTION)'));
});
  
