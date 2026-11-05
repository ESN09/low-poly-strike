// Serveur de Low-Poly Strike : sert le jeu (index.html) + relaie le mode "En ligne" (1v1, /ws) et la mêlée 2-8 joueurs (/ffa) par WebSocket.
// Aucune dépendance : il suffit de Node.js (v14+).   Lancement :  node server.js
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');
const PORT = process.env.PORT || 3000;
const rooms = new Map();   // 1v1 : code -> {a, b}
const frooms = new Map();  // mêlée : code -> {peers: Map(id -> peer), host: id}
const MAXP = 8;

// classement (en mémoire + sauvegarde dans lb.json ; sur l'offre gratuite de Render le fichier peut être effacé à chaque redémarrage)
const LBF = path.join(process.env.LB_DIR || __dirname, 'lb.json');
let LB = {};
try { LB = JSON.parse(fs.readFileSync(LBF, 'utf8')) || {}; } catch (e) { LB = {}; }
let lbDirty = false;
setInterval(() => { if (lbDirty) { lbDirty = false; try { fs.writeFileSync(LBF, JSON.stringify(LB)); } catch (e) {} } }, 15000).unref();
function lbTop(key) { return Object.values(LB).sort((a, b) => b[key] - a[key] || b.l - a.l).filter(e => e[key] > 0).slice(0, 10).map(e => ({ n: e.n, w: e.w, l: e.l })); }
const lbRate = new Map(), lbNew = new Map();   // limites par IP
function ipOf(req) { return String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim(); }
function lbPost(req, res) {
  const ip = ipOf(req), now = Date.now();
  if (now - (lbRate.get(ip) || 0) < 4000) { res.writeHead(429, { 'Content-Type': 'application/json' }); return res.end('{}'); }
  lbRate.set(ip, now);
  if (lbRate.size > 5000) lbRate.clear();
  let body = '';
  req.on('data', c => { body += c; if (body.length > 500) req.destroy(); });
  req.on('end', () => {
    try {
      const m = JSON.parse(body);
      const n = String(m.n || '').replace(/[<>&"]/g, '').trim().slice(0, 14), w = Math.floor(+m.w), l = Math.floor(+m.l);
      const k0 = n.toLowerCase(), old = LB[k0];
      // anti-triche simple : plafonds, progression limitée par envoi, 5 nouveaux pseudos max par heure et par IP
      let ok = n && w >= 0 && w <= 120 && l >= 1 && l <= 80;
      if (ok && old && (w > old.w + 25 || l > old.l + 8)) ok = false;
      if (ok && !old) {
        const c = lbNew.get(ip) || { t: now, n: 0 };
        if (now - c.t > 3600000) { c.t = now; c.n = 0; }
        if (c.n >= 5 || w > 80 || l > 60) ok = false; else { c.n++; lbNew.set(ip, c); }
      }
      if (ok) {
        const k = k0, e = LB[k] || { n, w: 0, l: 1 };
        e.n = n; e.w = Math.max(e.w, w); e.l = Math.max(e.l, l); LB[k] = e; lbDirty = true;
        if (Object.keys(LB).length > 2000) { const keep = Object.values(LB).sort((a, b) => b.w + b.l - a.w - a.l).slice(0, 1000); LB = {}; keep.forEach(x => LB[x.n.toLowerCase()] = x); }
      }
    } catch (e) {}
    res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ w: lbTop('w'), l: lbTop('l') }));
  });
}

// profils en ligne (code secret de 8 caractères -> données de progression)
const PROFF = path.join(process.env.LB_DIR || __dirname, 'profiles.json');
let PROF = {};
try { PROF = JSON.parse(fs.readFileSync(PROFF, 'utf8')) || {}; } catch (e) { PROF = {}; }
let profDirty = false;
setInterval(() => { if (profDirty) { profDirty = false; try { fs.writeFileSync(PROFF, JSON.stringify(PROF)); } catch (e) {} } }, 20000).unref();
function profile(req, res, u) {
  const code = String(u.searchParams.get('c') || '').toUpperCase();
  const J = (st, o) => { res.writeHead(st, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(o)); };
  if (!/^[A-Z0-9]{6,8}$/.test(code)) return J(400, {});
  if (req.method !== 'POST') return PROF[code] ? J(200, PROF[code]) : J(404, {});
  const ip = ipOf(req), now = Date.now();
  if (now - (lbRate.get('p' + ip) || 0) < 3000) return J(429, {});
  lbRate.set('p' + ip, now);
  let body = '';
  req.on('data', c => { body += c; if (body.length > 3000) req.destroy(); });
  req.on('end', () => {
    try {
      const m = JSON.parse(body);
      if (typeof m.xp !== 'number' || !Array.isArray(m.own) || !Array.isArray(m.wk)) return J(400, {});
      if (!PROF[code] && Object.keys(PROF).length >= 3000) return J(507, {});
      PROF[code] = m; profDirty = true; J(200, { ok: 1 });
    } catch (e) { J(400, {}); }
  });
}

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/profile')) return profile(req, res, new URL(req.url, 'http://x'));
  if (req.url === '/rooms') {
    const L = [];
    for (const [c, r] of frooms) if (r.pub && !r.started && r.peers.size < MAXP) L.push({ c, n: r.peers.size, h: r.hn || '' });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); return res.end(JSON.stringify(L.slice(0, 12)));
  }
  if (req.url === '/ping') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('lps'); }
  if (req.url === '/lb') {
    if (req.method === 'POST') return lbPost(req, res);
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); return res.end(JSON.stringify({ w: lbTop('w'), l: lbTop('l') }));
  }
  const f = ['index.html', 'low-poly-strike.html'].map(n => path.join(__dirname, n)).find(p => fs.existsSync(p));
  if (!f) { res.writeHead(404); return res.end('Le fichier du jeu est introuvable : mets index.html (ou low-poly-strike.html) dans le meme dossier que server.js'); }
  fs.readFile(f, (err, data) => {
    if (err) { res.writeHead(500); return res.end('Erreur de lecture du jeu'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

function frame(str) {
  const p = Buffer.from(str), n = p.length;
  let h;
  if (n < 126) h = Buffer.from([0x81, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, p]);
}
const send = (peer, txt) => { try { if (peer && !peer.sock.destroyed) peer.sock.write(frame(txt)); } catch (e) {} };

// lecture des trames WebSocket d'un client ; onText(texte) pour chaque message texte, onEnd() à la fermeture
function attach(peer, onText, onEnd) {
  const sock = peer.sock;
  sock.on('error', onEnd);
  sock.on('close', onEnd);
  sock.on('data', chunk => {
    peer.buf = Buffer.concat([peer.buf, chunk]);
    if (peer.buf.length > 1 << 20) return onEnd();
    for (;;) {
      const b = peer.buf;
      if (b.length < 2) return;
      const op = b[0] & 15, masked = b[1] & 128;
      let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      const total = off + (masked ? 4 : 0) + len;
      if (b.length < total) return;
      let payload = b.slice(off + (masked ? 4 : 0), total);
      if (masked) { const m = b.slice(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      peer.buf = b.slice(total);
      if (op === 8) return onEnd();
      if (op === 9) { try { sock.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); } catch (e) {} continue; }
      if (op === 1) onText(payload.toString());
    }
  });
}

/* ---------- 1v1 ---------- */
function other(peer) {
  const r = rooms.get(peer.room);
  if (!r) return null;
  return r.a === peer ? r.b : r.a;
}
function leave(peer) {
  if (peer.gone) return;
  peer.gone = true;
  const r = rooms.get(peer.room);
  if (r) {
    const o = r.a === peer ? r.b : r.a;
    rooms.delete(peer.room);
    if (o) { send(o, '{"sys":"left"}'); try { o.sock.end(); } catch (e) {} }
  }
  try { peer.sock.destroy(); } catch (e) {}
}
function duel(sock, code, role) {
  const peer = { sock, buf: Buffer.alloc(0), room: code, gone: false };
  if (!code) { send(peer, '{"sys":"nf"}'); peer.room = null; sock.end(); return; }
  if (role === 'host') {
    if (rooms.has(code)) { send(peer, '{"sys":"taken"}'); peer.gone = true; sock.end(); return; }
    rooms.set(code, { a: peer, b: null });
  } else {
    const r = rooms.get(code);
    if (!r) { send(peer, '{"sys":"nf"}'); peer.gone = true; sock.end(); return; }
    if (r.b) { send(peer, '{"sys":"full"}'); peer.gone = true; sock.end(); return; }
    r.b = peer;
    send(r.a, '{"sys":"ready"}'); send(peer, '{"sys":"ready"}');
  }
  attach(peer, txt => { const o = other(peer); if (o) send(o, txt); }, () => leave(peer));
}

/* ---------- mêlée 2-8 joueurs ---------- */
function fleave(peer) {
  if (peer.gone) return;
  peer.gone = true;
  const r = frooms.get(peer.room);
  try { peer.sock.destroy(); } catch (e) {}
  if (!r) return;
  r.peers.delete(peer.id);
  if (peer.id === r.host) {
    for (const p of r.peers.values()) { send(p, '{"sys":"hostleft"}'); try { p.sock.end(); } catch (e) {} }
    frooms.delete(peer.room);
    return;
  }
  for (const p of r.peers.values()) send(p, JSON.stringify({ sys: 'left', id: peer.id }));
  if (!r.peers.size) frooms.delete(peer.room);
}
function ffa(sock, code, role, pub) {
  const peer = { sock, buf: Buffer.alloc(0), room: code, gone: false, id: -1 };
  const refuse = s => { send(peer, JSON.stringify({ sys: s })); peer.gone = true; sock.end(); };
  if (!code) return refuse('nf');
  let r = frooms.get(code);
  if (role === 'host') {
    if (r) return refuse('taken');
    r = { peers: new Map(), host: 0, pub: !!pub, started: false, hn: '' };
    frooms.set(code, r);
  } else {
    if (!r) return refuse('nf');
    if (r.peers.size >= MAXP) return refuse('full');
  }
  let id = 0;
  while (r.peers.has(id)) id++;
  peer.id = id;
  const others = [...r.peers.keys()];
  r.peers.set(id, peer);
  send(peer, JSON.stringify({ sys: 'hello', id, players: others }));
  for (const p of r.peers.values()) if (p !== peer) send(p, JSON.stringify({ sys: 'join', id }));
  attach(peer, txt => {
    if (txt.length > 4096) return;
    if (peer.id === r.host) {
      if (txt.startsWith('{"t":"cfg"')) r.started = true;
      else if (txt.startsWith('{"t":"nm"')) { try { r.hn = String(JSON.parse(txt).n || '').replace(/[<>&"]/g, '').slice(0, 14); } catch (e) {} }
    }
    for (const p of r.peers.values()) if (p !== peer) send(p, txt);
  }, () => fleave(peer));
}

server.on('upgrade', (req, sock) => {
  const u = new URL(req.url, 'http://x');
  const key = req.headers['sec-websocket-key'];
  if ((u.pathname !== '/ws' && u.pathname !== '/ffa') || !key) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const code = (u.searchParams.get('room') || '').toUpperCase().slice(0, 12), role = u.searchParams.get('role');
  if (u.pathname === '/ffa') ffa(sock, code, role, u.searchParams.get('pub') === '1'); else duel(sock, code, role);
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n  Low-Poly Strike est en ligne !\n');
  console.log('  Sur ce PC          : http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list) if (i.family === 'IPv4' && !i.internal) console.log('  Pour tes collegues : http://' + i.address + ':' + PORT + '   (meme reseau)');
  console.log('\n  Laisse cette fenetre ouverte pendant la partie (Ctrl+C pour arreter).\n');
});
