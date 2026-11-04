// Serveur de Low-Poly Strike : sert le jeu (index.html) + relaie le mode "En ligne" par WebSocket.
// Aucune dépendance : il suffit de Node.js (v14+).   Lancement :  node server.js
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');
const PORT = process.env.PORT || 3000;
const rooms = new Map(); // code -> {a, b}

const server = http.createServer((req, res) => {
  if (req.url === '/ping') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('lps'); }
  const f = ['index.html', 'low-poly-strike.html'].map(n => path.join(__dirname, n)).find(p => fs.existsSync(p));
  if (!f) { res.writeHead(404); return res.end('Le fichier du jeu est introuvable : mets index.html (ou low-poly-strike.html) dans le meme dossier que server.js'); }
  fs.readFile(f, (err, data) => {
    if (err) { res.writeHead(500); return res.end('Erreur de lecture du jeu'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
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

server.on('upgrade', (req, sock) => {
  const u = new URL(req.url, 'http://x');
  const key = req.headers['sec-websocket-key'];
  if (u.pathname !== '/ws' || !key) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const code = (u.searchParams.get('room') || '').toUpperCase().slice(0, 12), role = u.searchParams.get('role');
  const peer = { sock, buf: Buffer.alloc(0), room: code, gone: false };
  sock.on('error', () => leave(peer));
  sock.on('close', () => leave(peer));

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

  sock.on('data', chunk => {
    peer.buf = Buffer.concat([peer.buf, chunk]);
    if (peer.buf.length > 1 << 20) return leave(peer);
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
      if (op === 8) return leave(peer);
      if (op === 9) { try { sock.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); } catch (e) {} continue; }
      if (op === 1) { const o = other(peer); if (o) send(o, payload.toString()); }
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n  Low-Poly Strike est en ligne !\n');
  console.log('  Sur ce PC          : http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces()))
    for (const i of list) if (i.family === 'IPv4' && !i.internal) console.log('  Pour tes collegues : http://' + i.address + ':' + PORT + '   (meme reseau)');
  console.log('\n  Laisse cette fenetre ouverte pendant la partie (Ctrl+C pour arreter).\n');
});
