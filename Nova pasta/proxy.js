/**
 * moo-proxy — WebSocket + HTTP relay for ParmaBots
 *
 * Each deployed instance gives the bots a separate IP address,
 * bypassing the moomoo.io per-IP connection limit.
 *
 * Deploy N copies of this for N different IPs.
 * Then in the game console:
 *
 *   window._botConfig.wsProxies = [
 *     'wss://proxy1.your-host.com',
 *     'wss://proxy2.your-host.com',
 *     // ... one per deployed instance
 *   ];
 *
 * The HTTP endpoint (?url=...) is used automatically by the bot
 * engine for Altcha challenge fetches — no extra config needed.
 *
 * Install:  npm install
 * Run:      node proxy.js
 *           PORT=8080 node proxy.js   (default port)
 */

'use strict';

const http      = require('http');
const https     = require('https');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// ── HTTP server (also hosts WebSocket upgrade) ────────────────────
const server = http.createServer((req, res) => {
    // CORS preflight
    res.setHeader('Access-Control-Allow-Origin',  '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    // Health check
    if (req.url === '/' || req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('moo-proxy ok\n');
        return;
    }

    // HTTP relay — ?url=<encoded target URL>
    let target;
    try {
        const params = new URL(req.url, 'http://x').searchParams;
        target = params.get('url') || params.get('target');
        if (!target) throw new Error('missing url param');
        new URL(target); // validate
    } catch (e) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Bad request: missing or invalid ?url= parameter\n');
        return;
    }

    const mod = target.startsWith('https') ? https : http;
    const proxyReq = mod.get(target, { timeout: 8000 }, proxyRes => {
        res.writeHead(proxyRes.statusCode, {
            'Content-Type':                proxyRes.headers['content-type'] || 'application/json',
            'Access-Control-Allow-Origin': '*',
        });
        proxyRes.pipe(res);
    });
    proxyReq.on('error', err => {
        console.error('[HTTP proxy error]', err.message);
        if (!res.headersSent) { res.writeHead(502); }
        res.end();
    });
    proxyReq.on('timeout', () => {
        proxyReq.destroy();
        if (!res.headersSent) { res.writeHead(504); }
        res.end();
    });
});

// ── WebSocket relay ───────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on('connection', (client, req) => {
    let targetUrl;
    try {
        const params = new URL(req.url, 'http://x').searchParams;
        const raw = params.get('target');
        if (!raw) throw new Error('missing target');
        targetUrl = decodeURIComponent(raw);
        new URL(targetUrl); // validate
    } catch (e) {
        client.close(1008, 'Missing or invalid ?target= parameter');
        return;
    }

    let upstream;
    try {
        upstream = new WebSocket(targetUrl);
        upstream.binaryType = 'arraybuffer';
    } catch (e) {
        client.close(1011, 'Cannot connect to target');
        return;
    }

    console.log(`[WS] relay → ${targetUrl}`);

    upstream.on('open', () => {
        // Client → upstream
        client.on('message', (data, isBinary) => {
            if (upstream.readyState === WebSocket.OPEN)
                upstream.send(data, { binary: isBinary });
        });
        // Upstream → client
        upstream.on('message', (data, isBinary) => {
            if (client.readyState === WebSocket.OPEN)
                client.send(data, { binary: isBinary });
        });
    });

    upstream.on('close',  (code, reason) => client.close(code, reason));
    upstream.on('error',  err => {
        console.error('[WS upstream error]', err.message);
        client.close(1011, 'Upstream error');
    });
    client.on('close',  () => { if (upstream.readyState < 2) upstream.close(); });
    client.on('error',  ()  => { if (upstream.readyState < 2) upstream.close(); });
});

server.listen(PORT, () => {
    console.log(`moo-proxy listening on port ${PORT}`);
});
