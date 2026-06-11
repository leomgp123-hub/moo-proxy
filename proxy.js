/**
 * moo-proxy — WebSocket + HTTP relay for ParmaBots
 *
 * Deploy N copies of this for N different IPs.
 * Then in the proxy field of the bot UI:
 *   wss://your-service.onrender.com
 *
 * Install:  npm install
 * Run:      node proxy.js
 *           PORT=8080 node proxy.js
 */

'use strict';

const http      = require('http');
const https     = require('https');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

const UPSTREAM_HEADERS = {
    'Origin':     'https://moomoo.io',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer':    'https://moomoo.io/',
};

// ── HTTP server ───────────────────────────────────────────────────
const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin',  '*');
    res.setHeader('Access-Control-Allow-Headers', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    if (req.url === '/' || req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end(`moo-proxy ok | active=${activeConnections} total=${totalConnections}\n`);
        return;
    }

    // HTTP relay — ?url= ou ?target= (usado pelo BotAltcha para tokens)
    let target;
    try {
        const params = new URL(req.url, 'http://x').searchParams;
        target = params.get('url') || params.get('target');
        if (!target) throw new Error('missing url param');
        new URL(target);
    } catch (e) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Bad request: missing or invalid ?url= parameter\n');
        return;
    }

    const mod = target.startsWith('https') ? https : http;
    const proxyReq = mod.get(target, {
        timeout: 8000,
        headers: {
            'Origin':     'https://moomoo.io',
            'Referer':    'https://moomoo.io/',
            'User-Agent': UPSTREAM_HEADERS['User-Agent'],
        },
    }, proxyRes => {
        res.writeHead(proxyRes.statusCode, {
            'Content-Type':                proxyRes.headers['content-type'] || 'application/json',
            'Access-Control-Allow-Origin': '*',
        });
        proxyRes.pipe(res);
    });
    proxyReq.on('error',   err => { console.error('[HTTP] relay error:', err.message); if (!res.headersSent) res.writeHead(502); res.end(); });
    proxyReq.on('timeout', ()  => { proxyReq.destroy(); if (!res.headersSent) res.writeHead(504); res.end(); });
});

// ── WebSocket relay ───────────────────────────────────────────────
const wss = new WebSocket.Server({ server });
let totalConnections  = 0;
let activeConnections = 0;

wss.on('connection', (client, req) => {
    totalConnections++;
    activeConnections++;
    const id       = totalConnections;
    const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?';

    // Parse ?target=
    let targetUrl;
    try {
        const params = new URL(req.url, 'http://x').searchParams;
        const raw = params.get('target');
        if (!raw) throw new Error('missing target');
        targetUrl = decodeURIComponent(raw);
        new URL(targetUrl);
    } catch (e) {
        console.warn(`[WS] #${id} rejected (${clientIp}) — ${e.message}`);
        activeConnections--;
        client.close(1008, 'Missing or invalid ?target= parameter');
        return;
    }

    console.log(`[WS] #${id} connect from ${clientIp} | active=${activeConnections}`);
    console.log(`[WS] #${id} relay → ${targetUrl.slice(0, 100)}`);

    let upstream;
    try {
        upstream = new WebSocket(targetUrl, { headers: UPSTREAM_HEADERS });
    } catch (e) {
        console.error(`[WS] #${id} failed to create upstream:`, e.message);
        activeConnections--;
        client.close(1011, 'Cannot connect to target');
        return;
    }

    // Upstream aberto — ligar os pipes de mensagens
    upstream.on('open', () => {
        console.log(`[WS] #${id} upstream OPEN ✓`);

        client.on('message', (data, isBinary) => {
            if (upstream.readyState === WebSocket.OPEN)
                upstream.send(data, { binary: isBinary });
        });

        upstream.on('message', (data, isBinary) => {
            if (client.readyState === WebSocket.OPEN)
                client.send(data, { binary: isBinary });
        });
    });

    // Upstream fechou
    upstream.on('close', (code, reason) => {
        const r = reason && reason.length ? reason.toString() : '(none)';
        if (code === 1006) {
            console.error(`[WS] #${id} upstream CLOSED 1006 — IP BLOQUEADO pelo moomoo.io (datacenter IP rejeitado)`);
        } else {
            console.log(`[WS] #${id} upstream closed code=${code} reason=${r}`);
        }
        if (client.readyState < 2) client.close(code, reason);
    });

    upstream.on('error', err => {
        console.error(`[WS] #${id} upstream error: ${err.message}`);
        if (client.readyState < 2) client.close(1011, 'Upstream error');
    });

    // Cliente desconectou
    client.on('close', (code, reason) => {
        activeConnections--;
        const r = reason && reason.length ? reason.toString() : '(none)';
        console.log(`[WS] #${id} client closed code=${code} reason=${r} | active=${activeConnections}`);
        if (upstream.readyState < 2) upstream.close();
    });

    client.on('error', err => {
        console.error(`[WS] #${id} client error: ${err.message}`);
        if (upstream.readyState < 2) upstream.close();
    });
});

server.listen(PORT, () => {
    console.log(`moo-proxy listening on port ${PORT}`);
});
