const UPSTREAM_TIMEOUT_MS = 8000;

function tFetch(url, init = {}) {
	const c = new AbortController();
	const t = setTimeout(() => c.abort(), UPSTREAM_TIMEOUT_MS);
	return fetch(url, { ...init, signal: c.signal }).finally(() => clearTimeout(t));
}

export default {
	async fetch(request) {
		const url = new URL(request.url);

		if (request.method === 'OPTIONS') {
			return new Response(null, {
				headers: {
					'Access-Control-Allow-Origin': '*',
					'Access-Control-Allow-Methods': 'GET',
					'Access-Control-Max-Age': '86400',
				},
			});
		}

		if (url.pathname.startsWith('/binance-ws/')) {
			if (request.headers.get('Upgrade') !== 'websocket') {
				return new Response('Expected WebSocket', { status: 426 });
			}
			const upstream = `https://data-stream.binance.vision${url.pathname.slice('/binance-ws'.length)}${url.search}`;
			const upstreamResp = await fetch(upstream, { headers: { Upgrade: 'websocket' } });
			const upstreamWS = upstreamResp.webSocket;
			if (!upstreamWS) {
				return new Response('Upstream did not upgrade', { status: 502 });
			}
			upstreamWS.accept();
			const pair = new WebSocketPair();
			const [client, server] = Object.values(pair);
			server.accept();
			server.addEventListener('message', e => { try { upstreamWS.send(e.data); } catch(_){} });
			upstreamWS.addEventListener('message', e => { try { server.send(e.data); } catch(_){} });
			const closeBoth = () => { try{server.close();}catch(_){} try{upstreamWS.close();}catch(_){} };
			server.addEventListener('close', closeBoth);
			upstreamWS.addEventListener('close', closeBoth);
			server.addEventListener('error', closeBoth);
			upstreamWS.addEventListener('error', closeBoth);
			return new Response(null, { status: 101, webSocket: client });
		}

		if (url.pathname.startsWith('/yahoo/')) {
			const target = `https://query1.finance.yahoo.com${url.pathname.slice('/yahoo'.length)}${url.search}`;
			try {
				const resp = await tFetch(target, {
					headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
					cf: { cacheTtl: 0, cacheEverything: false },
				});
				return new Response(resp.body, {
					status: resp.status,
					headers: {
						'Content-Type': resp.headers.get('Content-Type') || 'application/json',
						'Access-Control-Allow-Origin': '*',
						'Cache-Control': 'no-store, no-cache, must-revalidate',
					},
				});
			} catch (err) {
				return new Response(JSON.stringify({ error: err.message }), {
					status: 502,
					headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' },
				});
			}
		}

		if (url.pathname.startsWith('/binance/')) {
			const target = `https://api-gcp.binance.com${url.pathname.slice('/binance'.length)}${url.search}`;
			try {
				const resp = await tFetch(target, {
					headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
					cf: { cacheTtl: 0, cacheEverything: false },
				});
				return new Response(resp.body, {
					status: resp.status,
					headers: {
						'Content-Type': resp.headers.get('Content-Type') || 'application/json',
						'Access-Control-Allow-Origin': '*',
						'Cache-Control': 'no-store, no-cache, must-revalidate',
					},
				});
			} catch (err) {
				return new Response(JSON.stringify({ error: err.message }), {
					status: 502,
					headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' },
				});
			}
		}

		return new Response('Not Found', { status: 404 });
	},
};
