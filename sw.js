/* 鱼虾足球游戏大战 PWA service worker
   静态资源优先缓存；页面导航网络优先，断网时回退到缓存。 */
const CACHE_NAME = 'zhuj-cup-2026-v0.8.0';
const PRECACHE = [
  './',
  './index.html',
  './ranking.html',
  './privacy.html',
  './terms.html',
  './code-of-conduct.html',
  './report.html',
  './support.html',
  './legal.css',
  './manifest.webmanifest',
  './favicon.ico',
  './js/three.min.js',
  './js/online.js',
  './js/pwa.js',
  './fb/20_hd.jpg',
  './fb/21.jpg',
  './fb/24.jpg',
  './fb/32.jpg',
  './icons/icon-72-noface.png',
  './icons/icon-96-noface.png',
  './icons/icon-144-noface.png',
  './icons/icon-192-noface.png',
  './icons/icon-384-noface.png',
  './icons/icon-512-noface.png',
  './icons/icon-512-maskable-noface.png',
  './icons/apple-touch-icon-noface.png',
  './icons/favicon-32-noface.png'
];

self.addEventListener('install', event => {
  event.waitUntil((async () => {
    const cache = await caches.open(CACHE_NAME);
    await cache.addAll(PRECACHE);
    await self.skipWaiting();
  })());
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)));
    await self.clients.claim();
  })());
});

self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);
  // 只接管本站静态资源；MQTT 的 WebSocket 不会被 fetch 拦截。
  if (url.origin !== self.location.origin) return;

  if (req.mode === 'navigate') {
    event.respondWith((async () => {
      try {
        const fresh = await fetch(req);
        const cache = await caches.open(CACHE_NAME);
        cache.put(req, fresh.clone());
        return fresh;
      } catch (e) {
        const hit = await caches.match(req, { ignoreSearch: true });
        if (hit) return hit;
        return (await caches.match('./index.html')) ||
          new Response('鱼虾足球游戏大战 当前离线，请联网后重试。', {
            status: 503,
            headers: { 'Content-Type': 'text/plain; charset=utf-8' }
          });
      }
    })());
    return;
  }

  event.respondWith((async () => {
    const hit = await caches.match(req, { ignoreSearch: true });
    if (hit) return hit;
    try {
      const fresh = await fetch(req);
      if (fresh && fresh.ok) {
        const cache = await caches.open(CACHE_NAME);
        cache.put(req, fresh.clone());
      }
      return fresh;
    } catch (e) {
      return new Response('', { status: 504, statusText: 'Offline' });
    }
  })());
});
