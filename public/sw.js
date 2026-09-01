// Служебный сценарий нужен только для того, чтобы браузер считал сайт
// устанавливаемым. Намеренно НИЧЕГО не кэшируем: лента живая, и закэшированные
// ответы показывали бы устаревшие новости и котировки. Все запросы уходят в сеть
// как обычно; при обрыве связи отдаём короткий понятный ответ.

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));

self.addEventListener('fetch', event => {
  const req = event.request;
  if (req.method !== 'GET') return;                 // остальное не трогаем
  event.respondWith(
    fetch(req).catch(() => {
      if (req.mode === 'navigate') {
        return new Response(
          '<!doctype html><meta charset="utf-8">' +
          '<style>body{background:#0b0d10;color:#e6e9ee;font:15px system-ui;' +
          'display:flex;align-items:center;justify-content:center;height:100vh;margin:0;text-align:center}</style>' +
          '<div>Нет связи с сетью.<br>Лента обновится, когда соединение вернётся.</div>',
          { headers: { 'Content-Type': 'text/html; charset=utf-8' } }
        );
      }
      return new Response('', { status: 504 });
    })
  );
});
