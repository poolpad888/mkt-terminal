#!/bin/bash
# Установщик посредника Минфина. Запускать на российском сервере от root.
set -e

cat >/opt/mfrelay.py <<'PYEOF'
import http.server, urllib.request, urllib.parse

KEY = 'mk7301fx'
HOST = 'https://minfin.gov.ru'

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        if q.get('k', [''])[0] != KEY:
            self.send_response(403); self.end_headers()
            self.wfile.write(b'no'); return
        p = q.get('p', ['/ru/press-center/'])[0]
        if not p.startswith('/'):
            p = '/' + p
        url = HOST + p
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.9',
            })
            r = urllib.request.urlopen(req, timeout=15)
            data = r.read()
            self.send_response(200)
            self.send_header('Content-Type', r.headers.get('Content-Type', 'text/plain'))
            self.end_headers()
            self.wfile.write(data)
        except urllib.error.HTTPError as e:
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(('UPSTREAM_HTTP_' + str(e.code)).encode())
        except Exception as e:
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(('UPSTREAM_ERR ' + str(e)).encode())

    def log_message(self, *a):
        pass

http.server.ThreadingHTTPServer(('0.0.0.0', 8080), H).serve_forever()
PYEOF

cat >/etc/systemd/system/mfrelay.service <<'UEOF'
[Unit]
Description=Minfin relay
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/mfrelay.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
UEOF

systemctl daemon-reload
systemctl enable --now mfrelay
command -v ufw >/dev/null 2>&1 && ufw allow 8080 >/dev/null 2>&1 || true
sleep 1
systemctl is-active mfrelay && echo "ГОТОВО. Посредник работает на порту 8080."
