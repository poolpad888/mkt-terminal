#!/bin/bash
# Установщик посредника Минфина через настоящий браузерный движок.
# Запускать на российском сервере от root.
set -e

echo "[1/4] Ставлю Python, Chromium и зависимости…"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv >/dev/null

echo "[2/4] Ставлю Playwright (браузерный движок)…"
python3 -m venv /opt/mfvenv
/opt/mfvenv/bin/pip install -q --upgrade pip
/opt/mfvenv/bin/pip install -q playwright
/opt/mfvenv/bin/playwright install --with-deps chromium >/dev/null 2>&1 || /opt/mfvenv/bin/playwright install chromium

echo "[3/4] Пишу посредника…"
cat >/opt/mfrelay.py <<'PYEOF'
import http.server, urllib.parse, threading
from playwright.sync_api import sync_playwright

KEY = 'mk7301fx'
HOST = 'https://minfin.gov.ru'
_lock = threading.Lock()
_bctx = {'b': None, 'p': None}

def get_page():
    if _bctx['p'] is None:
        pw = sync_playwright().start()
        b = pw.chromium.launch(args=['--no-sandbox', '--disable-dev-shm-usage'])
        ctx = b.new_context(
            locale='ru-RU',
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        )
        _bctx['b'] = b
        _bctx['p'] = ctx.new_page()
    return _bctx['p']

def fetch(path):
    with _lock:
        page = get_page()
        resp = page.goto(HOST + path, wait_until='domcontentloaded', timeout=25000)
        code = resp.status if resp else 0
        body = page.content()
        return code, body

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
        try:
            code, body = fetch(p)
            out = ('HTTP_' + str(code) + '\n' + body).encode('utf-8', 'replace')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(out)
        except Exception as e:
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(('UPSTREAM_ERR ' + str(e)).encode())

    def log_message(self, *a):
        pass

http.server.ThreadingHTTPServer(('0.0.0.0', 8080), H).serve_forever()
PYEOF

echo "[4/4] Запускаю службу…"
cat >/etc/systemd/system/mfrelay.service <<'UEOF'
[Unit]
Description=Minfin relay
After=network.target

[Service]
ExecStart=/opt/mfvenv/bin/python3 /opt/mfrelay.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
UEOF

systemctl daemon-reload
systemctl enable --now mfrelay
command -v ufw >/dev/null 2>&1 && ufw allow 8080 >/dev/null 2>&1 || true
sleep 3
systemctl is-active mfrelay && echo "ГОТОВО. Посредник на браузерном движке работает на порту 8080."
