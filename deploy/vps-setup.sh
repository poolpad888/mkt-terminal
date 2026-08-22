#!/usr/bin/env bash
# Установка FINFACTS на чистый Ubuntu VPS. Запускать от root.
# Повторный запуск безопасен: скрипт обновит код и перезапустит службу.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

APP_DIR=/opt/finfacts
DOMAIN=finfacts.ru

echo "== 1/6 Пакеты =="
apt-get update -y -qq
apt-get install -y -qq curl git ca-certificates nginx postgresql openssl python3-certbot-nginx >/dev/null

echo "== 2/6 Node.js =="
if ! command -v node >/dev/null 2>&1 || [ "$(node -v | sed 's/v//' | cut -d. -f1)" -lt 18 ]; then
  curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >/dev/null
  apt-get install -y -qq nodejs >/dev/null
fi
echo "node $(node -v)"

echo "== 3/6 База данных =="
systemctl enable --now postgresql >/dev/null 2>&1 || true
if [ -f /root/.finfacts_dbpass ]; then
  DBPASS=$(cat /root/.finfacts_dbpass)
else
  DBPASS=$(openssl rand -hex 16)
  echo "$DBPASS" > /root/.finfacts_dbpass
  chmod 600 /root/.finfacts_dbpass
fi
sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='finfacts'" | grep -q 1 \
  || sudo -u postgres psql -qc "CREATE ROLE finfacts LOGIN"
sudo -u postgres psql -qc "ALTER ROLE finfacts PASSWORD '$DBPASS'"
sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='finfacts'" | grep -q 1 \
  || sudo -u postgres createdb -O finfacts finfacts

echo "== 4/6 Приложение =="
if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" fetch origin main -q && git -C "$APP_DIR" reset --hard origin/main -q
else
  git clone -q https://github.com/poolpad888/mkt-terminal.git "$APP_DIR"
fi
cd "$APP_DIR"
npm install --omit=dev --no-audit --no-fund >/dev/null 2>&1 || npm install >/dev/null

cat > /etc/systemd/system/finfacts.service <<UNIT
[Unit]
Description=FINFACTS news server
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
WorkingDirectory=$APP_DIR
Environment=PORT=3000
Environment=DATABASE_URL=postgres://finfacts:$DBPASS@127.0.0.1:5432/finfacts
ExecStart=/usr/bin/node server.js
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable finfacts >/dev/null 2>&1
systemctl restart finfacts

echo "== 5/6 Nginx =="
cat > /etc/nginx/sites-available/finfacts <<NGX
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name $DOMAIN www.$DOMAIN _;
    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_http_version 1.1;
    }
}
NGX
ln -sf /etc/nginx/sites-available/finfacts /etc/nginx/sites-enabled/finfacts
rm -f /etc/nginx/sites-enabled/default
nginx -t -q && systemctl reload nginx

echo "== 6/6 Сертификат =="
MYIP=$(curl -fsS -4 https://api.ipify.org || hostname -I | awk '{print $1}')
DNSIP=$(getent ahostsv4 "$DOMAIN" | awk 'NR==1{print $1}' || true)
if [ -n "$DNSIP" ] && [ "$DNSIP" = "$MYIP" ]; then
  certbot --nginx -d "$DOMAIN" -d "www.$DOMAIN" --agree-tos --register-unsafely-without-email -n --redirect || \
  certbot --nginx -d "$DOMAIN" --agree-tos --register-unsafely-without-email -n --redirect || \
  echo "Сертификат выпустить не удалось — запустите скрипт ещё раз позже."
else
  echo "Домен $DOMAIN пока указывает не сюда (нужно: $MYIP, сейчас: ${DNSIP:-нет записи})."
  echo "Смените A-запись и запустите скрипт повторно — он выпустит сертификат."
fi

sleep 3
echo "=================================================="
systemctl is-active finfacts >/dev/null && echo "СЛУЖБА: работает" || echo "СЛУЖБА: НЕ ЗАПУСТИЛАСЬ (смотрите: journalctl -u finfacts -n 50)"
curl -fsS -o /dev/null -w "САЙТ ПО АДРЕСУ http://$MYIP : код %{http_code}\n" "http://127.0.0.1" || true
echo "ГОТОВО."
