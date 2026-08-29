// finfacts — сборщик новостей через Telegram MTProto (gramjs)
// Отдельный процесс. Слушает каналы в реальном времени и пишет в ту же
// базу Postgres, что и основной сервер. Падение сборщика не ломает сайт:
// веб-парсинг в server.js продолжает работать как страховка.
//
// Первый запуск (интерактивно, в консоли):  node tg/collector.js --login
// Обычный запуск (служба):                  node tg/collector.js

const fs   = require('fs');
const path = require('path');
const { TelegramClient, Api } = require('telegram');
const { StringSession }       = require('telegram/sessions');
const { NewMessage }          = require('telegram/events');
const { Pool }                = require('pg');

const API_ID   = parseInt(process.env.TG_API_ID || '0', 10);
const API_HASH = process.env.TG_API_HASH || '';
const SESSION_FILE = path.join(__dirname, 'session.txt');
const TEXT_LIMIT = 1000;

// ── Каналы: username → отображаемое имя. Заполняется из channels.json,
// чтобы список правился без правки кода.
let CHANNELS = {};
try { CHANNELS = JSON.parse(fs.readFileSync(path.join(__dirname, 'channels.json'), 'utf8')); }
catch (e) { console.log('channels.json не найден — слушаю все подписки'); }

// ── Обрезка текста по концу предложения (та же логика, что в server.js) ─
function cutText(t, max) {
  if (t.length <= max) return t;
  const cut = t.slice(0, max);
  const dot = Math.max(cut.lastIndexOf('. '), cut.lastIndexOf('! '), cut.lastIndexOf('? '));
  if (dot > max * 0.45) return cut.slice(0, dot + 1);
  const sp = cut.lastIndexOf(' ');
  return (sp > 0 ? cut.slice(0, sp) : cut) + '…';
}

const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, max: 2 })
  : null;
if (!pool) { console.log('нет DATABASE_URL — писать некуда'); process.exit(1); }

// Новый пост: вставляем, при конфликте по id ничего не трогаем.
async function insertPost(row) {
  await pool.query(
    `INSERT INTO news (id, ts, src, src_name, url, body, lvl, src_count)
     VALUES ($1,$2,$3,$4,$5,$6,0,1) ON CONFLICT (id) DO NOTHING`,
    [row.id, row.time, row.src, row.srcName, row.url, row.text]);
  console.log('+ ' + row.srcName + ': ' + row.text.slice(0, 60).replace(/\n/g, ' '));
}

// Правка: обновляем только текст. Важность (lvl) оставляем исходную —
// так решено: пересчёт мог бы поднять уровень уже показанной новости.
async function updatePost(row) {
  const r = await pool.query('UPDATE news SET body = $2 WHERE id = $1', [row.id, row.text]);
  if (r.rowCount) console.log('~ правка ' + row.srcName + ' ' + row.id);
}

function rowFrom(msg, uname, name) {
  const text = (msg.message || '').trim();
  if (!text || text.length < 15) return null;
  return {
    id:      uname + '-' + msg.id,
    src:     uname,
    srcName: name || uname,
    url:     'https://t.me/' + uname + '/' + msg.id,
    time:    new Date(msg.date * 1000).toISOString(),
    text:    cutText(text, TEXT_LIMIT),
  };
}

(async () => {
  const saved = fs.existsSync(SESSION_FILE) ? fs.readFileSync(SESSION_FILE, 'utf8').trim() : '';
  if (!saved && !process.argv.includes('--login')) {
    console.log('Сессии нет. Запустите один раз: node tg/collector.js --login');
    process.exit(1);
  }
  const client = new TelegramClient(new StringSession(saved), API_ID, API_HASH,
    { connectionRetries: 20, autoReconnect: true });

  if (!saved) {
    const input = require('readline/promises')
      .createInterface({ input: process.stdin, output: process.stdout });
    await client.start({
      phoneNumber: () => input.question('Номер телефона (в формате +7...): '),
      password:    () => input.question('Пароль двухфакторной защиты: '),
      phoneCode:   () => input.question('Код из Телеграма: '),
      onError:     e  => console.log('ошибка входа: ' + e.message),
    });
    fs.writeFileSync(SESSION_FILE, client.session.save(), { mode: 0o600 });
    console.log('Сессия сохранена в tg/session.txt — больше вход не понадобится.');
    input.close();
  } else {
    await client.connect();
  }

  const me = await client.getMe();
  console.log('вошёл как ' + (me.username || me.phone) + ', слушаю каналы');

  client.addEventHandler(async (ev) => {
    try {
      const chat = await ev.getChat();
      const uname = chat && chat.username;
      if (!uname) return;
      if (Object.keys(CHANNELS).length && !CHANNELS[uname]) return;
      const row = rowFrom(ev.message, uname, CHANNELS[uname]);
      if (row) await insertPost(row);
    } catch (e) { console.log('ошибка обработки: ' + e.message); }
  }, new NewMessage({}));

  // Правки приходят отдельным типом апдейта, не через NewMessage.
  client.addEventHandler(async (upd) => {
    if (!(upd instanceof Api.UpdateEditChannelMessage)) return;
    try {
      const msg = upd.message;
      const ch = await client.getEntity(msg.peerId);
      const uname = ch && ch.username;
      if (!uname) return;
      if (Object.keys(CHANNELS).length && !CHANNELS[uname]) return;
      const row = rowFrom(msg, uname, CHANNELS[uname]);
      if (row) await updatePost(row);
    } catch (e) { console.log('ошибка правки: ' + e.message); }
  });

  for (const sig of ['SIGTERM', 'SIGINT'])
    process.on(sig, async () => { await pool.end(); process.exit(0); });
})();
