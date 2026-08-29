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
const DEBUG = process.env.TG_DEBUG === '1';

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
// Разрыв соединения с базой в фоне выбрасывает событие error на пуле.
// Без этого обработчика Node роняет весь процесс.
pool.on('error', e => console.log('база, фоновая ошибка: ' + e.message));

// ── Очередь на случай недоступности базы ────────────────────────────────
// Если запись не удалась, пост не теряем: кладём в очередь и пробуем снова.
// Очередь ограничена, чтобы при долгой аварии не съесть память.
const QUEUE_MAX = 500;
const queue = [];
// Отметка последнего события. Объявлена здесь, а не рядом со сторожем:
// обработчики пишут в неё, и при объявлении ниже по файлу обращение
// до инициализации уронило бы процесс.
let lastEvent = Date.now();
async function withRetry(fn, row, what) {
  try { await fn(row); }
  catch (e) {
    console.log('не записалось (' + what + ') ' + row.id + ': ' + e.message);
    if (queue.length < QUEUE_MAX) queue.push({ fn, row, what });
    else console.log('очередь переполнена, пост потерян: ' + row.id);
  }
}
setInterval(async () => {
  if (!queue.length) return;
  const batch = queue.splice(0, 50);
  console.log('повторяю ' + batch.length + ' записей из очереди');
  for (const t of batch) await withRetry(t.fn, t.row, t.what);
}, 30000);

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
    // Важно: именно start(), а не connect(). При голом connect() gramjs
    // подключается, но поток обновлений от сервера не запускается —
    // сообщения в каналах выходят, а обработчик молчит.
    // С готовой сессией start() ничего не спрашивает.
    await client.start({
      phoneNumber: () => { throw new Error('сессия недействительна, нужен повторный вход: node tg/collector.js --login'); },
      password:    () => { throw new Error('сессия недействительна, нужен повторный вход'); },
      phoneCode:   () => { throw new Error('сессия недействительна, нужен повторный вход'); },
      onError:     e  => console.log('ошибка подключения: ' + e.message),
    });
  }

  // Прогрев: без обращения к списку диалогов сервер может не начать
  // присылать обновления, а getChat() в обработчике — не найти канал.
  try {
    const d = await client.getDialogs({ limit: 200 });
    console.log('диалогов в кэше: ' + d.length);
  } catch (e) { console.log('не удалось прогреть список диалогов: ' + e.message); }

  const me = await client.getMe();
  console.log('вошёл как ' + (me.username || me.phone) + ', слушаю каналы');

  client.addEventHandler(async (ev) => {
    lastEvent = Date.now();
    try {
      const chat = await ev.getChat();
      const uname = chat && chat.username;
      if (!uname) { if (DEBUG) console.log('· событие без username канала'); return; }
      if (Object.keys(CHANNELS).length && !CHANNELS[uname]) {
        if (DEBUG) console.log('· пропущен, нет в списке: ' + uname);
        return;
      }
      const row = rowFrom(ev.message, uname, CHANNELS[uname]);
      if (row) await withRetry(insertPost, row, 'новый');
    } catch (e) { console.log('ошибка обработки: ' + e.message); }
  }, new NewMessage({}));

  // Правки приходят отдельным типом апдейта, не через NewMessage.
  client.addEventHandler(async (upd) => {
    lastEvent = Date.now();
    if (!(upd instanceof Api.UpdateEditChannelMessage)) return;
    try {
      const msg = upd.message;
      const ch = await client.getEntity(msg.peerId);
      const uname = ch && ch.username;
      if (!uname) return;
      if (Object.keys(CHANNELS).length && !CHANNELS[uname]) return;
      const row = rowFrom(msg, uname, CHANNELS[uname]);
      if (row) await withRetry(updatePost, row, 'правка');
    } catch (e) { console.log('ошибка правки: ' + e.message); }
  });

  // Необработанный отказ промиса в новых версиях Node роняет процесс.
  // Для долгоживущей службы лучше записать в журнал и продолжить работу.
  process.on('unhandledRejection', e => console.log('необработанный отказ: ' + (e && e.message || e)));
  process.on('uncaughtException',  e => console.log('исключение: ' + (e && e.message || e)));

  // Сторож: если полчаса нет ни одного события, соединение скорее всего
  // повисло. Выходим с ошибкой — systemd поднимет процесс заново.
  setInterval(() => {
    if (Date.now() - lastEvent > 1800000) {
      console.log('полчаса тишины — перезапускаюсь');
      process.exit(1);
    }
  }, 60000);

  for (const sig of ['SIGTERM', 'SIGINT'])
    process.on(sig, async () => { try { await pool.end(); } catch (e) {} process.exit(0); });
})();
