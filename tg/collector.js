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
  // У закрытых каналов вместо адреса номер: ссылка на пост тогда вида
  // t.me/c/<номер>/<пост> — она открывается только у подписчиков канала.
  const priv = uname.startsWith('id:');
  const url = priv
    ? 'https://t.me/c/' + uname.slice(3) + '/' + msg.id
    : 'https://t.me/' + uname + '/' + msg.id;
  return {
    id:      uname.replace('id:', 'c') + '-' + msg.id,
    src:     uname,
    srcName: name || uname,
    url,
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

  // Телеграм начинает присылать обновления только после запроса состояния.
  // Без этого соединение живое, а событий нет — ровно то, что мы видели.
  try {
    const st = await client.invoke(new Api.updates.GetState({}));
    console.log('состояние обновлений получено, pts=' + st.pts);
  } catch (e) { console.log('не удалось получить состояние: ' + e.message); }

  const me = await client.getMe();
  console.log('вошёл как ' + (me.username || me.phone) + ', слушаю каналы');

  // Сверка списка с подписками: node tg/collector.js --list
  // Выводит все каналы, на которые подписан аккаунт, с настоящими адресами,
  // и отдельно — записи нашего списка, которых среди подписок нет.
  // Нужно, чтобы ловить опечатки в адресах и мёртвые каналы-двойники.
  if (process.argv.includes('--list')) {
    const dialogs = await client.getDialogs({ limit: 300 });
    const have = new Set();
    const rows = [];
    for (const d of dialogs) {
      const e = d.entity;
      if (!e || !e.broadcast) continue;
      // У закрытых каналов публичного адреса нет — обращаемся по номеру.
      const key = e.username || ('id:' + e.id);
      have.add(key);
      const last = d.message && d.message.date
        ? new Date(d.message.date * 1000).toISOString().slice(0, 16).replace('T', ' ')
        : '—';
      rows.push([CHANNELS[key] ? ' ' : '+', key, e.title || '', last]);
    }
    rows.sort((a, b) => a[1].localeCompare(b[1]));
    console.log('\nПодписки аккаунта (+ = нет в нашем списке):');
    for (const [f, u, t, l] of rows) console.log(f + ' ' + u.padEnd(28) + l + '  ' + t);
    const miss = Object.keys(CHANNELS).filter(u => !have.has(u));
    console.log('\nВ списке есть, а в подписках нет — ' + miss.length + ':');
    for (const u of miss) console.log('  ' + u + '  (' + CHANNELS[u] + ')');
    await client.disconnect();
    process.exit(0);
  }

  // Разовая выкачка: node tg/collector.js --backfill [сколько]
  // Берёт последние сообщения из каждого канала, пишет в базу и выходит.
  // Нужна для проверки записи и для первичного наполнения ленты.
  if (process.argv.includes('--backfill')) {
    const idx = process.argv.indexOf('--backfill');
    const limit = parseInt(process.argv[idx + 1], 10) || 5;
    let ok = 0, fail = 0;
    for (const uname of Object.keys(CHANNELS)) {
      try {
        const msgs = await client.getMessages(uname, { limit });
        for (const m of msgs) {
          const row = rowFrom(m, uname, CHANNELS[uname]);
          if (row) { await withRetry(insertPost, row, 'выкачка'); ok++; }
        }
      } catch (e) { fail++; console.log('не вышло с ' + uname + ': ' + e.message); }
      await new Promise(r => setTimeout(r, 1200)); // не частим, чтобы не ловить лимиты
    }
    console.log('выкачка завершена: записей ' + ok + ', каналов с ошибкой ' + fail);
    await pool.end();
    process.exit(0);
  }

  // Разбираем сырые обновления сами. Надстройка NewMessage в этой версии
  // библиотеки событий не отдавала, а сырой поток идёт исправно.
  client.addEventHandler(async (upd) => {
    lastEvent = Date.now();
    const cls = (upd && upd.className)
      || (upd && upd.constructor && upd.constructor.name)
      || typeof upd;
    if (DEBUG && !/UpdateUserStatus|UpdateUserTyping|UpdateConnectionState/.test(cls))
      console.log('· сырое событие: ' + cls);

    // Имя класса приходит не всегда — в этой версии библиотеки часть
    // обновлений идёт без него. Поэтому проверяем ещё и по типу объекта.
    const isNew  = /^UpdateNew(Channel)?Message$/.test(cls)
      || upd instanceof Api.UpdateNewChannelMessage
      || upd instanceof Api.UpdateNewMessage;
    const isEdit = /^UpdateEdit(Channel)?Message$/.test(cls)
      || upd instanceof Api.UpdateEditChannelMessage
      || upd instanceof Api.UpdateEditMessage;
    if (!isNew && !isEdit) {
      // Запасной путь: тип не опознан, но внутри лежит сообщение с текстом.
      // Лучше попробовать записать (дубли отсекутся по id), чем потерять.
      if (!(upd && upd.message && typeof upd.message === 'object' && upd.message.message)) return;
      if (DEBUG) console.log('· неопознанное событие с сообщением, пробую записать');
    }

    try {
      const msg = upd.message;
      if (!msg || !msg.peerId) return;
      const ch = await client.getEntity(msg.peerId);
      const uname = ch && ch.username;
      if (!uname) { if (DEBUG) console.log('· канал без имени, пропуск'); return; }
      if (Object.keys(CHANNELS).length && !CHANNELS[uname]) {
        if (DEBUG) console.log('· пропущен, нет в списке: ' + uname);
        return;
      }
      const row = rowFrom(msg, uname, CHANNELS[uname]);
      if (!row) return;
      if (isEdit) await withRetry(updatePost, row, 'правка');
      else        await withRetry(insertPost, row, 'новый');
    } catch (e) { console.log('ошибка обработки: ' + e.message); }
  });

  // Необработанный отказ промиса в новых версиях Node роняет процесс.
  // Для долгоживущей службы лучше записать в журнал и продолжить работу.
  process.on('unhandledRejection', e => console.log('необработанный отказ: ' + (e && e.message || e)));
  process.on('uncaughtException',  e => console.log('исключение: ' + (e && e.message || e)));

  // ── Опрос ──────────────────────────────────────────────────────────────
  // Поток обновлений от Телеграма в этой связке не работает: приходят
  // только служебные UpdateConnectionState. Поэтому спрашиваем сами.
  // Один запрос списка диалогов отдаёт последнее сообщение сразу по всем
  // каналам — это дёшево и не упирается в ограничения.
  const POLL_MS = parseInt(process.env.TG_POLL_MS || '4000', 10);
  const seen = new Map();   // username → id последнего обработанного поста
  let first = true;

  async function poll() {
    try {
      const dialogs = await client.getDialogs({ limit: 100 });
      lastEvent = Date.now();
      for (const d of dialogs) {
        const ent = d.entity;
        if (!ent) continue;
        const uname = ent.username || ('id:' + ent.id);
        if (!CHANNELS[uname]) continue;
        const msg = d.message;
        if (!msg || !msg.id) continue;

        const prev = seen.get(uname);
        if (prev === undefined) { seen.set(uname, msg.id); continue; }
        if (msg.id <= prev) continue;

        // Между опросами могло выйти несколько постов — добираем пропущенные.
        let batch = [msg];
        if (msg.id > prev + 1) {
          try {
            const more = await client.getMessages(ent, { minId: prev, limit: 20 });
            if (more && more.length) batch = more;
          } catch (e) { if (DEBUG) console.log('· не добрал ' + uname + ': ' + e.message); }
        }
        batch.sort((a, b) => a.id - b.id);
        for (const m of batch) {
          const row = rowFrom(m, uname, CHANNELS[uname]);
          if (row) await withRetry(insertPost, row, 'новый');
        }
        seen.set(uname, msg.id);
      }
      if (first) { first = false; console.log('опрос запущен, интервал ' + POLL_MS + ' мс'); }
    } catch (e) { console.log('опрос: ' + e.message); }
    setTimeout(poll, POLL_MS);
  }
  poll();

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
