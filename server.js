// MKT-TERMINAL — сервер сбора новостей
// Node.js, без внешних зависимостей.
// Логика: раз в CACHE_SEC секунд (по запросу) собирает посты из открытых
// веб-версий Telegram-каналов (t.me/s/...) и RSS Финама, склеивает,
// убирает точные дубли, классифицирует важность, отдаёт JSON.

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const PORT = process.env.PORT || 10000;
const CACHE_SEC = 180;          // как часто обновлять ленту
const KEEP_HOURS = 26;          // сколько часов новостей держать
const PER_SOURCE_LIMIT = 40;    // максимум постов с одного источника за проход

// ── Источники ────────────────────────────────────────────────────────────
const TG = [
  ['markettwits',        'MarketTwits'],
  ['prime1',             'ПРАЙМ'],
  ['interfaxonline',     'Интерфакс'],
  ['rbc_news',           'РБК'],
  ['ruinvestingcom',     'Investing.com'],
  ['frank_media',        'Frank Media'],
  ['vedomosti',          'Ведомости'],
  ['kommersant',         'Коммерсантъ'],
  ['forbesrussia',       'Forbes'],
  ['bcs_express',        'БКС Экспресс'],
  ['omyinvestments',     'Мои Инвестиции'],
  ['AK47pfl',            'РДВ'],
  ['cbrstocks',          'Сигналы РЦБ'],
  ['truevalue',          'Truevalue'],
  ['centralbank_russia', 'Банк России'],
  ['russianmacro',       'MMI'],
  ['spydell_finance',    'Spydell Finance'],
  ['cbonds',             'Cbonds'],
  ['oil_capital',        'Нефть и Капитал'],
  ['stocksi',            'STOCKSI'],
  ['bbbreaking',          'Раньше всех'],
  ['Bonds_lab',           'Bonds lab'],
  ['PresidentDonaldTrumpRU', 'Трамп'],
  ['antonchehovanalitk',  'Аналитика Чехова'],
];
const RSS = [
  ['https://www.finam.ru/analysis/conews/rsspoint/', 'Финам'],
  ['https://www.moex.com/export/news.aspx?cat=100', 'Мосбиржа'],
  ['https://minfin.gov.ru/ru/press-center/rss/', 'Минфин'],
];
// Реклама, платные подписки и набор в закрытые группы — не новости
const PROMO_SOURCES = new Set(['antonchehovanalitk']);
const PROMO_RE = /вип[\s-]*канал|vip[\s-]*канал|платн[а-яё]+\s+(канал|подписк|групп)|подписк[а-яё]*\s+на\s+(закрыт|вип|vip)|закрыт[а-яё]+\s+(канал|групп)|открыт[а-яё]+\s+набор|набор\s+в\s+(групп|команд)|мест[а-яё]*\s+осталось|осталось\s+\d+\s+мест|успей\s+записат|запись\s+открыт|_bot(?![a-z])|бот\s+для\s+оплат|оплат[а-яё]*\s+(вип|vip|подписк)|тариф|промокод|скидк[а-яё]+\s+на\s+подписк|реклам[а-яё]*\s*[:\-]|erid|по\s+вопросам\s+рекламы|прайс|сотрудничеств|партнёрск|партнерск/i;

// Технический мусор Мосбиржи и Минфина в ленту не пускаем
const RSS_SKIP = {
  'Мосбиржа': /технически[ей]\s+работ|смена\s+ip|сетев[а-яё]+\s+оборудован|плановы[ей]\s+изменени|тестировани|коллокаци|colocation|сертификат|версии\s+по|релиз[а-яё]*\s+систем/i,
  'Минфин': /вакансии|конкурс\s+на\s+замещение|общественн[а-яё]+\s+обсуждени|антикоррупц/i,
};

// ── Классификатор важности ──────────────────────────────────────────────
// Безусловно важные темы (список согласован с пользователем)
const HARD_RE = [
  ['Банк России / ставка', /(?<![а-яё])цб(?![а-яё])|центробанк|банк[а-яё]?\s+росси|ключев[а-яё]+\s+ставк|(?<![а-яё])ставк[аеиу](?![а-яё])|набиуллин|заботкин/i],
  ['Минфин / бюджет', /минфин|(?<![а-яё])фнс(?![а-яё])|госдолг|бюджет[а-яё]*\s*(дефицит|правил)/i],
  ['Путин', /путин/i],
  ['Мишустин', /мишустин/i],
  ['Совбез', /совбез|совет[а-яё]?\s+безопасност/i],
  ['ФРС / Пауэлл', /(?<![а-яё])фрс(?![а-яё])|пауэлл|(?<![a-z])fed(?![a-z])|(?<![a-z])fomc(?![a-z])/i],
  ['санкции', /санкци|эмбарго|(?<![a-z])sdn(?![a-z])|чёрн[а-яё]+\s+список|черн[а-яё]+\s+список/i],
  ['ОПЕК', /(?<![а-яё])опек(?![а-яё])|(?<![a-z])opec(?![a-z])/i],
  ['обыски', /обыск|выемк[аи]\s+документ|следствен[а-яё]+\s+действи/i],
  ['заморозка счетов', /заморо[зж][а-яё]*\s*(счет|счёт|актив|средств)|блокиров[а-яё]*\s*(счет|счёт|актив)/i],
  ['дефолт / невыплата', /дефолт|невыплат|не\s+выплат[а-яё]*\s*(купон|долг)|техническ[а-яё]+\s+дефолт|просроч[а-яё]+\s+платеж/i],
  ['отмена дивидендов', /отмен[а-яё]*\s*дивиденд|не\s+будет\s+выплачивать\s+дивиденд|отказ[а-яё]*\s*от\s*дивиденд|рекоменд[а-яё]*\s*не\s*выплачивать/i],
  ['делистинг', /делистинг|исключ[а-яё]+\s+из\s+котировальн/i],
  ['приостановка торгов', /приостанов[а-яё]*\s*торг|остановк[а-яё]*\s*торг|дискретн[а-яё]+\s+аукцион/i],
  ['отзыв лицензии', /отзыв[а-яё]*\s*лицензи|лишил[а-яё]*\s*лицензи|аннулир[а-яё]*\s*лицензи/i],
  ['национализация', /национализац|национализир|обращен[а-яё]+\s+в\s+собственност\s+государств|изъяти[а-яё]+\s+в\s+пользу\s+государств/i],
  ['арест активов', /арест[а-яё]*\s*(актив|имуществ|акци|счет|счёт)|суд\s+арестовал|наложил\s+арест/i],
  ['экстренное заседание', /экстренн[а-яё]+\s+(заседани|совещани|засед)|внеочередн[а-яё]+\s+заседани|срочн[а-яё]+\s+совещани/i],
  ['обвал / кризис', /(?<![а-яё])обвал|(?<![а-яё])кризис/i],
  ['инфляция', /инфляц|инфляционн[а-яё]+\s+ожидани|(?<![а-яё])ипц(?![а-яё])|потребительск[а-яё]+\s+цен|бизнес-климат|индикатор[а-яё]*\s+бизнес|росстат/i],
  ['переговоры', /переговор/i],
  ['планка', /(?<![а-яё])планк[аеиу](?![а-яё])|верхн[а-яё]+\s+планк|нижн[а-яё]+\s+планк/i],
  ['аукцион', /(?<![а-яё])аукцион/i],
];
const HARD_SIGNS = ['⚠️','💥','🔥','🚨'];

function classify(text) {
  const reasons = [];
  let lvl = 0;
  for (const s of HARD_SIGNS) if (text.includes(s)) { lvl = 2; reasons.push('значок ' + s); break; }
  if (lvl < 2) {
    for (const [name, rx] of HARD_RE) if (rx.test(text)) { lvl = 2; reasons.push(name); break; }
  }
  return { lvl, reasons };
}

// ── Тикеры: явные ($SBER) + по названиям компаний ───────────────────────
const COMPANY_TICKERS = [
  ['SBER', /сбербанк|(?<![а-яё])сбер[ауе]?(?![а-яё])/i],
  ['GAZP', /газпром(?!\s*нефт)/i],
  ['SIBN', /газпром\s*нефт/i],
  ['LKOH', /лукойл/i],
  ['ROSN', /роснефт/i],
  ['NVTK', /новатэк/i],
  ['TATN', /татнефт/i],
  ['SNGS', /сургутнефтегаз/i],
  ['GMKN', /норникел|норильский\s*никел/i],
  ['YDEX', /яндекс/i],
  ['VTBR', /(?<![а-яё])втб(?![а-яё])/i],
  ['T', /т-банк|тинькофф|т-технологи/i],
  ['MOEX', /(?<!индекс\s)(?<!индекса\s)(?<!индексе\s)(?<!индекс )мосбирж|(?<!индекс\s)московск[а-яё]+\s+бирж/i],
  ['PLZL', /(?<![а-яё])полюс(?![а-яё])/i],
  ['CHMF', /северстал/i],
  ['NLMK', /(?<![а-яёa-z])нлмк(?![а-яёa-z])/i],
  ['MAGN', /(?<![а-яёa-z])ммк(?![а-яёa-z])|магнитогорск[а-яё]+\s+мет/i],
  ['ALRS', /алроса/i],
  ['AFLT', /аэрофлот/i],
  ['MGNT', /(?<![а-яё])магнит(?![а-яё])/i],
  ['X5', /(?<![a-z0-9])x5(?![a-z0-9])|пятёрочк|пятерочк/i],
  ['OZON', /озон(?![а-яё])|(?<![a-z])ozon(?![a-z])/i],
  ['MTSS', /(?<![а-яёa-z])мтс(?![а-яёa-z])/i],
  ['RUAL', /русал/i],
  ['PHOR', /фосагро/i],
  ['AFKS', /(?<![а-яё])афк(?![а-яё])/i],
  ['HYDR', /русгидро/i],
  ['IRAO', /интер\s*рао/i],
  ['FLOT', /совкомфлот/i],
  ['SVCB', /совкомбанк/i],
  ['BSPB', /банк[а-яё]*\s+«?санкт-петербург|(?<![а-яё])бспб(?![а-яё])/i],
  ['UGLD', /южуралзолот|(?<![а-яё])югк(?![а-яё])/i],
  ['SMLT', /[«"]самол[её]т[»"]|гк\s*«?самол[её]т/i],
  ['PIKK', /(?<![а-яё])пик(?![а-яё])/i],
  ['VKCO', /(?<![a-z])vk(?![a-z])|вконтакте/i],
  ['POSI', /positive\s*tech|позитив\s*текнолоджиз/i],
  ['ASTR', /(?<![а-яё])астра(?![а-яё])/i],
  ['RTKM', /ростелеком/i],
  ['TRNFP', /транснефт/i],
  ['FEES', /россети/i],
  ['RENI', /ренессанс\s*страхован/i],
  ['ENPG', /эн\+|en\+/i],
  ['SGZH', /сегежа/i],
  ['MTLR', /мечел/i],
  ['RASP', /распадск/i],
  ['UPRO', /юнипро/i],
  ['LSRG', /(?<![а-яё])лср(?![а-яё])/i],
  ['ETLN', /(?<![а-яё])эталон(?![а-яё])/i],
  ['LENT', /(?<![а-яё])лента(?![а-яё])(?!\s*новост)/i],
  ['HEAD', /headhunter|хедхантер|hh\.ru/i],
  // товары, валюты и индексы (не акции Мосбиржи)
  ['BRENT',  /brent|брент|(?<![а-яё])опек|(?<![a-z])opec(?![a-z])|urals|юралс|баррел|(?<!газпром\s)(?<!газпром)(?<!тат)(?<!сургут)(?<![а-яё])нефт[ьияеюо][а-яё]*(?![а-яё])/i],
  ['WTI',    /(?<![a-z])wti(?![a-z])|техасск[а-яё]+\s+нефт|американск[а-яё]+\s+нефт/i],
  ['GAS',    /(?<![а-яё])спг(?![а-яё])|(?<![a-z])lng(?![a-z])|газопровод|цен[а-яё]*\s+на\s+газ|газов[а-яё]+\s+рынок|ttf/i],
  ['GOLD',   /(?<![а-яё])золот[а-яё]*(?![а-яё])|(?<![a-z])xau(?![a-z])/i],
  ['SILVER', /(?<![а-яё])серебр[а-яё]*(?![а-яё])|(?<![a-z])xag(?![a-z])/i],
  ['USDRUB', /(?<![а-яё])доллар[а-яё]*(?![а-яё])|курс\s+рубл|(?<![a-z])usd\s*\/?\s*rub|рубл[а-яё]*\s+к\s+доллар/i],
  ['CNYRUB', /(?<![а-яё])юан[а-яё]*(?![а-яё])|(?<![a-z])cny\s*\/?\s*rub/i],
  ['EURUSD', /(?<![a-z])eur\s*\/?\s*usd|евро\s+к\s+доллар/i],
  ['IMOEX',  /индекс[а-яё]*\s+мосбирж|(?<![a-z])imoex(?![a-z])|индекс[а-яё]*\s+ртс|(?<![a-z])rts(?![a-z])/i],
];
function tickers(text) {
  const out = new Set();
  const re = /\$([A-Z]{2,6})(?![A-Za-z])/g;
  let m; while ((m = re.exec(text))) out.add(m[1]);
  for (const [tk, rx] of COMPANY_TICKERS) if (rx.test(text)) out.add(tk);
  return [...out].slice(0, 5);
}
// ── Теги: из хэштегов + по смыслу текста ────────────────────────────────
const TOPIC_TAGS = [
  ['нефть', /нефт|(?<![a-z])opec(?![a-z])|(?<![а-яё])опек|brent|urals|бензин|(?<![а-яё])азс(?![а-яё])|(?<![а-яё])нпз(?![а-яё])/i],
  ['газ', /(?<![а-яё])газ[аоуе]?м?(?![а-яё])|(?<![а-яё])спг(?![а-яё])|(?<![a-z])lng(?![a-z])|газопровод|газов[а-яё]+/i],
  ['ЦБ и ставка', /(?<![а-яё])цб(?![а-яё])|центробанк|банк[а-яё]?\s+росси|ключев[а-яё]+\s+ставк|(?<![а-яё])ставк[аеиу]|(?<![а-яё])фрс(?![а-яё])|(?<![a-z])ecb(?![a-z])|(?<![а-яё])ецб(?![а-яё])/i],
  ['валюта', /рубл|доллар|(?<![а-яё])юан|(?<![а-яё])евро(?![а-яёa-z])|курс[а-яё]*\s+валют|(?<![a-z])usd(?![a-z])|(?<![a-z])cny(?![a-z])|(?<![a-z])eur(?![a-z])/i],
  ['акции', /акци[ийяе]|дивиденд|(?<![a-z])ipo(?![a-z])|байбэк|buyback|котировк|индекс\s+мосбирж|(?<![a-z])imoex(?![a-z])|(?<![a-z])rts(?![a-z])/i],
  ['облигации', /облигаци|(?<![а-яё])офз(?![а-яё])|(?<![а-яё])бонд[а-яё]*(?![а-яё])|купон|евробонд|долгов[а-яё]+\s+рынок/i],
  ['банки', /банк(?![а-яё]?\s+росси)|кредит|ипотек|вклад|депозит/i],
  ['металлы', /золот|серебр|платин|паллад|никел|(?<![а-яё])мед[ьи](?![а-яё])|алюмини|(?<![а-яё])стал[еиь]н?|железн[а-яё]+\s+руд/i],
  ['санкции', /санкци|эмбарго|заморозк|потолок\s+цен|(?<![a-z])ofac(?![a-z])|(?<![a-z])sdn(?![a-z])/i],
  ['инфляция', /инфляци|потребительск[а-яё]+\s+цен|(?<![а-яё])ипц(?![а-яё])|рост[а-яё]?\s+цен/i],
  ['бюджет', /бюджет|минфин|налог|(?<![а-яё])ндс(?![а-яё])|ндфл|ндпи|госдолг/i],
  ['недвижимость', /недвижимост|застройщик|новостройк|жиль[её]/i],
  ['крипта', /биткоин|bitcoin|(?<![a-z])btc(?![a-z])|эфириум|ethereum|криптовалют|стейблкоин/i],
  ['США', /(?<![а-яё])сша(?![а-яё])|америк|вашингтон|уолл-стрит|wall\s*street|nasdaq|s&p|dow\s*jones/i],
  ['Китай', /кита[йяе]|пекин|шанхайск|гонконг|(?<![а-яё])юан/i],
  ['геополитика', /переговор|перемири|конфликт|украин|ближн[а-яё]+\s+восток|(?<![а-яё])иран(?![а-яё])|израил/i],
];
function hashtags(text) {
  const out = new Set();
  const re = /#([а-яёa-z0-9_]{2,25})/gi;
  let m; while ((m = re.exec(text))) out.add(m[1].toLowerCase());
  for (const [tag, rx] of TOPIC_TAGS) if (rx.test(text)) out.add(tag);
  return [...out].slice(0, 6);
}

// ── HTTP-загрузка с редиректами и gzip ──────────────────────────────────
function fetchUrl(url, redirects = 3) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',
        'Accept': 'text/html,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip',
        'Accept-Language': 'ru,en;q=0.8',
      },
      timeout: 12000,
    }, res => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location && redirects > 0) {
        res.resume();
        const next = new URL(res.headers.location, url).href;
        return resolve(fetchUrl(next, redirects - 1));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
      const chunks = [];
      const stream = res.headers['content-encoding'] === 'gzip' ? res.pipe(zlib.createGunzip()) : res;
      stream.on('data', c => chunks.push(c));
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
      stream.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(new Error('timeout')); });
    req.on('error', reject);
  });
}

// ── Разбор HTML-страницы t.me/s/канал ───────────────────────────────────
function decodeEntities(s) {
  return s
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#0?39;/g, "'").replace(/&nbsp;/g, ' ')
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n));
}
function stripHtml(s) {
  return decodeEntities(
    s.replace(/<br\s*\/?>/gi, '\n')
     .replace(/<\/(p|div)>/gi, '\n')
     .replace(/<[^>]+>/g, '')
  ).replace(/\n{3,}/g, '\n\n').trim();
}
function parseTelegram(html, username, srcName) {
  const items = [];
  const blocks = html.split('tgme_widget_message_wrap');
  for (let i = 1; i < blocks.length; i++) {
    const b = blocks[i];
    const post = /data-post="([^"]+)"/.exec(b);
    const dt = /datetime="([^"]+)"/.exec(b);
    const txt = /tgme_widget_message_text[^>]*>([\s\S]*?)<\/div>/.exec(b);
    if (!post || !dt || !txt) continue;
    const text = stripHtml(txt[1]);
    if (!text || text.length < 15) continue;
    if (PROMO_SOURCES.has(username) && PROMO_RE.test(text)) continue;
    const msgId = post[1].split('/')[1];
    items.push({
      id: username + '-' + msgId,
      src: username,
      srcName,
      url: 'https://t.me/' + post[1],
      time: dt[1],
      text: text.slice(0, 900),
    });
  }
  return items.slice(-PER_SOURCE_LIMIT);
}

// ── Разбор RSS ──────────────────────────────────────────────────────────
function parseRss(xml, srcName) {
  const items = [];
  const blocks = xml.split(/<item[\s>]/i).slice(1);
  for (const b of blocks.slice(0, PER_SOURCE_LIMIT)) {
    const g = tag => {
      const m = new RegExp('<' + tag + '[^>]*>([\\s\\S]*?)</' + tag + '>', 'i').exec(b);
      return m ? stripHtml(m[1].replace(/<!\[CDATA\[|\]\]>/g, '')) : '';
    };
    const title = g('title'), link = g('link'), pub = g('pubDate');
    if (!title || !link) continue;
    const skip = RSS_SKIP[srcName];
    if (skip && skip.test(title)) continue;
    const desc = g('description');
    items.push({
      id: 'rss-' + Buffer.from(link).toString('base64url').slice(0, 24),
      src: 'rss-' + srcName, srcName,
      url: link,
      time: pub ? new Date(pub).toISOString() : new Date().toISOString(),
      text: title + (desc && desc !== title ? '\n' + desc.slice(0, 300) : ''),
    });
  }
  return items;
}

// ── Точные дубли: нормализованный отпечаток текста ──────────────────────
function fingerprint(text) {
  return text.toLowerCase().replace(/[^a-zа-яё0-9]+/g, ' ').trim().slice(0, 140);
}

// Значимые слова новости для сравнения по смыслу
const STOP = new Set(('и в на не что с по за от до для как это его ее её их тот эта они оно мы вы также при или если чем уже еще ещё году года год лет млн млрд трлн руб рублей процент процента процентов пункта пунктов около более менее может стал стала стало были было будет через после ранее сообщил сообщила сообщает сообщили заявил заявила заявили рассказал отметил считает данным словам итогам которая который которые может также свой свои этом этой согласно').split(' '));
// Синонимы биржевого языка: разные формулировки одной новости → одно слово
const SYN_RAW = {
  'вниз':      'сниз сокра упал упали умень опуст просе рухну обвал потер сниже паден минус ослаб подеш откат',
  'вверх':     'вырос повыс увели подро приба подня взлет скачо ускор рост роста подор укреп плюс',
  'ставка':    'ставк ключе',
  'решение':   'решил решен поста утвер приня одобр',
  'запрет':    'запре огран блоки заморо прио остан отозв',
  'прогноз':   'прогн ожида оценк ожидае предп консе',
  'инфляция':  'инфля ипц',
  'встреча':   'перег встре самми конта диало',
  'санкции':   'санкц рестр эмбар',
  'дивиденды': 'дивид выпла',
  'отчет':     'отчет отчит резул прибы выруч убыто мсфо рсбу',
  'сказал':    'заяви сказа отмет подче добав указа сообщ',
  'люди':      'росси насел гражд',
  'цбрф':      'цбрф центр регул набиу',
};
const SYN = new Map();
for (const [key, list] of Object.entries(SYN_RAW))
  for (const w of list.split(' ')) SYN.set(w, key);

function wordSet(text) {
  const w = text.toLowerCase().replace(/ё/g, 'е').replace(/[^a-zа-я0-9,.\s]+/g, ' ')
    .replace(/(\d+),(\d+)/g, '$1.$2')
    .replace(/[^a-zа-я0-9.\s]+/g, ' ')
    .split(/\s+/)
    .map(t => /^\d/.test(t) ? t.replace(/\.$/, '') : t.replace(/[.]+/g, ''))
    .map(t => /^\d/.test(t) ? t.replace(/\.0$/, '') : t)
    .filter(t => (/^\d/.test(t) ? t.length >= 2 : t.length >= 3) && !STOP.has(t))
    .map(t => {
      if (/^\d/.test(t)) return t;
      for (let L = Math.min(6, t.length); L >= 3; L--) {   // ищем синоним по началу слова
        const hit = SYN.get(t.slice(0, L));
        if (hit) return hit;
      }
      return t.slice(0, 5);                                // иначе грубая основа
    });
  return new Set(w);
}
function similar(a, b) {
  if (!a.size || !b.size) return false;
  let inter = 0;
  const [small, big] = a.size <= b.size ? [a, b] : [b, a];
  for (const t of small) if (big.has(t)) inter++;
  const contain = inter / small.size;                // маленький почти целиком внутри большого
  const jacc = inter / (a.size + b.size - inter);
  return jacc >= 0.45 || contain >= 0.7;
}

// ── Сборка ленты ────────────────────────────────────────────────────────
let cache = { at: 0, feed: null };
const health = {};

async function build() {
  const jobs = [
    ...TG.map(([u, n]) => fetchUrl('https://t.me/s/' + u)
      .then(h => { const it = parseTelegram(h, u, n); health[u] = { ok: true, n: it.length }; return it; })
      .catch(e => { health[u] = { ok: false, err: e.message }; return []; })),
    ...RSS.map(([url, n]) => fetchUrl(url)
      .then(x => { const it = parseRss(x, n); health[n] = { ok: true, n: it.length }; return it; })
      .catch(e => { health[n] = { ok: false, err: e.message }; return []; })),
  ];
  const all = (await Promise.all(jobs)).flat();
  const bad = Object.entries(health).filter(([,v]) => !v.ok).map(([k,v]) => k + '(' + v.err + ')');
  console.log('BUILD: items=' + all.length + ' okSources=' + Object.values(health).filter(v=>v.ok).length + '/' + Object.keys(health).length + (bad.length ? ' fail: ' + bad.join(', ') : ''));

  const cutoff = Date.now() - KEEP_HOURS * 3600 * 1000;
  const fresh = all.filter(x => new Date(x.time).getTime() > cutoff);
  fresh.sort((a, b) => new Date(b.time) - new Date(a.time));

  // дедупликация: точные дубли + похожие по смыслу в окне 3 часов
  const seen = new Map();
  const out = [];
  const WINDOW = 3 * 3600 * 1000;
  for (let i = fresh.length - 1; i >= 0; i--) {   // от старых к новым
    const x = fresh[i];
    const fp = fingerprint(x.text);
    let keep = seen.get(fp) || null;
    if (!keep) {
      const ws = wordSet(x.text);
      const t = new Date(x.time).getTime();
      for (let j = out.length - 1; j >= 0; j--) {
        const y = out[j];
        if (t - new Date(y.time).getTime() > WINDOW) break;
        if (y.src === x.src) continue;             // внутри одного канала не склеиваем
        if (similar(ws, y._ws)) { keep = y; break; }
      }
      if (!keep) {
        x._ws = ws;
        seen.set(fp, x);
        out.push(x);
        continue;
      }
    }
    keep.srcCount = (keep.srcCount || 1) + 1;
    if (!keep.alsoIn) keep.alsoIn = [];
    if (!keep.alsoIn.includes(x.srcName) && keep.srcName !== x.srcName) keep.alsoIn.push(x.srcName);
  }
  out.reverse();                                   // снова: новые сверху
  for (const x of out) delete x._ws;

  for (const x of out) {
    const c = classify(x.text);
    x.lvl = c.lvl; x.reasons = c.reasons;
    x.tk = tickers(x.text);
    x.tags = hashtags(x.text);
  }
  const dups = fresh.length - out.length;
  console.log('DEDUP: kept=' + out.length + ' merged=' + dups);
  return { updated: new Date().toISOString(), count: out.length, items: out };
}

async function getFeed() {
  const now = Date.now();
  if (cache.feed && now - cache.at < CACHE_SEC * 1000) return cache.feed;
  try {
    const feed = await build();
    if (feed.count > 0 || !cache.feed) cache = { at: now, feed };
  } catch (e) {
    if (!cache.feed) cache = { at: now, feed: { updated: null, count: 0, items: [], error: e.message } };
  }
  return cache.feed;
}

// ── Котировки Мосбиржи (открытый интерфейс ISS, кэш 60 сек) ────────────
let qCache = { at: 0, data: null };
async function getQuotes() {
  const now = Date.now();
  if (qCache.data && now - qCache.at < 60 * 1000) return qCache.data;
  try {
    const raw = await fetchUrl('https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,LAST,LASTTOPREVPRICE');
    const j = JSON.parse(raw);
    const out = {};
    for (const row of (j.marketdata && j.marketdata.data) || []) {
      const [secid, last, chg] = row;
      if (secid) out[secid] = { last, chg };
    }
    qCache = { at: now, data: { updated: new Date().toISOString(), quotes: out } };
  } catch (e) {
    if (!qCache.data) qCache = { at: now, data: { updated: null, quotes: {}, error: e.message } };
  }
  return qCache.data;
}

// ── HTTP-сервер ─────────────────────────────────────────────────────────
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png' };

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  try {
    if (u.pathname === '/api/feed') {
      const feed = await getFeed();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=60' });
      return res.end(JSON.stringify(feed));
    }
    if (u.pathname === '/api/quotes') {
      const q = await getQuotes();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=30' });
      return res.end(JSON.stringify(q));
    }
    if (u.pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ cacheAgeSec: cache.at ? Math.round((Date.now() - cache.at) / 1000) : null, sources: health }, null, 2));
    }
    // статика
    let p = u.pathname === '/' ? '/index.html' : u.pathname;
    if (p === '/fonts' || p === '/шрифты' || p === encodeURI('/шрифты')) p = '/fonts.html';
    if (p === '/map' || p === '/карта' || p === encodeURI('/карта')) p = '/map.html';
    p = path.normalize(p).replace(/^(\.\.[/\\])+/, '');
    const file = path.join(__dirname, 'public', p);
    if (file.startsWith(path.join(__dirname, 'public')) && fs.existsSync(file) && fs.statSync(file).isFile()) {
      res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
      return res.end(fs.readFileSync(file));
    }
    // любой другой адрес — на главную (для ссылок на новость /n/…)
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(fs.readFileSync(path.join(__dirname, 'public', 'index.html')));
  } catch (e) {
    res.writeHead(500); res.end('error');
  }
});

server.listen(PORT, () => console.log('MKT-TERMINAL on :' + PORT));
// прогреваем кэш при старте
getFeed().catch(() => {});
