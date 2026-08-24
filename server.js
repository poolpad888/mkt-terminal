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
const CACHE_SEC = 20;           // как часто обновлять ленту (фоновый опрос)
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
  ['PresidentDonaldTrumpRU', 'Трамп на русском'],
  ['antonchehovanalitk',  'Аналитика Чехова'],
  ['banksta',             'Банкста'],
  ['proeconomics',        'Proeconomics'],
  ['if_market_news',      'IF News'],
  ['smartlabnews',        'Смартлаб'],
  ['dohod',               'ДОХОДЪ'],
  ['probonds',            'PRObonds'],
  ['angrybonds',          'Angry Bonds'],
  ['c0ldness',            'Холодный расчёт'],
  ['unexpectedvalue',     'Unexpected Value'],
  ['tinkoff_invest_official', 'Т-Инвестиции'],
  ['economika',           'Экономика'],
  ['russianeconomism',    'Русский экономизм'],
  ['expert_ra',           'Эксперт РА'],
  ['fm_invest',           'Frank Инвестиции'],
  ['dprunews',            'Деловой Петербург'],
  ['businessgazeta',      'БИЗНЕС Online'],
  ['realnoevremya',       'Реальное время'],
  ['ksonline',            'Континент Сибирь'],
  ['expertsouth',         'Эксперт Юг'],
  ['dk_ru_news',          'Деловой квартал'],
  ['Full_Time_Trading',    'Full Time Trading'],
  ['xtxixty',              'Твёрдые цифры'],
  ['acirussia',            'ACI Russia'],
  ['if_bonds',             'IF Bonds'],
  ['russianjunkbonds',     'ВДО'],
  ['vtbmyinvestments',     'ВТБ Инвестиции'],
];
const RSS = [
  ['https://www.finam.ru/analysis/conews/rsspoint/', 'Финам'],
  ['https://www.moex.com/export/news.aspx?cat=100', 'Мосбиржа'],
  ['https://abireg.ru/rss/rss4.xml', 'Абирег'],
];
// Регулятор: опрашиваем отдельно и часто — решения по ставке выходят здесь
// раньше, чем в каналах. Помечаются как reg, показываются особой плашкой.
const FAST_RSS = [
  ['https://www.cbr.ru/rss/RssNews', 'ЦБ РФ'],
  ['https://www.cbr.ru/rss/RssPress', 'ЦБ РФ пресс-релизы'],
  ['https://www.cbr.ru/rss/eventrss', 'ЦБ РФ события'],
];
// Минфин: RSS на сайте отдаёт 503, берём официальный канал с открытой веб-версией
const FAST_TG = [
  ['minfin',  'Минфин России', 'fin'],
];
// Комментарии представителей ведомств помечаем той же плашкой
const CBR_NAMES = /(?<![а-яёa-z])(набиуллин|заботкин|тремасов|юдаева|чистюхин|полякова|скоробогатова|данилов|зубарев|цб\s*рф|центробанк|банк\s+росси|центральн[а-яё]+\s+банк\s+росси)/i;
const FIN_NAMES    = /(?<![а-яёa-z])(силуанов|моисеев|сазанов|чебесков|колычев|иванов\s+ирина|минфин)/i;
const FIN_EXCLUDE  = /минфин[а-яё]*\s+(украин|сша|белорус|китая|германи|франци|японии|великобритани|польши|прибалт|евро)|минфин[а-яё]*\s+на\s+украин/i;
const FRS_NAMES  = /(?<![а-яёa-z])(фрс(?![а-яё])|пауэлл|уоллер|джефферсон|куглер|(?<![a-z])fed(?![a-z])|(?<![a-z])fomc(?![a-z]))/i;
const FED_NAMES  = /(?<![а-яёa-z])(бессент|минфин[а-яё]*\s+сша|сша\s+минфин|us\s+treasury|treasury\s+department)/i;

function markByName(x) {
  if (x.reg) return x;
  const t = (x.text || '');
  if (CBR_NAMES.test(t)) { x.reg = true; x.mark = 'reg'; }
  else if (FRS_NAMES.test(t)) { x.reg = true; x.mark = 'frs'; }
  else if (FED_NAMES.test(t)) { x.reg = true; x.mark = 'fed'; }
  else if (FIN_NAMES.test(t) && !FIN_EXCLUDE.test(t)) { x.reg = true; x.mark = 'fin'; }
  return x;
}

const FAST_SEC = 3;                  // опрос регулятора раз в 3 секунды
let fastItems = [];                  // последнее, что удалось прочитать
let fastBusy = false;
async function fastPoll() {
  if (fastBusy) return;
  fastBusy = true;
  try {
    const got = await Promise.all([
      ...FAST_RSS.map(([url, n]) => fetchUrl(url)
        .then(x => { const it = parseRss(x, n); health[n] = { ok: true, n: it.length }; return it; })
        .catch(e => { health[n] = { ok: false, err: e.message }; return []; })),
      ...FAST_TG.map(([u, n, mark]) => fetchUrl('https://t.me/s/' + u)
        .then(h => { const it = parseTelegram(h, u, n); for (const x of it) x.mark = mark;
                     health[u] = { ok: true, n: it.length }; return it; })
        .catch(e => { health[u] = { ok: false, err: e.message }; return []; })),
    ]);
    const flat = got.flat();
    for (const x of flat) { if (!x.mark) x.mark = 'reg'; x.reg = true; }   // reg — ЦБ, fin — Минфин
    if (flat.length) fastItems = flat;
  } finally { fastBusy = false; }
}
// Реклама, платные подписки и набор в закрытые группы — не новости
const PROMO_SOURCES = new Set(['antonchehovanalitk','banksta','probonds','angrybonds','economika','smartlabnews']);
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
      text: (text.length > 250 ? text.slice(0, 250).replace(/\s+\S*$/, '') + '…' : text),
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

// ── База данных (история новостей) ─────────────────────────────────────
// Включается, только если задана переменная DATABASE_URL. Без неё сервер
// работает как раньше — просто ничего не сохраняет.
let pool = null;
if (process.env.DATABASE_URL) {
  const { Pool } = require('pg');
  pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 3 });
  pool.query(`
    CREATE TABLE IF NOT EXISTS news (
      id         text PRIMARY KEY,
      ts         timestamptz,
      src        text,
      src_name   text,
      url        text,
      body       text,
      lvl        int  DEFAULT 0,
      src_count  int  DEFAULT 1,
      first_seen timestamptz DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS news_ts_idx ON news (ts DESC);
  `).then(() => console.log('DB: таблица news готова'))
    .catch(e => { console.log('DB init error: ' + e.message); pool = null; });
}

async function saveToDb(items) {
  if (!pool || !items.length) return;
  const seen = new Set();                    // в пачке не должно быть двух строк с одним id
  items = items.filter(x => seen.has(x.id) ? false : (seen.add(x.id), true));
  try {
    const cols = ['id','ts','src','src_name','url','body','lvl','src_count'];
    let saved = 0;
    for (let i = 0; i < items.length; i += 100) {
      const chunk = items.slice(i, i + 100);
      const vals = [], ph = [];
      chunk.forEach((x, j) => {
        const b = j * cols.length;
        ph.push('(' + cols.map((_, k) => '$' + (b + k + 1)).join(',') + ')');
        vals.push(x.id, x.time || null, x.src || null, x.srcName || null,
                  x.url || null, x.text || '', x.lvl || 0, x.srcCount || 1);
      });
      const r = await pool.query(
        'INSERT INTO news (' + cols.join(',') + ') VALUES ' + ph.join(',') +
        ' ON CONFLICT (id) DO UPDATE SET src_count = GREATEST(news.src_count, EXCLUDED.src_count), lvl = GREATEST(news.lvl, EXCLUDED.lvl)',
        vals);
      saved += r.rowCount;
    }
    const regN = items.filter(x => x.reg).length;
    console.log('DB: записано строк ' + saved + ' (из ' + items.length + ' в сборке, из них регулятор ' + regN + ')');
  } catch (e) {
    console.log('DB save error: ' + e.message);
  }
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
  const all = (await Promise.all(jobs)).flat().concat(fastItems);
  for (const x of all) markByName(x);
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
    if (x.reg) { keep.reg = true; keep.mark = keep.mark || x.mark; }   // регулятор важнее пересказа
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

let building = false;                       // защита от наложения сборок
async function refresh() {
  if (building) return;
  building = true;
  try {
    const feed = await build();
    if (feed.count > 0 || !cache.feed) cache = { at: Date.now(), feed };
    if (feed.count > 0) saveToDb(feed.items);   // в базу — не дожидаясь, фоном
  } catch (e) {
    if (!cache.feed) cache = { at: Date.now(), feed: { updated: null, count: 0, items: [], error: e.message } };
  } finally { building = false; }
}
setInterval(refresh, CACHE_SEC * 1000);     // фоновый опрос: лента свежая, пока сервер не спит
fastPoll();
setInterval(fastPoll, FAST_SEC * 1000);     // регулятор — отдельно и часто

// Самоокрик: раз в 10 минут заходим на собственный публичный адрес,
// чтобы бесплатный тариф Render не усыплял сервер и новости копились круглосуточно.
const SELF_URL = process.env.SELF_URL || 'https://mkt-terminal.onrender.com/api/health';
setInterval(() => {
  fetchUrl(SELF_URL).then(() => console.log('WAKE: ok'))
                    .catch(e => console.log('WAKE: ' + e.message));
}, 10 * 60 * 1000);

async function getFeed() {
  if (!cache.feed) await refresh();         // первый заход после пробуждения — собираем сразу
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

// ── Сводная панель котировок: свои данные из четырёх источников ─────────
// Yahoo (мировые площадки), Мосбиржа (акции и индекс), ЦБ (курсы), CoinGecko (крипта).
// Кэш 90 сек; каждый источник падает независимо — панель показывает то, что пришло.
let pCache = { at: 0, data: null, busy: null };

// ── счётчик уникальных посетителей ──
const visitors = { today: new Set(), total: 0, date: new Date().toDateString() };
function trackVisit(ip) {
  const today = new Date().toDateString();
  if (visitors.date !== today) { visitors.today.clear(); visitors.date = today; }
  if (!visitors.today.has(ip)) { visitors.today.add(ip); visitors.total++; }
}

function pFmt(v) {
  if (v == null || !isFinite(v)) return '—';
  const a = Math.abs(v);
  const d = a >= 10000 ? 0 : a >= 100 ? 1 : a >= 1 ? 2 : 4;
  return v.toLocaleString('ru-RU', { minimumFractionDigits: d, maximumFractionDigits: d });
}
const R = (n, p, c) => ({ n, p: pFmt(p), c: (c == null || !isFinite(c)) ? null : Math.round(c * 100) / 100 });

async function buildPanel() {
  const G = {
    'Сырьё':   new Array(6).fill(null),
    'Индексы': new Array(5).fill(null),
    'Валюты':  new Array(4).fill(null),
    'Крипта':  new Array(4).fill(null),
    'Акции':   new Array(8).fill(null),
  };
  const jobs = [];

  // мировые рынки — фьючерсы срочной секции Мосбиржи (Brent, газ, металлы,
  // S&P 500, Nasdaq, EUR/USD): по каждому базовому активу берём самый
  // торгуемый контракт
  const WANT = {
    BR:   ['Нефть Brent',    'Сырьё',   0],
    NG:   ['Газ (США)',      'Сырьё',   1],
    GOLD: ['Золото',         'Сырьё',   2],
    SILV: ['Серебро',        'Сырьё',   3],
    PLT:  ['Платина',        'Сырьё',   4],
    PLD:  ['Палладий',       'Сырьё',   5],
    SPYF: ['S&P 500 · ф.',   'Индексы', 0],
    NASD: ['Nasdaq · ф.',    'Индексы', 1],
    ED:   ['EUR/USD · ф.',   'Валюты',  3],
  };
  jobs.push(fetchUrl('https://iss.moex.com/iss/engines/futures/markets/forts/securities.json?iss.meta=off&iss.only=securities,marketdata&securities.columns=SECID,ASSETCODE&marketdata.columns=SECID,LAST,LASTTOPREVPRICE,VALTODAY')
    .then(raw => {
      const j = JSON.parse(raw);
      const sc = Object.fromEntries(j.securities.columns.map((c, i) => [c, i]));
      const mc = Object.fromEntries(j.marketdata.columns.map((c, i) => [c, i]));
      const mdBy = {};
      for (const r of j.marketdata.data) mdBy[r[mc.SECID]] = r;
      const best = {};
      for (const r of j.securities.data) {
        const code = String(r[sc.ASSETCODE] || '').toUpperCase();
        if (!WANT[code]) continue;
        const m = mdBy[r[sc.SECID]];
        if (!m || m[mc.LAST] == null || !m[mc.LAST]) continue;
        const vol = m[mc.VALTODAY] || 0;
        if (!best[code] || vol > best[code].vol)
          best[code] = { vol, last: m[mc.LAST], chg: m[mc.LASTTOPREVPRICE] };
      }
      for (const code in best) {
        const [name, g, i] = WANT[code];
        G[g][i] = R(name, best[code].last, best[code].chg);
      }
    }));

  // индексы Мосбиржи — по всем площадкам секции, чтобы поймать и РТС
  jobs.push(fetchUrl('https://iss.moex.com/iss/engines/stock/markets/index/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,CURRENTVALUE,LASTCHANGEPRC')
    .then(raw => {
      const j = JSON.parse(raw);
      for (const [secid, val, chg] of (j.marketdata && j.marketdata.data) || []) {
        if (val == null) continue;
        if (secid === 'IMOEX' && !G['Индексы'][2]) G['Индексы'][2] = R('МосБиржа', val, chg);
        if (secid === 'RTSI'  && !G['Индексы'][3]) G['Индексы'][3] = R('РТС', val, chg);
        if (secid === 'RGBI'  && !G['Индексы'][4]) G['Индексы'][4] = R('ОФЗ · RGBI', val, chg);
      }
    }));

  // официальные курсы ЦБ (зеркало в JSON, есть вчерашнее значение)
  jobs.push(fetchUrl('https://www.cbr-xml-daily.ru/daily_json.js').then(raw => {
    const v = JSON.parse(raw).Valute || {};
    const cur = (code, name, i) => {
      const x = v[code];
      if (x && x.Value != null) {
        const chg = x.Previous ? (x.Value - x.Previous) / x.Previous * 100 : null;
        G['Валюты'][i] = R(name, x.Value / (x.Nominal || 1), chg);
      }
    };
    cur('USD', 'USD/RUB · ЦБ', 0); cur('EUR', 'EUR/RUB · ЦБ', 1); cur('CNY', 'CNY/RUB · ЦБ', 2);
  }));

  // крипта — CoinGecko
  jobs.push(fetchUrl('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,ripple&vs_currencies=usd&include_24hr_change=true')
    .then(raw => {
      const j = JSON.parse(raw);
      const coin = (id, name, i) => {
        const x = j[id];
        if (x && x.usd != null) G['Крипта'][i] = R(name, x.usd, x.usd_24h_change);
      };
      coin('bitcoin', 'Bitcoin', 0); coin('ethereum', 'Ethereum', 1);
      coin('solana', 'Solana', 2); coin('ripple', 'XRP', 3);
    }));

  // российские акции — уже собранный /api/quotes
  jobs.push(getQuotes().then(q => {
    const st = (sec, name, i) => {
      const x = q.quotes && q.quotes[sec];
      if (x && x.last != null) G['Акции'][i] = R(name, x.last, x.chg);
    };
    st('SBER', 'Сбер', 0); st('GAZP', 'Газпром', 1); st('LKOH', 'Лукойл', 2); st('ROSN', 'Роснефть', 3);
    st('GMKN', 'Норникель', 4); st('VTBR', 'ВТБ', 5); st('TATN', 'Татнефть', 6); st('YDEX', 'Яндекс', 7);
  }));

  await Promise.allSettled(jobs);
  const groups = Object.entries(G)
    .map(([name, rows]) => ({ name, rows: rows.filter(Boolean) }))
    .filter(g => g.rows.length);
  return { updated: new Date().toISOString(), groups };
}

async function getPanel() {
  const now = Date.now();
  if (pCache.data && now - pCache.at < 90 * 1000) return pCache.data;
  if (!pCache.busy) pCache.busy = buildPanel()
    .then(d => { pCache = { at: Date.now(), data: d, busy: null }; return d; })
    .catch(e => { pCache.busy = null; return pCache.data || { updated: null, groups: [], error: e.message }; });
  return pCache.busy;
}

// ── HTTP-сервер ─────────────────────────────────────────────────────────
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png' };

const server = process.on('unhandledRejection', r => console.error('[unhandledRejection]', r));
process.on('uncaughtException',  e => console.error('[uncaughtException]', e));

function addSec(res) {
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
}

http.createServer(async (req, res) => {
  addSec(res);
  const clientIp = (req.headers['x-forwarded-for']||'').split(',')[0].trim() || req.socket.remoteAddress || 'unknown';
  try {
    const u = new URL(req.url, 'http://x');
    if (u.pathname === '/') trackVisit(clientIp);
    if (u.pathname === '/api/feed') {
      const feed = await getFeed();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(JSON.stringify(feed));
    }
    if (u.pathname === '/api/history') {
      // Поиск по архиву в базе: ?q=слово. Отдаём до 200 совпадений, новые сверху.
      const q = (u.searchParams.get('q') || '').trim().slice(0, 80);
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      if (!pool)  return res.end(JSON.stringify({ items: [], error: 'архив не подключён' }));
      if (q.length < 2) return res.end(JSON.stringify({ items: [] }));
      try {
        const r = await pool.query(
          `SELECT id, ts, src, src_name, url, body, lvl, src_count
             FROM news
            WHERE body ILIKE '%' || $1 || '%'
            ORDER BY ts DESC NULLS LAST
            LIMIT 200`, [q]);
        const items = r.rows.map(w => {
          const x = { id: w.id, time: w.ts, src: w.src, srcName: w.src_name,
                      url: w.url, text: w.body, lvl: w.lvl || 0,
                      srcCount: w.src_count || 1 };
          x.tk = tickers(x.text); x.tags = hashtags(x.text);
          return x;
        });
        return res.end(JSON.stringify({ items }));
      } catch (e) {
        return res.end(JSON.stringify({ items: [], error: e.message }));
      }
    }
    if (u.pathname === '/api/panel') {
      const q = await getPanel();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=45' });
      return res.end(JSON.stringify(q));
    }
    if (u.pathname === '/api/quotes') {
      const q = await getQuotes();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=30' });
      return res.end(JSON.stringify(q));
    }
    if (u.pathname === '/api/stats') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(JSON.stringify({ today: visitors.today.size, total: visitors.total, date: visitors.date }));
    }
    if (u.pathname === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify({ cacheAgeSec: cache.at ? Math.round((Date.now() - cache.at) / 1000) : null, sources: health }, null, 2));
    }
    // статика
    let p = u.pathname === '/' ? '/index.html' : u.pathname;
    if (p === '/fonts' || p === '/шрифты' || p === encodeURI('/шрифты')) p = '/fonts.html';
    if (p === '/map' || p === '/карта' || p === encodeURI('/карта')) p = '/map.html';
    if (p === '/cbr' || p === '/цб' || p === encodeURI('/цб')) p = '/cbr.html';
    if (p === '/quotes' || p === '/котировки' || p === encodeURI('/котировки')) p = '/quotes.html';
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
