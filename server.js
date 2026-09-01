// MKT-TERMINAL — сервер сбора новостей
// Node.js, без внешних зависимостей.
// Логика: раз в CACHE_SEC секунд (по запросу) собирает посты из открытых
// веб-версий Telegram-каналов (t.me/s/...) и RSS Финама, склеивает,
// убирает точные дубли, классифицирует важность, отдаёт JSON.

const http = require('http');
const https = require('https');
const { execFile } = require('child_process');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');

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
  ['DK_RU_news',          'Деловой квартал'],
  ['TheEconomisto',       'The Economist'],
  ['dmitrypolevoy',       'Полевой'],
  ['xtxixty',              'Твёрдые цифры'],
  ['acirussia',            'ACI Russia'],
  ['if_bonds',             'IF Bonds'],
  ['russianjunkbonds',     'ВДО'],
  ['vtbmyinvestments',     'ВТБ Инвестиции'],
];
const RSS = [
  ['https://rosstat.gov.ru/rss/news.rss', 'Росстат'],
  // Минфин: старый адрес /rss отдавал 503, этот работает. Третий элемент —
  // плашка: иначе новость с сайта ведомства осталась бы без пометки.
  ['https://minfin.gov.ru/rss_news?mod=news&lim=50', 'Минфин России', 'fin'],
  ['https://minfin.gov.ru/rss_news?mod=lib&lim=50', 'Минфин документы', 'fin'],
  ['https://www.finam.ru/analysis/conews/rsspoint/', 'Финам'],
  ['https://www.moex.com/export/news.aspx?cat=100', 'Мосбиржа'],
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

// ── Рутина, которую не подсвечиваем ─────────────────────────────────────
// Формально попадает под «важные» темы, по сути — техническая публикация.
// Уровень важности снимается всегда; плашка ведомства — если третьим полем true.
const MUTE = [
  ['вакансии и карьера',
   /карьер[а-яё]*\s+в\s+(банке\s+росси|цб)|(?<![а-яё])ваканси|стажировк|(?<![а-яё])карьерн[а-яё]+|конкурс\s+на\s+замещение|приглашаем\s+на\s+работу/i, true],
  ['ставка валютного свопа',
   /валютн[а-яё]+\s+своп|своп[а-яё]*\s+в\s+юан|юанев[а-яё]+\s+своп|своп[а-яё]*\s+с\s+юан/i, true],
  ['решения по участникам рынка',
   /решени[а-яё]*\s+банка\s+росси[а-яё]*\s+в\s+отношении\s+участник|в\s+отношении\s+участников\s+финансов[а-яё]+\s+рынка/i, false],
  ['инсайдерская информация',
   /инсайдерск[а-яё]+\s+информаци/i, true],
  ['ставки MIACR',
   /(?<![a-z])miacr(?![a-z])|(?<![а-яё])миакр(?![а-яё])/i, true],
];
// ── Тематический отбор ─────────────────────────────────────────────────
// Региональные и общие каналы вперемешку с финансами дают городскую хронику:
// уборка снега, ДТП, спорт, культура. Отсекаем такое, но только когда в тексте
// нет ни одного денежного или политического признака. Правило намеренно
// осторожное: при малейшем сомнении новость остаётся в ленте.
const OFF_TOPIC = [
  ['быт и город',    /уборк[а-яё]* снега|снегоуборочн|благоустройств|субботник|ямочн[а-яё]* ремонт|гололёд|отключени[а-яё]* (?:воды|горячей|отоплени)|расчистк|коммунальн[а-яё]* служб/i],
  ['происшествия',   /ДТП|столкнули?с[а-яё]* (?:авто|машин)|наезд на пешеход|пожар в (?:доме|квартире|школе|ТЦ)|утонул|пропал без вести|поножовщин|изнасилован|зарезал|застрелил|задержан[а-яё]* за (?:кражу|грабёж|разбой)/i],
  ['спорт',          /матч[ае]?[ ,.:;]|чемпионат|сборная России по|футбол|хоккей|биатлон|олимпиад|турнир[ае]? по|забил гол|тренер команды/i],
  ['культура и шоу', /премьера (?:фильма|сериала)|сериал[ае]?[ ,.:;]|кинопрокат|концерт[ае]?[ ,.:;]|актёр|актрис|певиц|рэпер|блогер|фестивал/i],
  ['быт и советы',   /гороскоп|знак[а-яё]* зодиака|рецепт[ыа]?[ ,.:;]|похуде|диет[аыуой]|витамин|врач[а-яё]* (?:рассказал|назвал|предупредил)/i],
];
// Денежные, деловые и политические признаки — «якоря темы».
const ON_TOPIC = /рубл|доллар|евро|юан|процент|ставк|инфляц|бюджет|налог|акци[ийюя]|облигац|бирж|Банк России|ЦБ|ВВП|экспорт|импорт|санкц|выручк|прибыл|убыт|инвест|кредит|ипотек|тариф|подорожа|подешеве|рынок|курс|IPO|дивиденд|капитализац|котиров|нефт|газа|президент|министр|правительств|Госдума|Совфед|закон|указ|переговор|саммит|пошлин|эмбарго|дефицит|профицит|[$₽€]|\d+[.,]?\d*\s?(?:%|млрд|млн|трлн|тыс)/i;

function offTopic(text) {
  if (ON_TOPIC.test(text)) return null;          // есть якорь темы — не трогаем
  for (const [name, rx] of OFF_TOPIC) if (rx.test(text)) return name;
  return null;
}

function muted(text) {
  for (const [name, rx, drop] of MUTE) if (rx.test(text)) return { name, drop };
  return null;
}

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
// Сколько знаков поста показываем в ленте. Режем по концу предложения,
// а не по счётчику символов, чтобы текст не обрывался на полуслове.
const TEXT_LIMIT = 1000;
function cutText(t, max) {
  t = (t || '').trim();
  if (t.length <= max) return t;
  const head = t.slice(0, max);
  const m = head.match(/^[\s\S]*[.!?…](?=["\u00bb)\s]|$)/);   // до последнего целого предложения
  if (m && m[0].trim().length >= max * 0.45) return m[0].trim();
  return head.replace(/\s+\S*$/, '').trim() + '…';
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
      text: cutText(text, TEXT_LIMIT),
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
      text: title + (desc && desc !== title ? '\n' + cutText(desc, TEXT_LIMIT) : ''),
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
  return jacc >= 0.38 || contain >= 0.62;   // порог снижен: ловим более вольные пересказы
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

// ── Записи от сборщика Телеграма ────────────────────────────────────────
// Отдельная служба finfacts-tg читает каналы через личный аккаунт и кладёт
// посты в ту же таблицу news. Здесь мы их оттуда забираем и подмешиваем в
// ленту: только так до сайта доходят закрытые каналы, куда веб-парсер не
// попадает. Первый заход тянет всё окно, дальше — только новое, с запасом
// в 10 минут назад (пост мог прийти позже своего времени публикации).
let dbBuf = [];
let dbSince = null;

async function dbPull() {
  if (!pool) return;
  const winStart = Date.now() - KEEP_HOURS * 3600 * 1000;
  const from = dbSince
    ? new Date(Math.max(winStart, dbSince - 10 * 60 * 1000))
    : new Date(winStart);
  try {
    const r = await pool.query(
      'SELECT id, ts, src, src_name, url, body FROM news WHERE ts > $1 ORDER BY ts DESC LIMIT 4000',
      [from.toISOString()]);
    for (const w of r.rows) {
      if (!w.body || !w.src) continue;
      const t = new Date(w.ts).getTime();
      if (!dbSince || t > dbSince) dbSince = t;
      dbBuf.push({ id: w.id, time: new Date(w.ts).toISOString(), src: w.src,
                   srcName: w.src_name || w.src, url: w.url, text: w.body });
    }
    // Буфер держим полным окном: сборка идёт раз в 20 секунд, подклейка — раз
    // в секунду, и обе читают отсюда. Если бы буфер вычищался по факту чтения,
    // сборка не увидела бы то, что подклейка уже забрала, и затирала бы её.
    const m = new Map();
    for (const x of dbBuf) if (new Date(x.time).getTime() > winStart) m.set(x.id, x);
    dbBuf = [...m.values()].sort((a, b) => new Date(b.time) - new Date(a.time));
  } catch (e) { console.log('DB read: ' + e.message); }
}

async function build() {
  await dbPull();
  const jobs = [
    ...TG.map(([u, n]) => fetchUrl('https://t.me/s/' + u)
      .then(h => { const it = parseTelegram(h, u, n); health[u] = { ok: true, n: it.length }; return it; })
      .catch(e => { health[u] = { ok: false, err: e.message }; return []; })),
    ...RSS.map(([url, n, mark]) => fetchUrl(url)
      .then(x => { const it = parseRss(x, n);
                   if (mark) for (const y of it) { y.mark = mark; y.reg = true; }
                   health[n] = { ok: true, n: it.length }; return it; })
      .catch(e => { health[n] = { ok: false, err: e.message }; return []; })),
  ];
  // Один и тот же пост приходит и от парсера, и от сборщика — номер поста
  // у них общий, поэтому склеиваем по нему заранее. Иначе дальше новость
  // сочли бы двумя разными и приписали ей два источника.
  const raw = (await Promise.all(jobs)).flat().concat(fastItems, dbBuf);
  const byId = new Map();
  for (const x of raw) {
    const p = byId.get(x.id);
    if (!p) { byId.set(x.id, x); continue; }
    if (x.reg && !p.reg) { p.reg = true; p.mark = p.mark || x.mark; }
    if (x.text && x.text.length > (p.text || '').length) p.text = x.text;
  }
  const all = [...byId.values()];
  for (const x of all) markByName(x);
  const bad = Object.entries(health).filter(([,v]) => !v.ok).map(([k,v]) => k + '(' + v.err + ')');
  console.log('BUILD: items=' + all.length + ' okSources=' + Object.values(health).filter(v=>v.ok).length + '/' + Object.keys(health).length + (bad.length ? ' fail: ' + bad.join(', ') : ''));

  const cutoff = Date.now() - KEEP_HOURS * 3600 * 1000;
  let offN = 0;
  const offBy = {};
  const fresh = all.filter(x => {
    if (new Date(x.time).getTime() <= cutoff) return false;
    if (x.reg) return true;                      // сообщения ведомств не фильтруем
    const off = offTopic(x.text);
    if (off) { offN++; offBy[off] = (offBy[off] || 0) + 1; return false; }
    return true;
  });
  fresh.sort((a, b) => new Date(b.time) - new Date(a.time));

  // дедупликация: точные дубли + похожие по смыслу в окне 3 часов
  const seen = new Map();
  const out = [];
  const WINDOW = 6 * 3600 * 1000;          // было 3 часа — растянули, пересказы приходят с задержкой
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
    const mu = muted(x.text);
    if (mu) {                                    // техническая публикация — не выделяем
      x.lvl = 0; x.reasons = [];
      if (mu.drop) { x.reg = false; delete x.mark; }
    }
    x.tk = tickers(x.text);
    x.tags = hashtags(x.text);
  }
  const dups = fresh.length - out.length;
  console.log('DEDUP: kept=' + out.length + ' merged=' + dups);
  if (offN) console.log('OFFTOPIC: убрано ' + offN + ' — ' +
    Object.entries(offBy).map(([k, v]) => k + ' ' + v).join(', '));
  return { updated: new Date().toISOString(), count: out.length, items: out };
}

let building = false;                       // защита от наложения сборок
async function refresh() {
  if (building) return;
  building = true;
  try {
    const had = cache.feed ? new Set(cache.feed.items.map(x => x.id)) : null;
    const feed = await build();
    if (had && cache.feed && feed.count > 0) {
      const now = new Set(feed.items.map(x => x.id));
      // Только свежие: старьё и настоящие пересказы сборка убирает по делу,
      // мигают же именно недавние посты, которых парсер ещё не увидел.
      const edge = Date.now() - 10 * 60 * 1000;
      const keepBack = cache.feed.items.filter(x =>
        !now.has(x.id) && new Date(x.time).getTime() > edge);
      if (keepBack.length) {
        feed.items = keepBack.concat(feed.items).sort((a, b) => new Date(b.time) - new Date(a.time));
        feed.count = feed.items.length;
        console.log('KEEP: вернул ' + keepBack.length);
      }
    }
    if (feed.count > 0 || !cache.feed) cache = { at: Date.now(), feed };
    if (had && feed.count > 0) {
      let fresh = 0;
      for (const x of feed.items) if (!had.has(x.id)) fresh++;
      if (fresh) ssePush(fresh);
    }
    if (feed.count > 0) saveToDb(feed.items);   // в базу — не дожидаясь, фоном
  } catch (e) {
    if (!cache.feed) cache = { at: Date.now(), feed: { updated: null, count: 0, items: [], error: e.message } };
  } finally { building = false; }
}
setInterval(refresh, CACHE_SEC * 1000);     // фоновый опрос: лента свежая, пока сервер не спит

// Быстрая подклейка: раз в секунду читаем только базу (сборщик пишет туда сразу)
// и вставляем новые записи в готовую ленту. Сеть не трогаем — обход источников
// остаётся редким, здесь лишь один запрос к локальной базе.
// ── открытые линии к браузерам: сервер сам сообщает о новых постах ──
// Держим список подключений и на каждую новость пишем в них короткий сигнал.
// Браузер, получив его, сразу дёргает ленту — ждать опроса не нужно.
const sseClients = new Set();
const sseIp = new Map();   // соединение → адрес: пока канал открыт, вкладка открыта
function ssePush(n) {
  const msg = 'data: ' + JSON.stringify({ n, at: Date.now() }) + '\n\n';
  for (const c of sseClients) { try { c.write(msg); } catch (e) { sseClients.delete(c); } }
}
setInterval(() => {                      // раз в 25 с — двоеточие-пустышка, чтобы линия не закрылась
  for (const c of sseClients) {
    try { c.write(': ping\n\n'); } catch (e) { sseClients.delete(c); sseIp.delete(c); continue; }
    // вкладка открыта — значит человек на сайте, даже если она в фоне и опрос
    // ленты браузером приторможен. Заодно засчитываем заход в новые сутки,
    // если страницу не закрывали через полночь.
    const ip = sseIp.get(c);
    if (ip) trackVisit(ip, null, null, true);
  }
}, 25000);

let quickBusy = false;
async function quickPull() {
  if (quickBusy || building || !cache.feed) return;
  quickBusy = true;
  try {
    await dbPull();
    const have = new Set(cache.feed.items.map(x => x.id));
    // Отпечатки уже показанного: сборка склеивает пересказы одной новости,
    // и без этой проверки мы возвращали бы в ленту то, что она законно убрала.
    const haveFp = new Set(cache.feed.items.map(x => fingerprint(x.text)));
    const newest = cache.feed.items.length ? new Date(cache.feed.items[0].time).getTime() : 0;
    const add = [];
    for (const r of dbBuf) {
      if (have.has(r.id)) continue;
      if (new Date(r.time).getTime() <= newest - 60 * 1000) continue;   // не тянем старое
      if (haveFp.has(fingerprint(r.text))) continue;
      const c = classify(r.text);
      const mu = muted(r.text);
      const x = Object.assign({}, r, { lvl: c.lvl, reasons: c.reasons, tk: tickers(r.text), tags: hashtags(r.text) });
      if (mu) { x.lvl = 0; x.reasons = []; if (mu.drop) { x.reg = false; delete x.mark; } }
      add.push(x);
    }
    if (!add.length) return;
    const items = add.concat(cache.feed.items).sort((a, b) => new Date(b.time) - new Date(a.time));
    cache = { at: Date.now(), feed: { updated: new Date().toISOString(), count: items.length, items } };
    console.log('QUICK: +' + add.length);
    ssePush(add.length);
  } catch (e) {
  } finally { quickBusy = false; }
}
setInterval(quickPull, 1000);
fastPoll();
setInterval(fastPoll, FAST_SEC * 1000);     // регулятор — отдельно и часто

// Самоокрик нужен был только бесплатному тарифу Render, который усыплял сервер.
// На своём VPS служба работает постоянно, поэтому окрик выключен: включается
// обратно, только если явно задать SELF_URL.
if (process.env.SELF_URL) {
  setInterval(() => {
    fetchUrl(process.env.SELF_URL).then(() => console.log('WAKE: ok'))
                                  .catch(e => console.log('WAKE: ' + e.message));
  }, 10 * 60 * 1000);
}

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

// ── статистика посещений ────────────────────────────────────────────────
// Онлайн в моменте, уникальные за день / неделю / месяц, время на сайте,
// источники переходов, устройства и популярные страницы.
// Всё лежит в stats.json рядом с server.js — переживает рестарт службы.
// Сами IP не храним: только короткий хэш, обратно адрес из него не получить.
const STATS_FILE  = path.join(__dirname, 'stats.json');
const ONLINE_MS   = 120000;   // «в сети» — активность за последние 2 минуты
const SESSION_GAP = 90000;    // пауза дольше 90 секунд — считаем новым визитом
const DAYS_KEEP   = 400;      // сколько дней истории держим
const IPS_KEEP    = 35;       // за сколько дней держим хэши (нужно для окон 7 и 30 дней)

const dayKey = (back = 0) => new Date(Date.now() - back * 86400000)
  .toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' });          // YYYY-MM-DD
const ipHash = ip => crypto.createHash('sha1').update('finfacts:' + ip).digest('hex').slice(0, 12);

// какие адреса считаем страницами (а не фоновыми запросами)
const PAGE_NAMES = { '/': 'лента', '/map': 'карта', '/quotes': 'котировки',
                     '/cbr': 'ЦБ', '/fonts': 'шрифты', '/stats': 'статистика' };
const PAGE_ALIAS = { '/карта': '/map', '/котировки': '/quotes', '/цб': '/cbr',
                     '/шрифты': '/fonts', '/статистика': '/stats' };
function pageOf(pathname) {
  let x = pathname;
  try { x = decodeURIComponent(x); } catch (e) {}
  if (PAGE_ALIAS[x]) x = PAGE_ALIAS[x];
  return PAGE_NAMES[x] ? x : null;
}

let S = { date: dayKey(), views: 0, todayIps: [], allIps: [], days: {} };
try { S = Object.assign(S, JSON.parse(fs.readFileSync(STATS_FILE, 'utf8'))); } catch (e) {}
let todaySet = new Set(S.todayIps || []);
const allSet = new Set(S.allIps || []);
if (S.date !== dayKey()) { S.date = dayKey(); todaySet = new Set(); S.views = 0; }
let ipSrc = 'н/д';            // откуда берётся адрес посетителя, для диагностики
const online = new Map();     // хэш ip → время последней активности
let statsDirty = false;

// запись дня; старый формат {u,v} дополняется новыми полями на месте
function dayRec(d) {
  let r = S.days[d];
  if (!r) r = S.days[d] = {};
  if (typeof r.u   !== 'number') r.u = 0;      // уникальных
  if (typeof r.v   !== 'number') r.v = 0;      // открытий страниц
  if (typeof r.sec !== 'number') r.sec = 0;    // суммарное время на сайте, секунд
  if (typeof r.ses !== 'number') r.ses = 0;    // визитов (сессий)
  if (!r.ips) r.ips = [];                      // хэши посетителей этого дня
  if (!r.ref) r.ref = {};                      // откуда пришли
  if (!r.dev) r.dev = {};                      // m мобильный, d десктоп, t планшет, b бот
  if (!r.pg)  r.pg  = {};                      // какие страницы открывали
  return r;
}

function refHost(req) {
  const r = req && req.headers && req.headers.referer;
  if (!r) return 'прямые заходы';
  try {
    const h = new URL(r).hostname.replace(/^www\./, '');
    if (/(^|\.)finfacts\.ru$/i.test(h)) return 'прямые заходы';
    return h.slice(0, 60);
  } catch (e) { return 'прямые заходы'; }
}
function devKind(req) {
  const ua = (req && req.headers && req.headers['user-agent']) || '';
  if (/bot|crawler|spider|curl|wget|python-requests|headless|yandex|google/i.test(ua)) return 'b';
  if (/iPad|Tablet|PlayBook|Silk/i.test(ua)) return 't';
  if (/Mobi|Android|iPhone|iPod|Windows Phone/i.test(ua)) return 'm';
  return 'd';
}

// page — адрес открытой страницы, либо null для фонового запроса ленты
function trackVisit(ip, page, req, countUnique) {
  const d = dayKey();
  if (S.date !== d) { S.date = d; todaySet = new Set(); S.views = 0; }
  const h = ipHash(ip);
  const now = Date.now();
  const r = dayRec(d);

  // время на сайте складываем из промежутков между сигналами живой вкладки
  const prev = online.get(h);
  if (prev && now - prev <= SESSION_GAP) r.sec += Math.round((now - prev) / 1000);
  else r.ses++;
  online.set(h, now);

  if (countUnique && !page) { todaySet.add(h); allSet.add(h); }

  if (page) {
    S.views++;
    todaySet.add(h);
    allSet.add(h);
    const nm = PAGE_NAMES[page] || page.slice(0, 40);
    r.pg[nm]  = (r.pg[nm]  || 0) + 1;
    const src = refHost(req);
    r.ref[src] = (r.ref[src] || 0) + 1;
    const dv = devKind(req);
    r.dev[dv] = (r.dev[dv] || 0) + 1;
  }
  r.u = todaySet.size;
  r.v = S.views;
  statsDirty = true;
}

function onlineNow() {
  const now = Date.now();
  for (const [h, t] of online) if (now - t > SESSION_GAP * 4) online.delete(h);
  const cut = now - ONLINE_MS;
  const set = new Set();
  for (const [h, t] of online) if (t >= cut) set.add(h);
  // главный признак: открытый канал живой линии. Он держится всё время, пока
  // вкладка не закрыта, поэтому долго открытая страница из счётчика не выпадает.
  for (const ip of sseIp.values()) set.add(ipHash(ip));
  return set.size;
}

// уникальные за скользящее окно в N дней
function windowUnique(n) {
  const set = new Set(todaySet);
  for (let i = 0; i < n; i++) {
    const r = S.days[dayKey(i)];
    if (r && r.ips) for (const h of r.ips) set.add(h);
  }
  return set.size;
}
// сумма словарей (ref / dev / pg) за N дней, отсортированная по убыванию
function aggr(field, n) {
  const acc = {};
  for (let i = 0; i < n; i++) {
    const r = S.days[dayKey(i)];
    if (!r || !r[field]) continue;
    for (const k in r[field]) acc[k] = (acc[k] || 0) + r[field][k];
  }
  return Object.entries(acc).sort((a, b) => b[1] - a[1]);
}
function spanTime(n) {
  let sec = 0, ses = 0;
  for (let i = 0; i < n; i++) {
    const r = S.days[dayKey(i)];
    if (r) { sec += r.sec || 0; ses += r.ses || 0; }
  }
  return { sec, ses, avg: ses ? Math.round(sec / ses) : 0 };
}

function saveStats() {
  if (!statsDirty) return;
  statsDirty = false;
  S.todayIps = [...todaySet];
  S.allIps = [...allSet];
  dayRec(S.date).ips = [...todaySet];
  const keys = Object.keys(S.days).sort();
  while (keys.length > DAYS_KEEP) delete S.days[keys.shift()];
  for (const k of keys.slice(0, Math.max(0, keys.length - IPS_KEEP)))
    if (S.days[k]) S.days[k].ips = [];        // старые хэши не нужны, место не занимают
  try {
    fs.writeFileSync(STATS_FILE + '.tmp', JSON.stringify(S));
    fs.renameSync(STATS_FILE + '.tmp', STATS_FILE);
  } catch (e) {}
}
setInterval(saveStats, 20000);
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { saveStats(); process.exit(0); });

function pFmt(v) {
  if (v == null || !isFinite(v)) return '—';
  const a = Math.abs(v);
  const d = a >= 10000 ? 0 : a >= 100 ? 1 : a >= 1 ? 2 : 4;
  return v.toLocaleString('ru-RU', { minimumFractionDigits: d, maximumFractionDigits: d });
}
const R = (n, p, c) => ({ n, p: pFmt(p), c: (c == null || !isFinite(c)) ? null : Math.round(c * 100) / 100 });

// ── HyperLiquid ─────────────────────────────────────────────────────────
// Биржа бессрочных контрактов. Отдаёт цены в долларах и работает круглосуточно,
// в отличие от фьючерсов Мосбиржи, которые замирают на выходных.
// Требует POST с телом JSON, поэтому fetchUrl (только GET) тут не годится.
// Площадки: '' — основная (крипта), 'xyz' — сторонняя, с сырьём, акциями и индексами.
function hlInfo(body, ms = 8000) {
  return new Promise((resolve, reject) => {
    const data = Buffer.from(JSON.stringify(body));
    const req = https.request({
      hostname: 'api.hyperliquid.xyz', path: '/info', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': data.length },
      timeout: ms,
    }, res => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('hl ' + res.statusCode)); }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); } catch (e) { reject(e); } });
    });
    req.on('timeout', () => req.destroy(new Error('hl timeout')));
    req.on('error', reject);
    req.end(data);
  });
}

// Из ответа metaAndAssetCtxs делаем карту: тикер → { px, chg%, vol }.
// Процент считаем сами от вчерашнего закрытия — как для курсов ЦБ.
// Инструменты с нулевым оборотом отбрасываем: там цена стоит на месте
// и меняться не будет (URANIUM, CORN, WHEAT, VIX, DXY и прочие пустышки).
function hlMap(resp) {
  const uni = (resp && resp[0] && resp[0].universe) || [];
  const ctx = (resp && resp[1]) || [];
  const out = {};
  uni.forEach((u, i) => {
    const c = ctx[i]; if (!c) return;
    const px = parseFloat(c.markPx), prev = parseFloat(c.prevDayPx);
    const vol = parseFloat(c.dayNtlVlm) || 0;
    if (!isFinite(px) || vol <= 0) return;
    // На сторонних площадках имя приходит с приставкой — 'xyz:GOLD'.
    // На основной приставки нет: просто 'BTC'. Отрезаем, чтобы ключ был единым.
    const key = String(u.name).split(':').pop();
    out[key] = { px, vol, chg: isFinite(prev) && prev ? (px - prev) / prev * 100 : null };
  });
  return out;
}

// Доходности гособлигаций США. Показываем в процентах годовых с двумя знаками.
const RY = (n, p, c) => ({
  n,
  p: (p == null || !isFinite(p)) ? '—'
     : p.toLocaleString('ru-RU', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + '%',
  c: (c == null || !isFinite(c)) ? null : Math.round(c * 100) / 100,
});

// Кривая доходности берётся из базы ФРБ Сент-Луиса: открытая выгрузка в CSV,
// без ключей и регистрации. Данные суточные, поэтому держим их в отдельном
// кэше на полчаса — панель опрашивается раз в две секунды, дёргать источник
// так часто незачем и невежливо.
const TSY = [['DGS2', 'UST 2 года'], ['DGS5', 'UST 5 лет'], ['DGS10', 'UST 10 лет'], ['DGS30', 'UST 30 лет']];
const tsyCache = { at: 0, busy: false, rows: new Array(4).fill(null) };

// База ФРБ Сент-Луиса капризна к транспорту: по HTTP/2 она обрывает поток
// внутренней ошибкой, а встроенный загрузчик Node не умеет откатываться на
// HTTP/1.1 и получает обрыв соединения. Поэтому зовём curl с явным HTTP/1.1 —
// так источник отдаёт выгрузку нормально. Если curl нет, пробуем обычный путь.
function fetchViaCurl(url) {
  return new Promise((resolve, reject) => {
    const bin = fs.existsSync('/usr/bin/curl') ? '/usr/bin/curl' : 'curl';
    // Никакой подделки под браузер: источник душит именно такие запросы —
    // с признаком браузера соединение висит до тайм-аута, без него отвечает
    // за полсекунды. Идём как есть, обычным curl.
    execFile(bin, ['-sS', '--http1.1', '-m', '20', '-L',
      url], { maxBuffer: 8 * 1024 * 1024 }, (err, out, errOut) => {
      if (err) {
        if (err.code === 'ENOENT') return reject(new Error('curl не найден'));
        const why = String(errOut || '').trim().split('\n')[0] || ('код выхода ' + err.code);
        return reject(new Error(why));
      }
      if (!out || !out.trim()) return reject(new Error('пустой ответ'));
      resolve(out);
    });
  });
}

// Один дневной ряд из базы ФРБ: последнее значение и предыдущее для расчёта
// изменения. Пропуски за выходные помечены точкой — отбрасываем их.
function fredSeries(id, from) {
  const url = 'https://fred.stlouisfed.org/graph/fredgraph.csv?id=' + id + '&cosd=' + from;
  return fetchViaCurl(url)
    .catch(e => { console.log('FRED curl:', id, e.message); return fetchUrl(url); })
    .then(raw => {
      const v = String(raw).trim().split('\n').slice(1)
        .map(l => parseFloat(l.split(',')[1]))
        .filter(x => isFinite(x) && x > 0);
      if (!v.length) throw new Error('нет значений');
      return { last: v[v.length - 1], prev: v.length > 1 ? v[v.length - 2] : null };
    });
}

// Nasdaq 100. На площадке xyz его нет — там свой индекс XYZ100, это другая
// корзина. Поэтому берём настоящий индекс из той же базы; данные дневные.
const nasCache = { at: 0, busy: false, row: null };
function nasRefresh() {
  if (nasCache.busy || Date.now() - nasCache.at < 30 * 60 * 1000) return;
  nasCache.busy = true;
  const from = new Date(Date.now() - 25 * 864e5).toISOString().slice(0, 10);
  fredSeries('NASDAQ100', from).then(({ last, prev }) => {
    nasCache.row = R('Nasdaq 100', last, prev ? (last - prev) / prev * 100 : null);
    console.log('NAS: ' + last);
  }).catch(e => console.log('NAS fail:', e.message))
    .then(() => { nasCache.at = Date.now(); nasCache.busy = false; });
}
nasRefresh();

function tsyRefresh() {
  if (tsyCache.busy || Date.now() - tsyCache.at < 30 * 60 * 1000) return;
  tsyCache.busy = true;
  const from = new Date(Date.now() - 25 * 864e5).toISOString().slice(0, 10);
  Promise.allSettled(TSY.map(([id, name], i) =>
    fetchViaCurl('https://fred.stlouisfed.org/graph/fredgraph.csv?id=' + id + '&cosd=' + from)
      .catch(e => { console.log('TSY curl:', id, e.message); return fetchUrl('https://fred.stlouisfed.org/graph/fredgraph.csv?id=' + id + '&cosd=' + from); })
      .then(raw => {
      // в выгрузке пропуски за выходные помечены точкой — отбрасываем их
      const v = String(raw).trim().split('\n').slice(1)
        .map(l => parseFloat(l.split(',')[1]))
        .filter(x => isFinite(x) && x > 0);
      if (!v.length) return;
      const last = v[v.length - 1], prev = v.length > 1 ? v[v.length - 2] : null;
      tsyCache.rows[i] = RY(name, last, prev ? (last - prev) / prev * 100 : null);
    }).catch(e => { console.log('TSY fail:', id, e.message); })
  )).then(() => {
    const ok = tsyCache.rows.filter(Boolean).length;
    console.log('TSY: получено ' + ok + ' из ' + TSY.length);
    tsyCache.at = Date.now(); tsyCache.busy = false;
  }).catch(() => { tsyCache.busy = false; });
}
tsyRefresh();

// Официальные курсы ЦБ публикуются с четырьмя знаками после запятой —
// показываем их как есть, не сокращая по величине, как делает pFmt.
const R4 = (n, p, c) => ({
  n,
  p: (p == null || !isFinite(p)) ? '—'
     : p.toLocaleString('ru-RU', { minimumFractionDigits: 4, maximumFractionDigits: 4 }),
  c: (c == null || !isFinite(c)) ? null : Math.round(c * 100) / 100,
});

async function buildPanel() {
  const G = {
    'Сырьё':   new Array(8).fill(null),
    'Индексы': new Array(4).fill(null),
    'Трежерис': new Array(4).fill(null),
    'Валюты':  new Array(6).fill(null),
    'Крипта':  new Array(4).fill(null),
    'Акции':   new Array(7).fill(null),
  };
  const jobs = [];

  tsyRefresh();                                   // обновится в фоне, панель не ждёт
  G['Трежерис'] = tsyCache.rows.slice();
  nasRefresh();
  if (nasCache.row) G['Индексы'][1] = nasCache.row;

  // Сырьё, мировые индексы и EUR/USD — HyperLiquid, площадка xyz.
  // Раньше здесь были фьючерсы Мосбиржи: они в рублях и стоят на выходных.
  // Взяты только инструменты с живым оборотом; пустышки отсеивает hlMap.
  const XYZ = {
    BRENTOIL: ['Нефть Brent',  'Сырьё',   0, R],
    CL:       ['Нефть WTI',    'Сырьё',   1, R],
    NATGAS:   ['Газ (США)',    'Сырьё',   2, R],
    GOLD:     ['Золото',       'Сырьё',   3, R],
    SILVER:   ['Серебро',      'Сырьё',   4, R],
    PLATINUM: ['Платина',      'Сырьё',   5, R],
    PALLADIUM:['Палладий',     'Сырьё',   6, R],
    COPPER:   ['Медь',         'Сырьё',   7, R],
    SP500:    ['S&P 500',      'Индексы', 0, R],
    EUR:      ['EUR/USD',      'Валюты',  3, R4],
  };
  jobs.push(hlInfo({ type: 'metaAndAssetCtxs', dex: 'xyz' }).then(resp => {
    const m = hlMap(resp);
    for (const tick in XYZ) {
      const x = m[tick]; if (!x) continue;
      const [name, g, i, fmt] = XYZ[tick];
      if (G[g][i]) continue;                 // слот уже занят: у Nasdaq несколько написаний
      G[g][i] = fmt(name, x.px, x.chg);
    }
  }));

  // индексы Мосбиржи — по всем площадкам секции, чтобы поймать и РТС
  jobs.push(fetchUrl('https://iss.moex.com/iss/engines/stock/markets/index/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,CURRENTVALUE,LASTCHANGEPRC')
    .then(raw => {
      const j = JSON.parse(raw);
      for (const [secid, val, chg] of (j.marketdata && j.marketdata.data) || []) {
        if (val == null) continue;
        if (secid === 'IMOEX' && !G['Индексы'][2]) G['Индексы'][2] = R('МосБиржа', val, chg);
        if (secid === 'RGBI'  && !G['Индексы'][3]) G['Индексы'][3] = R('ОФЗ · RGBI', val, chg);
      }
    }));

  // биржевые курсы валютного рынка Мосбиржи (режим CETS, расчёты «завтра»).
  // Доллар вернулся в биржевой режим 16 февраля 2026 года; тикер в разных
  // источниках зовут по-разному, поэтому принимаем оба написания.
  jobs.push(fetchUrl('https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,LAST,LASTCHANGEPRCNT')
    .then(raw => {
      const j = JSON.parse(raw);
      for (const [secid, last, chg] of (j.marketdata && j.marketdata.data) || []) {
        if (last == null || !isFinite(last) || last <= 0) continue;
        if ((secid === 'USD000UTSTOM' || secid === 'USDRUB_TOM') && !G['Валюты'][4])
          G['Валюты'][4] = R4('USDRUB_TOM', last, chg);
        if (secid === 'CNYRUB_TOM' && !G['Валюты'][5])
          G['Валюты'][5] = R4('CNYRUB_TOM', last, chg);
      }
    }));

  // официальные курсы ЦБ (зеркало в JSON, есть вчерашнее значение)
  jobs.push(fetchUrl('https://www.cbr-xml-daily.ru/daily_json.js').then(raw => {
    const v = JSON.parse(raw).Valute || {};
    const cur = (code, name, i) => {
      const x = v[code];
      if (x && x.Value != null) {
        const chg = x.Previous ? (x.Value - x.Previous) / x.Previous * 100 : null;
        G['Валюты'][i] = R4(name, x.Value / (x.Nominal || 1), chg);
      }
    };
    cur('USD', 'USD/RUB · ЦБ', 0); cur('EUR', 'EUR/RUB · ЦБ', 1); cur('CNY', 'CNY/RUB · ЦБ', 2);
  }));

  // Крипта — основная площадка HyperLiquid. Раньше был CoinGecko: он на
  // бесплатном тарифе отказывал при частом опросе и строки пропадали.
  // Здесь лимиты щедрее, а обороты по этим четырём — крупнейшие на бирже.
  const COINS = { BTC: ['Bitcoin', 0], ETH: ['Ethereum', 1], SOL: ['Solana', 2], XRP: ['XRP', 3] };
  jobs.push(hlInfo({ type: 'metaAndAssetCtxs' }).then(resp => {
    const m = hlMap(resp);
    for (const tick in COINS) {
      const x = m[tick]; if (!x) continue;
      const [name, i] = COINS[tick];
      G['Крипта'][i] = R(name, x.px, x.chg);
    }
  }));

  // российские акции — уже собранный /api/quotes
  jobs.push(getQuotes().then(q => {
    const st = (sec, name, i) => {
      const x = q.quotes && q.quotes[sec];
      if (x && x.last != null) G['Акции'][i] = R(name, x.last, x.chg);
    };
    st('SBER', 'Сбер', 0); st('GAZP', 'Газпром', 1); st('LKOH', 'Лукойл', 2); st('ROSN', 'Роснефть', 3);
    st('GMKN', 'Норникель', 4); st('VTBR', 'ВТБ', 5); st('YDEX', 'Яндекс', 6);
  }));

  await Promise.allSettled(jobs);
  const groups = Object.entries(G)
    .map(([name, rows]) => ({ name, rows: rows.filter(Boolean) }))
    .filter(g => g.rows.length);
  return { updated: new Date().toISOString(), groups };
}

async function getPanel() {
  const now = Date.now();
  if (pCache.data && now - pCache.at < 2 * 1000) return pCache.data;   // 2 с: HyperLiquid держит такой темп, CoinGecko не держал
  if (!pCache.busy) pCache.busy = buildPanel()
    .then(d => { pCache = { at: Date.now(), data: d, busy: null }; return d; })
    .catch(e => { pCache.busy = null; return pCache.data || { updated: null, groups: [], error: e.message }; });
  return pCache.busy;
}

// ── HTTP-сервер ─────────────────────────────────────────────────────────
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png' };

process.on('unhandledRejection', r => console.error('[unhandledRejection]', r));
process.on('uncaughtException',  e => console.error('[uncaughtException]', e));

function addSec(res) {
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
}

const srv = http.createServer(async (req, res) => {
  addSec(res);
  // Если перед сайтом стоит nginx, настоящий адрес приходит заголовком. Когда
  // ни один из них не проставлен, мы видим всех как localhost и склеиваем в
  // одного человека — поэтому запоминаем, откуда взяли адрес, и показываем это
  // в /api/stats полем ipSrc.
  const _xff = (req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  const _real = (req.headers['x-real-ip'] || '').trim();
  const clientIp = _xff || _real || req.socket.remoteAddress || 'unknown';
  ipSrc = _xff ? 'x-forwarded-for' : (_real ? 'x-real-ip' : 'socket:' + (req.socket.remoteAddress || '?'));
  try {
    const u = new URL(req.url, 'http://x');
    const _pg = pageOf(u.pathname);
    if (_pg) trackVisit(clientIp, _pg, req);
    else if (u.pathname === '/api/feed' || u.pathname === '/api/panel') trackVisit(clientIp, null, req);
    if (u.pathname === '/api/live') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8',
        'Cache-Control': 'no-cache, no-transform',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no'
      });
      res.write('retry: 3000\n\n');
      res.write(': hello\n\n');
      sseClients.add(res);
      sseIp.set(res, clientIp);
      trackVisit(clientIp, null, req, true);
      req.on('close', () => { sseClients.delete(res); sseIp.delete(res); });
      return;
    }
    if (u.pathname === '/api/feed') {
      const feed = await getFeed();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(JSON.stringify(feed));
    }
    if (u.pathname === '/api/history') {
      // Поиск по архиву в базе: ?q=слово. Отдаём до 200 совпадений, новые сверху.
      const q = (u.searchParams.get('q') || '').trim().slice(0, 80);
      // before=<метка времени> — подгрузка ленты вглубь: отдаём записи старше неё.
      const before = (u.searchParams.get('before') || '').trim();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      if (!pool)  return res.end(JSON.stringify({ items: [], error: 'архив не подключён' }));
      if (!before && q.length < 2) return res.end(JSON.stringify({ items: [] }));
      try {
        const r = before
          ? await pool.query(
              `SELECT id, ts, src, src_name, url, body, lvl, src_count
                 FROM news
                WHERE ts < $1
                ORDER BY ts DESC NULLS LAST
                LIMIT 150`, [before])
          : await pool.query(
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
          markByName(x);
          return x;
        });
        return res.end(JSON.stringify({ items }));
      } catch (e) {
        return res.end(JSON.stringify({ items: [], error: e.message }));
      }
    }
    if (u.pathname === '/api/panel') {
      const q = await getPanel();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(JSON.stringify(q));
    }
    if (u.pathname === '/api/quotes') {
      const q = await getQuotes();
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=30' });
      return res.end(JSON.stringify(q));
    }
    if (u.pathname === '/api/stats') {
      const n = Math.min(Math.max(parseInt(u.searchParams.get('days') || '30', 10) || 30, 1), DAYS_KEEP);
      const days = Object.keys(S.days).sort().slice(-n).map(d => {
        const r = S.days[d];
        return { d, u: r.u || 0, v: r.v || 0, sec: r.sec || 0, ses: r.ses || 0 };
      });
      const t  = dayRec(S.date);
      const dv = {}; for (const [k, c] of aggr('dev', 30)) dv[k] = c;
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(JSON.stringify({
        online: onlineNow(),
        live: sseIp.size,                                 // сколько вкладок держит связь
        ipSrc,                              // уникальных за последние 2 минуты
        today:  todaySet.size,                            // уникальных сегодня (Москва)
        views:  S.views,                                  // открытий страниц сегодня
        week:   windowUnique(7),                          // уникальных за 7 дней
        month:  windowUnique(30),                         // уникальных за 30 дней
        total:  allSet.size,                              // уникальных за всё время
        avgSec:   t.ses ? Math.round(t.sec / t.ses) : 0,  // средний визит сегодня, секунд
        avgSec30: spanTime(30).avg,                       // средний визит за 30 дней
        todaySec: t.sec,                                  // суммарное время сегодня
        date: S.date,
        days,                                             // история по дням
        ref:   aggr('ref', 30).slice(0, 12),              // источники переходов за 30 дней
        dev:   dv,                                        // устройства за 30 дней
        pages: aggr('pg', 30).slice(0, 10)                // страницы за 30 дней
      }));
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
    if (p === '/stats' || p === '/статистика' || p === encodeURI('/статистика')) p = '/stats.html';
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

srv.listen(PORT, () => console.log('MKT-TERMINAL on :' + PORT));
// прогреваем кэш при старте
getFeed().catch(() => {});
