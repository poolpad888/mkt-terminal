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
  ['c0ldness',            'Холодный расчёт'],
  ['unexpectedvalue',     'Unexpected Value'],
  ['tinkoff_invest_official', 'Т-Инвестиции'],
  ['economika',           'Экономика'],
  ['russianeconomism',    'Русский экономизм'],
  ['expert_ra',           'Эксперт РА'],
  ['fm_invest',           'Frank Инвестиции'],
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
const PROMO_SOURCES = new Set(['antonchehovanalitk','banksta','probonds','economika','smartlabnews']);
const PROMO_RE = /вип[\s-]*канал|vip[\s-]*канал|платн[а-яё]+\s+(канал|подписк|групп)|подписк[а-яё]*\s+на\s+(закрыт|вип|vip)|закрыт[а-яё]+\s+(канал|групп)|открыт[а-яё]+\s+набор|набор\s+в\s+(групп|команд)|мест[а-яё]*\s+осталось|осталось\s+\d+\s+мест|успей\s+записат|запись\s+открыт|_bot(?![a-z])|бот\s+для\s+оплат|оплат[а-яё]*\s+(вип|vip|подписк)|тариф|промокод|скидк[а-яё]+\s+на\s+подписк|реклам[а-яё]*\s*[:\-]|erid|по\s+вопросам\s+рекламы|прайс|сотрудничеств|партнёрск|партнерск/i;

// Отчёты о собственных сделках — это не новость рынка, а личный журнал автора.
// Ключ — имя канала в адресе, значение — что именно из него не берём.
const TG_SKIP = {
  antonchehovanalitk: /(взял|беру|взяли|открыл|открыва[ею]|набрал|добрал)[а-яё]*\s+(лонг|шорт|позици)|(закрыл|закрыва[ею]|фиксир[а-яё]+|вышел|выхожу)[а-яё]*\s+(лонг|шорт|позици|сделк|по\s+рынку)|закрыл[а-яё]*\s+в\s+(плюс|минус)|стоп\s+(в\s+)?безубыт|тейк\s*профит/i,
};

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
// Кавычки-ёлочки, тире и прочая типографика приходят кодами — без них текст
// пестрит вставками вида «laquo». Список покрывает то, что реально встречается
// в лентах ведомств и деловых изданий.
const ENTS = {
  amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ',
  laquo: '«', raquo: '»', ldquo: '«', rdquo: '»', bdquo: '„', lsquo: '‘', rsquo: '’',
  ndash: '–', mdash: '—', minus: '−', hellip: '…', middot: '·', bull: '•',
  deg: '°', plusmn: '±', times: '×', frac12: '½', sect: '§', para: '¶',
  copy: '©', reg: '®', trade: '™', euro: '€', pound: '£', yen: '¥', cent: '¢',
  rarr: '→', larr: '←', harr: '↔', shy: '', zwj: '', thinsp: ' ', ensp: ' ', emsp: ' ',
};
function decodeEntities(s) {
  return s
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCodePoint(parseInt(h, 16)))
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(+n))
    .replace(/&([a-z][a-z0-9]{1,9});/gi, (m, n) => {
      const v = ENTS[n] !== undefined ? ENTS[n] : ENTS[n.toLowerCase()];
      return v !== undefined ? v : m;
    })
    .replace(/&amp;/g, '&');
}
function stripHtml(s) {
  // Разметка в лентах часто приходит закодированной: снимаем теги, раскодируем
  // и снимаем ещё раз — иначе после раскодирования теги появляются заново и
  // протекают в текст новости вместе со служебными комментариями.
  const снять = t => t
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/(p|div|li|tr)>/gi, '\n')
    .replace(/<[^>]+>/g, '');
  let out = decodeEntities(снять(s));
  if (/<[a-z!\/]/i.test(out)) out = decodeEntities(снять(out));
  return out.replace(/\r/g, '').replace(/[ \t]+/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
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
    const tgSkip = TG_SKIP[username];
    if (tgSkip && tgSkip.test(text)) continue;          // сделки автора, не новости
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
// ── Время записи без даты ──────────────────────────────────────────────
// Если в ленте нет даты или она в непонятном формате, раньше подставлялось
// текущее время. Из-за этого старая запись каждый раз выглядела свежей: она
// уходила из ленты через сутки и возвращалась снова, уже с новым временем.
// Теперь время первой встречи запоминается и больше не меняется.
const SEEN_FILE = path.join(__dirname, 'first-seen.json');
let FIRST = {};
try { FIRST = JSON.parse(fs.readFileSync(SEEN_FILE, 'utf8')) || {}; } catch (e) {}
let firstDirty = false;

function saveFirst() {
  if (!firstDirty) return;
  try {
    fs.writeFileSync(SEEN_FILE + '.tmp', JSON.stringify(FIRST));
    fs.renameSync(SEEN_FILE + '.tmp', SEEN_FILE);
    firstDirty = false;
  } catch (e) {}
}
setInterval(() => {
  const край = Date.now() - 14 * 864e5;                 // две недели хватает с запасом
  for (const k in FIRST) if (FIRST[k] < край) { delete FIRST[k]; firstDirty = true; }
  saveFirst();
}, 5 * 60 * 1000);

function itemTime(pub, id) {
  const t = pub ? Date.parse(pub) : NaN;
  if (isFinite(t)) return new Date(t).toISOString();     // дата есть и разобралась
  if (!FIRST[id]) { FIRST[id] = Date.now(); firstDirty = true; }
  return new Date(FIRST[id]).toISOString();
}

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
    // Опознаём запись по её постоянному номеру, а не по ссылке. У ЦБ ссылки
    // ненадёжны: в одной ленте это просто разделы сайта, повторяющиеся у разных
    // новостей, в другой — адреса с якорями, которые меняются. Стоило якорю
    // измениться, и старая запись выглядела новой. Номер же неизменен.
    // Заслон от старых записей. Ленты ведомств держат публикации неделями, и
    // любая ошибка в опознании записи возвращала их в ленту. Если дата понятна
    // и она старше суток с запасом — запись не берём вовсе.
    const t = pub ? Date.parse(pub) : NaN;
    if (isFinite(t) && Date.now() - t > 48 * 3600 * 1000) continue;

    const guid = g('guid');
    const ключ = guid ? srcName + '|' + guid : link;
    // Раньше код записи брался из первых символов ссылки — а у новостей одного
    // сайта начало адреса совпадает, поэтому разные записи получали один код.
    // Берём хеш: он различает записи целиком, а не по первым буквам.
    const id = 'rss-' + crypto.createHash('sha1').update(ключ).digest('base64url').slice(0, 24);
    items.push({
      id,
      src: 'rss-' + srcName, srcName,
      url: link,
      time: itemTime(pub, id),
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
// Отпечаток и набор слов по коду записи — переживают сборки. Ограничиваем
// размер, чтобы память не росла бесконечно.
const PREP = new Map();
function prepOf(x) {
  let v = PREP.get(x.id);
  if (!v || v.len !== x.text.length) {
    v = { fp: fingerprint(x.text), ws: wordSet(x.text), len: x.text.length };
    if (PREP.size > 6000) { let n = 0; for (const k of PREP.keys()) { PREP.delete(k); if (++n > 2000) break; } }
    PREP.set(x.id, v);
  }
  return v;
}

const WS = new WeakMap();
function wsOf(x) {
  let v = WS.get(x);
  if (!v) { v = wordSet(x.text); WS.set(x, v); }
  return v;
}

// Схожесть по словам путает противоположные сообщения: «повысил до 18» и
// «снизил до 16» состоят почти из одних и тех же слов. Поэтому перед склейкой
// сверяем то, что несёт смысл: числа и направление движения.
const ANTI = [
  [/повыси|подня|увеличи|рост|вырос|подорожа|укрепи/i, /снизи|сократи|уменьши|паден|упал|подешеве|ослаб/i],
  [/прибыл|доход/i, /убыт|потер/i],
  [/разреши|одобри|подписа|утверди/i, /запрети|отклони|заблокирова|наложи/i],
];
function numsOf(t) {
  const out = new Set();
  for (const m of String(t).matchAll(/\d+(?:[.,]\d+)?/g)) {
    const v = m[0].replace(',', '.');
    if (parseFloat(v) >= 1) out.add(v);          // мелочь вроде «1» в датах не считаем значимой
  }
  return out;
}
function conflicts(t1, t2) {
  for (const [a, b] of ANTI) {
    if ((a.test(t1) && b.test(t2)) || (b.test(t1) && a.test(t2))) return true;
  }
  const n1 = numsOf(t1), n2 = numsOf(t2);
  if (n1.size && n2.size) {
    let common = 0;
    for (const v of n1) if (n2.has(v)) common++;
    if (!common) return true;                    // ни одного общего числа — вероятно разные события
  }
  return false;
}

function similar(a, b, near) {
  if (!a.size || !b.size) return false;
  let inter = 0;
  const [small, big] = a.size <= b.size ? [a, b] : [b, a];
  for (const t of small) if (big.has(t)) inter++;
  const contain = inter / small.size;                // маленький почти целиком внутри большого
  const jacc = inter / (a.size + b.size - inter);
  // В пределах четверти часа один и тот же сюжет расходится по каналам почти
  // всегда, поэтому там судим мягче. Дальше по времени требуем большего
  // сходства, чтобы не склеить два разных события на одну тему.
  return near ? (jacc >= 0.26 || contain >= 0.48)
              : (jacc >= 0.38 || contain >= 0.62);
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

  // дедупликация: точные дубли + похожие по смыслу в окне 6 часов.
  // Сборка идёт каждые двадцать секунд, а записи между сборками те же самые,
  // поэтому отпечаток и набор слов считаем один раз и помним по коду записи.
  // Время тоже разбираем один раз: раньше строка даты парсилась на каждом
  // из сотен тысяч сравнений.
  const t0 = Date.now();
  const seen = new Map();
  const out = [];
  const WINDOW = 6 * 3600 * 1000;          // было 3 часа — растянули, пересказы приходят с задержкой
  const NEAR = 15 * 60 * 1000;            // «рядом» — четверть часа
  for (const x of fresh) x._t = new Date(x.time).getTime();
  for (let i = fresh.length - 1; i >= 0; i--) {   // от старых к новым
    const x = fresh[i];
    const pre = prepOf(x);
    const fp = pre.fp;
    let keep = seen.get(fp) || null;
    if (!keep) {
      const ws = pre.ws;
      const t = x._t;
      for (let j = out.length - 1; j >= 0; j--) {
        const y = out[j];
        const dt = t - y._t;
        if (dt > WINDOW) break;
        if (y.src === x.src) continue;             // внутри одного канала не склеиваем
        if (similar(ws, y._ws, dt <= NEAR) && !conflicts(x.text, y.text)) { keep = y; break; }
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
  for (const x of out) { delete x._ws; delete x._t; }
  const dedupMs = Date.now() - t0;

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
  console.log('DEDUP: kept=' + out.length + ' merged=' + dups + ' за ' + dedupMs + ' мс');
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
    // Между полными сборками новости подклеиваются здесь, поэтому те же проверки
    // нужны и тут: иначе пересказ из другого канала висит в ленте до сборки.
    const recent = cache.feed.items.slice(0, 200);

    const add = [];
    for (const r of dbBuf) {
      if (have.has(r.id)) continue;
      if (new Date(r.time).getTime() <= newest - 60 * 1000) continue;   // не тянем старое
      if (haveFp.has(fingerprint(r.text))) continue;
      if (!r.reg && offTopic(r.text)) continue;                         // не наша тема

      const ws = wordSet(r.text);
      const rt = new Date(r.time).getTime();
      let twin = null;
      for (const y of recent) {
        const dt = rt - new Date(y.time).getTime();
        if (Math.abs(dt) > 6 * 3600 * 1000) continue;
        if (y.src === r.src) continue;                                  // внутри канала не склеиваем
        if (similar(ws, wsOf(y), Math.abs(dt) <= 15 * 60 * 1000) && !conflicts(r.text, y.text)) { twin = y; break; }
      }
      if (twin) {                        // дубль: саму новость не публикуем,
        twin.srcCount = (twin.srcCount || 1) + 1;                       // а источник дописываем
        if (!twin.alsoIn) twin.alsoIn = [];
        if (!twin.alsoIn.includes(r.srcName) && twin.srcName !== r.srcName) twin.alsoIn.push(r.srcName);
        if (r.reg) { twin.reg = true; twin.mark = twin.mark || r.mark; }
        have.add(r.id);
        continue;
      }
      const c = classify(r.text);
      const mu = muted(r.text);
      const x = Object.assign({}, r, { lvl: c.lvl, reasons: c.reasons, tk: tickers(r.text), tags: hashtags(r.text) });
      if (mu) { x.lvl = 0; x.reasons = []; if (mu.drop) { x.reg = false; delete x.mark; } }
      WS.set(x, ws);
      recent.unshift(x);                 // чтобы следующий дубль в этой же пачке поймался
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
                     '/cbr': 'ЦБ', '/fonts': 'шрифты', '/stats': 'статистика',
                     '/pick': 'свой набор' };
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
  if (typeof r.peak !== 'number') r.peak = 0;  // наибольший онлайн за день
  if (typeof r.nw   !== 'number') r.nw = 0;    // впервые пришедшие
  if (!Array.isArray(r.hh) || r.hh.length !== 24) r.hh = new Array(24).fill(0);  // активность по часам
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
// Перевод суток. Раньше он случался только при заходе посетителя: если ночью
// никого не было, вчерашние «открытия сегодня» висели до первого события.
// Теперь зовём это ещё и по таймеру, и перед выдачей статистики.
function rollDay() {
  const d = dayKey();
  if (S.date === d) return false;
  S.date = d; todaySet = new Set(); S.views = 0;
  statsDirty = true;
  return true;
}
setInterval(rollDay, 60 * 1000);

// Раз в полминуты запоминаем наибольшее число людей на сайте за сутки.
setInterval(() => {
  rollDay();
  const n = onlineNow();
  const r = dayRec(S.date);
  if (n > r.peak) { r.peak = n; statsDirty = true; }
}, 30 * 1000);

function trackVisit(ip, page, req, countUnique) {
  rollDay();
  const d = dayKey();
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
    if (!allSet.has(h)) r.nw++;                // впервые за всё время наблюдений
    todaySet.add(h);
    allSet.add(h);
    r.hh[new Date().getHours()]++;
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

// Сводки, которые считаются по истории дней: сравнение с вчера, средние,
// рекордный день, доля автоматических обращений.
function extraStats() {
  const keys = Object.keys(S.days).sort();
  const last30 = keys.slice(-30);
  const yd = S.days[dayKey(1)] || {};
  const today = S.days[S.date] || {};

  let sumU = 0, sumV = 0, best = { d: null, u: 0 };
  for (const k of last30) {
    const r = S.days[k] || {};
    sumU += r.u || 0;
    sumV += r.v || 0;
    if ((r.u || 0) > best.u) best = { d: k, u: r.u || 0 };
  }
  // доля автоматических обращений за 30 дней
  let bots = 0, allDev = 0;
  for (const k of last30) {
    const dv = (S.days[k] || {}).dev || {};
    for (const kind in dv) { allDev += dv[kind]; if (kind === 'b') bots += dv[kind]; }
  }
  const tu = today.u || 0, yu = yd.u || 0;
  return {
    yesterday: yu,
    dayDelta: yu ? Math.round((tu - yu) / yu * 100) : null,   // рост к вчера, процентов
    avgDay30: last30.length ? Math.round(sumU / last30.length) : 0,
    views30: sumV,
    bestDay: best.d ? { d: best.d, u: best.u } : null,
    botShare: allDev ? Math.round(bots / allDev * 100) : 0,
    activeDays: keys.length,
    liveNow: sseIp.size,                                     // вкладок держат связь
    returnShare: (function () {                              // доля вернувшихся за 30 дней
      let u = 0, nw = 0;
      for (const k of last30) { const r = S.days[k] || {}; u += r.u || 0; nw += r.nw || 0; }
      return u ? Math.round((u - nw) / u * 100) : 0;
    })(),
    busiestHour: (function () {                              // самый людный час сегодня
      const hh = today.hh || [];
      let bi = -1, bv = 0;
      for (let i = 0; i < hh.length; i++) if (hh[i] > bv) { bv = hh[i]; bi = i; }
      return bi < 0 ? null : { h: bi, v: bv };
    })(),
  };
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
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { saveStats(); try { shareSave(); saveFirst(); } catch (e) {} process.exit(0); });

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
        // IMOEX2 — тот же индекс, но с учётом утренней и вечерней сессий:
        // вне основных торгов он живёт, а обычный IMOEX стоит. Берём его,
        // а простой IMOEX держим про запас, если дополнительный не пришёл.
        if (secid === 'IMOEX2') G['Индексы'][2] = R('МосБиржа', val, chg);
        if (secid === 'IMOEX'  && !G['Индексы'][2]) G['Индексы'][2] = R('МосБиржа', val, chg);
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

// ── Короткие ссылки на свой набор ──────────────────────────────────────
// Список кодов может быть длинным, поэтому в ссылку его не пишем: сохраняем
// набор на сервере и выдаём короткий код. Код детерминированный — одинаковый
// набор всегда даёт одну и ту же ссылку, дублей в файле не появляется.
const SHARE_FILE = path.join(__dirname, 'shares.json');
let SHARES = {};
try { SHARES = JSON.parse(fs.readFileSync(SHARE_FILE, 'utf8')) || {}; } catch (e) {}
let sharesDirty = false;

function shareSave() {
  if (!sharesDirty) return;
  try {
    fs.writeFileSync(SHARE_FILE + '.tmp', JSON.stringify(SHARES));
    fs.renameSync(SHARE_FILE + '.tmp', SHARE_FILE);
    sharesDirty = false;
  } catch (e) {}
}
setInterval(shareSave, 30 * 1000);

function shareCode(ids) {
  const norm = ids.join(',');
  const h = crypto.createHash('sha1').update('ff-share:' + norm).digest();
  // шесть символов из цифр и строчных букв: миллиард с лишним сочетаний
  const ALF = 'abcdefghijkmnpqrstuvwxyz23456789';           // без похожих l, o, 0, 1
  let code = '';
  for (let i = 0; i < 6; i++) code += ALF[h[i] % ALF.length];
  return code;
}

function sharePut(ids) {
  const code = shareCode(ids);
  const было = SHARES[code];
  if (!было || было.ids.join(',') !== ids.join(',')) {
    SHARES[code] = { ids, at: Date.now() };
    sharesDirty = true;
  }
  // подчищаем то, чем не пользовались больше полугода
  const край = Date.now() - 180 * 864e5;
  for (const k in SHARES) if ((SHARES[k].at || 0) < край) { delete SHARES[k]; sharesDirty = true; }
  return code;
}

// ── Пробная страница: свой набор котировок ─────────────────────────────
// Каталог всего, что мы умеем показывать, и выдача по выбранным кодам.
// Кэш каталога — час: список бумаг меняется редко, а котировки берём отдельно.
let catCache = { at: 0, data: null, busy: null };

async function buildCatalog() {
  const out = [];
  const jobs = [];

  // акции Мосбиржи
  jobs.push(fetchUrl('https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,SHORTNAME')
    .then(raw => {
      const j = JSON.parse(raw);
      for (const [secid, name] of (j.securities && j.securities.data) || [])
        if (secid) out.push({ id: 'moex:' + secid, name: name || secid, tk: secid, g: 'Акции' });
    }).catch(() => {}));

  // облигации: ОФЗ на доске TQOB и корпоративные на TQCB
  for (const [board, grp] of [['TQOB', 'ОФЗ'], ['TQCB', 'Облигации компаний']])
    jobs.push(fetchUrl('https://iss.moex.com/iss/engines/stock/markets/bonds/boards/' + board +
        '/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,SHORTNAME')
      .then(raw => {
        const j = JSON.parse(raw);
        for (const [secid, name] of (j.securities && j.securities.data) || [])
          if (secid) out.push({ id: 'bond:' + board + ':' + secid, name: name || secid, tk: secid, g: grp });
      }).catch(() => {}));

  // индексы Мосбиржи
  jobs.push(fetchUrl('https://iss.moex.com/iss/engines/stock/markets/index/securities.json?iss.meta=off&iss.only=securities&securities.columns=SECID,SHORTNAME')
    .then(raw => {
      const j = JSON.parse(raw);
      // Названия с биржи длинные («Индекс МосБиржи (все сессии)»,
      // «Индекс МосБиржи гособлигаций»); в узкой панели нужны короткие.
      const КОРОТКО = { IMOEX: 'Индекс Мосбиржи', IMOEX2: 'Индекс Мосбиржи', RGBI: 'Индекс RGBI',
                        RTSI: 'Индекс РТС', MOEXBC: 'Индекс голубых фишек', RUCBITR: 'Индекс корп. облигаций' };
      for (const [secid, name] of (j.securities && j.securities.data) || [])
        if (secid) out.push({ id: 'idx:' + secid, name: КОРОТКО[secid] || name || secid, tk: secid, g: 'Индексы' });
    }).catch(() => {}));

  // сырьё, мировые индексы и крипта с площадки
  // На этой площадке в одной куче и сырьё, и мировые индексы, и валютные пары.
  // Раскладываем по смыслу, чтобы в наборе они попадали в свои разделы.
  const XYZ_ИМЕНА = {
    BRENTOIL: 'Нефть Brent', CL: 'Нефть WTI', NATGAS: 'Газ (США)', GOLD: 'Золото',
    SILVER: 'Серебро', PLATINUM: 'Платина', PALLADIUM: 'Палладий', COPPER: 'Медь',
    SP500: 'S&P 500', EUR: 'EUR/USD',
  };
  const XYZ_ГРУППЫ = { SP500: 'Индексы', NAS100: 'Индексы', EUR: 'Валюты', GBP: 'Валюты', JPY: 'Валюты' };
  jobs.push(hlInfo({ type: 'metaAndAssetCtxs', dex: 'xyz' })
    .then(r => {
      for (const k in hlMap(r))
        out.push({ id: 'xyz:' + k, name: XYZ_ИМЕНА[k] || k, tk: k, g: XYZ_ГРУППЫ[k] || 'Сырьё' });
    })
    .catch(() => {}));
  jobs.push(hlInfo({ type: 'metaAndAssetCtxs' })
    .then(r => { for (const k in hlMap(r)) out.push({ id: 'hl:' + k, name: k, tk: k, g: 'Крипта' }); })
    .catch(() => {}));

  // курсы ЦБ
  jobs.push(fetchUrl('https://www.cbr-xml-daily.ru/daily_json.js')
    .then(raw => {
      const j = JSON.parse(raw);
      for (const k in (j.Valute || {}))
        out.push({ id: 'cbr:' + k, name: j.Valute[k].Name + ' · ЦБ', tk: k, g: 'Валюты' });
    }).catch(() => {}));

  // валютный рынок Мосбиржи и доходности
  out.push({ id: 'sel:USDRUB_TOM', name: 'USDRUB_TOM', tk: 'USDRUB_TOM', g: 'Валюты' });
  out.push({ id: 'sel:CNYRUB_TOM',  name: 'CNYRUB_TOM', tk: 'CNYRUB_TOM', g: 'Валюты' });
  for (const [id, nm] of TSY) out.push({ id: 'ust:' + id, name: nm, tk: id, g: 'Трежерис' });
  out.push({ id: 'ust:NASDAQ100', name: 'Nasdaq 100', tk: 'NASDAQ100', g: 'Индексы' });

  await Promise.allSettled(jobs);
  out.sort((a, b) => a.g.localeCompare(b.g, 'ru') || a.name.localeCompare(b.name, 'ru'));
  return { updated: new Date().toISOString(), items: out };
}

async function getCatalog() {
  const now = Date.now();
  if (catCache.data && now - catCache.at < 3600 * 1000) return catCache.data;
  if (!catCache.busy) catCache.busy = buildCatalog()
    .then(d => { catCache = { at: Date.now(), data: d, busy: null }; return d; })
    .catch(e => { catCache.busy = null; return catCache.data || { items: [], error: e.message }; });
  return catCache.busy;
}

// Котировки по выбранным кодам. Тянем только те источники, которые нужны.
async function getPicked(ids) {
  const want = { moex: [], idx: [], xyz: [], hl: [], cbr: [], sel: [], ust: [], bond: [] };
  for (const id of ids) {
    const i = id.indexOf(':'); if (i < 0) continue;
    const k = id.slice(0, i), v = id.slice(i + 1);
    if (want[k]) want[k].push(v);                 // у облигаций v выглядит как 'TQOB:SU26238RMFS4'
  }
  const res = {};
  const jobs = [];

  if (want.moex.length) jobs.push(getQuotes().then(q => {
    for (const t of want.moex) { const x = q.quotes[t]; if (x && x.last != null) res['moex:' + t] = { p: x.last, c: x.chg }; }
  }).catch(() => {}));

  if (want.idx.length) jobs.push(fetchUrl('https://iss.moex.com/iss/engines/stock/markets/index/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,CURRENTVALUE,LASTCHANGEPRC')
    .then(raw => {
      const j = JSON.parse(raw);
      for (const [secid, val, chg] of (j.marketdata && j.marketdata.data) || [])
        if (want.idx.includes(secid) && val != null) res['idx:' + secid] = { p: val, c: chg };
    }).catch(() => {}));

  if (want.xyz.length) jobs.push(hlInfo({ type: 'metaAndAssetCtxs', dex: 'xyz' }).then(r => {
    const m = hlMap(r);
    for (const t of want.xyz) if (m[t]) res['xyz:' + t] = { p: m[t].px, c: m[t].chg };
  }).catch(() => {}));

  if (want.hl.length) jobs.push(hlInfo({ type: 'metaAndAssetCtxs' }).then(r => {
    const m = hlMap(r);
    for (const t of want.hl) if (m[t]) res['hl:' + t] = { p: m[t].px, c: m[t].chg };
  }).catch(() => {}));

  if (want.cbr.length) jobs.push(fetchUrl('https://www.cbr-xml-daily.ru/daily_json.js').then(raw => {
    const j = JSON.parse(raw);
    for (const t of want.cbr) {
      const x = (j.Valute || {})[t]; if (!x) continue;
      const nom = x.Nominal || 1;
      const chg = x.Previous ? (x.Value / nom - x.Previous / nom) / (x.Previous / nom) * 100 : null;
      res['cbr:' + t] = { p: x.Value / nom, c: chg };
    }
  }).catch(() => {}));

  // облигации: цена идёт в процентах от номинала, поэтому показываем и доходность
  if (want.bond.length) {
    const доски = [...new Set(want.bond.map(v => v.split(':')[0]))];
    for (const board of доски) jobs.push(fetchUrl('https://iss.moex.com/iss/engines/stock/markets/bonds/boards/' + board +
        '/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,LAST,LASTCHANGEPRCNT,YIELD')
      .then(raw => {
        const j = JSON.parse(raw);
        for (const [secid, last, chg, yld] of (j.marketdata && j.marketdata.data) || []) {
          const key = 'bond:' + board + ':' + secid;
          if (last == null || !want.bond.includes(board + ':' + secid)) continue;
          res[key] = { p: last, c: chg, yld: (yld == null || !isFinite(yld)) ? null : yld };
        }
      }).catch(() => {}));
  }

  if (want.sel.length) jobs.push(fetchUrl('https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities.json?iss.meta=off&iss.only=marketdata&marketdata.columns=SECID,LAST,LASTCHANGEPRCNT')
    .then(raw => {
      const j = JSON.parse(raw);
      for (const [secid, last, chg] of (j.marketdata && j.marketdata.data) || []) {
        if (last == null) continue;
        if (want.sel.includes(secid)) res['sel:' + secid] = { p: last, c: chg };
        // биржа зовёт доллар то так, то этак — приводим к одному коду
        if ((secid === 'USD000UTSTOM' || secid === 'USDRUB_TOM') && want.sel.includes('USDRUB_TOM'))
          res['sel:USDRUB_TOM'] = { p: last, c: chg };
      }
    }).catch(() => {}));

  for (const t of want.ust) {
    if (t === 'NASDAQ100') { nasRefresh(); if (nasCache.row) res['ust:NASDAQ100'] = { p: nasCache.row.p, c: nasCache.row.c, txt: true }; continue; }
    tsyRefresh();
    const i = TSY.findIndex(([id]) => id === t);
    if (i >= 0 && tsyCache.rows[i]) res['ust:' + t] = { p: tsyCache.rows[i].p, c: tsyCache.rows[i].c, txt: true };
  }

  await Promise.allSettled(jobs);
  return { updated: new Date().toISOString(), quotes: res };
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
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json; charset=utf-8', '.webmanifest': 'application/manifest+json; charset=utf-8' };

process.on('unhandledRejection', r => console.error('[unhandledRejection]', r));
process.on('uncaughtException',  e => console.error('[uncaughtException]', e));

// ── Календарь событий ────────────────────────────────────────────────────
// Первый слой — заседания Банка России и ФРС. Их графики известны на год
// вперёд и публикуются самими регуляторами, поэтому держим списком: это
// надёжнее, чем разбирать страницы, и не зависит от доступности сайтов.
// Время московское. Позже сюда добавятся Росстат, аукционы ОФЗ и компании.
// Поля: дата, название, пояснение, прогноз, предыдущее, факт.
// Прогноз — консенсус; факт заполняется после события. Пустая строка значит
// «нет данных» и показывается точкой, а не выдуманным числом.
const CAL_CBR = [
  ['2026-02-13', 'Решение по ключевой ставке', 'с обновлением прогноза',    '', '16,00%', '15,50%'],
  ['2026-03-20', 'Решение по ключевой ставке', '',                          '', '15,50%', '15,00%'],
  ['2026-04-24', 'Решение по ключевой ставке', '',                          '', '15,00%', '14,50%'],
  ['2026-06-19', 'Решение по ключевой ставке', 'с обновлением прогноза',    '', '14,50%', '14,25%'],
  ['2026-07-24', 'Решение по ключевой ставке', '',                          '', '14,25%', '14,00%'],
  ['2026-09-11', 'Решение по ключевой ставке', 'пресс-конференция в 15:00', '', '14,00%', ''],
  ['2026-10-23', 'Решение по ключевой ставке', 'с обновлением прогноза',    '', '', ''],
  ['2026-12-18', 'Решение по ключевой ставке', '',                          '', '', ''],
];
const CAL_FED = [
  // Проверено по протоколам FOMC на federalreserve.gov: весь 2026 год диапазон
  // держится 3,50–3,75%, последнее снижение — 10 декабря 2025. Прогнозы не выдумываем.
  ['2026-01-28', 'Решение ФРС по ставке', '',                        '', '3,50–3,75%', '3,50–3,75%'],
  ['2026-03-18', 'Решение ФРС по ставке', 'с прогнозами участников', '', '3,50–3,75%', '3,50–3,75%'],
  ['2026-04-29', 'Решение ФРС по ставке', '',                        '', '3,50–3,75%', '3,50–3,75%'],
  ['2026-06-17', 'Решение ФРС по ставке', 'с прогнозами участников', '', '3,50–3,75%', '3,50–3,75%'],
  ['2026-07-29', 'Решение ФРС по ставке', '',                        '', '3,50–3,75%', '3,50–3,75%'],
  ['2026-09-16', 'Решение ФРС по ставке', 'с прогнозами участников', '', '3,50–3,75%', ''],
  ['2026-11-04', 'Решение ФРС по ставке', '',                        '', '', ''],
  ['2026-12-16', 'Решение ФРС по ставке', 'с прогнозами участников', '', '', ''],
  // 2027 — предварительное расписание FOMC от 5 сентября 2025 (пресс-релиз ФРС).
  ['2027-01-27', 'Решение ФРС по ставке', '',                        '', '', ''],
  ['2027-03-17', 'Решение ФРС по ставке', 'с прогнозами участников', '', '', ''],
  ['2027-04-28', 'Решение ФРС по ставке', '',                        '', '', ''],
  ['2027-06-09', 'Решение ФРС по ставке', 'с прогнозами участников', '', '', ''],
  ['2027-07-28', 'Решение ФРС по ставке', '',                        '', '', ''],
  ['2027-09-15', 'Решение ФРС по ставке', 'с прогнозами участников', '', '', ''],
  ['2027-10-27', 'Решение ФРС по ставке', '',                        '', '', ''],
  ['2027-12-08', 'Решение ФРС по ставке', 'с прогнозами участников', '', '', ''],
];

// Повторяющиеся события — задаются правилом, а не списком дат. Числа
// (прогноз/предыдущее/факт) для них не известны заранее, поэтому не выдумываем.
// Поле q — слова для поиска связанной новости в ленте: пробел = «и»,
// вертикальная черта = «или». Сравнение по началу слова, без учёта регистра.
// Состоявшиеся аукционы ОФЗ: дата, привлечено (номинал), спрос.
// Берём из официальных итогов Минфина, публикуемых в день аукциона.
const CAL_OFZ_FACT = {
  // prev — спрос, fact — сколько привлекли по номиналу. Панель узкая,
  // поэтому пишем коротко: «1,42 трлн» вместо «спрос 1425 млрд рублей».
  '2026-09-02': { prev: '1,42 трлн', fact: '1,00 трлн' },
};

function calRules(начало, конец) {
  const out = [];
  const d0 = new Date(начало + 'T12:00:00+03:00'), d1 = new Date(конец + 'T12:00:00+03:00');
  for (let d = new Date(d0); d <= d1; d.setDate(d.getDate() + 1)) {
    const ymd = d.toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' });
    const дн = d.getUTCDay(), число = d.getUTCDate(), мес = d.getUTCMonth() + 1;
    if (дн === 3) {                                   // среда
      const ф = CAL_OFZ_FACT[ymd] || {};
      out.push({ id: 'ofz-' + ymd, date: ymd, time: '11:00', kind: 'fin', who: 'Минфин России', cc: 'ru',
                 name: 'Аукцион ОФЗ', note: '', prev: ф.prev || '', fact: ф.fact || '',
                 q: 'офз аукцион|офз размещ', hot: false });
      out.push({ id: 'cpi-' + ymd, date: ymd, time: '19:00', kind: 'stat', who: 'Росстат', cc: 'ru',
                 name: 'Инфляция за неделю', note: '', q: 'инфляц недел|росстат инфляц|недельн инфляц', hot: false });
    }
    // Запасы нефти. Институт нефти (API) — по вторникам 16:30 по Вашингтону,
    // Управление энергетики (EIA) — по средам 10:30. Летом мск +7, зимой +8.
    // На праздничных неделях выпуск сдвигается — это отмечено в пояснении.
    {
      const летом = мес >= 4 && мес <= 10;
      if (дн === 2)
        out.push({ id: 'api-' + ymd, date: ymd, time: летом ? '23:30' : '00:30', kind: 'stat',
                   who: 'Американский институт нефти', cc: 'us', name: 'Запасы нефти, оценка API',
                   note: 'предварительные данные', q: 'запас нефт api|api запас', hot: false });
      if (дн === 3)
        out.push({ id: 'eia-' + ymd, date: ymd, time: летом ? '17:30' : '18:30', kind: 'stat',
                   who: 'Управление энергетической информации США', cc: 'us',
                   name: 'Запасы нефти в США, EIA', note: 'на праздничных неделях выпуск сдвигается',
                   q: 'запас нефт сша|eia запас|запас нефт', hot: true });
    }
    // Народный банк Китая: ставка LPR объявляется 20-го числа в 9:00 по Пекину
    // (04:00 мск); если 20-е — выходной, переносится на ближайший рабочий день.
    if ((число === 20 && дн >= 1 && дн <= 5) || (число === 21 && дн === 1) || (число === 22 && дн === 1)) {
      out.push({ id: 'pboc-' + ymd, date: ymd, time: '04:00', kind: 'cbr', who: 'Народный банк Китая', cc: 'cn',
                 name: 'Ставка LPR Китая', note: '1 год и 5 лет', prev: '3,00% / 3,50%',
                 q: 'народн банк кита|нбк ставк|lpr|кита ставк', hot: false });
    }
    if (дн === 2 && число >= 15 && число <= 21) {      // третий вторник месяца
      out.push({ id: 'ind-' + ymd, date: ymd, time: '19:00', kind: 'stat', who: 'Росстат', cc: 'ru',
                 name: 'Промышленное производство', note: 'за прошлый месяц',
                 q: 'промышленн производств|промпроизводств', hot: false });
    }
    if (дн === 3 && число >= 8 && число <= 14) {      // вторая среда месяца
      out.push({ id: 'uscpi-' + ymd, date: ymd, time: (мес >= 4 && мес <= 10) ? '15:30' : '16:30',
                 kind: 'fed', who: 'Бюро статистики труда США', cc: 'us', name: 'Инфляция в США, ИПЦ',
                 note: 'за прошлый месяц', q: 'инфляц сша|ипц сша|cpi сша', hot: true });
    }
    if (дн === 4 && число >= 22) {                    // последний четверг месяца
      out.push({ id: 'gdp-' + ymd, date: ymd, time: (мес >= 4 && мес <= 10) ? '15:30' : '16:30',
                 kind: 'fed', who: 'Бюро экономического анализа США', cc: 'us', name: 'ВВП США, оценка',
                 note: '', q: 'ввп сша', hot: false });
    }
    if (дн === 5 && число <= 7) {                     // первая пятница месяца
      // 08:30 по Вашингтону: летом 15:30 мск, зимой 16:30 мск
      const летом = мес >= 4 && мес <= 10;
      out.push({ id: 'nfp-' + ymd, date: ymd, time: летом ? '15:30' : '16:30', kind: 'fed',
                 who: 'Бюро статистики труда США', cc: 'us', name: 'Занятость вне сельского хозяйства',
                 note: '', q: 'занятост сша|nonfarm|payrolls|рынок труда сша', hot: true });
    }
  }
  return out;
}

// ── Заседания зарубежных центробанков по официальным расписаниям ──────────
// Даты взяты с сайтов самих банков, не из вторых рук. Время — московское.
// ЕЦБ: решение в 14:15 CET = 16:15 мск (зимой), 15:15 мск при летнем времени в ЕС.
// ecb.europa.eu/press/calendars/mgcgc
const CAL_ECB = [
  ['2026-09-10', '16:15'], ['2026-10-29', '16:15'], ['2026-12-17', '16:15'],
  ['2027-02-04', '16:15'], ['2027-03-18', '16:15'],
];
// Банк Англии: объявление в 12:00 по Лондону = 14:00 мск летом, 15:00 зимой.
// bankofengland.co.uk/monetary-policy/upcoming-mpc-dates
const CAL_BOE = [
  ['2026-09-17', '14:00'], ['2026-11-05', '15:00'], ['2026-12-17', '15:00'],
];
// Банк Японии: решение днём второго дня, около 06:00 мск.
// boj.or.jp/en/mopo/mpmsche_minu
const CAL_BOJ = [
  ['2026-09-18', '06:00'], ['2026-10-30', '06:00'], ['2026-12-18', '06:00'],
];
// ЦБ Турции: расписание на 2026 год опубликовано банком, решение в 14:00 по Анкаре = 14:00 мск.
// tcmb.gov.tr — календарь заседаний
const CAL_TCMB = [
  ['2026-09-10', '14:00'], ['2026-10-22', '14:00'], ['2026-12-10', '14:00'],
];
// Германия: индекс делового климата Ifo, даты с ifo.de (10:30 по Берлину).
// Летом это 11:30 мск, зимой 12:30. Ifo — главный опережающий индикатор ФРГ.
const CAL_IFO = [
  ['2026-09-24', '11:30'], ['2026-10-26', '12:30'], ['2026-11-24', '12:30'], ['2026-12-17', '12:30'],
];
// ОПЕК+: заседания мониторингового комитета раз в два месяца, по пресс-релизам opec.org
const CAL_OPEC = [
  ['2026-10-04'], ['2026-12-06'],
];

function calEvents(начало, конец) {
  const out = [];
  for (const [d, name, note, f, prev, fact] of CAL_CBR)
    out.push({ id: 'cbr-' + d, date: d, time: '13:30', kind: 'cbr', who: 'Банк России', cc: 'ru',
               name, note, f, prev, fact, q: 'ключев ставк|банк росси ставк', hot: true });
  for (const [d, name, note, f, prev, fact] of CAL_FED)
    out.push({ id: 'fed-' + d, date: d, time: '21:00', kind: 'fed', who: 'Федеральная резервная система', cc: 'us',
               name, note, f, prev, fact, q: 'фрс ставк|fomc', hot: true });
  for (const [d, t] of CAL_ECB)
    out.push({ id: 'ecb-' + d, date: d, time: t, kind: 'cbr', who: 'Европейский центральный банк', cc: 'eu',
               name: 'Ставка ЕЦБ', note: 'решение по ставке', q: 'ецб ставк|ecb ставк', hot: true });
  for (const [d, t] of CAL_BOE)
    out.push({ id: 'boe-' + d, date: d, time: t, kind: 'cbr', who: 'Банк Англии', cc: 'gb',
               name: 'Ставка Банка Англии', note: 'решение по ставке', q: 'банк англи ставк', hot: true });
  for (const [d, t] of CAL_BOJ)
    out.push({ id: 'boj-' + d, date: d, time: t, kind: 'cbr', who: 'Банк Японии', cc: 'jp',
               name: 'Ставка Банка Японии', note: 'решение по ставке', q: 'банк япони ставк', hot: true });
  for (const [d, t] of CAL_TCMB)
    out.push({ id: 'tcmb-' + d, date: d, time: t, kind: 'cbr', who: 'ЦБ Турции', cc: 'tr',
               name: 'Ставка ЦБ Турции', note: 'решение по ставке', q: 'цб турци ставк|турци ставк', hot: false });
  for (const [d, t] of CAL_IFO)
    out.push({ id: 'ifo-' + d, date: d, time: t, kind: 'stat', who: 'Институт Ifo', cc: 'de',
               name: 'Деловой климат в Германии, Ifo', note: 'за текущий месяц', q: 'ifo|деловой климат герман', hot: false });
  for (const [d] of CAL_OPEC)
    out.push({ id: 'opec-' + d, date: d, time: '15:00', kind: 'fin', who: 'ОПЕК+', cc: '',
               name: 'Заседание ОПЕК+', note: 'мониторинговый комитет', q: 'опек|opec', hot: true });
  if (начало && конец) out.push(...calRules(начало, конец));
  out.sort((a, b) => (a.date + a.time).localeCompare(b.date + b.time));
  return out;
}

// Направление факта относительно предыдущего значения: по нему подсвечиваем
// число зелёным или красным. Сравниваем первое число из строки, чтобы
// работало и с диапазонами вида «4,00–4,25%».
function calMove(prev, fact) {
  const n = t => { const m = String(t || '').match(/-?\d+[.,]?\d*/); return m ? parseFloat(m[0].replace(',', '.')) : null; };
  const a = n(prev), b = n(fact);
  if (a == null || b == null || a === b) return '';
  return b > a ? 'up' : 'dn';
}

// Момент события в московском времени, приведённый к абсолютному времени.
function calTs(ev) {
  return new Date(ev.date + 'T' + ev.time + ':00+03:00').getTime();
}

function addSec(res) {
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
}

// ── Быстрая отдача ───────────────────────────────────────────────────────
// Раньше всё уходило несжатым и без отметок версии: лента в сотни килобайт
// качалась целиком каждые пятнадцать секунд, даже если не менялась. Теперь:
//  • ответы больше килобайта сжимаются, если браузер это умеет (в 4–5 раз меньше);
//  • у ответа есть отметка версии (ETag) — если у браузера уже такая, отдаём
//    пустой 304 вместо тела;
//  • сжатую копию одного и того же тела не пересчитываем — она кэшируется по
//    отметке, так что тысяча вкладок обходится одним сжатием.
// ── Версия страницы ──────────────────────────────────────────────────────
// Отпечаток главной страницы вшивается в неё при отдаче и добавляется в
// заголовок каждого ответа ленты. Если у открытой вкладки отпечаток другой —
// значит на сервере уже новая версия, и страница просит себя обновить.
// Считаем по содержимому файла, поэтому любая правка — новая версия.
let pageVer = { mtime: 0, ver: '' };
function pageVersion() {
  try {
    const f = path.join(__dirname, 'public', 'index.html');
    const st = fs.statSync(f);
    if (st.mtimeMs !== pageVer.mtime) {
      const raw = fs.readFileSync(f);
      pageVer = { mtime: st.mtimeMs, ver: crypto.createHash('sha1').update(raw).digest('base64url').slice(0, 10) };
    }
  } catch (e) {}
  return pageVer.ver;
}

const gzCache = new Map();             // etag → сжатое тело
function send(req, res, status, headers, body) {
  const buf = Buffer.isBuffer(body) ? body : Buffer.from(String(body));
  const etag = 'W/"' + crypto.createHash('sha1').update(buf).digest('base64url').slice(0, 16) + '"';
  const h = Object.assign({ 'ETag': etag, 'Vary': 'Accept-Encoding', 'X-Page-Version': pageVersion() }, headers);
  if (status === 200 && req.headers['if-none-match'] === etag) {
    res.writeHead(304, h); return res.end();
  }
  const wantsGz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
  if (wantsGz && buf.length > 1024) {
    let gz = gzCache.get(etag);
    if (!gz) {
      gz = zlib.gzipSync(buf, { level: 6 });
      if (gzCache.size > 64) gzCache.delete(gzCache.keys().next().value);   // держим последние
      gzCache.set(etag, gz);
    }
    h['Content-Encoding'] = 'gzip';
    h['Content-Length'] = gz.length;
    res.writeHead(status, h); return res.end(gz);
  }
  h['Content-Length'] = buf.length;
  res.writeHead(status, h); return res.end(buf);
}
const sendJson = (req, res, obj, extra) =>
  send(req, res, 200, Object.assign({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-cache' }, extra || {}), JSON.stringify(obj));

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
    if (u.pathname === '/api/calendar') {
      // ?from=YYYY-MM-DD&days=N — окно по дням, по умолчанию от сегодня на 60 дней
      // вперёд и на 7 назад: панель показывает и что будет, и что недавно вышло.
      const сегодня = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' });
      const from = /^\d{4}-\d{2}-\d{2}$/.test(u.searchParams.get('from') || '') ? u.searchParams.get('from') : null;
      const days = Math.min(Math.max(parseInt(u.searchParams.get('days') || '60', 10) || 60, 1), 400);
      const начало = from || new Date(Date.now() - 7 * 864e5).toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' });
      const конец = new Date(new Date(начало + 'T00:00:00+03:00').getTime() + days * 864e5)
        .toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' });
      // Считаем на 40 дней раньше запрошенного окна: нужно, чтобы у первого
      // видимого события нашлось предыдущее того же рода для строки «прошлый раз».
      const запас = new Date(new Date(начало + 'T00:00:00+03:00').getTime() - 40 * 864e5)
        .toLocaleDateString('sv-SE', { timeZone: 'Europe/Moscow' });
      const события = calEvents(запас, конец).filter(e => e.date >= запас && e.date <= конец);
      const now = Date.now();
      for (const e of события) {
        const t = calTs(e); e.ts = t; e.past = t < now;
        // У аукциона ОФЗ «пред.» — это спрос, а не прошлое значение того же
        // показателя, поэтому сравнивать их бессмысленно: стрелку не рисуем.
        e.move = (e.fact && !e.id.startsWith('ofz-')) ? calMove(e.prev, e.fact) : '';
      }
      // Для каждого события ищем предыдущее такого же рода (по имени и ведомству):
      // панель показывает под отсчётом, что было в прошлый раз.
      const поРоду = {};
      for (const e of события) {
        const род = e.kind + '|' + e.name;
        const пред = поРоду[род];
        if (пред) e.prevEv = { id: пред.id, date: пред.date, time: пред.time, prev: пред.prev || '', fact: пред.fact || '', move: пред.move || '' };
        поРоду[род] = e;
      }
      const видимые = события.filter(e => e.date >= начало && e.date <= конец);
      const ближайшее = видимые.find(e => !e.past) || null;   // ближайшее по времени

      return sendJson(req, res, { today: сегодня, from: начало, to: конец, next: ближайшее ? ближайшее.id : null, events: видимые });
    }
    if (u.pathname === '/api/feed') {
      const feed = await getFeed();
      return sendJson(req, res, feed);
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
      return sendJson(req, res, q);
    }
    // пробная страница со своим набором котировок
    if (u.pathname === '/api/catalog') {
      const c = await getCatalog();
      return sendJson(req, res, c, { 'Cache-Control': 'public, max-age=600' });
    }
    // короткая ссылка: сохранить набор и выдать код / получить набор по коду
    if (u.pathname === '/api/share') {
      if (req.method === 'POST') {
        let тело = '';
        req.on('data', c => { тело += c; if (тело.length > 8000) req.destroy(); });
        await new Promise(r => req.on('end', r));
        let ids = [];
        try { ids = (JSON.parse(тело).ids || []).map(String).filter(Boolean).slice(0, 200); } catch (e) {}
        if (!ids.length) { res.writeHead(400, { 'Content-Type': 'application/json' }); return res.end('{"error":"пустой набор"}'); }
        const code = sharePut(ids);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        return res.end(JSON.stringify({ code }));
      }
      const code = (u.searchParams.get('s') || '').slice(0, 12);
      const rec = SHARES[code];
      if (rec) { rec.at = Date.now(); sharesDirty = true; }
      res.writeHead(rec ? 200 : 404, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(JSON.stringify(rec ? { ids: rec.ids } : { error: 'набор не найден' }));
    }
    if (u.pathname === '/api/picked') {
      const ids = (u.searchParams.get('ids') || '').split(',').map(x => x.trim()).filter(Boolean).slice(0, 60);
      const d = ids.length ? await getPicked(ids) : { updated: null, quotes: {} };
      return sendJson(req, res, d);
    }
    if (u.pathname === '/api/quotes') {
      const q = await getQuotes();
      return sendJson(req, res, q, { 'Cache-Control': 'public, max-age=30' });
    }
    if (u.pathname === '/api/stats') {
      rollDay();                       // чтобы после полуночи не показать вчерашнее
      const n = Math.min(Math.max(parseInt(u.searchParams.get('days') || '30', 10) || 30, 1), DAYS_KEEP);
      const days = Object.keys(S.days).sort().slice(-n).map(d => {
        const r = S.days[d];
        return { d, u: r.u || 0, v: r.v || 0, sec: r.sec || 0, ses: r.ses || 0 };
      });
      const t  = dayRec(S.date);
      const dv = {}; for (const [k, c] of aggr('dev', 30)) dv[k] = c;
      return sendJson(req, res, {
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
        pages: aggr('pg', 30).slice(0, 10),               // страницы за 30 дней
        peak:  t.peak || 0,                               // рекорд онлайна сегодня
        peak30: Math.max(0, ...Object.keys(S.days).slice(-30).map(k => S.days[k].peak || 0)),
        newToday: t.nw || 0,                              // впервые пришедшие сегодня
        retToday: Math.max(0, (t.u || 0) - (t.nw || 0)),  // вернувшиеся
        perVisit: t.ses ? Math.round((t.v || 0) / t.ses * 10) / 10 : 0,  // открытий за визит
        hours: t.hh || new Array(24).fill(0),             // активность по часам
        ...extraStats()                                   // сравнения и итоги за 30 дней
      });
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
    if (p === '/pick') p = '/pick.html';                  // пробная: свой набор котировок
    p = path.normalize(p).replace(/^(\.\.[/\\])+/, '');
    const file = path.join(__dirname, 'public', p);
    if (file.startsWith(path.join(__dirname, 'public')) && fs.existsSync(file) && fs.statSync(file).isFile()) {
      const ext = path.extname(file);
      // страницы могут меняться в любой момент — браузер сверяет отметку при каждом
      // заходе; картинки и описание приложения меняются редко — держим сутки
      const cc = (ext === '.html' || ext === '.json' || ext === '.js') ? 'no-cache' : 'public, max-age=86400';
      let body = fs.readFileSync(file);
      if (p === '/index.html') body = Buffer.from(body.toString('utf8').replace('__PAGE_VER__', pageVersion()));
      return send(req, res, 200, { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Cache-Control': cc }, body);
    }
    // любой другой адрес — на главную (для ссылок на новость /n/…)
    return send(req, res, 200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' },
      fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8').replace('__PAGE_VER__', pageVersion()));
  } catch (e) {
    res.writeHead(500); res.end('error');
  }
});

srv.listen(PORT, () => console.log('MKT-TERMINAL on :' + PORT));
// прогреваем кэш при старте
getFeed().catch(() => {});
