#!/usr/bin/env bash
# Быстрая проверка живого сервера. Запуск: bash tests/smoke.sh [адрес]
# По умолчанию — локальный. На сервере: bash tests/smoke.sh https://finfacts.ru
B="${1:-http://localhost:${PORT:-10092}}"
fail=0
chk(){ local want="$1" what="$2"; shift 2; local code; code=$(curl -s -o /dev/null -w '%{http_code}' -m 20 "$@"); 
  if [ "$code" = "$want" ]; then echo "  ок     $what ($code)"; else echo "  ПРОВАЛ $what: ждали $want, получили $code"; fail=1; fi; }
json(){ local what="$1" url="$2" field="$3"; local v; v=$(curl -s -m 20 "$url" | python3 -c "import json,sys;d=json.load(sys.stdin);print(d$field)" 2>/dev/null);
  if [ -n "$v" ] && [ "$v" != "None" ]; then echo "  ок     $what: $v"; else echo "  ПРОВАЛ $what: пусто"; fail=1; fi; }

echo "СТРАНИЦЫ"
for p in / /pick /stats /map /quotes /cbr /fonts /стат; do chk 200 "страница $p" "$B$p"; done
chk 200 "неизвестный адрес ведёт на главную" "$B/n/что-угодно"
echo "СЛУЖЕБНЫЕ ФАЙЛЫ"
chk 200 "описание приложения" "$B/manifest.webmanifest"
chk 200 "служебный сценарий" "$B/sw.js"
chk 200 "иконка" "$B/icons/icon-192.png"
chk 200 "версия" "$B/version.json"
echo "API"
chk 200 "здоровье" "$B/health"
json "лента: записей" "$B/api/feed" "['items'].__len__()"
json "панель: групп" "$B/api/panel" "['groups'].__len__()"
json "статистика: онлайн" "$B/api/stats" "['online']"
json "каталог: позиций" "$B/api/catalog" "['items'].__len__()"
chk 200 "выборка без кодов" "$B/api/picked"
chk 200 "выборка с кодами" "$B/api/picked?ids=moex:SBER,hl:BTC"
chk 400 "короткая ссылка: пустой набор" -X POST -H 'Content-Type: application/json' -d '{"ids":[]}' "$B/api/share"
chk 200 "короткая ссылка: создать" -X POST -H 'Content-Type: application/json' -d '{"ids":["moex:SBER"]}' "$B/api/share"
chk 404 "короткая ссылка: несуществующий код" "$B/api/share?s=zzzzzz"
chk 200 "мусор в параметрах не роняет" "$B/api/picked?ids=%00%ff,,,:::"
chk 200 "длинный запрос не роняет" "$B/api/feed?q=$(head -c 3000 /dev/zero | tr '\0' 'a')"
echo "СЖАТИЕ И КЭШ"
enc=$(curl -s -m 20 -H 'Accept-Encoding: gzip' -I "$B/" | grep -i content-encoding | tr -d '\r')
[ -n "$enc" ] && echo "  ок     главная сжата ($enc)" || { echo "  ПРОВАЛ главная не сжата"; fail=1; }
et=$(curl -s -m 20 -I "$B/api/feed" | grep -i '^etag' | cut -d' ' -f2 | tr -d '\r')
[ -n "$et" ] && chk 304 "лента с той же отметкой → 304" -H "If-None-Match: $et" "$B/api/feed" || { echo "  ПРОВАЛ у ленты нет отметки версии"; fail=1; }
pv=$(curl -s -m 20 -I "$B/api/panel" | grep -i x-page-version | tr -d '\r')
[ -n "$pv" ] && echo "  ок     версия страницы в заголовке ($pv)" || { echo "  ПРОВАЛ нет заголовка версии"; fail=1; }
grep -q "__PAGE_VER__" <(curl -s -m 20 "$B/") && { echo "  ПРОВАЛ в странице остался незаполненный отпечаток"; fail=1; } || echo "  ок     отпечаток версии вшит в страницу"
echo
[ $fail = 0 ] && echo "ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ" || echo "ЕСТЬ ПРОВАЛЫ"
exit $fail
