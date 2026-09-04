# Проверка интерфейса в настоящем браузере. Данные ленты и панели подменяются
# на тестовые, чтобы прогон не зависел от внешних источников.
# Запуск: PORT=10092 node server.js &  затем  python3 tests/ui.py
import json, os, sys, time
from playwright.sync_api import sync_playwright

БАЗА=os.environ.get('BASE','http://localhost:'+os.environ.get('PORT','10092'))
ПРОВАЛОВ=[]
def ок(усл, что):
    print(('  ок    ' if усл else '  ПРОВАЛ')+' '+что)
    if not усл: ПРОВАЛОВ.append(что)

def лента():
    now=int(time.time()*1000); items=[]
    for i in range(40):
        x={'id':'t%d'%i,'src':'tg-src%d'%(i%5),'srcName':'Канал %d'%(i%5),'url':'https://t.me/x/%d'%i,
           'time':time.strftime('%Y-%m-%dT%H:%M:%S.000Z',time.gmtime((now-i*90000)/1000)),
           'text':'Новость номер %d про ставку и рубль #ставка #рубль'%i,'tags':['ставка','рубль'],'tk':['SBER'] if i%4==0 else [],
           'lvl':2 if i%6==0 else 0,'reasons':['ставка'] if i%6==0 else [],'srcCount':3 if i%7==0 else 1,
           'alsoIn':['Канал 9','Канал 8'] if i%7==0 else []}
        if i%10==1: x.update({'reg':True,'mark':'reg','srcName':'ЦБ РФ'})
        if i%10==2: x.update({'reg':True,'mark':'fin','srcName':'Минфин России'})
        items.append(x)
    return {'updated':time.strftime('%Y-%m-%dT%H:%M:%SZ'),'items':items,'tags':[['ставка',20],['рубль',20]],'health':[]}

def панель():
    return {'updated':time.strftime('%Y-%m-%dT%H:%M:%SZ'),'groups':[
      {'name':'Сырьё','rows':[{'n':'Нефть Brent','p':'95,08','c':4.76},{'n':'Золото','p':'4 335,9','c':-2.53}]},
      {'name':'Индексы','rows':[{'n':'S&P 500','p':'7 663,3','c':0.46},{'n':'МосБиржа','p':'2 193,7','c':0.40}]},
      {'name':'Акции','rows':[{'n':'Сбер','p':'275,23','c':0.35}]}]}

def каталог():
    return {'items':[{'id':'moex:SBER','name':'Сбербанк','tk':'SBER','g':'Акции'},
                     {'id':'moex:GAZP','name':'Газпром','tk':'GAZP','g':'Акции'},
                     {'id':'hl:BTC','name':'Bitcoin','tk':'BTC','g':'Крипта'},
                     {'id':'xyz:BRENTOIL','name':'Нефть Brent','tk':'BRENTOIL','g':'Сырьё'},
                     {'id':'bond:TQOB:SU26238RMFS4','name':'ОФЗ 26238','tk':'SU26238RMFS4','g':'ОФЗ'}]}

def picked(ids):
    q={}
    for i in ids:
        q[i]={'p':100.0+len(i),'c':0.5}
        if i.startswith('bond:'): q[i]['yld']=14.87
    return {'updated':'x','quotes':q}

with sync_playwright() as p:
    b=p.chromium.launch()
    # служебный сценарий пропускает запросы через себя и мешает подмене — отключаем
    ctx=b.new_context(viewport={'width':1400,'height':900},service_workers='block')
    pg=ctx.new_page()
    ош=[]; pg.on('pageerror', lambda e: ош.append(str(e)[:200]))
    заголовок={'X-Page-Version':None}
    def route(r):
        u=r.request.url
        h={'Content-Type':'application/json; charset=utf-8'}
        if заголовок['X-Page-Version']: h['X-Page-Version']=заголовок['X-Page-Version']
        if '/api/feed' in u: return r.fulfill(status=200,headers=h,body=json.dumps(лента()))
        if '/api/panel' in u: return r.fulfill(status=200,headers=h,body=json.dumps(панель()))
        if '/api/catalog' in u: return r.fulfill(status=200,headers=h,body=json.dumps(каталог()))
        if '/api/picked' in u:
            ids=[s for s in r.request.url.split('ids=')[1].split('&')[0].replace('%3A',':').replace('%2C',',').split(',') if s]
            return r.fulfill(status=200,headers=h,body=json.dumps(picked(ids)))
        if 'fonts.g' in u: return r.abort()
        r.continue_()
    pg.route('**/*', route)

    print('ГЛАВНАЯ')
    pg.goto(БАЗА+'/', wait_until='load'); pg.wait_for_timeout(2500)
    ок(pg.locator('.row').count()>=30, 'лента отрисована: %d строк'%pg.locator('.row').count())
    ок(pg.locator('.row.lvl2').count()>0, 'важные новости с рамкой: %d'%pg.locator('.row.lvl2').count())
    ок(pg.locator('.fband').count()>0, 'плашки на месте: %d'%pg.locator('.fband').count())
    ок(pg.locator('.srcs').count()>0, 'пометка «несколько источников» есть')
    ок(pg.locator('.chip').count()>=3, 'фильтры собраны: %d'%pg.locator('.chip').count())

    print('ФИЛЬТР И ПОИСК')
    pg.locator('.chip.imp').first.click(); pg.wait_for_timeout(400)
    n_imp=pg.locator('.row:visible').count()
    ок(0<n_imp<40, 'фильтр «Важно» сузил ленту до %d'%n_imp)
    pg.locator('.chip').first.click(); pg.wait_for_timeout(300)
    pg.fill('#q','номер 7'); pg.wait_for_timeout(500)
    ок(pg.locator('.row:visible').count()==1, 'живой поиск нашёл одну новость')
    pg.fill('#q',''); pg.wait_for_timeout(300)

    print('ВЕРСИЯ')
    ок(pg.locator('#ver').inner_text().startswith('v'), 'метка версии: '+pg.locator('#ver').inner_text())
    pg.hover('#ver'); pg.wait_for_timeout(500)
    ок(pg.locator('.verbox.on').count()==1, 'окошко изменений открылось')
    ок('до этого' in pg.locator('.verbox').inner_text(), 'в окошке есть прошлая версия')
    pg.mouse.move(700,600); pg.wait_for_timeout(600)
    ок(pg.locator('.verbox.on').count()==0, 'окошко закрылось при уходе мыши')

    print('ПАНЕЛЬ КОТИРОВОК')
    pg.click('#sideBtn'); pg.wait_for_timeout(1200)
    ок('side-on' in pg.get_attribute('body','class'), 'панель открылась')
    ок(pg.locator('.qrow').count()>=5, 'котировки отрисованы: %d'%pg.locator('.qrow').count())
    ок(pg.locator('#smood').is_visible(), 'настроение рынка показано')
    ок(pg.locator('.sh .sbeta').count()==1, 'пометка беты в заголовке')

    print('НАСТРОЙКА НАБОРА')
    pg.click('#sideEdit'); pg.wait_for_timeout(1500)
    ок('side-edit' in pg.get_attribute('body','class'), 'режим настройки включился')
    ок(pg.locator('#sedit').is_visible(), 'поле поиска показано')
    ок(pg.locator('.qrow .qx').first.is_visible(), 'крестики у строк появились')
    before=pg.locator('.qrow').count()
    pg.fill('#sq','офз'); pg.wait_for_timeout(600)
    ок(pg.locator('.sres .si').count()>=1, 'поиск нашёл инструмент')
    pg.locator('.sres .si').first.click(); pg.wait_for_timeout(1500)
    ок(pg.locator('.qrow').count()>=1, 'после добавления строки есть: %d'%pg.locator('.qrow').count())
    ок(pg.locator('.qy').count()>=1, 'у облигации показана доходность в скобках')
    ок(pg.locator('.qy').evaluate('e=>getComputedStyle(e).fontStyle')=='normal', 'доходность без курсива')
    before=pg.locator('.qrow').count()
    pg.locator('.qrow .qx').first.click(); pg.wait_for_timeout(1500)
    ок(pg.locator('.qrow').count()==before-1, 'крестик убрал строку')
    ок(pg.locator('#sClear').count()==1 and pg.locator('#sShare').count()==1 and pg.locator('#sDone').count()==0,
       'кнопки: по умолчанию, очистить всё, поделиться (без «готово»)')
    ок(pg.evaluate("JSON.parse(localStorage.getItem('ff-pick-v1')||'[]').length")>0, 'набор сохранён в браузере')
    pg.click('#sReset'); pg.wait_for_timeout(1500)
    ок(pg.evaluate("localStorage.getItem('ff-pick-v1')") is None, '«по умолчанию» очистило сохранённый набор')

    print('ПРОСЬБА ОБНОВИТЬ')
    заголовок['X-Page-Version']='другая-версия'
    pg.wait_for_timeout(3500)
    ок(pg.locator('#updbar.on').count()==1, 'плашка обновления показалась при смене версии')
    pg.click('#updX'); pg.wait_for_timeout(300)
    ок(pg.locator('#updbar.on').count()==0, 'крестик убрал плашку')
    заголовок['X-Page-Version']=None

    print('ТЕЛЕФОН')
    m=b.new_context(viewport={'width':390,'height':844},is_mobile=True,has_touch=True,service_workers='block')
    mp=m.new_page(); mp.route('**/*',route); mошибки=[]; mp.on('pageerror',lambda e: mошибки.append(str(e)[:200]))
    mp.goto(БАЗА+'/',wait_until='load'); mp.wait_for_timeout(2500)
    ок(mp.locator('.row').count()>=30, 'лента на телефоне отрисована')
    w=mp.evaluate('document.documentElement.scrollWidth'); ок(w<=390, 'нет горизонтальной прокрутки (ширина %d)'%w)
    ок(mp.locator('#instBtn').count()==1, 'кнопка ярлыка присутствует в разметке')
    hh=mp.locator('header').bounding_box()['height']; ок(38<=hh<=44, 'высота шапки на телефоне: %d'%hh)
    m.close()

    print('СТРАНИЦА /pick')
    pp=ctx.new_page(); pp.route('**/*',route); pп=[]; pp.on('pageerror',lambda e: pп.append(str(e)[:200]))
    pp.goto(БАЗА+'/pick',wait_until='load'); pp.wait_for_timeout(2000)
    ок(pp.locator('.grp').count()>=1, 'разделы отрисованы: %d'%pp.locator('.grp').count())
    ок(pp.locator('.disc .beta').count()==1, 'пометка беты и оговорка на месте')
    pp.fill('#q','газ'); pp.wait_for_timeout(500)
    ок(pp.locator('.res.on .it').count()>=1, 'поиск на странице работает')
    ок(not pп, 'JS-ошибок на /pick нет')

    print('СТРАНИЦА /stats')
    sp=ctx.new_page(); sп=[]; sp.on('pageerror',lambda e: sп.append(str(e)[:200]))
    sp.route('**/fonts.g*', lambda r: r.abort())
    sp.goto(БАЗА+'/stats',wait_until='load'); sp.wait_for_timeout(2500)
    ок(sp.locator('.tile').count()==20, 'плиток статистики: %d'%sp.locator('.tile').count())
    ок(sp.locator('#hours div').count()>=24, 'график по часам отрисован')
    ок(not sп, 'JS-ошибок на /stats нет')

    ок(not ош, 'JS-ошибок на главной нет'+(': '+'; '.join(ош[:3]) if ош else ''))
    b.close()

print()
print('ПРОВАЛОВ: %d'%len(ПРОВАЛОВ) if ПРОВАЛОВ else 'ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ')
sys.exit(1 if ПРОВАЛОВ else 0)
