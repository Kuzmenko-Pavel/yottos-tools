# encoding: utf-8
# This Python file uses the following encoding: utf-8
import datetime
import pymssql
import uuid

import dateutil.parser
import pymongo

MONGO_HOST = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

MONGO_WORKER_HOST_POOL = ['srv-2.yottos.com:27017']

MONGO_DATABASE = 'getmyad_db'
MONGO_WORKER_DATABASE = 'getmyad'
otype = type


def _mongo_connection(host):
    u"""Возвращает Connection к серверу MongoDB"""
    try:
        connection = pymongo.MongoClient(host=host)
    except pymongo.errors.AutoReconnect:
        # Пауза и повторная попытка подключиться
        from time import sleep
        sleep(1)
        connection = pymongo.MongoClient(host=host)
    return connection


def _mongo_main_db():
    u"""Возвращает подключение к базе данных MongoDB"""
    return _mongo_connection(MONGO_HOST)[MONGO_DATABASE]


def _mongo_worker_db_pool():
    """Возвращает подключение к базе данных MongoDB Worker"""
    pool = []
    # now = datetime.datetime.now()
    # first_db = 'rg_%s' % now.hour
    # second_db = 'rg_%s' % (now - datetime.timedelta(minutes=60)).hour
    # mongo_worker_database = list([first_db, second_db])
    # mongo_worker_database.append('getmyad')
    mongo_worker_database = list(['getmyad_log', ])
    for host in MONGO_WORKER_HOST_POOL:
        try:
            for base_name in mongo_worker_database:
                pool.append(_mongo_connection(host)[base_name])
        except Exception as e:
            print(e, host)
    return pool


def mssql_connection_adload():
    pymssql.set_max_connections(450)
    conn = pymssql.connect(host='srv-3.yottos.com',
                           user='web',
                           password='odif8duuisdofj',
                           database='Adload',
                           as_dict=True,
                           charset='cp1251')
    conn.autocommit(True)
    return conn


def get_currency_cost(currency):
    """ Возвращает курс валюты ``currency``.

        Если валюта не найдена, вернёт 0.
    """
    connection_adload = mssql_connection_adload()
    with connection_adload.cursor(as_dict=True) as cursor:
        cursor.execute('SELECT g.cost as cost  FROM GetMyAd_CurrencyCost as g WHERE currency=%s', (currency,))
        row = cursor.fetchone()
        if row is None:
            return 0.0
        return float(row['cost'])


def add_click(offer_id, campaign_id, click_datetime=None, social=None, cost_percent_click=None):
    """ Запись перехода на рекламное предложение ``offer_id`` с адреса
        ``ip`.

        ``offer_id``
            GUID рекламного предложения.

        ``ip``
            IP посетителя, сделавшего клик.
        
        ``click_datetime``
            Дата и время, за которую будет записан клик. По умолчанию
            принимается за текущее время. Если передаётся, то должно
            быть сторокой в ISO формате (YYYY-MM-DDTHH:MM:SS[.mmmmmm]).

        Возвращает структуру следующего формата::
            
            {'ok': Boolean,     # Успешно ли выполнилась операция
             'error': String,   # Описание ошибки, если ok == False
             'cost': Decimal    # Сумма, списанная с рекламодателя (в $)
            }
    """
    try:
        uuid.UUID(offer_id)
    except ValueError, ex:
        print ex
        return {'ok': False, 'error': 'offer_id should be uuid string! %s' % ex}

    try:
        dt = dateutil.parser.parse(click_datetime)
    except (ValueError, AttributeError):
        dt = datetime.datetime.now()

    if social is None:
        social = False

    if cost_percent_click is None:
        cost_percent_click = 100
    try:
        connection_adload = mssql_connection_adload()
        click_cost = 0.0

        # Записываем переход
        print "Записываем переход"
        social = int(social)
        try:
            with connection_adload.cursor(as_dict=True) as cursor:
                cursor.callproc('ClickAdd', (offer_id, campaign_id, None, dt, social, cost_percent_click))
                for row in cursor:
                    print row
                    click_cost = float(row.get('ClickCost', 0.0))
        except Exception as ex:
            print ex
            return {'ok': False, 'error': str(ex)}

        # Пересчёт стоимости клика по курсу
        print "Пересчёт стоимости клика по курсу"
        if not social:
            currency_cost = get_currency_cost('$')
            if currency_cost > 0:
                click_cost /= currency_cost
        else:
            click_cost = 0.0
        if not social and click_cost == 0.0:
            return {'ok': False, 'error': "adload click cost 0"}
        print "Offer: %s Cost %s" % (offer_id, click_cost)
        return {'ok': True, 'cost': click_cost}

    except Exception, ex:
        print ex
        return {'ok': False, 'error': str(ex)}


def _partner_click_cost(db, informer_id, adload_cost):
    """ Возвращает цену клика для сайта-партнёра.

        ``informer_id``
            ID информера, по которому произошёл клик.

        ``adload_cost``
            Цена клика для рекламодателя. Используется в случае плавающей
            цены.
    """
    try:
        user = db.informer.find_one({'guid': informer_id}, ['user', 'cost'])
        if user.get('cost', 'None') == 'None':
            user_cost = db.users.find_one({'login': user['user']}, {'cost': 1, '_id': 0})
            percent = int(user_cost.get('cost', {}).get('ALL', {}).get('click', {}).get('percent', 50))
            cost_min = float(user_cost.get('cost', {}).get('ALL', {}).get('click', {}).get('cost_min', 0.01))
            cost_max = float(user_cost.get('cost', {}).get('ALL', {}).get('click', {}).get('cost_max', 1.00))
            print "Account COST percent %s, cost_min %s, cost_max %s" % (percent, cost_min, cost_max)
        else:
            percent = int(user.get('cost', {}).get('ALL', {}).get('click', {}).get('percent', 50))
            cost_min = float(user.get('cost', {}).get('ALL', {}).get('click', {}).get('cost_min', 0.01))
            cost_max = float(user.get('cost', {}).get('ALL', {}).get('click', {}).get('cost_max', 1.00))
            print "Informer COST percent %s, cost_min %s, cost_max %s" % (percent, cost_min, cost_max)

        cost = round(adload_cost * percent / 100, 2)
        if cost_min and cost < cost_min:
            cost = cost_min
        if cost_max and cost > cost_max:
            cost = cost_max
    except Exception as e:
        print e
        cost = 0
    return cost


def process_click(url,
                  ip,
                  click_datetime,
                  offer_id,
                  campaign_id,
                  informer_id,
                  token,
                  valid,
                  referer,
                  user_agent,
                  cookie,
                  view_seconds,
                  branch,
                  conformity,
                  social,
                  request):
    print "/----------------------------------------------------------------------/"
    print "process click %s \t %s" % (ip, click_datetime)
    if not isinstance(click_datetime, datetime.datetime):
        print(type(click_datetime), click_datetime)
        click_datetime = datetime.datetime.strptime(click_datetime, '%Y-%m-%dT%H:%M:%S.%f')

    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()

    def log_error(reason):
        db.clicks.error.insert_one({'ip': ip, 'offer': offer_id, 'dt': click_datetime, 'token': token,
                                    'inf': informer_id, 'url': url, 'reason': reason,
                                    'error_id': error_id, 'campaignId': campaign_id, 'referer': referer,
                                    'user_agent': user_agent, 'cookie': cookie, 'view_seconds': view_seconds}
                                   )

    def log_reject(reason):
        db.clicks.rejected.insert_one({'ip': ip, 'offer': offer_id, 'dt': click_datetime, 'token': token,
                                       'inf': informer_id, 'url': url, 'reason': reason,
                                       'error_id': error_id, 'campaignId': campaign_id, 'referer': referer,
                                       'user_agent': user_agent, 'cookie': cookie, 'view_seconds': view_seconds}
                                      )

    # С тестовыми кликами ничего не делаем
    check_block = {
        'block': False,
        'filter': False,
        'time_filter_click': 15,
        'cost_percent_click': 100
    }
    account_id = ''
    error_id = 0
    manager = ''
    manager_g = ''
    getmyad_user_id = ''

    try:
        campaign = db.campaign.find_one({'guid': campaign_id}, {'manager': True, 'account': True, '_id': False})
        account_id = campaign.get('account', '')
        manager = campaign.get('manager', '').encode('utf-8')
        informer = db.informer.find_one({'guid': informer_id}, {'user': True, '_id': False})
        account_g = db.users.find_one({'login': informer['user']}, {'managerGet': 1,
                                                                    'blocked': 1,
                                                                    'cost_percent_click': 1,
                                                                    'time_filter_click': 1,
                                                                    'guid': 1,
                                                                    '_id': 0})
        getmyad_user_id = account_g.get("guid", "")
        manager_g = account_g.get("managerGet", "")
        check_block['cost_percent_click'] = account_g.get('cost_percent_click', 100)
        check_block['time_filter_click'] = account_g.get('time_filter_click', 15)
    except Exception as e:
        print e

    try:
        print "Token = %s" % token.encode('utf-8')
    except Exception as e:
        print e
    try:
        print "Cookie = %s" % cookie
    except Exception as e:
        print e
    try:
        print "IP = %s" % ip
    except Exception as e:
        print e
    try:
        print "REFERER = %s" % referer.encode('utf-8')
    except Exception as e:
        print e
    try:
        print "USER AGENT = %s" % user_agent.encode('utf-8')
    except Exception as e:
        print e
    try:
        print "OfferId = %s" % offer_id.encode('utf-8')
    except Exception as e:
        print e
    try:
        print "Informer = %s" % informer_id.encode('utf-8')
    except Exception as e:
        print e
    try:
        print "CampaignId = %s" % campaign_id.encode('utf-8')
    except Exception as e:
        print e
    try:
        print "Branch = %s" % branch
    except Exception as e:
        print e
    try:
        print "User View-Click = %s" % view_seconds
    except Exception as e:
        print e
    try:
        print "Manager = %s" % manager
    except Exception as e:
        print e
    try:
        print "Manager G = %s" % manager_g
    except Exception as e:
        print e

    adload_cost = 0
    cost = 0
    try:
        print "Adload request"
        adload_response = add_click(offer_id, campaign_id, click_datetime.isoformat(), social, check_block['cost_percent_click'])
        adload_ok = adload_response.get('ok', False)
        print "Adload OK - %s" % adload_ok
        if not adload_ok and 'error' in adload_response:
            error_id = 0
            log_error('Adload вернул ошибку: %s' %
                      adload_response['error'])
        adload_cost = adload_response.get('cost', 0)
        print "Adload COST %s" % adload_cost
    except Exception, ex:
        adload_ok = False
        error_id = 0
        log_error(u'Ошибка при обращении к adload: %s' % str(ex))
        print "adload failed"
    # Сохраняем клик в GetMyAd
    click_obj = {"ip": ip,
                 "offer": offer_id,
                 "campaignId": campaign_id,
                 "dt": click_datetime,
                 "inf": informer_id,
                 "account_id": account_id,
                 "getmyad_user_id": getmyad_user_id,
                 "unique": True,
                 "cost": cost,
                 "adload_cost": adload_cost,
                 "income": adload_cost - cost,
                 "url": url,
                 "branch": branch,
                 "conformity": conformity,
                 "social": social,
                 "referer": referer,
                 "user_agent": user_agent,
                 "cookie": cookie,
                 "adload_manager": manager,
                 "getmyad_manager": manager_g,
                 "view_seconds": view_seconds,
                 "request": request
                 }
    if not social and adload_ok:
        cost = _partner_click_cost(db, informer_id, adload_cost)
        print "Payable click at the price of %s" % cost
        click_obj['cost'] = cost
        click_obj['income'] = adload_cost - cost
    elif social and adload_ok:
        print "Social click"
    else:
        print "No click"
        return False
    db.clicks.insert_one(click_obj)

    print "Click complite"
    print "/----------------------------------------------------------------------/"
    return True
