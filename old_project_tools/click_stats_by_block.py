# This Python file uses the following encoding: utf-8
import os
import sys

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import datetime
from collections import defaultdict
import sys
from pymongo import MongoClient
import pymssql

sys.stdout = sys.stderr


def mssql_connection_adload():
    """

    Returns:

    """
    pymssql.set_max_connections(450)
    conn = pymssql.connect(host='srv-3.yottos.com',
                           user='web',
                           password='odif8duuisdofj',
                           database='AdLoad',
                           as_dict=True,
                           charset='cp1251')
    conn.autocommit(True)
    return conn


accounts = defaultdict(lambda: 'NOT TITLE')
connection_adload = mssql_connection_adload()
cursor = connection_adload.cursor()
cursor.execute('''SELECT lower([UserID]) as UserID, [Login] FROM [AdLoad].[dbo].[Users]''')
for row in cursor:
    accounts[row['UserID']] = row['Login'].decode('utf-8')
cursor.close()

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db
informersBySite = {}
informersByTitle = {}
for informer in db.informer.find({},
                                 {'guid': True, 'domain': True, 'admaker': True, 'user': True, 'title': True}):
    try:
        userGuid = db.users.find_one({"login": informer.get('user', 'NOT DOMAIN')}, {'guid': 1, '_id': 0})
        informersBySite[informer['guid']] = informer.get('domain', 'NOT DOMAIN')
        informersByTitle[informer['guid']] = informer.get('title', 'NOT DOMAIN')
    except:
        pass

campaigns = defaultdict(lambda: 'NOT TITLE')
campaigns_by_account = defaultdict(lambda: 'NOT_TITLE')

for item in db.campaign.find({}, {'guid': True, 'title': True, 'account': True, 'yottosHideSiteMarker': True}):
    campaigns[item.get('guid')] = item.get('title')
    campaigns_by_account[item.get('guid')] = item.get('account')

for item in db.campaign.archive.find({}, {'guid': True, 'title': True, 'account': True, 'yottosHideSiteMarker': True}):
    campaigns[item.get('guid')] = item.get('title')
    campaigns_by_account[item.get('guid')] = item.get('account')

pipeline = [
    {'$match':
        {
            'dt': {'$gte': datetime.datetime(2019, 5, 1, 0, 0), '$lt': datetime.datetime(2019, 5, 7, 0, 0)},
            'getmyad_user_id': '8b1e69b8-8f20-11e7-b157-002590d97638'
        }
    },
    {'$group':
        {
            '_id': {
                'adv': '$inf',
                'campaign': '$campaignId',
            },
            'count': {'$sum': 1}
        }
    }
]
cursor = db.clicks.aggregate(pipeline=pipeline, cursor={})
stats = defaultdict(int)
for doc in cursor:
    adv = doc['_id']['adv']
    campaign = doc['_id']['campaign']
    print('%s\t%s\t%s\t%s\t%s' % (informersBySite.get(adv),
                                  informersByTitle.get(adv),
                                  accounts.get(campaigns_by_account.get(campaign)),
                                  campaigns.get(campaign),
                                  str(doc['count']))
          )
