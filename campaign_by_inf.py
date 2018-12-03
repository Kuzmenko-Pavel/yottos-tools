# This Python file uses the following encoding: utf-8
import sys

import os
from pymongo import MongoClient
from collections import defaultdict
import datetime

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


informers = defaultdict(lambda: 'NOT TITLE')
for item in db.informer.find({}, {'guid': True, 'domain': True}):
    informers[item.get('guid')] = item.get('domain')


campaignIds = []
for x in db.campaign.find({'status':'working'}, {'guid': 1}):
    campaignIds.append(x['guid'])

campaigns = defaultdict(lambda: 'NOT TITLE')
campaigns_by_account = defaultdict(lambda: 'NOT_TITLE')
for item in db.campaign.find({}, {'guid': True, 'title': True, 'account': True, 'yottosHideSiteMarker': True}):
    campaigns[item.get('guid')] = item.get('title')
    campaigns_by_account[item.get('guid')] = item.get('account')

for item in db.campaign.archive.find({}, {'guid': True, 'title': True, 'account': True, 'yottosHideSiteMarker': True}):
    campaigns[item.get('guid')] = item.get('title')
    campaigns_by_account[item.get('guid')] = item.get('account')

date = datetime.datetime.now()
date_start = datetime.datetime(date.year, date.month, date.day, 0, 0)
date_end = date_start - datetime.timedelta(days=1)


pipeline = [
    {'$match':
        {
            'dt': {'$gte': date_end, '$lt': date_start},
            'campaignId': {'$in': campaignIds},
            'adload_cost': {'$gt': 0},
            'adload_manager': 'Рома',
        }
    },
    {'$group':
         {
             '_id': {
                 # 'campaign': '$campaignId',
                 'adv': '$inf'
             },
             'count': {
              '$sum': 1
             }
            ,
          }
    },
    {'$sort': {"count": -1}}
]
cursor = db.clicks.aggregate(pipeline=pipeline, allowDiskUse=True, useCursor=True)

for doc in cursor:
    adv = doc['_id']['adv']
    adv_t = informers[adv]
    campaign = ''#doc['_id']['campaign']
    campaign_t = campaigns[campaign]
    count = doc['count']
    print('%s %s %s %s %s' % (adv, adv_t, campaign, campaign_t, count))