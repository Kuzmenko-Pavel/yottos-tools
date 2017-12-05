# This Python file uses the following encoding: utf-8
import sys

import os

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import datetime
from collections import defaultdict
import urllib
import urlparse
import sys
from pymongo import Connection
import xlwt
import StringIO
import ftplib
import collections

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
font0 = xlwt.Font()
font0.name = 'Times New Roman'
font0.colour_index = 0
font0.height = 360
font0.bold = True
style0 = xlwt.XFStyle()
style0.font = font0

font1 = xlwt.Font()
font1.name = 'Times New Roman'
font1.colour_index = 0
font1.height = 256
font1.bold = False
style1 = xlwt.XFStyle()
style1.font = font1

conn = Connection(host=main_db_host)
db = conn.getmyad_db
informersBySite = {}
informersByItemsNumber = {}
informersByUsers = {}
informersByTitle = {}
informerList = []
for informer in db.informer.find({},
                                 {'guid': True, 'domain': True, 'admaker': True, 'user': True, 'title': True}):
    try:
        userGuid = db.users.find_one({"login": informer.get('user', 'NOT DOMAIN')}, {'guid': 1, '_id': 0})
        domainGuid = None
        for domains in db.domain.find({"login": informer.get('user', 'NOT DOMAIN')}, {'domains': 1, '_id': 0}):
            for key, value in domains['domains'].iteritems():
                if value == informer.get('domain', 'NOT DOMAIN'):
                    domainGuid = key
        informersBySite[informer['guid']] = (informer.get('domain', 'NOT DOMAIN'), domainGuid)
        informersByItemsNumber[informer['guid']] = informer.get('admaker', {}).get('Main', {}).get(
            'itemsNumber', 4)
        informersByUsers[informer['guid']] = (informer.get('user', 'NOT DOMAIN'), userGuid.get('guid', ''))
        informersByTitle[informer['guid']] = informer.get('title', 'NOT DOMAIN')
        informerList.append(informer['guid'])
    except:
        pass

campaign_list = [x['guid'] for x in db.campaign.find({'account':'3e23c722-7446-4723-8985-b2fe120ac3a2'})]
print campaign_list
pipeline = [
    {'$match':
         {
            'dt': {'$gte': datetime.datetime(2017, 11, 14, 0, 0), '$lt': datetime.datetime(2017, 11, 15, 0, 0)},
            'campaignId': {'$in': campaign_list}
         }
     },
    {'$group':
         {'_id': {'adv': '$inf', "day":{"$dayOfMonth": "$dt"}, "month":{"$month": "$dt"}}, 'count': {'$sum': 1}}
     }
]
camp = {}
cursor = db.clicks.aggregate(pipeline=pipeline, cursor={})
stats = defaultdict(int)
for doc in cursor:
    stats[informersBySite[doc['_id']['adv']][0]] = stats[informersBySite[doc['_id']['adv']][0]] + doc['count']
for x, y in stats.iteritems():
    print "%s \t %s " % (x, y)