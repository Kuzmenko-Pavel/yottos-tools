# This Python file uses the following encoding: utf-8
import os
import sys

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import sys
from pymongo import MongoClient
from datetime import datetime
from collections import defaultdict

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db
acc = 'fdf0ae5a-fed5-4195-af8d-a4b14e52498f'
start_day = datetime(2019, 2, 4, 0, 0)
stop_day = datetime(2019, 2, 11, 0, 0)

informers = defaultdict(lambda: 'NOT TITLE')
for item in db.informer.find({}, {'guid': True, 'domain': True}):
    informers[item.get('guid')] = item.get('domain')

campaigns = defaultdict(lambda: 'NOT TITLE')

for item in db.campaign.find({'account': acc}, {'guid': True, 'title': True}):
    campaigns[item.get('guid')] = item.get('title')

for item in db.campaign.archive.find({'account': acc}, {'guid': True, 'title': True}):
    campaigns[item.get('guid')] = item.get('title')

pipeline = [
    {'$match':
         {'account_id': acc,
          'dt': {'$gte': start_day, '$lt': stop_day},
          }
     },
    {'$group':
        {
            '_id': {
            'campaignId': '$campaignId',
            'block': '$inf'
            },
            'click': {
                '$sum': {"$cond": [
                    {"$eq": ["$unique", True]},
                    1,
                    0
                ]
                }
            },
            'all_click': {
                '$sum': 1
            },
            'summa': {
                '$sum': {"$cond": [
                    {"$eq": ["$unique", True]},
                    '$adload_cost',
                    0
                ]
                }
            }
        }
    }
]

data = defaultdict(lambda: defaultdict(lambda: [0, 0, 0]))
cursor = db.clicks.aggregate(pipeline=pipeline, cursor={})
for doc in cursor:
    data[doc['_id']['campaignId']][doc['_id']['block']][0] += int(doc.get('all_click', 0))
    data[doc['_id']['campaignId']][doc['_id']['block']][1] += int(doc.get('click', 0))
    data[doc['_id']['campaignId']][doc['_id']['block']][2] += float(doc.get('summa', 0))

for q,w in data.iteritems():
    print campaigns.get(q)
    for a,s in w.iteritems():
        print "\t%s\t%s\t%s\t%s\t%s" % (informers.get(a), a, s[0], s[1], s[2])