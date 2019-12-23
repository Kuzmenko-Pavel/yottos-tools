# This Python file uses the following encoding: utf-8
import sys

import os

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import sys
from pymongo import Connection
from datetime import datetime


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


conn = Connection(host=main_db_host)
db = conn.getmyad_db
acc = '3e23c722-7446-4723-8985-b2fe120ac3a2'
block = '04639798-67a1-11e5-9a3c-002590d590d0'
start_day = datetime(2017, 3, 1, 0, 0)
stop_day = datetime(2017, 3, 11, 0, 0)

pipeline = [
    {'$match':
         {'account_id': acc,
          'inf': block,
          'dt': {'$gte': start_day, '$lt': stop_day},
          }
     },
    {'$group':
         {'_id': {
             "day": {"$dayOfMonth": "$dt"},
             'campaignId': '$campaignId',
             'campaignTitle': '$campaignTitle',
             'getmyad_manager': '$getmyad_manager',
             'adload_manager': '$adload_manager',
         },
             "d": {"$last": "$dt"},
             'count': {'$sum': 1},
             'adload_cost': {'$sum': '$adload_cost'},
             'cost': {'$sum': '$cost'},
             'income': {'$sum': '$income'}
         }
     },
    {'$sort':
         {'d': 1}
     }
]

cursor = db.clicks.aggregate(pipeline=pipeline, cursor={})
count = 0
for doc in cursor:

    print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (doc['_id']['day'],
                                    doc['_id']['campaignId'],
                                    doc['_id']['campaignTitle'],
                                    doc['_id']['getmyad_manager'],
                                    doc['_id']['adload_manager'],
                                    doc['count'],
                                    doc['adload_cost'],
                                    doc['cost'],
                                    doc['income'])
    count += doc['count']
print count