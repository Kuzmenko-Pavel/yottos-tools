# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient
from datetime import datetime


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

start_day = datetime(2018, 7, 20, 0, 0)
stop_day = datetime(2018, 7, 31, 0, 0)


pipeline = [
    {'$match':
         {'campaignId': '93e2f254-55a7-4d8e-b506-0b2f60e064f2',
          'dt': {'$gte': start_day, '$lt': stop_day},
          }
     },
    {'$group':
         {'_id': {
             'inf': '$inf',
             "day": {"$dayOfMonth": "$dt"},
         },
            "d": {"$last": "$dt"},
            "i": {"$last": "$inf"},
            'count': {'$sum': 1},
         }
     },
    {'$sort':
         {'d': 1, 'i': -1}
     }
]

cursor = db.clicks.aggregate(pipeline=pipeline, cursor={})
for doc in cursor:
    print '%s\t%s\t%s' % (doc['_id']['day'], doc['_id']['inf'], doc['count'])