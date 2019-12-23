# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient
from collections import defaultdict


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


# informers = defaultdict(int)
offers = defaultdict(int)

# for item in db.informer.find({}, {'guid': True, 'guid_int': True}):
#     informers[item.get('guid')] = item.get('guid_int')

for item in db.offer.find({}, {'guid': True, 'guid_int': True}):
    offers[item.get('guid')] = item.get('guid_int')


# for k, v in informers.iteritems():
#     result = db.stats_daily.rating.update_many({'adv': k}, {'$set': {'adv_int': v}})
#     print(result.matched_count)
for k, v in offers.iteritems():
    result = db.stats_daily.rating.update_many({'guid': k}, {'$set': {'guid_int': v}})
    print(result.matched_count)