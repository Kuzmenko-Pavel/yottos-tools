# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient
from uuid import UUID
from collections import defaultdict

sys.stdout = sys.stderr

d = defaultdict(lambda: defaultdict(int))
main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db
db2 = conn.db2


def uuid_to_long(uuid):
    return long(UUID(uuid.encode('utf-8')).int >> 64 & ((1 << 64) / 2) - 2)


cur = db2.offer.find({'retargeting': False})
for doc in cur:
    d[doc['guid']]['full_impressions'] = doc.get('impressions', 0)
    d[doc['guid']]['full_clicks'] = doc.get('clicks', 0)
    d[doc['guid']]['hash'] = doc.get('hash', 0)

c = 0
cur = db.offer.find({'retargeting': False})
for doc in cur:
    if doc['guid'] in d:
        if doc['hash'] != d[doc['guid']]['hash']:
            c += 1
            # db.offer.update(
            #     {'guid': doc['guid']},
            #     {
            #         '$inc': {
            #             'full_impressions': d[doc['guid']]['full_impressions'],
            #             'full_clicks': d[doc['guid']]['full_clicks'],
            #         }
            #     },
            #     upsert=False
            # )

print c
