# This Python file uses the following encoding: utf-8
import sys
import csv
from pymongo import Connection
from collections import Counter, defaultdict
import datetime


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = Connection(host=main_db_host)
db = conn.getmyad_db
cursor = db.clicks.find({'campaignId': {'$in': [
    '6a5d98a8-e3c3-4566-8e94-2dfac0661478',
    '15b47392-6009-4144-aa5f-6c11bb1d8fe2',
    'f2b5a763-1ba7-4843-abd7-6ca76ee62c08',
    'd859761a-05d0-4c89-88ad-7404d5c39d40',
    'a73f61a5-fd70-44df-a74e-80fe1ea1608a',
    '05d72032-86ca-49b8-bb11-b9aa311fb0ab',
    '82f738cf-922e-40f3-bcbe-c5782e2e69a2',
    '99457768-0dfe-4b28-8f5b-e75d9590a0fd',
    '39ebb35a-f531-464d-b983-f49aec0e94ec']
    }
    }
)
inf = {}
for block in db.informer.find({}, {'guid': 1, 'user': 1, 'domain': 1, 'title': 1}):
    inf[block['guid']] = block


clicks = defaultdict(lambda: defaultdict(int))
for click in cursor:
    data = datetime.datetime.fromordinal(click['dt'].toordinal())
    dt = data.strftime("%Y-%m-%d")
    guid = click['inf']
    clicks[dt][guid] +=1


with open('dt-block-count.csv', "wb") as file:
    writer = csv.writer(file, delimiter=',')
    for key, val in clicks.iteritems():
        dt = key
        for k,v in val.iteritems():
            block = inf[k]
            writer.writerow([dt,
                             k,
                             v,
                             block['user'].encode('utf-8'),
                            block['domain'].encode('utf-8'),
                             block['title'].encode('utf-8')])

# with open('view_number.csv', "wb") as file:
#     writer = csv.writer(file, delimiter=',')
#     c = Counter(show_inf)
#     for row in c.most_common():
#         block = inf[row[0]]
#         writer.writerow([block['user'].encode('utf-8'), block['domain'].encode('utf-8'), block['title'].encode('utf-8'), block['guid'],  row[1]])
