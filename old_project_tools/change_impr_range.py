# This Python file uses the following encoding: utf-8
import sys
import datetime
from pymongo import MongoClient


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

date_start = datetime.datetime(2019, 4, 1, 0, 0)
date_end = datetime.datetime(2019, 5, 1, 0, 0)
q = {
    'adv':'42deb032-4566-11e5-a9a8-002590d97638',
    'date':{
        '$gte': date_start,
        '$lt': date_end
    }
}

count = 0

for item in db['stats.daily.adv'].find(q):
    count += item.get('impressions_block')

print count

percent = 1300000 / (count / 100.0)

c = 0

for item in db['stats.daily.adv'].find(q):
    impressions_block = item.get('impressions_block')
    if impressions_block:
        c += int((impressions_block/100.0) * (100 - percent))
        impressions_block = int((impressions_block/100.0) * (100 - percent))
        # db['stats.daily.adv'].update_one({'_id': item.get('_id')}, {'$set': {'impressions_block': impressions_block}})


print c

q = {
    'domain':'eurointegration.com.ua',
    'date':{
        '$gte': date_start,
        '$lt': date_end
    }
}

count = 0

for item in db['stats.daily.domain'].find(q):
    count += item.get('impressions_block')

print count

percent = 1300000 / (count / 100.0)

c=0

for item in db['stats.daily.domain'].find(q):
    impressions_block = item.get('impressions_block')
    if impressions_block:
        c += int((impressions_block/100.0) * (100 - percent))
        impressions_block = int((impressions_block/100.0) * (100 - percent))
        # db['stats.daily.domain'].update_one({'_id': item.get('_id')}, {'$set': {'impressions_block': impressions_block}})

print c


q = {
    'user':'eurointegration.com.ua',
    'date':{
        '$gte': date_start,
        '$lt': date_end
    }
}

count = 0

for item in db['stats.daily.user'].find(q):
    count += item.get('impressions_block')

print count

percent = 1300000 / (count / 100.0)

c=0

for item in db['stats.daily.user'].find(q):
    impressions_block = item.get('impressions_block')
    if impressions_block:
        c += int((impressions_block / 100.0) * (100 - percent))
        impressions_block = int((impressions_block/100.0) * (100 - percent))
        # db['stats.daily.user'].update_one({'_id': item.get('_id')}, {'$set': {'impressions_block': impressions_block}})

print c
