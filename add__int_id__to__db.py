# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient
from uuid import UUID
from collections import defaultdict

sys.stdout = sys.stderr

d = defaultdict(long)
main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db
db2 = conn.db2


def uuid_to_long(uuid):
    return long(UUID(uuid.encode('utf-8')).int >> 64 & ((1 << 64) / 2) - 2)

pipeline = [
    {'$group': {
        '_id': {'adv': '$adv', 'campaignId': '$campaignId', 'guid': '$guid'},
        'ids': {'$push':  {'id': "$_id"}}
    },
    }
]

cur = db.stats_daily.rating.aggregate(pipeline=pipeline, cursor={}, allowDiskUse=True)
count = 0
for doc in cur:
    if len(doc['ids']) == 2:
        try:
            first = db.stats_daily.rating.find_one({'_id': doc['ids'][0]['id']})
            second = db.stats_daily.rating.find_one({'_id': doc['ids'][1]['id']})
            if first and second:
                full_impressions = int(second.get('full_impressions', 0)) + int(first.get('full_impressions', 0))
                impressions = int(second.get('impressions', 0)) + int(first.get('impressions', 0))
                old_impressions = int(second.get('old_impressions', 0)) + int(first.get('old_impressions', 0))
                clicks = int(second.get('clicks', 0)) + int(first.get('clicks', 0))
                full_clicks = int(second.get('full_clicks', 0)) + int(first.get('full_clicks', 0))
                old_clicks = int(second.get('old_clicks', 0)) + int(first.get('old_clicks', 0))
                f_rating = first.get('rating', 0)
                f_full_rating = first.get('full_rating', 0)
                s_rating = second.get('rating', 0)
                s_full_rating = second.get('full_rating', 0)
                if uuid_to_long(second['guid']) == long(second['guid_int']):
                    print 'S', second['guid_int'], '-', first['guid_int'], '-', second['guid'], '-', first['guid']
                    second['full_impressions'] = full_impressions
                    second['impressions'] = impressions
                    second['old_impressions'] = old_impressions
                    second['clicks'] = clicks
                    second['full_clicks'] = full_clicks
                    second['old_clicks'] = old_clicks
                    second['rating'] = f_rating
                    second['full_rating'] = f_full_rating
                    db.stats_daily.rating.save(second)
                    db.stats_daily.rating.delete_one({'_id': first['_id']})

                elif uuid_to_long(first['guid']) == long(first['guid_int']):
                    print 'F', first['guid_int'], '-', second['guid_int'], '-', first['guid'], '-', second['guid']
                    first['full_impressions'] = full_impressions
                    first['impressions'] = impressions
                    first['old_impressions'] = old_impressions
                    first['clicks'] = clicks
                    first['full_clicks'] = full_clicks
                    first['old_clicks'] = old_clicks
                    first['rating'] = s_rating
                    first['full_rating'] = s_full_rating
                    db.stats_daily.rating.save(first)
                    db.stats_daily.rating.delete_one({'_id': second['_id']})
                else:
                    count +=1
                    pass
                    #print first, second
        except Exception as e:
            print e


print count

# cur = db.domain.find({})
# for doc in cur:
#     old = db2.domain.find_one({'login': doc['login']})
#     if old is None:
#         print doc

# cur = db.advertise.category.find({}, {'guid': 1, '_id': 0})
# for doc in cur:
#     db.advertise.category.update({'guid': doc['guid']},
#                                  {'$set': {
#                                      'guid_int': uuid_to_long(doc['guid'])
#                                  }}
#                                  )

# cur = db['users'].find({}, {'guid': 1, '_id': 0})
# for doc in cur:
#     db['users'].update({'guid': doc['guid']},
#                                  {'$set': {
#                                      'guid_int': uuid_to_long(doc['guid'])
#                                  }}
#                                  )


# cur = db['domain'].find({})
# for doc in cur:
#     nd = {}
#     for k, v in doc.get('domains', {}).iteritems():
#         nd[str(uuid_to_long(k))] = v
#     db['domain'].update({'login': doc['login']},
#                         {'$set': {
#                             'domains_int': nd
#                         }}
#                         )


# cur = db['domain'].find({})
# for doc in cur:
#     for k, v in doc.get('domains_int', {}).iteritems():
#         d[long(k)] += 1
#         if long(k) >= 9223372036854775807:
#             print k


# cur = db['users'].find({}, {'guid_int': 1, '_id': 0})
# for doc in cur:
#     d[doc['guid_int']] += 1
#     if long(doc['guid_int']) >= 9223372036854775807:
#         print doc['guid_int']

# cur = db['informer'].find({}, {'guid': 1, '_id': 0})
# for doc in cur:
#     guid_int = uuid_to_long(doc['guid'])
#     d[guid_int] += 1
#     if long(guid_int) >= 9223372036854775807:
#         print guid_int
#
# result = max(d.iteritems(), key=lambda x: x[1])
# print result