# -*- coding: utf-8 -*-
from pymongo import MongoClient
import datetime

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


def calc(db, dateS, dateE):
    print('%s - %s' % (dateS, dateE))

    pipeline = [
        {'$match':
             {'date': {'$gte': dateS, '$lt': dateE}, 'impressions': {'$gte': 0}}
         },
        {'$group': {
            '_id': '$ip',
            'i_b': {'$sum': {"$cond": [{"$gt": ["$impressions_block", 0]}, 1, 0]}},
            'i_b_v': {'$sum': {"$cond": [{"$gt": ["$impressions_block_valid", 0]}, 1, 0]}},
            'i_b_n_v': {'$sum': {"$cond": [{"$gt": ["$impressions_block_not_valid", 0]}, 1, 0]}},
            'i_n_v_v_b': {'$sum': {"$cond": [
                {'$and': [
                    {"$lt": ["$impressions_block_valid", 1]},
                    {"$gt": ["$impressions_block_not_valid", 0]}
                ]},
                1,
                0
            ]}},
            'i_v_v_b': {'$sum': {"$cond": [
                {'$and': [
                    {"$gt": ["$impressions_block_valid", 0]},
                    {"$lt": ["$impressions_block_not_valid", 1]}
                ]},
                1,
                0
            ]}},
            'i_r': {'$sum': {"$cond": [{"$gt": ["$impressions_retargeting", 0]}, 1, 0]}},
            'i_r_v': {'$sum': {"$cond": [{"$gt": ["$retargeting_impressions", 0]}, 1, 0]}},
            'c': {'$sum': {"$cond": [{"$gt": ["$all_clicks", 0]}, 1, 0]}},
            'b_r': {'$sum': {"$cond": [
                {'$and': [
                    {"$gt": ["$impressions_block", 0]},
                    {"$gt": ["$impressions_retargeting", 0]}
                ]},
                1,
                0
            ]}},
            'r_n_b': {'$sum': {"$cond": [
                {'$and': [
                    {"$lt": ["$impressions_block", 1]},
                    {"$gt": ["$impressions_retargeting", 0]}
                ]},
                1,
                0
            ]}},
            'b_n_r': {'$sum': {"$cond": [
                {'$and': [
                    {"$gt": ["$impressions_block", 0]},
                    {"$lt": ["$impressions_retargeting", 1]}
                ]},
                1,
                0
            ]}}
        },
        },
        {'$group': {
            '_id': None,
            'all': {'$sum':1},
            'user_block': {'$sum': {"$cond": [{"$gt": ["$i_b", 0]}, 1, 0]}},
            'user_block_valid': {'$sum': {"$cond": [{"$gt": ["$i_b_v", 0]}, 1, 0]}},
            'user_block_not_valid': {'$sum': {"$cond": [{"$gt": ["$i_b_n_v", 0]}, 1, 0]}},
            'user_not_view_valid_block': {'$sum': {"$cond": [{"$gt": ["$i_n_v_v_b", 0]}, 1, 0]}},
            'user_view_only_valid_block': {'$sum': {"$cond": [{"$gt": ["i_v_v_b", 0]}, 1, 0]}},
            'user_retargeting': {'$sum': {"$cond": [{"$gt": ["$i_r", 0]}, 1, 0]}},
            'user_view_retargeting': {'$sum': {"$cond": [{"$gt": ["$i_r_v", 0]}, 1, 0]}},
            'clicks': {'$sum': {
                "$cond": [
                    {"$gt": ["$c", 0]},
                    1,
                    0
                ]}
            },
            'user_not_clicks': {'$sum': {"$cond": [{"$lt": ["$c", 1]}, 1, 0]}},
            'user_block_and_retargeting': {'$sum': {"$cond": [{"$gt": ["$b_r", 0]}, 1, 0]}},
            'user_block_only': {'$sum': {"$cond": [{"$gt": ["$b_n_r", 0]}, 1, 0]}},
            'retargeting_only': {'$sum': {"$cond": [{"$gt": ["$r_n_b", 0]}, 1, 0]}},
        },
        }
    ]

    cursor = db.ip.stats.daily.raw.aggregate(pipeline=pipeline, cursor={}, allowDiskUse=True)

    for doc in cursor:
        for k, v in doc.iteritems():
            if v:
                print('%s - %s' % (k, v))


dateS = datetime.datetime(2018, 02, 27, 0, 0)
dateE = datetime.datetime(2018, 04, 20, 0, 0)
calc(db, dateS, dateE)