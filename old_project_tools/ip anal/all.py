# -*- coding: utf-8 -*-
from pymongo import MongoClient
import datetime
from collections import defaultdict

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


def calc(db, dateS, dateE):
    with open('res_all.txt', 'w') as the_file:
        the_file.write('%s - %s' % (dateS, dateE))
        the_file.write("\n")
        pipeline = [
            {'$match':
                 {'date': {'$gte': dateS, '$lt': dateE}}
             },
            {'$group': {
                '_id': '$ip',
                'impressions': {'$sum': '$impressions'},
                'place_impressions': {'$sum': '$place_impressions'},
                'social_impressions': {'$sum': '$social_impressions'},
                'retargeting_impressions': {'$sum': '$retargeting_impressions'},
                'recommended_impressions': {'$sum': '$recommended_impressions'},
                'impressions_block': {'$sum': '$impressions_block'},
                'impressions_block_valid': {'$sum': '$impressions_block_valid'},
                'impressions_block_not_valid': {'$sum': '$impressions_block_not_valid'},
                'impressions_retargeting': {'$sum': '$impressions_retargeting'},
                'all_clicks': {'$sum': '$all_clicks'},
                'unique_clicks': {'$sum': '$unique_clicks'},
                'place_clicks': {'$sum': '$place_clicks'},
                'social_clicks': {'$sum': '$social_clicks'},
                'retargeting_clicks': {'$sum': '$retargeting_clicks'},
                'recommended_clicks': {'$sum': '$recommended_clicks'},
                'impressions_exist': {'$sum': {"$cond": [{"$gt": ["$impressions", 0]}, 1, 0]}},
                'place_impressions_exist': {'$sum': {"$cond": [{"$gt": ["$place_impressions", 0]}, 1, 0]}},
                'social_impressions_exist': {'$sum': {"$cond": [{"$gt": ["$social_impressions", 0]}, 1, 0]}},
                'retargeting_impressions_exist': {'$sum': {"$cond": [{"$gt": ["$retargeting_impressions", 0]}, 1, 0]}},
                'recommended_impressions_exist': {'$sum': {"$cond": [{"$gt": ["$recommended_impressions", 0]}, 1, 0]}},
                'impressions_block_exist': {'$sum': {"$cond": [{"$gt": ["$impressions_block", 0]}, 1, 0]}},
                'impressions_block_valid_exist': {'$sum': {"$cond": [{"$gt": ["$impressions_block_valid", 0]}, 1, 0]}},
                'impressions_block_not_valid_exist': {'$sum': {"$cond": [{"$gt": ["$impressions_block_not_valid", 0]}, 1, 0]}},
                'impressions_retargeting_exist': {'$sum': {"$cond": [{"$gt": ["$impressions_retargeting", 0]}, 1, 0]}},
                'all_clicks_exist': {'$sum': {"$cond": [{"$gt": ["$all_clicks", 0]}, 1, 0]}},
                'unique_clicks_exist': {'$sum': {"$cond": [{"$gt": ["$unique_clicks", 0]}, 1, 0]}},
                'place_clicks_exist': {'$sum': {"$cond": [{"$gt": ["$place_clicks", 0]}, 1, 0]}},
                'social_clicks_exist': {'$sum': {"$cond": [{"$gt": ["$social_clicks", 0]}, 1, 0]}},
                'retargeting_clicks_exist': {'$sum': {"$cond": [{"$gt": ["$retargeting_clicks", 0]}, 1, 0]}},
                'recommended_clicks_exist': {'$sum': {"$cond": [{"$gt": ["$recommended_clicks", 0]}, 1, 0]}},
            },
            }
        ]

        counter = defaultdict(lambda: defaultdict(int))
        Unic = 'Уники'
        Impressions = 'Показы всех предложений'
        Place_impressions = 'Показы предложений места размешения'
        Social_impressions = 'Показы предложений социальные'
        Retargeting_impressions = 'Показы предложений ретаргетинг'
        Recommended_impressions = 'Показы предложений рекомендованные'
        Impressions_block = 'Показы рекламных блоков'
        Impressions_block_valid = 'Валидные показы рекламных блоков'
        Impressions_block_not_valid = 'Не валидные показы рекламных блоков'
        Impressions_retargeting = 'Пометка ретаргетингом'
        All_clicks = 'Все клики'
        Unique_clicks = 'Все уникальные клики'
        Place_clicks = 'Все клики места размешения'
        Social_clicks = 'Все клики социальные'
        Retargeting_clicks = 'Все клики ретаргетинг'
        Recommended_clicks = 'Все клики рекомендованные'


        AllIp = 'Все уники'
        GetmyadIP = 'Уники партнерки'
        GetmyadIP_N = 'Уники партнерки %s день'
        AdloadIP = 'Уники ретаргетинга'
        AdloadIP_N = 'Уники ретаргетинга %s день'
        GetmyadAndAdloadIP = 'Пересечение Партенки и Ретаргетинга'
        OnlyGetmyadIP = 'Уники партнерки без ретаргетинга'
        OnlyGetmyadIP_N = 'Уники партнерки без ретаргетинга %s день'
        OnlyAdloadIP = 'Уники ретаргетинга без партнерки'
        OnlyAdloadIP_N = 'Уники ретаргетинга без партнерки %s день'
        GetmyadNotViewIP = 'Уники партнерки не видившие рекламу'
        GetmyadNotViewIP_N = 'Уники партнерки не видившие рекламу %s день'
        GetmyadNotViewRetargetingIP = 'Уники партнерки не видевшие рекламу ретаргетинга'
        AllClickIP = 'Все уники кликавшие'
        AllClickIP_N = 'Все уники кликавшие %s день'
        PlaceClickIP = 'Все уники кликавшие по местам размешения'
        PlaceClickIP_N = 'Все уники кликавшие по местам размешения %s день'
        RetargetingClickIP = 'Все уники кликавшие по ретаргетингу'
        RetargetingClickIP_N = 'Все уники кликавшие по ретаргетингу %s день'
        OnlyRetargetingClickIP = 'Все уники пересечения кликавшие по ретаргетингу'
        OnlyRetargetingClickIP_N = 'Все уники пересечения кликавшие по ретаргетингу %s день'
        RetargetingNotClickIP = 'Все уники пересечения не кликавшие по ретаргетингу'
        RetargetingNotClickIP_N = 'Все уники пересечения не кликавшие по ретаргетингу %s день'



        cursor = db.ip.stats.daily.raw.aggregate(pipeline=pipeline, cursor={}, allowDiskUse=True)
        for doc in cursor:
            impressions = doc['impressions']
            place_impressions = doc['place_impressions']
            social_impressions = doc['social_impressions']
            retargeting_impressions = doc['retargeting_impressions']
            recommended_impressions = doc['recommended_impressions']
            impressions_block = doc['impressions_block']
            impressions_block_valid = doc['impressions_block_valid']
            impressions_block_not_valid = doc['impressions_block_not_valid']
            impressions_retargeting = doc['impressions_retargeting']
            all_clicks = doc['all_clicks']
            unique_clicks = doc['unique_clicks']
            place_clicks = doc['place_clicks']
            social_clicks = doc['social_clicks']
            retargeting_clicks = doc['retargeting_clicks']
            recommended_clicks = doc['recommended_clicks']

            impressions_exist = doc['impressions_exist']
            place_impressions_exist = doc['place_impressions_exist']
            social_impressions_exist = doc['social_impressions_exist']
            retargeting_impressions_exist = doc['retargeting_impressions_exist']
            recommended_impressions_exist = doc['recommended_impressions_exist']
            impressions_block_exist = doc['impressions_block_exist']
            impressions_block_valid_exist = doc['impressions_block_valid_exist']
            impressions_block_not_valid_exist = doc['impressions_block_not_valid_exist']
            impressions_retargeting_exist = doc['impressions_retargeting_exist']
            all_clicks_exist = doc['all_clicks_exist']
            unique_clicks_exist = doc['unique_clicks_exist']
            place_clicks_exist = doc['place_clicks_exist']
            social_clicks_exist = doc['social_clicks_exist']
            retargeting_clicks_exist = doc['retargeting_clicks_exist']
            recommended_clicks_exist = doc['recommended_clicks_exist']

            counter[AllIp][Unic] += 1
            counter[AllIp][Impressions] += impressions
            counter[AllIp][Place_impressions] += place_impressions
            counter[AllIp][Social_impressions] += social_impressions
            counter[AllIp][Retargeting_impressions] += retargeting_impressions
            counter[AllIp][Recommended_impressions] += recommended_impressions
            counter[AllIp][Impressions_block] += impressions_block
            counter[AllIp][Impressions_block_valid] += impressions_block_valid
            counter[AllIp][Impressions_block_not_valid] += impressions_block_not_valid
            counter[AllIp][Impressions_retargeting] += impressions_retargeting
            counter[AllIp][All_clicks] += all_clicks
            counter[AllIp][Unique_clicks] += unique_clicks
            counter[AllIp][Place_clicks] += place_clicks
            counter[AllIp][Social_clicks] += social_clicks
            counter[AllIp][Retargeting_clicks] += retargeting_clicks
            counter[AllIp][Recommended_clicks] += recommended_clicks

            if impressions_block_exist:
                counter[GetmyadIP][Unic] += 1
                counter[GetmyadIP][Impressions] += impressions
                counter[GetmyadIP][Place_impressions] += place_impressions
                counter[GetmyadIP][Social_impressions] += social_impressions
                counter[GetmyadIP][Retargeting_impressions] += retargeting_impressions
                counter[GetmyadIP][Recommended_impressions] += recommended_impressions
                counter[GetmyadIP][Impressions_block] += impressions_block
                counter[GetmyadIP][Impressions_block_valid] += impressions_block_valid
                counter[GetmyadIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[GetmyadIP][Impressions_retargeting] += impressions_retargeting
                counter[GetmyadIP][All_clicks] += all_clicks
                counter[GetmyadIP][Unique_clicks] += unique_clicks
                counter[GetmyadIP][Place_clicks] += place_clicks
                counter[GetmyadIP][Social_clicks] += social_clicks
                counter[GetmyadIP][Retargeting_clicks] += retargeting_clicks
                counter[GetmyadIP][Recommended_clicks] += recommended_clicks

                counter[GetmyadIP_N % impressions_block_exist][Unic] += 1
                counter[GetmyadIP_N % impressions_block_exist][Impressions] += impressions
                counter[GetmyadIP_N % impressions_block_exist][Place_impressions] += place_impressions
                counter[GetmyadIP_N % impressions_block_exist][Social_impressions] += social_impressions
                counter[GetmyadIP_N % impressions_block_exist][Retargeting_impressions] += retargeting_impressions
                counter[GetmyadIP_N % impressions_block_exist][Recommended_impressions] += recommended_impressions
                counter[GetmyadIP_N % impressions_block_exist][Impressions_block] += impressions_block
                counter[GetmyadIP_N % impressions_block_exist][Impressions_block_valid] += impressions_block_valid
                counter[GetmyadIP_N % impressions_block_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[GetmyadIP_N % impressions_block_exist][Impressions_retargeting] += impressions_retargeting
                counter[GetmyadIP_N % impressions_block_exist][All_clicks] += all_clicks
                counter[GetmyadIP_N % impressions_block_exist][Unique_clicks] += unique_clicks
                counter[GetmyadIP_N % impressions_block_exist][Place_clicks] += place_clicks
                counter[GetmyadIP_N % impressions_block_exist][Social_clicks] += social_clicks
                counter[GetmyadIP_N % impressions_block_exist][Retargeting_clicks] += retargeting_clicks
                counter[GetmyadIP_N % impressions_block_exist][Recommended_clicks] += recommended_clicks

            if impressions_retargeting_exist:
                counter[AdloadIP][Unic] += 1
                counter[AdloadIP][Impressions] += impressions
                counter[AdloadIP][Place_impressions] += place_impressions
                counter[AdloadIP][Social_impressions] += social_impressions
                counter[AdloadIP][Retargeting_impressions] += retargeting_impressions
                counter[AdloadIP][Recommended_impressions] += recommended_impressions
                counter[AdloadIP][Impressions_block] += impressions_block
                counter[AdloadIP][Impressions_block_valid] += impressions_block_valid
                counter[AdloadIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[AdloadIP][Impressions_retargeting] += impressions_retargeting
                counter[AdloadIP][All_clicks] += all_clicks
                counter[AdloadIP][Unique_clicks] += unique_clicks
                counter[AdloadIP][Place_clicks] += place_clicks
                counter[AdloadIP][Social_clicks] += social_clicks
                counter[AdloadIP][Retargeting_clicks] += retargeting_clicks
                counter[AdloadIP][Recommended_clicks] += recommended_clicks

                counter[AdloadIP_N % impressions_retargeting_exist][Unic] += 1
                counter[AdloadIP_N % impressions_retargeting_exist][Impressions] += impressions
                counter[AdloadIP_N % impressions_retargeting_exist][Place_impressions] += place_impressions
                counter[AdloadIP_N % impressions_retargeting_exist][Social_impressions] += social_impressions
                counter[AdloadIP_N % impressions_retargeting_exist][Retargeting_impressions] += retargeting_impressions
                counter[AdloadIP_N % impressions_retargeting_exist][Recommended_impressions] += recommended_impressions
                counter[AdloadIP_N % impressions_retargeting_exist][Impressions_block] += impressions_block
                counter[AdloadIP_N % impressions_retargeting_exist][Impressions_block_valid] += impressions_block_valid
                counter[AdloadIP_N % impressions_retargeting_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[AdloadIP_N % impressions_retargeting_exist][Impressions_retargeting] += impressions_retargeting
                counter[AdloadIP_N % impressions_retargeting_exist][All_clicks] += all_clicks
                counter[AdloadIP_N % impressions_retargeting_exist][Unique_clicks] += unique_clicks
                counter[AdloadIP_N % impressions_retargeting_exist][Place_clicks] += place_clicks
                counter[AdloadIP_N % impressions_retargeting_exist][Social_clicks] += social_clicks
                counter[AdloadIP_N % impressions_retargeting_exist][Retargeting_clicks] += retargeting_clicks
                counter[AdloadIP_N % impressions_retargeting_exist][Recommended_clicks] += recommended_clicks

            if impressions_block_exist and impressions_retargeting_exist:
                counter[GetmyadAndAdloadIP][Unic] += 1
                counter[GetmyadAndAdloadIP][Impressions] += impressions
                counter[GetmyadAndAdloadIP][Place_impressions] += place_impressions
                counter[GetmyadAndAdloadIP][Social_impressions] += social_impressions
                counter[GetmyadAndAdloadIP][Retargeting_impressions] += retargeting_impressions
                counter[GetmyadAndAdloadIP][Recommended_impressions] += recommended_impressions
                counter[GetmyadAndAdloadIP][Impressions_block] += impressions_block
                counter[GetmyadAndAdloadIP][Impressions_block_valid] += impressions_block_valid
                counter[GetmyadAndAdloadIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[GetmyadAndAdloadIP][Impressions_retargeting] += impressions_retargeting
                counter[GetmyadAndAdloadIP][All_clicks] += all_clicks
                counter[GetmyadAndAdloadIP][Unique_clicks] += unique_clicks
                counter[GetmyadAndAdloadIP][Place_clicks] += place_clicks
                counter[GetmyadAndAdloadIP][Social_clicks] += social_clicks
                counter[GetmyadAndAdloadIP][Retargeting_clicks] += retargeting_clicks
                counter[GetmyadAndAdloadIP][Recommended_clicks] += recommended_clicks

            if impressions_block_exist and not impressions_retargeting_exist:
                counter[OnlyGetmyadIP][Unic] += 1
                counter[OnlyGetmyadIP][Impressions] += impressions
                counter[OnlyGetmyadIP][Place_impressions] += place_impressions
                counter[OnlyGetmyadIP][Social_impressions] += social_impressions
                counter[OnlyGetmyadIP][Retargeting_impressions] += retargeting_impressions
                counter[OnlyGetmyadIP][Recommended_impressions] += recommended_impressions
                counter[OnlyGetmyadIP][Impressions_block] += impressions_block
                counter[OnlyGetmyadIP][Impressions_block_valid] += impressions_block_valid
                counter[OnlyGetmyadIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[OnlyGetmyadIP][Impressions_retargeting] += impressions_retargeting
                counter[OnlyGetmyadIP][All_clicks] += all_clicks
                counter[OnlyGetmyadIP][Unique_clicks] += unique_clicks
                counter[OnlyGetmyadIP][Place_clicks] += place_clicks
                counter[OnlyGetmyadIP][Social_clicks] += social_clicks
                counter[OnlyGetmyadIP][Retargeting_clicks] += retargeting_clicks
                counter[OnlyGetmyadIP][Recommended_clicks] += recommended_clicks

                counter[OnlyGetmyadIP_N % impressions_block_exist][Unic] += 1
                counter[OnlyGetmyadIP_N % impressions_block_exist][Impressions] += impressions
                counter[OnlyGetmyadIP_N % impressions_block_exist][Place_impressions] += place_impressions
                counter[OnlyGetmyadIP_N % impressions_block_exist][Social_impressions] += social_impressions
                counter[OnlyGetmyadIP_N % impressions_block_exist][Retargeting_impressions] += retargeting_impressions
                counter[OnlyGetmyadIP_N % impressions_block_exist][Recommended_impressions] += recommended_impressions
                counter[OnlyGetmyadIP_N % impressions_block_exist][Impressions_block] += impressions_block
                counter[OnlyGetmyadIP_N % impressions_block_exist][Impressions_block_valid] += impressions_block_valid
                counter[OnlyGetmyadIP_N % impressions_block_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[OnlyGetmyadIP_N % impressions_block_exist][Impressions_retargeting] += impressions_retargeting
                counter[OnlyGetmyadIP_N % impressions_block_exist][All_clicks] += all_clicks
                counter[OnlyGetmyadIP_N % impressions_block_exist][Unique_clicks] += unique_clicks
                counter[OnlyGetmyadIP_N % impressions_block_exist][Place_clicks] += place_clicks
                counter[OnlyGetmyadIP_N % impressions_block_exist][Social_clicks] += social_clicks
                counter[OnlyGetmyadIP_N % impressions_block_exist][Retargeting_clicks] += retargeting_clicks
                counter[OnlyGetmyadIP_N % impressions_block_exist][Recommended_clicks] += recommended_clicks

            if not impressions_block_exist and impressions_retargeting_exist:
                counter[OnlyAdloadIP][Unic] += 1
                counter[OnlyAdloadIP][Impressions] += impressions
                counter[OnlyAdloadIP][Place_impressions] += place_impressions
                counter[OnlyAdloadIP][Social_impressions] += social_impressions
                counter[OnlyAdloadIP][Retargeting_impressions] += retargeting_impressions
                counter[OnlyAdloadIP][Recommended_impressions] += recommended_impressions
                counter[OnlyAdloadIP][Impressions_block] += impressions_block
                counter[OnlyAdloadIP][Impressions_block_valid] += impressions_block_valid
                counter[OnlyAdloadIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[OnlyAdloadIP][Impressions_retargeting] += impressions_retargeting
                counter[OnlyAdloadIP][All_clicks] += all_clicks
                counter[OnlyAdloadIP][Unique_clicks] += unique_clicks
                counter[OnlyAdloadIP][Place_clicks] += place_clicks
                counter[OnlyAdloadIP][Social_clicks] += social_clicks
                counter[OnlyAdloadIP][Retargeting_clicks] += retargeting_clicks
                counter[OnlyAdloadIP][Recommended_clicks] += recommended_clicks

                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Unic] += 1
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Impressions] += impressions
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Place_impressions] += place_impressions
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Social_impressions] += social_impressions
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Retargeting_impressions] += retargeting_impressions
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Recommended_impressions] += recommended_impressions
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Impressions_block] += impressions_block
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Impressions_block_valid] += impressions_block_valid
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Impressions_retargeting] += impressions_retargeting
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][All_clicks] += all_clicks
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Unique_clicks] += unique_clicks
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Place_clicks] += place_clicks
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Social_clicks] += social_clicks
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Retargeting_clicks] += retargeting_clicks
                counter[OnlyAdloadIP_N % impressions_retargeting_exist][Recommended_clicks] += recommended_clicks

            if impressions_block_exist and not impressions_block_valid_exist:
                counter[GetmyadNotViewIP][Unic] += 1
                counter[GetmyadNotViewIP][Impressions] += impressions
                counter[GetmyadNotViewIP][Place_impressions] += place_impressions
                counter[GetmyadNotViewIP][Social_impressions] += social_impressions
                counter[GetmyadNotViewIP][Retargeting_impressions] += retargeting_impressions
                counter[GetmyadNotViewIP][Recommended_impressions] += recommended_impressions
                counter[GetmyadNotViewIP][Impressions_block] += impressions_block
                counter[GetmyadNotViewIP][Impressions_block_valid] += impressions_block_valid
                counter[GetmyadNotViewIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[GetmyadNotViewIP][Impressions_retargeting] += impressions_retargeting
                counter[GetmyadNotViewIP][All_clicks] += all_clicks
                counter[GetmyadNotViewIP][Unique_clicks] += unique_clicks
                counter[GetmyadNotViewIP][Place_clicks] += place_clicks
                counter[GetmyadNotViewIP][Social_clicks] += social_clicks
                counter[GetmyadNotViewIP][Retargeting_clicks] += retargeting_clicks
                counter[GetmyadNotViewIP][Recommended_clicks] += recommended_clicks

                counter[GetmyadNotViewIP_N % impressions_block_exist][Unic] += 1
                counter[GetmyadNotViewIP_N % impressions_block_exist][Impressions] += impressions
                counter[GetmyadNotViewIP_N % impressions_block_exist][Place_impressions] += place_impressions
                counter[GetmyadNotViewIP_N % impressions_block_exist][Social_impressions] += social_impressions
                counter[GetmyadNotViewIP_N % impressions_block_exist][Retargeting_impressions] += retargeting_impressions
                counter[GetmyadNotViewIP_N % impressions_block_exist][Recommended_impressions] += recommended_impressions
                counter[GetmyadNotViewIP_N % impressions_block_exist][Impressions_block] += impressions_block
                counter[GetmyadNotViewIP_N % impressions_block_exist][Impressions_block_valid] += impressions_block_valid
                counter[GetmyadNotViewIP_N % impressions_block_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[GetmyadNotViewIP_N % impressions_block_exist][Impressions_retargeting] += impressions_retargeting
                counter[GetmyadNotViewIP_N % impressions_block_exist][All_clicks] += all_clicks
                counter[GetmyadNotViewIP_N % impressions_block_exist][Unique_clicks] += unique_clicks
                counter[GetmyadNotViewIP_N % impressions_block_exist][Place_clicks] += place_clicks
                counter[GetmyadNotViewIP_N % impressions_block_exist][Social_clicks] += social_clicks
                counter[GetmyadNotViewIP_N % impressions_block_exist][Retargeting_clicks] += retargeting_clicks
                counter[GetmyadNotViewIP_N % impressions_block_exist][Recommended_clicks] += recommended_clicks

            if impressions_block_exist and impressions_retargeting_exist and not impressions_block_valid_exist:
                counter[GetmyadNotViewRetargetingIP][Unic] += 1
                counter[GetmyadNotViewRetargetingIP][Impressions] += impressions
                counter[GetmyadNotViewRetargetingIP][Place_impressions] += place_impressions
                counter[GetmyadNotViewRetargetingIP][Social_impressions] += social_impressions
                counter[GetmyadNotViewRetargetingIP][Retargeting_impressions] += retargeting_impressions
                counter[GetmyadNotViewRetargetingIP][Recommended_impressions] += recommended_impressions
                counter[GetmyadNotViewRetargetingIP][Impressions_block] += impressions_block
                counter[GetmyadNotViewRetargetingIP][Impressions_block_valid] += impressions_block_valid
                counter[GetmyadNotViewRetargetingIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[GetmyadNotViewRetargetingIP][Impressions_retargeting] += impressions_retargeting
                counter[GetmyadNotViewRetargetingIP][All_clicks] += all_clicks
                counter[GetmyadNotViewRetargetingIP][Unique_clicks] += unique_clicks
                counter[GetmyadNotViewRetargetingIP][Place_clicks] += place_clicks
                counter[GetmyadNotViewRetargetingIP][Social_clicks] += social_clicks
                counter[GetmyadNotViewRetargetingIP][Retargeting_clicks] += retargeting_clicks
                counter[GetmyadNotViewRetargetingIP][Recommended_clicks] += recommended_clicks

            if all_clicks_exist:
                counter[AllClickIP][Unic] += 1
                counter[AllClickIP][Impressions] += impressions
                counter[AllClickIP][Place_impressions] += place_impressions
                counter[AllClickIP][Social_impressions] += social_impressions
                counter[AllClickIP][Retargeting_impressions] += retargeting_impressions
                counter[AllClickIP][Recommended_impressions] += recommended_impressions
                counter[AllClickIP][Impressions_block] += impressions_block
                counter[AllClickIP][Impressions_block_valid] += impressions_block_valid
                counter[AllClickIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[AllClickIP][Impressions_retargeting] += impressions_retargeting
                counter[AllClickIP][All_clicks] += all_clicks
                counter[AllClickIP][Unique_clicks] += unique_clicks
                counter[AllClickIP][Place_clicks] += place_clicks
                counter[AllClickIP][Social_clicks] += social_clicks
                counter[AllClickIP][Retargeting_clicks] += retargeting_clicks
                counter[AllClickIP][Recommended_clicks] += recommended_clicks

                counter[AllClickIP_N % all_clicks_exist][Unic] += 1
                counter[AllClickIP_N % all_clicks_exist][Impressions] += impressions
                counter[AllClickIP_N % all_clicks_exist][Place_impressions] += place_impressions
                counter[AllClickIP_N % all_clicks_exist][Social_impressions] += social_impressions
                counter[AllClickIP_N % all_clicks_exist][Retargeting_impressions] += retargeting_impressions
                counter[AllClickIP_N % all_clicks_exist][Recommended_impressions] += recommended_impressions
                counter[AllClickIP_N % all_clicks_exist][Impressions_block] += impressions_block
                counter[AllClickIP_N % all_clicks_exist][Impressions_block_valid] += impressions_block_valid
                counter[AllClickIP_N % all_clicks_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[AllClickIP_N % all_clicks_exist][Impressions_retargeting] += impressions_retargeting
                counter[AllClickIP_N % all_clicks_exist][All_clicks] += all_clicks
                counter[AllClickIP_N % all_clicks_exist][Unique_clicks] += unique_clicks
                counter[AllClickIP_N % all_clicks_exist][Place_clicks] += place_clicks
                counter[AllClickIP_N % all_clicks_exist][Social_clicks] += social_clicks
                counter[AllClickIP_N % all_clicks_exist][Retargeting_clicks] += retargeting_clicks
                counter[AllClickIP_N % all_clicks_exist][Recommended_clicks] += recommended_clicks

            if place_clicks_exist:
                counter[PlaceClickIP][Unic] += 1
                counter[PlaceClickIP][Impressions] += impressions
                counter[PlaceClickIP][Place_impressions] += place_impressions
                counter[PlaceClickIP][Social_impressions] += social_impressions
                counter[PlaceClickIP][Retargeting_impressions] += retargeting_impressions
                counter[PlaceClickIP][Recommended_impressions] += recommended_impressions
                counter[PlaceClickIP][Impressions_block] += impressions_block
                counter[PlaceClickIP][Impressions_block_valid] += impressions_block_valid
                counter[PlaceClickIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[PlaceClickIP][Impressions_retargeting] += impressions_retargeting
                counter[PlaceClickIP][All_clicks] += all_clicks
                counter[PlaceClickIP][Unique_clicks] += unique_clicks
                counter[PlaceClickIP][Place_clicks] += place_clicks
                counter[PlaceClickIP][Social_clicks] += social_clicks
                counter[PlaceClickIP][Retargeting_clicks] += retargeting_clicks
                counter[PlaceClickIP][Recommended_clicks] += recommended_clicks

                counter[PlaceClickIP_N % place_clicks_exist][Unic] += 1
                counter[PlaceClickIP_N % place_clicks_exist][Impressions] += impressions
                counter[PlaceClickIP_N % place_clicks_exist][Place_impressions] += place_impressions
                counter[PlaceClickIP_N % place_clicks_exist][Social_impressions] += social_impressions
                counter[PlaceClickIP_N % place_clicks_exist][Retargeting_impressions] += retargeting_impressions
                counter[PlaceClickIP_N % place_clicks_exist][Recommended_impressions] += recommended_impressions
                counter[PlaceClickIP_N % place_clicks_exist][Impressions_block] += impressions_block
                counter[PlaceClickIP_N % place_clicks_exist][Impressions_block_valid] += impressions_block_valid
                counter[PlaceClickIP_N % place_clicks_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[PlaceClickIP_N % place_clicks_exist][Impressions_retargeting] += impressions_retargeting
                counter[PlaceClickIP_N % place_clicks_exist][All_clicks] += all_clicks
                counter[PlaceClickIP_N % place_clicks_exist][Unique_clicks] += unique_clicks
                counter[PlaceClickIP_N % place_clicks_exist][Place_clicks] += place_clicks
                counter[PlaceClickIP_N % place_clicks_exist][Social_clicks] += social_clicks
                counter[PlaceClickIP_N % place_clicks_exist][Retargeting_clicks] += retargeting_clicks
                counter[PlaceClickIP_N % place_clicks_exist][Recommended_clicks] += recommended_clicks

            if retargeting_clicks_exist:
                counter[RetargetingClickIP][Unic] += 1
                counter[RetargetingClickIP][Impressions] += impressions
                counter[RetargetingClickIP][Place_impressions] += place_impressions
                counter[RetargetingClickIP][Social_impressions] += social_impressions
                counter[RetargetingClickIP][Retargeting_impressions] += retargeting_impressions
                counter[RetargetingClickIP][Recommended_impressions] += recommended_impressions
                counter[RetargetingClickIP][Impressions_block] += impressions_block
                counter[RetargetingClickIP][Impressions_block_valid] += impressions_block_valid
                counter[RetargetingClickIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[RetargetingClickIP][Impressions_retargeting] += impressions_retargeting
                counter[RetargetingClickIP][All_clicks] += all_clicks
                counter[RetargetingClickIP][Unique_clicks] += unique_clicks
                counter[RetargetingClickIP][Place_clicks] += place_clicks
                counter[RetargetingClickIP][Social_clicks] += social_clicks
                counter[RetargetingClickIP][Retargeting_clicks] += retargeting_clicks
                counter[RetargetingClickIP][Recommended_clicks] += recommended_clicks

                counter[RetargetingClickIP_N % retargeting_clicks_exist][Unic] += 1
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Impressions] += impressions
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Place_impressions] += place_impressions
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Social_impressions] += social_impressions
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Retargeting_impressions] += retargeting_impressions
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Recommended_impressions] += recommended_impressions
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Impressions_block] += impressions_block
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Impressions_block_valid] += impressions_block_valid
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Impressions_retargeting] += impressions_retargeting
                counter[RetargetingClickIP_N % retargeting_clicks_exist][All_clicks] += all_clicks
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Unique_clicks] += unique_clicks
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Place_clicks] += place_clicks
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Social_clicks] += social_clicks
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Retargeting_clicks] += retargeting_clicks
                counter[RetargetingClickIP_N % retargeting_clicks_exist][Recommended_clicks] += recommended_clicks

            if impressions_block_exist and impressions_retargeting_exist and retargeting_clicks_exist and not place_clicks_exist:
                counter[OnlyRetargetingClickIP][Unic] += 1
                counter[OnlyRetargetingClickIP][Impressions] += impressions
                counter[OnlyRetargetingClickIP][Place_impressions] += place_impressions
                counter[OnlyRetargetingClickIP][Social_impressions] += social_impressions
                counter[OnlyRetargetingClickIP][Retargeting_impressions] += retargeting_impressions
                counter[OnlyRetargetingClickIP][Recommended_impressions] += recommended_impressions
                counter[OnlyRetargetingClickIP][Impressions_block] += impressions_block
                counter[OnlyRetargetingClickIP][Impressions_block_valid] += impressions_block_valid
                counter[OnlyRetargetingClickIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[OnlyRetargetingClickIP][Impressions_retargeting] += impressions_retargeting
                counter[OnlyRetargetingClickIP][All_clicks] += all_clicks
                counter[OnlyRetargetingClickIP][Unique_clicks] += unique_clicks
                counter[OnlyRetargetingClickIP][Place_clicks] += place_clicks
                counter[OnlyRetargetingClickIP][Social_clicks] += social_clicks
                counter[OnlyRetargetingClickIP][Retargeting_clicks] += retargeting_clicks
                counter[OnlyRetargetingClickIP][Recommended_clicks] += recommended_clicks

                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Unic] += 1
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Impressions] += impressions
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Place_impressions] += place_impressions
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Social_impressions] += social_impressions
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Retargeting_impressions] += retargeting_impressions
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Recommended_impressions] += recommended_impressions
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Impressions_block] += impressions_block
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Impressions_block_valid] += impressions_block_valid
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Impressions_retargeting] += impressions_retargeting
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][All_clicks] += all_clicks
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Unique_clicks] += unique_clicks
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Place_clicks] += place_clicks
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Social_clicks] += social_clicks
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Retargeting_clicks] += retargeting_clicks
                counter[OnlyRetargetingClickIP_N % retargeting_clicks_exist][Recommended_clicks] += recommended_clicks

            if impressions_block_exist and impressions_retargeting_exist and not retargeting_clicks_exist:
                counter[RetargetingNotClickIP][Unic] += 1
                counter[RetargetingNotClickIP][Impressions] += impressions
                counter[RetargetingNotClickIP][Place_impressions] += place_impressions
                counter[RetargetingNotClickIP][Social_impressions] += social_impressions
                counter[RetargetingNotClickIP][Retargeting_impressions] += retargeting_impressions
                counter[RetargetingNotClickIP][Recommended_impressions] += recommended_impressions
                counter[RetargetingNotClickIP][Impressions_block] += impressions_block
                counter[RetargetingNotClickIP][Impressions_block_valid] += impressions_block_valid
                counter[RetargetingNotClickIP][Impressions_block_not_valid] += impressions_block_not_valid
                counter[RetargetingNotClickIP][Impressions_retargeting] += impressions_retargeting
                counter[RetargetingNotClickIP][All_clicks] += all_clicks
                counter[RetargetingNotClickIP][Unique_clicks] += unique_clicks
                counter[RetargetingNotClickIP][Place_clicks] += place_clicks
                counter[RetargetingNotClickIP][Social_clicks] += social_clicks
                counter[RetargetingNotClickIP][Retargeting_clicks] += retargeting_clicks
                counter[RetargetingNotClickIP][Recommended_clicks] += recommended_clicks

                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Unic] += 1
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Impressions] += impressions
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Place_impressions] += place_impressions
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Social_impressions] += social_impressions
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Retargeting_impressions] += retargeting_impressions
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Recommended_impressions] += recommended_impressions
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Impressions_block] += impressions_block
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Impressions_block_valid] += impressions_block_valid
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Impressions_block_not_valid] += impressions_block_not_valid
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Impressions_retargeting] += impressions_retargeting
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][All_clicks] += all_clicks
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Unique_clicks] += unique_clicks
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Place_clicks] += place_clicks
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Social_clicks] += social_clicks
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Retargeting_clicks] += retargeting_clicks
                counter[RetargetingNotClickIP_N % impressions_retargeting_exist][Recommended_clicks] += recommended_clicks

        not_core = [AllIp, GetmyadIP, AdloadIP, GetmyadAndAdloadIP, OnlyGetmyadIP, OnlyAdloadIP, GetmyadNotViewIP, GetmyadNotViewRetargetingIP,
                    AllClickIP, PlaceClickIP, RetargetingClickIP, OnlyRetargetingClickIP, RetargetingNotClickIP]

        core = [GetmyadIP_N, AdloadIP_N, OnlyGetmyadIP_N, OnlyAdloadIP_N, GetmyadNotViewIP_N, AllClickIP_N, PlaceClickIP_N,
                RetargetingClickIP_N, OnlyRetargetingClickIP_N, RetargetingNotClickIP_N]

        values = [Unic, Impressions, Place_impressions, Social_impressions, Retargeting_impressions, Recommended_impressions,
                  Impressions_block, Impressions_block_valid, Impressions_block_not_valid, Impressions_retargeting, All_clicks,
                  Unique_clicks, Place_clicks, Social_clicks, Retargeting_clicks, Recommended_clicks]

        # not_core = [AllIp, GetmyadIP, AdloadIP, GetmyadAndAdloadIP]
        #
        # core = [GetmyadIP_N, AdloadIP_N, ]
        #
        # values = [Unic, ]

        for item in not_core:
            the_file.write(item)
            the_file.write("\n")
            val = counter[item]
            for x in values:
                the_file.write(' '.join(['\t', str(x), str(val[x])]))
                the_file.write("\n")

        for item in core:
            the_file.write(item)
            the_file.write("\n")
            for y in range(1, 30):
                key = item % y
                val = counter[key]
                the_file.write(' '.join(['\t', key]))
                the_file.write("\n")
                for x in values:
                    the_file.write(' '.join(['\t', '\t', str(x), str(val[x])]))
                    the_file.write("\n")


# dateS = datetime.datetime(2018, 02, 27, 0, 0)
# dateE = datetime.datetime(2018, 02, 28, 0, 0)
# calc(db, dateS, dateE)
# print ''
dateS = datetime.datetime(2018, 02, 27, 0, 0)
dateE = datetime.datetime(2018, 04, 02, 0, 0)
calc(db, dateS, dateE)