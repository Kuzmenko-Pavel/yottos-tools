# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient
from collections import defaultdict


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


for item in db.campaign.find({}):
    showConditions = item.get('showConditions')
    if showConditions:
        social = item.get('social', False)
        retargeting = showConditions.get('retargeting', False)
        thematic = showConditions.get('thematic', False)
        campaign_type = int(showConditions.get('campaign_type', 0))
        if campaign_type == 0:
            if retargeting:
                campaign_type = 2
            elif social:
                campaign_type = 4
            else:
                campaign_type = 1

        if campaign_type == 2:
            thematic = False
            retargeting = True
            social = False
        elif campaign_type == 3:
            thematic = True
            retargeting = False
            social = False
        elif campaign_type == 4:
            thematic = False
            retargeting = False
            social = True
        else:
            thematic = False
            retargeting = False
            social = False
        db.campaign.update_one({'guid': item['guid']}, {
            '$set': {
                'social': social,
                'retargeting': retargeting,
                'thematic': thematic,
                'showConditions.social': social,
                'showConditions.retargeting': retargeting,
                'showConditions.thematic': thematic,
                'showConditions.campaign_type': campaign_type,
            }
        })
