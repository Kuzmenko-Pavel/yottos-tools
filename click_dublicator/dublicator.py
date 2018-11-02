#!env/bin/python
# encoding: utf-8

import sys
from datetime import datetime
import logging
from pymongo import MongoClient
import tasks

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

def process():
    if len(sys.argv) <> 4:
        logging.debug("Invalid usage. sys.argv = %s" % sys.argv)
        print "Usage: dublicator.py informer_id campaign_id count"
        print "./dublicator.py c6c1eebc-0502-11e8-ae50-002590d97638 3b8fa6ca-6bf0-4b38-9129-0afcfc0ce224 3"
        exit()

    adv = sys.argv[1]
    campaign = sys.argv[2]
    count = int(sys.argv[3])
    print("Informer=%s Campaign=%s Count=%s" % (adv, campaign, count))
    conn = MongoClient(host=main_db_host)
    db = conn.getmyad_db
    cursor = db.clicks.find({'inf': adv, 'campaignId': campaign}).sort('_id').limit(count)
    for doc in cursor:
        informer_id = doc['inf']
        campaign_id = doc['campaignId']
        offer_id = doc['offer']
        ip = doc['ip']
        url = doc['url']
        token = doc['cookie']
        referer = doc['referer']
        user_agent = doc['user_agent']
        cookie = doc['cookie']
        view_seconds = doc['view_seconds']
        tasks.process_click(url, ip, datetime.now(), offer_id, campaign_id, informer_id,
                            token, True, referer, user_agent, cookie, view_seconds)


if __name__ == '__main__':
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    process()