#!env/bin/python
# encoding: utf-8

import random
from datetime import datetime
from pymongo import MongoClient
import time
import tasks
import argparse

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

def process():
    './dublicator.py --block c6c1eebc-0502-11e8-ae50-002590d97638 --adload_account d82b0c93-0347-4e88-ac4c-2eedabcf61cd --count 500 --skip 1'
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--campaign', help='campaign help')
    parser.add_argument('--block', help='block help')
    parser.add_argument('--adload_account', help='adload_account help')
    parser.add_argument('--getmyad_account', help='getmyad_account help')
    parser.add_argument('--adload_manager', help='adload_manager help')
    parser.add_argument('--getmyad_manager', help='getmyad_manager help')
    parser.add_argument('--count', help='count help', default=10)
    parser.add_argument('--limit', help='count help')
    parser.add_argument('--skip', help='count help')
    parser.add_argument('--sleep', help='count help', default=0)
    args = parser.parse_args()
    q = {}
    count = int(args.count)
    sleep = int(args.sleep)
    if args.skip:
        skip = int(args.skip)
    else:
        skip = count + 100
    if args.limit:
        limit = int(args.limit)
    else:
        limit = count
    if args.campaign:
        q['campaignId'] = args.campaign
    if args.block:
        q['inf'] = args.block
    if args.adload_account:
        q['account_id'] = args.adload_account
    if args.getmyad_account:
        q['getmyad_user_id'] = args.getmyad_account
    if args.adload_manager:
        q['adload_manager'] = args.adload_manager
    if args.getmyad_manager:
        q['getmyad_manager'] = args.getmyad_manager

    if not q:
        print('Set params')
        return
    else:
        print q
    conn = MongoClient(host=main_db_host)
    db = conn.getmyad_db

    if 'campaignId' not in q:
        campaignIds = []
        for x in db.campaign.find({'status':'working'}, {'guid': 1}):
            campaignIds.append(x['guid'])
        q['campaignId'] = {'$in': campaignIds}

    cursor = db.clicks.find(q).sort('dt', -1).limit(limit).skip(skip)
    cliks = []
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
        branch = doc['branch']
        conformity = doc['conformity']
        social = doc['social']
        request = doc['request']
        cliks.append([url, ip, datetime.now(), offer_id, campaign_id, informer_id,
                      token, True, referer, user_agent, cookie, view_seconds, branch, conformity, social, request])
    c = 0
    print('%s click found for %s clic' % (len(cliks), count))
    while c < count:
        random.shuffle(cliks)
        campaignIdList = []
        for x in db.campaign.find({'status':'working'}, {'guid': 1}):
            campaignIdList.append(x['guid'])
        print("Found %s campaigns", len(campaignIdList))
        for clik in cliks:
            time.sleep(sleep)
            if clik[4] in campaignIdList:
                clik[2] = datetime.now().replace(minute=random.randint(0, datetime.now().minute))
                if c < count:
                    if tasks.process_click(*clik):
                        print('YES %s' % c)
                        c += 1
                else:
                    break
            else:
                print('Campaign %s not fount' % clik[4])
        if c == 0:
            break


if __name__ == '__main__':
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    process()