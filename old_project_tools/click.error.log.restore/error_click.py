import sys
from pymongo import MongoClient
import datetime
from pprint import pprint

from tasks import process_click

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

date = datetime.datetime.now()
date = datetime.datetime(date.year, 9, 30, 21, 0)

cur = db.clicks.error.find({'dt': {'$gte': date}})

for doc in cur:
    url = doc.get('url')
    ip = doc.get('ip')
    click_datetime = doc.get('dt')
    offer_id = doc.get('offer')
    campaign_id = doc.get('campaignId')
    informer_id = doc.get('inf')
    token = doc.get('token')
    valid = True
    referer = doc.get('referer')
    user_agent = doc.get('user_agent')
    cookie = doc.get('cookie')
    view_seconds = doc.get('view_seconds')
    # process_click(url, ip, click_datetime, offer_id, campaign_id, informer_id, token, valid, referer, user_agent,
    #               cookie, view_seconds)
