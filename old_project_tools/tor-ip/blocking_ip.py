# This Python file uses the following encoding: utf-8
import sys
import os
import datetime

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
import sys
from pymongo import MongoClient, DESCENDING
import httpagentparser

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


ips = set()
cookies = set()
with open('blocking_ip', "r") as file:
    for line in file.readlines():
        ips.add(line.strip())

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


click_curs = db.clicks.find({'ip':{'$in': list(ips)}})
for doc in click_curs:
    cookies.add(doc.get('cookie', '').strip())

for ip in ips:
    for cookie in cookies:
        db.blacklist.ip.update_one({'ip': ip, 'cookie': cookie},
                                   {'$set': {'dt': datetime.datetime.now()}},
                                   upsert=True)
