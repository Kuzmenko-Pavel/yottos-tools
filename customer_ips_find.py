# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient
from collections import defaultdict



sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

ips = set()

for user in db.users.find({'ips': {'$exists':True}}, {'ips': 1}):
    for ip in user.get('ips', []):
        if ip != '95.69.249.86':
            ips.add(ip)

clicks = defaultdict(int)

for click in db.clicks.find({'ip': {'$in': list(ips)}}):
    clicks[click.get('ip')] += 1


for user in db.users.find({'ips': {'$exists': True}}, {'ips': 1, 'login':1}):
    for ip in user.get('ips', []):
        if ip in clicks.keys():
            print '%s\t%s\t%s' % (user['login'], ip, clicks[ip])