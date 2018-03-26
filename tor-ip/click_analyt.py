# This Python file uses the following encoding: utf-8
import sys
from collections import defaultdict, Counter
import os
import csv

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
import sys
from pymongo import MongoClient, DESCENDING
import httpagentparser

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


ips = set()
with open('ip', "r") as file:
    for line in file.readlines():
        ips.add(line.strip())

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


count = 0
inf_cl = defaultdict(int)
inf_cl_p = defaultdict(int)
ips_c = set()
click_curs = db.clicks.find({}, {'inf': 1, 'ip': 1}).sort('$natural', DESCENDING)
for click in click_curs:
    ips_c.add(click['ip'].strip())
    count += 1
    inf_cl[click['inf'].strip()] +=1
    if click['ip'].strip() in ips:
        inf_cl_p[click['inf'].strip()] += 1

for i in inf_cl_p.keys():
    print "%s\t%s\t%s" % (i, inf_cl[i], inf_cl_p[i])

print len(ips_c)
# click_curs = db.clicks.find({}, {'user_agent': 1}).sort('$natural', DESCENDING)
# platform = defaultdict(int)
# for click in click_curs:
#     info = httpagentparser.detect(click['user_agent'])
#     platform[info['platform']['name']] += 1
#
# print platform