# This Python file uses the following encoding: utf-8
import sys
from pymongo import MongoClient, DESCENDING
from collections import defaultdict
import os
import operator

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)


sys.stdout = sys.stderr

main_db_host = 'srv-2.yottos.com:27017'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_log

count = defaultdict(int)

cur = db.log.impressions.find({}, {'ip': 1}).sort('$natural', DESCENDING)


for doc in cur:
    count[doc['ip']] +=1

len(count)
print max(count.iteritems(), key=operator.itemgetter(1))[0]
newA = sorted(count.iteritems(), key=operator.itemgetter(1), reverse=True)[:50]
print newA