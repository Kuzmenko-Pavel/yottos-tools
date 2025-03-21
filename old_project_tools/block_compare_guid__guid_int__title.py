# This Python file uses the following encoding: utf-8
import os
import sys

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import sys
from pymongo import MongoClient
from collections import defaultdict

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

informers = defaultdict(lambda: 'NOT TITLE')
for item in db.informer.find({}, {'guid': True, 'domain': True, 'guid_int': True, 'user': True, 'title': True}):
    print('%s\t%s\t%s\t%s\t%s\t' % (item.get('guid_int', ''), item.get('guid', ''),
                                    item.get('title', ''), item.get('domain', ''), item.get('user', '')))
