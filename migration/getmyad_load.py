# This Python file uses the following encoding: utf-8
import os
import sys
import json
from bson import json_util

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import sys
from pymongo import MongoClient


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

accounts_dir = os.path.join(project_dir, 'accounts')
informer_patterns_dir = os.path.join(project_dir, 'informer_patterns')
thematic_dir = os.path.join(project_dir, 'thematic')


def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def to_json(path, data):
    with open(path, 'w') as outfile:
        outfile.write(json_util.dumps(data))


create_directory(accounts_dir)
create_directory(informer_patterns_dir)
create_directory(thematic_dir)

# for item in db.informer.patterns.find():
#     path = os.path.join(informer_patterns_dir, '%s.json' % str(item['_id']))
#     to_json(path, item)
#
# for item in db.thematic.find():
#     path = os.path.join(thematic_dir, '%s.json' % str(item['_id']))
#     to_json(path, item)
