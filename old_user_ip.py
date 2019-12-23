# This Python file uses the following encoding: utf-8
import os
import sys
import json

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import sys
from pymongo import MongoClient
from collections import defaultdict

sys.stdout = sys.stderr

main_db_host = 'srv-3.yottos.com:27017'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

ip_list = []
for item in db.users.find({}, {'ips': True}):
    ips = item.get('ips', [])
    for ip in ips:
        ip_list.append(ip)

ip_list = sorted(list(set(ip_list)))


to_json_path = os.path.abspath('.ips.json')
with open(to_json_path, "w") as write_file:
    try:
        json.dump(ip_list, write_file, indent=4, sort_keys=True)
    except Exception as e:
        print(e)