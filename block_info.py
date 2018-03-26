# This Python file uses the following encoding: utf-8
import sys
import csv
from pymongo import MongoClient
from collections import Counter, defaultdict
import datetime


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

inf = {}
users = {}
for block in db.informer.find({}, {'guid': 1, 'user': 1, 'domain': 1, 'title': 1}):
    inf[block['guid']] = block

for user in db.users.find({}, {'login': 1, 'managerGet': 1}):
    users[user['login']] = user

blocks = [
'01412e28-28f1-11e7-98b7-002590d97638',
'057b65dc-4a2f-11e5-ac7b-002590d8e030',
'1bcceac2-8148-11e5-b149-002590d97638',
'2d8efa2a-f6ad-11e7-b62e-002590d75952',
'2f76aaec-9901-11e5-ad65-002590d97638',
'435e5354-f42a-11e6-a52d-002590d97638',
'61b0139e-3141-11e6-a394-002590d97638',
'6e583676-3141-11e6-a394-002590d97638',
'72a618a8-05b9-11e8-ae50-002590d97638',
'7327478c-ede3-11e6-9727-002590d97638',
'8685a114-ca0d-11e7-b0c8-002590d97638',
'8996c160-4e29-11e5-9170-002590d8e030',
'9582144e-cc13-11e5-a26b-002590d97638',
'a0f18adc-d448-11e7-9d6c-002590d97638',
'a78ec398-1cc8-11e6-98fd-002590d97638',
'b5650f7e-baea-11e7-8120-002590d97638',
'c6c1eebc-0502-11e8-ae50-002590d97638',
'c847f0b4-3c66-11e7-98b7-002590d97638',
'dad294d0-fcb3-11e5-9778-002590d75952',
'e57c4556-91e3-11e5-9a47-002590d97638',
'ee6ef54c-1b98-11e7-8c29-002590d97638'
]

for item in blocks:
    bl = inf.get(item)
    if bl is not None:
        us = users.get(bl['user'])
        if us is not None:
            print '%s %s %s' % (bl['guid'], us['login'], us['managerGet'])
        else:
            print 'not user', bl
    else:
        print 'not block'
