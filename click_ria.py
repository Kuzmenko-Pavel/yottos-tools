# This Python file uses the following encoding: utf-8
import sys
from collections import defaultdict, OrderedDict, Counter
import os
import datetime
from urlparse import urlparse, parse_qs
import csv

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
import sys
from pymongo import Connection


class OrderedCounter(Counter, OrderedDict):
    'Counter that remembers the order elements are first encountered'

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, OrderedDict(self))

    def __reduce__(self):
        return self.__class__, (OrderedDict(self),)


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


conn = Connection(host=main_db_host)
db = conn.getmyad_db

s_date = datetime.datetime(2017, 8, 21, 0, 0)
e_date = datetime.datetime(2017, 9, 5, 0, 0)
click_curs = db.clicks.find({'campaignId' : {"$in": ['c035767b-9740-48f8-bb07-1cfd53e72d9c', '2e0075fc-4e36-491b-95f2-da7eb912aa35']},
    "dt" : {
        '$lt': e_date,
        '$gte': s_date,
    },
    'unique': True})
utm_medium = OrderedCounter()
utm_medium_by_date = defaultdict(OrderedCounter)

blocks = {}
for inf in db.informer.find({},{'guid':1, 'domain':1}):
    blocks[inf['guid']] = inf['domain']


for click in click_curs:
    dt = click['dt'].strftime('%y-%m-%d')
    utm_medium[blocks.get(click['inf'], '--')] +=1
    utm_medium_by_date[dt][blocks.get(click['inf'], '--')] +=1




print '----All----'
c = 0
for key, value in utm_medium.iteritems():
    c += value
    print "%s \t %s" % (key, value)

print "All click %s\n\n" % c

print '----Date----'
for k,v in utm_medium_by_date.iteritems():
    c = 0
    print "Date - %s" % k
    for key, value in v.iteritems():
        c += value
        print "%s \t %s" % (key, value)
    print "All click %s\n\n" % c