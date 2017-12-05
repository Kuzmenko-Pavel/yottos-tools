# This Python file uses the following encoding: utf-8
import sys
from collections import defaultdict, Counter
import os
import csv

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
import sys
from pymongo import Connection

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


conn = Connection(host=main_db_host)
db = conn.getmyad_db

request_init = db.clicks.find({'request': 'initial'}).count()
request_rota = db.clicks.find({'request': 'rotation'}).count()
rotation_percent = request_rota / ((request_init + request_rota) / 100)
print 'Click initial %s , rotation %s, rotation percent %s%%' % (request_init, request_rota, rotation_percent)

inf_stats_view_number = defaultdict(list)
inf_stats_capacity = dict()
inf_stats_exclude_capacity = defaultdict(list)
capacity_stats_view_number = defaultdict(list)
capacity_stats_exclude_capacity = defaultdict(list)
view_number = list()
exclude_capacity = list()

click_curs = db.clicks.find({'view_number': {'$exists': True}})
for click in click_curs:
    if click['capacity'] > 0:
        capacity_stats_view_number[str(click['capacity'])].append(int(round(click['view_number'])))
        capacity_stats_exclude_capacity[click['capacity']].append(int(round(click['exclude_capacity'])))
        inf_stats_view_number[click['inf']].append(int(round(click['view_number'])))
        view_number.append(int(round(click['view_number'])))
        inf_stats_capacity[click['inf']] = click['capacity']
        inf_stats_exclude_capacity[click['inf']].append(int(round(click['exclude_capacity'])))
        exclude_capacity.append(int(round(click['exclude_capacity'])))


with open('view_number.csv', "wb") as file:
    writer = csv.writer(file, delimiter=',')
    c = Counter(view_number)
    for row in c.most_common():
        writer.writerow(row)

with open('exclude_capacity.csv', "wb") as file:
    writer = csv.writer(file, delimiter=',')
    c = Counter(exclude_capacity)
    for row in c.most_common():
        writer.writerow(row)

for key, value in capacity_stats_view_number.iteritems():
    with open('capacity_stats_view_number_%s.csv' % key, "wb") as file:
        writer = csv.writer(file, delimiter=',')
        c = Counter(value)
        for row in c.most_common():
            writer.writerow(row)