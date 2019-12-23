# -*- coding: utf-8 -*-
import csv
import datetime
from collections import defaultdict


data = defaultdict(lambda: defaultdict(float))

with open('usd.csv', 'rb') as csvfile:
    rate = csv.reader(csvfile, delimiter='\t', quotechar='"')
    for row in rate:
        d = row[0]
        r = row[1]
        d = datetime.datetime.strptime(d, '%d.%m.%Y')
        data[(d.month, d.day)][d.year] = r

s_data = []
for k, v in data.iteritems():
    r = []
    for y in range(2008, 2020):
        rat = v[y]
        if rat <= 0:
            rat = ' '
        else:
            rat = str(rat)
        r.append(rat)
    s_data.append((k, r))

s_data.sort(key=lambda x:x[0])

print('Дата\t%s' % '\t'.join([str(x) for x in range(2008, 2020)]))
for item in s_data:
    print('%s-%s\t%s' % (item[0][0],item[0][1], '\t'.join(item[1])))