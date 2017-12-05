# This Python file uses the following encoding: utf-8
import sys

import os
import time

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import datetime
import urllib
import urlparse
import requests
import sys
from pymongo import Connection
import xlwt
import StringIO
import ftplib
import collections

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'
font0 = xlwt.Font()
font0.name = 'Times New Roman'
font0.colour_index = 0
font0.height = 360
font0.bold = True
style0 = xlwt.XFStyle()
style0.font = font0

font1 = xlwt.Font()
font1.name = 'Times New Roman'
font1.colour_index = 0
font1.height = 256
font1.bold = False
style1 = xlwt.XFStyle()
style1.font = font1

conn = Connection(host=main_db_host)
db = conn.getmyad_db
date = datetime.datetime.now()
date = datetime.datetime(date.year, date.month, date.day, 0, 0)
pipeline = [
    {'$match':
         {'dt': {'$gte': date - datetime.timedelta(days=35), '$lt': date}}
     },
    {'$group':
         {'_id': '$account_id'}
     }
]
acc = set()
cursor = db.clicks.aggregate(pipeline=pipeline, cursor={})
for doc in cursor:
    acc.add(doc['_id'])

link = []

for account_id in acc:
    wbk = xlwt.Workbook('utf-8')
    pipeline = [
        {'$match':
             {'account_id': account_id, 'dt': {'$gte': date - datetime.timedelta(days=35), '$lt': date}}
         },
        {'$group':
             {'_id': '$campaignId'}
         }
    ]
    camp = {}
    cursor = db.clicks.aggregate(pipeline=pipeline, cursor={})
    for doc in cursor:
        campaign = db.campaign.find_one({'guid': doc['_id']}, {'title': 1, 'guid': 1})
        if campaign:
            camp[campaign['guid']] = campaign['title']
            continue
        campaign = db.campaign.archive.find_one({'guid': doc['_id']}, {'title': 1, 'guid': 1})
        if campaign:
            camp[campaign['guid']] = campaign['title']

    camp_count = 0
    for k, v in camp.iteritems():
        print "== %s - %s - %s ==" % (k,v,account_id)
        cur = db.clicks.find({"campaignId": k, 'dt': {'$gte': date - datetime.timedelta(days=35), '$lt': date}}).sort(
            "dt", -1)
        buffer = {}
        for x in cur:
            url = x['url']
            url_parts = list(urlparse.urlparse(url))
            query = dict(urlparse.parse_qsl(url_parts[4]))
            utm_source = ''
            utm_campaign = ''
            utm_content = ''
            if query.has_key('utm_source'):
                utm_source = urllib.unquote(query['utm_source'].encode('utf-8'))
            if query.has_key('utm_campaign'):
                utm_campaign = urllib.unquote(query['utm_campaign'].encode('utf-8'))

            if query.has_key('utm_content'):
                utm_content = urllib.unquote(query['utm_content'].encode('utf-8'))

            cost = float(x['adload_cost'])
            data = datetime.datetime.fromordinal(x['dt'].toordinal())
            key = (data.strftime("%d.%m.%Y"), utm_source, utm_campaign, utm_content)
            val = buffer.get(key, [0.0, 0])
            buffer[key] = [val[0] + cost, val[1] + 1]

        v = v.replace('/', '')
        sheet_name = v[:27] + str(camp_count)
        try:
            sheet = wbk.add_sheet(sheet_name)
            sheet.write(0, 0, 'date', style0)
            sheet.write(0, 1, 'utm_source', style0)
            sheet.write(0, 2, 'utm_campaign', style0)
            sheet.write(0, 3, 'utm_content', style0)
            sheet.write(0, 4, 'count', style0)
            sheet.write(0, 5, 'cost', style0)
            sheet.col(0).width = 256 * 20
            sheet.col(1).width = 256 * 60
            sheet.col(2).width = 256 * 60
            sheet.col(3).width = 256 * 60
            sheet.col(4).width = 256 * 10
            sheet.col(5).width = 256 * 10
            sheet.row(0).height_mismatch = True
            sheet.row(0).height = 400
            count = 1
            order_buf = collections.OrderedDict(
                sorted(buffer.items(), key=lambda t: datetime.datetime.strptime(t[0][0], "%d.%m.%Y")))
            for key, value in order_buf.iteritems():
                sheet.write(count, 0, key[0], style1)
                sheet.write(count, 1, key[1], style1)
                sheet.write(count, 2, key[2], style1)
                sheet.write(count, 3, key[3], style1)
                sheet.write(count, 4, str(value[1]), style1)
                sheet.write(count, 5, str(value[0]), style1)
                sheet.row(count).height_mismatch = True
                sheet.row(count).height = 300
                count += 1
            camp_count += 1
        except Exception as e:
            print(e)
    buf = StringIO.StringIO()
    wbk.save(buf)
    buf.seek(0)
    ftp = ftplib.FTP(host='srv-3.yottos.com')
    ftp.login('cdn', '$www-app$')
    ftp.cwd('httpdocs')
    ftp.cwd('report')
    ftp.storbinary('STOR ' + str(account_id) + '.xls', buf)
    ftp.close()
    link.append('/report/%s.xls' % str(account_id))

time.sleep(180)
headers = {'X-Cache-Update': '1'}
cdns = ['cdn.srv-10.yottos.com', 'cdn.srv-11.yottos.com', 'cdn.srv-12.yottos.com']
for item in link:
    for cdn in cdns:
        url = 'http://%s%s' % (cdn, item)
        r = requests.get(url, headers=headers, verify=False)
        print('%s - %s' % (url, r.status_code))
