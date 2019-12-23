# -*- coding: utf-8 -*-
from pymongo import MongoClient
from collections import defaultdict

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db

use_cat = {}
cursor = db.domain.find()
for item in cursor:
    for guid, domain in item.get('domains', {}).iteritems():
        if domain in ['patterns.com', 'roomidea.ru/news', 'newspaper.com.ua']:
            continue
        categories = db.domain.categories.find({'domain': domain})
        for categor in categories:
            for c in categor.get('categories', []):
                use_cat[c] = True

cat = {}
global_category = []
cursor = db.advertise.category.find()
for item in cursor:
    if use_cat.get(item['guid'], False):
        print item['guid'], item['title']
        global_category.append(item['guid'])
        cat[str(item['guid'])] = item['title'].strip()
cursor = db.domain.find()
for item in cursor:
    login = item['login']
    for guid, domain in item.get('domains', {}).iteritems():
        local_category = []
        ext_category_title = []
        local_category_title = []
        categories = db.domain.categories.find({'domain': domain})
        for categor in categories:
            for c in categor.get('categories', []):
                if use_cat.get(c, False):
                    local_category.append(c)
                    local_category_title.append(cat.get(c, ''))
            if '7B293592-509C-11E1-9F32-00163E0300C1' in categor.get('categories', []):
                c = 'BB4BD068-012E-11E4-B708-002590D590D0'
                local_category.append(c)
                local_category_title.append(cat.get(c, ''))
                c = 'EC20D76E-1301-11E7-9F47-002590D97638'
                local_category.append(c)
                local_category_title.append(cat.get(c, ''))

        if local_category:
            paths = list(set(global_category) - set(local_category))
            if paths:
                for path in paths:
                    if path:
                        ext_category_title.append(cat.get(path, ''))
        print login, '\t', domain, '\t', ','.join(local_category_title), '\t', ','.join(ext_category_title)

campaign_settings = {}
cursor = db.campaign.archive.find()
for item in cursor:
    campaign_settings[str(item['guid']).upper()] = item
cursor = db.campaign.find()
for item in cursor:
    campaign_settings[str(item['guid']).upper()] = item

for v in campaign_settings.itervalues():
    title = v.get('title')
    showConditions = v.get('showConditions', {})
    local_category = []
    ext_category_title = []
    local_category_title = []
    if showConditions.get('showCoverage', 'all') == 'thematic':
        for c in showConditions.get('categories', []):
            if use_cat.get(c, False):
                local_category.append(c)
                local_category_title.append(cat.get(c, ''))
        if local_category:
            paths = list(set(global_category) - set(local_category))
            if paths:
                for path in paths:
                    if path:
                        ext_category_title.append(cat.get(path, ''))
    print title, '\t', ','.join(local_category_title), '\t', ','.join(ext_category_title)