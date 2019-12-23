# -*- coding: utf-8 -*-
from pymongo import MongoClient
from collections import defaultdict
import re
import os
from jinja2 import Template


import sys
reload(sys)
sys.setdefaultencoding('utf-8')


t_index = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body style="border: 0px; margin: 0px; padding: 0px; display: block;">
<table cellspacing="2" border="1" cellpadding="5" width="100%">
{% for user in users -%}
<tr>
<td>
{{ user }}
</td>
<td>
<a href='{{ user | lower | replace(".", "_") }}.html' target='_blank'>open</a>
</td>
</tr>
{%- endfor %}
</table>
</body>
</html>

"""


t_user = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body style="border: 0px; margin: 0px; padding: 0px; display: block;">
<table cellspacing="2" border="1" cellpadding="5" width="100%">
{% for guid, block in blocks.iteritems() -%}
<tr>
<td colspan='2'>
{{ block[0] }}
</td>
</tr>
<tr>
<td>
OLD
</td>
<td>
NEW
</td>
</tr>
<tr>
<td>
<iframe src="{{ guid }}.html" width="{{ block[1] }}px" height="{{ block[2] }}px">
</iframe>
</td>
<td>
<iframe src="{{ guid }}.html?adsbyyottos_v2=true" width="{{ block[1] }}px" height="{{ block[2] }}px">
</iframe>
</td>
</tr>
{%- endfor %}
</table>
</body>
</html>

"""


t_block = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body style="border: 0px; margin: 0px; padding: 0px; display: block;">
<div style="border: 0px; margin: 0px; padding: 0px; display: block;">
                    <ins class="adsbyyottos" style="display:block" 
                    data-ad-client="{{ guid }}"></ins> 
                    <script async defer src="https://cdn.yottos.com/adsbyyottos.js"></script>
</div>
</body>
</html>

"""



main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = MongoClient(host=main_db_host)
db = conn.getmyad_db


current_dir = os.path.dirname(os.path.abspath(__file__))
s_dir = os.path.join(current_dir, 'html')
if not os.path.exists(s_dir):
    os.makedirs(s_dir)

informers = defaultdict(lambda: defaultdict(list))
users = set()
for item in db.informer.find({}):
    guid = item['guid']
    user = item['user'].decode('utf-8')
    users.add(user)
    title = '%s - %s' % (item['domain'], item['title'])
    width = int(re.match('[0-9]+', item['admaker']['Main']['width']).group(0))
    height = int(re.match('[0-9]+', item['admaker']['Main']['height']).group(0))
    if item.get('dynamic', False):
        width = 600
        height = 200
    informers[user][guid] = [title, width, height]


html = Template(t_index, trim_blocks=True, lstrip_blocks=True, enable_async=True).render({'users': users})
with open(os.path.join(s_dir, 'index.html'), 'w') as the_file:
    the_file.write(html)

for q, w in informers.iteritems():
    html = Template(t_user, trim_blocks=True, lstrip_blocks=True, enable_async=True).render({'blocks': w})
    with open(os.path.join(s_dir, '%s.html' % q.lower().replace('.', '_')), 'w') as the_file:
        the_file.write(html)
    for a, s in w.iteritems():
        html = Template(t_block, trim_blocks=True, lstrip_blocks=True, enable_async=True).render({'guid': a})
        with open(os.path.join(s_dir, '%s.html' % a), 'w') as the_file:
            the_file.write(html)
