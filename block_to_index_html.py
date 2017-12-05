# This Python file uses the following encoding: utf-8

import sys
import re
from pymongo import Connection, ASCENDING
from jinja2 import Template


sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


t = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>
</head>
<body style="background: #c8591b;margin: 0;padding: 0;">
<table>
{% for adv in advs -%}
<tr>
<td>
<div style="margin: 0;padding: 0;display: inline-block;">
                    <ins class="adsbyyottos"
                         style="display:block; height: {{adv.height}}px; width: {{adv.width}}px;"
                         data-ad-client="{{ adv.guid }}"></ins>
                    <script async src="http://localhost:8000/js/loader.js"></script>
</div>
</td>
<td>
<div id="a{{ adv.guid }}" style="display:inline-block;"></div>
<script type="text/javascript">
yottos_advertise = "{{ adv.guid }}";
yottos_advertise_div_display = "a{{ adv.guid }}";
</script>
<script type="text/javascript" src="https://cdn.yottos.com/getmyad/_a.js"></script>
</td>
</tr>
{%- endfor %}
</table>
</body>
</html>

"""

conn = Connection(host=main_db_host)
db = conn.getmyad_db
curs = db.informer.find()
data = []
for inf in curs:
    try:
        width = int(re.match('[0-9]+', inf['admaker']['Main']['width']).group(0))
        height = int(re.match('[0-9]+', inf['admaker']['Main']['height']).group(0))
    except:
        pass
    try:
        border = int(re.match('[0-9]+', inf['admaker']['Main']['borderWidth']).group(0))
    except:
        border = 1
    width += border * 2
    height += border * 2
    adv = {}
    adv['guid'] = inf['guid']
    adv['width'] = width
    adv['height'] = height
    data.append(adv)

chunkSize=10
for i in xrange(0, len(data), chunkSize):
    chunk = data[i:i + chunkSize]
    html = Template(t, trim_blocks=True, lstrip_blocks=True, enable_async=True).render({'advs': chunk})
    with open('test_%s.html' % i, 'w') as the_file:
        the_file.write(html)
