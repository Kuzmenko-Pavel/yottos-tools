# This Python file uses the following encoding: utf-8
import sys
import os
import codecs
from collections import defaultdict, Counter

project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import sys
from pymongo import Connection, ASCENDING
from datetime import datetime

sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'

conn = Connection(host=main_db_host)
db = conn.getmyad_db
curs = db.informer.find()
with codecs.open('block.csv', 'w', encoding='utf8') as csv_file:
    text = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (u'Высота блока',
                                                         u'Ширина блока',
                                                         u'Высота РП',
                                                         u'Ширина РП',
                                                         u'Высота картинки',
                                                         u'Ширина картинки',
                                                         u'Высота названия',
                                                         u'Ширина названия',
                                                         u'Высота описания',
                                                         u'Ширина описания')
    csv_file.write(text)
    block_horizontal = defaultdict(list)
    block_vertical = defaultdict(list)
    block_square = defaultdict(list)
    block_c = Counter()
    for block in curs:
        height = block['admaker']['Main']['height']
        width = block['admaker']['Main']['width']
        adv_height = block['admaker']['Advertise']['height']
        adv_width = block['admaker']['Advertise']['width']
        img_height = block['admaker']['Image']['height']
        img_width = block['admaker']['Image']['width']
        title_height = block['admaker']['Header']['height']
        title_width = block['admaker']['Header']['width']
        desc_height = block['admaker']['Description']['height']
        desc_width = block['admaker']['Description']['width']
        text = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (height.replace('px', ''),
                                                             width.replace('px', ''),
                                                             adv_height.replace('px', ''),
                                                             adv_width.replace('px', ''),
                                                             img_height.replace('px', ''),
                                                             img_width.replace('px', ''),
                                                             title_height.replace('px', ''),
                                                             title_width.replace('px', ''),
                                                             desc_height.replace('px', ''),
                                                             desc_width.replace('px', '')
                                                             )
        csv_file.write(text)
        if height < width:
            block_horizontal[width].append(height)
        elif height > width:
            block_vertical[height].append(width)
        else:
            block_square[height].append(width)
        block_c[height + "x" + width] += 1

    print "block_horizontal"
    for k, v in block_horizontal.iteritems():
        print k, " - ", " ".join(list(set(v)))

    print "block_vertical"
    for k, v in block_vertical.iteritems():
        print k, " - ", " ".join(list(set(v)))

    print "block_square"
    for k, v in block_square.iteritems():
        print k, " - ", " ".join(list(set(v)))

    for k, v in block_c.iteritems():
        print k, v

    for i in block_c.most_common(50):
        print i