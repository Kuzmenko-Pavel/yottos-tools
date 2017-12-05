# This Python file uses the following encoding: utf-8
import sys
import os
from math import ceil


project_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(project_dir)
os.environ['PYTHON_EGG_CACHE'] = '/usr/lib/python2.7/dist-packages'
import sys
from pymongo import Connection
sys.stdout = sys.stderr

main_db_host = 'srv-5.yottos.com:27018,srv-5.yottos.com:27019,srv-5.yottos.com:27020'


def css_calck(width, height):
    block_width, block_height = width - 2, height - 2
    print block_width, block_height
    if height < width:
        return g_css_calck(block_width, block_height)
    else:
        return v_css_calck(block_width, block_height)


def calck_g_offer(adv_width):
    return 'G', (adv_width * 0.42) + 4


def calck_gv_offer(adv_width):
    return 'GV', ((adv_width / 2) + (round(320 / adv_width) * 14)) + 4


def calck_v_offer(adv_width):
    return 'V', (adv_width + (round(320 / adv_width) * 14)) + 4


def calck_bv_offer(adv_width):
    return 'BV', (adv_width + ((round(320 / adv_width) * 14) * 2)) + 4


def g_css_calck(block_width, block_height):
    count_column = 1.0
    count_row = 1.0
    if 130 < block_height < 200:
        adv_height = block_height
        count_column = round(block_width / (block_height - (round(320 / block_height) * 14)))
        adv_width = block_width/count_column
    else:
        if block_width > 350:
            count_column = ceil(block_width / 300.0)
        count_row = 1.0
        if count_column == 1.0:
            if block_height >= 300:
                count_row = ceil(block_height / 300.0)
            elif 200 < block_height < 300:
                count_row = round(block_height / 100.0)
        else:
            if block_height >= 300:
                count_row = round(block_height / 100.0)
            elif 190 <= block_height < 300:
                count_column = round(block_width / 180.0)

        adv_height = block_height / count_row
        adv_width = block_width / count_column
    if adv_height >= adv_width:
        if adv_height >= adv_width + (round(320 / adv_width) * 14):
            pass
        else:
            pass
    else:
        if adv_width >= adv_height + (round(320 / adv_width) * 14):
            pass
        else:
            pass
    print count_row * count_column, count_column, count_row
    return adv_width, adv_height


def v_css_calck(block_width, block_height):
    count_column = ceil(block_width / 300.0)
    adv_type = None
    adv_width = block_width / count_column
    adv_count_by_width = round(block_height / (adv_width + ((round(320 / adv_width) * 14) * 2)))
    if adv_width > 200 and adv_count_by_width < 4:
        adv_type, adv_height = calck_g_offer(adv_width)

    elif 160 <= adv_width <= 200 and adv_count_by_width < 4:
        adv_type, adv_height = calck_gv_offer(adv_width)

    elif adv_width < 160 and adv_count_by_width < 4:
        adv_type, adv_height = calck_v_offer(adv_width)

    else:
        adv_type, adv_height = calck_bv_offer(adv_width)

    adv_count = round(block_height / adv_height)
    while True:
        new_height = adv_count * adv_height
        if new_height > block_height:
            difference = (new_height - block_height)/adv_count
            if round(difference) < 4:
                adv_height = adv_height - difference
                break
            else:
                if adv_type in ['G', 'GV'] and (adv_height - difference > 80):
                    adv_height = adv_height - difference
                    break
                else:
                    adv_count -= 1
                    adv_height = block_height/adv_count
        else:
            difference = (block_height - new_height)/adv_count
            if round(difference):
                adv_height = adv_height + difference
                break
            else:
                if adv_type in ['GV'] and (adv_width > (adv_height + difference)):
                    adv_height = adv_height + difference
                    break
                if adv_type in ['V', 'BV'] and difference < (round(320 / adv_width) * 14):
                    adv_height = adv_height + difference
                    break
                print "SMALL", block_height - new_height, (block_height - new_height) / adv_count
                break
    print adv_count * count_column, adv_type
    return adv_width, adv_height


def css_offer(adv_width, adv_height):
    if adv_width < adv_height:
        print 'V'
    else:
        if (calck_g_offer(adv_width)[1] - adv_height) < (calck_gv_offer(adv_width)[1] - adv_height):
            print 'G'
        else:
            print 'GV'


conn = Connection(host=main_db_host)
db = conn.getmyad_db
curs = db.informer.find()
for block in curs:
    height = int(block['admaker']['Main']['height'].replace('px', ''))
    width = int(block['admaker']['Main']['width'].replace('px', ''))
    itemsNumber = int(block['admaker']['Main']['itemsNumber'])
    adv_height = int(block['admaker']['Advertise']['height'].replace('px', ''))
    adv_width = int(block['admaker']['Advertise']['width'].replace('px', ''))
    img_height = block['admaker']['Image']['height'].replace('px', '')
    img_width = block['admaker']['Image']['width'].replace('px', '')
    title_height = block['admaker']['Header']['height'].replace('px', '')
    title_width = block['admaker']['Header']['width'].replace('px', '')
    desc_height = block['admaker']['Description']['height'].replace('px', '')
    desc_width = block['admaker']['Description']['width'].replace('px', '')
    if width > height:
        print '\n'
        print block['guid']
        print itemsNumber, width, height
        cal_width, cal_height = css_calck(float(width), float(height))
        print "size %sx%s\norig %sx%s\ncalc %sx%s" % (width, height, adv_width, adv_height, int(cal_width), int(cal_height))
        css_offer(cal_width, cal_height)
        print '\n'


# print css_calck(float(141), float(400))
# print css_calck(float(297), float(600))
# print css_calck(float(139), float(400))
# print css_calck(float(927), float(150))
# print css_calck(float(612), float(200))
