#!/usr/bin/python
# encoding: utf-8

from mq import MQ

if __name__ == '__main__':
    mq = MQ(host='192.168.56.102', user='test', password='test', virtual_host='/')
    for x in xrange(1,1000):
        mq.account_update('helpers.com.ua')
        mq.informer_update('9200beb4-b468-11e5-a497-00e081bad801')
        mq.campaign_update('5975f145-d516-4c61-af9a-46601b934cb7')
        mq.campaign_update('cf7ff1a4-9e1d-40e2-9b05-39c27fd601d8')
        mq.campaign_update('8b2853e4-96dd-44f0-87cc-d11a7c63bd9b')

    for x in xrange(1,10):
        mq.campaign_stop('f8900a87-2f0b-4070-911c-dcc6979d2835')
        mq.campaign_start('f8900a87-2f0b-4070-911c-dcc6979d2835')