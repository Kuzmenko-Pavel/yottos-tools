# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
import sys
from os.path import dirname, abspath, join, exists
from os import makedirs, getpid
import socket
from datetime import datetime
from report_generator.models import get_engine
from report_generator.models.meta import DBScopedSession, ParentBase
from sqlalchemy import event
from report_generator.global_report_adload import generate_global_report_adload
from report_generator.global_report_getmyad import generate_global_report_getmyad
import transaction
from report_generator.models import ParentAccountExchangeRate

reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')

server_name = socket.gethostname()
date = datetime.now()
date_today = datetime(date.year, date.month, date.day, 0, 0)
current_dir = dirname(dirname(abspath(__file__)))
rep_dir = join(current_dir, 'report')
if not exists(rep_dir):
    makedirs(rep_dir)

store_dir = join(rep_dir, '%s-%s-%s' % (date_today.year, date_today.month, date_today.day))
if not exists(store_dir):
    makedirs(store_dir)

sqlalchemy_config = {
    'url': 'postgresql://x_project:x_project@srv-13.yottos.com:5432/x_project?client_encoding=utf8',
    'client_encoding': 'utf8',
    'pool_size': 1,
    'max_overflow': 100,
    'echo_pool': False,
    'echo': False,
    'pool_pre_ping': True,
    'pool_recycle': 3600,
    'pool_use_lifo': False
}
application_name = 'ReportGenerator on %s pid=%s' % (server_name, getpid())
engine = get_engine(sqlalchemy_config, prefix='', connect_args={"application_name": application_name})
ParentBase.metadata.bind = engine
DBScopedSession.configure(bind=engine)


@event.listens_for(engine, 'begin')
def receive_begin(conn):
    conn.execute('SET TRANSACTION READ ONLY')


rates = {}
db_session = DBScopedSession()
with transaction.manager:
    for rate in db_session.query(ParentAccountExchangeRate).all():
        rates[rate.money] = rate.rate


with transaction.manager:
    generate_global_report_adload(db_session, store_dir, rates)

with transaction.manager:
    generate_global_report_getmyad(db_session, store_dir, rates)
