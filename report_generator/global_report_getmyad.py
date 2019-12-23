# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'

import sys
from collections import defaultdict, OrderedDict
from sqlalchemy.orm import joinedload
from datetime import datetime, timedelta
from pytz import timezone
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from os.path import join, exists
from os import makedirs

from .models.money.types import MoneyType
from .models.money.defaults import to_money
from .models import ParentAccount
from .models.choiceTypes import ProjectType, AccountType, CampaignType

reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')


def generate_global_report_getmyad(db_session, store_dir, rates):
    store_dir = join(store_dir, 'getmyad')
    if not exists(store_dir):
        makedirs(store_dir)
    rate = rates[MoneyType.uah]
    now = datetime.now(timezone('Europe/Kiev'))
    now = datetime(now.year, now.month, now.day, 0, 0)
    now = now - timedelta(days=1)
    for account in db_session.query(ParentAccount).filter(ParentAccount.project == ProjectType.Getmyad,
                                                          ParentAccount.account_type == AccountType.Customer).all():
        data = {}