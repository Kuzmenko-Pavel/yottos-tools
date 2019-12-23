# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from datetime import datetime
from pytz import timezone
from sqlalchemy import Column, DateTime
from sqlalchemy.sql import func


class Timestamp(object):
    created = Column(DateTime(timezone=True), server_default=func.now(), default=datetime.now(timezone('Europe/Kiev')),
                     nullable=False)
    updated = Column(DateTime(timezone=True), server_default=func.now(), default=datetime.now(timezone('Europe/Kiev')),
                     nullable=False)

    deleted = Column(DateTime(timezone=True), server_default=func.now(), default=datetime.now(timezone('Europe/Kiev')),
                     nullable=False)
