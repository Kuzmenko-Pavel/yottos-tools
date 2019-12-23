# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from datetime import datetime
from pytz import timezone
from sqlalchemy_utils import force_auto_coercion, force_instant_defaults, ChoiceType
from .money.types import MoneyType
from sqlalchemy import Column, Integer, Float, Boolean, text, DateTime
from sqlalchemy.sql import func

from .__mixins__ import PrimaryKey
from .meta import ParentBase

force_auto_coercion()
force_instant_defaults()


class ParentAccountExchangeRate(PrimaryKey, ParentBase):
    __tablename__ = 'accounts_exchange_rates'

    money = Column(ChoiceType(MoneyType, impl=Integer()), default=MoneyType.usd,
                   server_default=text("'" + str(MoneyType.usd.value) + "'::integer"), unique=True)
    correct = Column(Float(precision=4), default=0, server_default='0')
    manual = Column(Boolean, default=True, server_default='true')
    auto_rate = Column(Float(precision=4), default=0, server_default='0')
    manual_rate = Column(Float(precision=4), default=0, server_default='0')
    rate = Column(Float(precision=4), default=0, server_default='0')
    updated = Column(DateTime(timezone=True), server_default=func.now(), default=datetime.now(timezone('Europe/Kiev')),
                     nullable=False)
    auto_updated = Column(DateTime(timezone=True), server_default=func.now(),
                          default=datetime.now(timezone('Europe/Kiev')),
                          nullable=False)
