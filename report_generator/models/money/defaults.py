# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

from collections import defaultdict

from .types import MoneyType

__author__ = 'kuzmenko-pavel'


def _to_float(value):
    if isinstance(value, int) or isinstance(value, float) or value is None:
        return value
    try:
        return float(value)
    except ValueError:
        try:
            return int(value)
        except ValueError:
            return value


def to_money(value, exchange_rates):
    value = _to_float(value)
    if isinstance(value, int) or isinstance(value, float):
        try:
            value = round((float(value) / exchange_rates), 4)
        except ZeroDivisionError:
            return value
    return value


def from_money(value, exchange_rates):
    value = _to_float(value)
    if isinstance(value, int) or isinstance(value, float):
        value = float(value) * exchange_rates
    return value


money = MoneyType.uah

account_exchange_rates = defaultdict(lambda x: 1.0)
account_exchange_rates[MoneyType.usd] = 24.72
account_exchange_rates[MoneyType.uah] = 1.0
account_exchange_rates[MoneyType.rur] = 0.353