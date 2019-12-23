# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from enum import Enum
from enum import EnumMeta


class DefaultEnumMeta(EnumMeta):
    def __call__(cls, value, *args, **kwargs):
        try:
            return EnumMeta.__call__(cls, value, *args, **kwargs)
        except Exception as e:
            return next(iter(cls))


class MoneyType(Enum):
    __metaclass__ = DefaultEnumMeta
    usd = 1
    uah = 2
    rur = 3
