# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship


class StatisticAccountRelation(object):
    @declared_attr
    def statistic(cls):
        return relationship("ParentAccountStatistic", uselist=False, foreign_keys='ParentAccountStatistic.id',
                            primaryjoin='ParentAccount.id == ParentAccountStatistic.id')

    @declared_attr
    def statistic_array(cls):
        return relationship("ParentAccountStatisticArray", uselist=False, foreign_keys='ParentAccountStatisticArray.id',
                            primaryjoin='ParentAccount.id == ParentAccountStatisticArray.id')
