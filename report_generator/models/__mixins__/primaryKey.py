# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import (Column, BigInteger)


class PrimaryKey(object):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
