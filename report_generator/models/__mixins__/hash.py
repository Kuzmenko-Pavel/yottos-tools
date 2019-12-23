# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import Column, String


class HASH(object):
    hash = Column(String(), nullable=False, index=True, unique=True)
