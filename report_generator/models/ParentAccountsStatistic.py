# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import (Column, ForeignKey)
from sqlalchemy.orm import relationship
from sqlalchemy_utils import (force_auto_coercion, force_instant_defaults)

from .meta import ParentBase
from .__mixins__ import AggrImpClickCtr

force_auto_coercion()
force_instant_defaults()


class ParentAccountStatistic(AggrImpClickCtr, ParentBase):
    __tablename__ = 'accounts_statistic'
    id = Column(ForeignKey('accounts.id'), primary_key=True, nullable=False)
    account = relationship('ParentAccount', foreign_keys=[id], uselist=False)
