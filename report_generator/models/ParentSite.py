# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'

from sqlalchemy import Column, ForeignKey, String, Integer, text
from sqlalchemy_utils import (force_auto_coercion, force_instant_defaults, ChoiceType)
from sqlalchemy.orm import relationship

from .meta import ParentBase
from .__mixins__ import *

force_auto_coercion()
force_instant_defaults()


class ParentSite(PrimaryKey, GUID, Timestamp, ParentBase):
    __tablename__ = 'sites'

    id_account = Column(ForeignKey('accounts.id'), nullable=False, index=True)

    name = Column(String, default="")

    account = relationship('ParentAccount', foreign_keys='ParentSite.id_account', uselist=False)

    statistic = relationship("ParentSiteStatistic", uselist=False,
                             foreign_keys='ParentSiteStatistic.id',
                             primaryjoin='ParentSite.id == ParentSiteStatistic.id')

    statistic_array = relationship("ParentSiteStatisticArray", uselist=False,
                                   foreign_keys='ParentSiteStatisticArray.id',
                                   primaryjoin='ParentSite.id == ParentSiteStatisticArray.id')

    blocks = relationship("ParentBlock", back_populates="site", collection_class=set, cascade="all", uselist=True)
