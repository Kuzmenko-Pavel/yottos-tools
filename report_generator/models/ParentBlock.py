# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'

from sqlalchemy import Column, ForeignKey, String, Integer, text
from sqlalchemy_utils import (force_auto_coercion, force_instant_defaults, ChoiceType)
from sqlalchemy.orm import relationship

from .choiceTypes import CampaignType, CampaignRemarketingType
from .meta import ParentBase
from .__mixins__ import *

force_auto_coercion()
force_instant_defaults()


class ParentBlock(PrimaryKey, GUID, Timestamp, ParentBase):
    __tablename__ = 'blocks'

    id_account = Column(ForeignKey('accounts.id'), nullable=False, index=True)

    id_site = Column(ForeignKey('sites.id'))

    name = Column(String, default="")

    account = relationship('ParentAccount', foreign_keys='ParentBlock.id_account', uselist=False)

    statistic = relationship("ParentBlockStatistic", uselist=False,
                             foreign_keys='ParentBlockStatistic.id',
                             primaryjoin='ParentBlock.id == ParentBlockStatistic.id')

    statistic_array = relationship("ParentBlockStatisticArray", uselist=False,
                                   foreign_keys='ParentBlockStatisticArray.id',
                                   primaryjoin='ParentBlock.id == ParentBlockStatisticArray.id')

    site = relationship('ParentSite', back_populates='blocks', foreign_keys='ParentBlock.id_site', uselist=False)
