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


class ParentCampaign(PrimaryKey, GUID, Timestamp, ParentBase):
    __tablename__ = 'campaigns'

    id_account = Column(ForeignKey('accounts.id'), nullable=False, index=True)

    name = Column(String, default="")

    campaign_type = Column(ChoiceType(CampaignType, impl=Integer()), default=CampaignType.new_auditory,
                           server_default=text("'" + str(CampaignType.new_auditory.value) + "'::integer"),
                           nullable=False)

    remarketing_type = Column(ChoiceType(CampaignRemarketingType, impl=Integer()),
                              default=CampaignRemarketingType.offer,
                              server_default=text("'" + str(CampaignRemarketingType.offer.value) + "'::integer"),
                              nullable=False)

    account = relationship('ParentAccount', foreign_keys='ParentCampaign.id_account', uselist=False)

    statistic = relationship("ParentCampaignStatistic", uselist=False,
                             foreign_keys='ParentCampaignStatistic.id',
                             primaryjoin='ParentCampaign.id == ParentCampaignStatistic.id')

    statistic_array = relationship("ParentCampaignStatisticArray", uselist=False,
                                   foreign_keys='ParentCampaignStatisticArray.id',
                                   primaryjoin='ParentCampaign.id == ParentCampaignStatisticArray.id')
