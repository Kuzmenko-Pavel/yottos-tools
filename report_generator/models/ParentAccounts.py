# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import (Column, Integer, String, text)
from sqlalchemy_utils import (ChoiceType, force_auto_coercion, force_instant_defaults)
from sqlalchemy.orm import relationship

from .meta import ParentBase
from .__mixins__ import *
from .choiceTypes import ProjectType, AccountType

force_auto_coercion()
force_instant_defaults()


class ParentAccount(PrimaryKey, GUID, ParentBase, StatisticAccountRelation):
    """
    """
    __tablename__ = 'accounts'

    name = Column(String, nullable=True)

    account_type = Column(ChoiceType(AccountType, impl=Integer()), default=AccountType.Customer,
                          server_default=text("'" + str(AccountType.Customer.value) + "'::integer"))

    project = Column(ChoiceType(ProjectType, impl=Integer()))

    blocks = relationship("ParentBlock", uselist=True, foreign_keys='ParentBlock.id_account',
                          primaryjoin='ParentAccount.id == ParentBlock.id_account')

    sites = relationship("ParentSite", uselist=True, foreign_keys='ParentSite.id_account',
                         primaryjoin='ParentAccount.id == ParentSite.id_account')
    campaigns = relationship("ParentCampaign", uselist=True, foreign_keys='ParentCampaign.id_account',
                             primaryjoin='ParentAccount.id == ParentCampaign.id_account')
