# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import (Column, Integer, String, text)
from sqlalchemy_utils import (ChoiceType, force_auto_coercion, force_instant_defaults)


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
