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

    def __getitem__(cls, name):
        return cls._member_map_[name]


class ProjectType(Enum):
    Root = 1
    Adload = 2
    Getmyad = 3


class AccountType(Enum):
    Root = 1
    Admin = 2
    Accountant = 3
    SuperManager = 4
    Manager = 5
    Agency = 6
    Customer = 7


class CampaignType(Enum):
    new_auditory = 1
    remarketing = 2
    thematic = 3
    relevant_auditory = 4
    social = 5


class CampaignRemarketingType(Enum):
    offer = 1
    account = 2