# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import Column
from sqlalchemy.sql import func
from sqlalchemy_utils import UUIDType, force_auto_coercion, force_instant_defaults
from uuid import uuid4

force_auto_coercion()
force_instant_defaults()


def get_uuid():
    return uuid4()


class GUID(object):
    guid = Column(UUIDType(binary=True), index=True, default=get_uuid, server_default=func.uuid_generate_v4())
