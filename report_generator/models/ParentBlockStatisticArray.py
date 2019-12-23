# -*- coding: UTF-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import (Column, ForeignKey)
from sqlalchemy.orm import relationship
from sqlalchemy_utils import (force_auto_coercion, force_instant_defaults)

from .meta import ParentBase
from .__mixins__ import AggrArrayImpClickCtr

force_auto_coercion()
force_instant_defaults()


class ParentBlockStatisticArray(AggrArrayImpClickCtr, ParentBase):
    __tablename__ = 'blocks_statistic_array'
    id = Column(ForeignKey('blocks.id'), primary_key=True, nullable=False)
    block = relationship('ParentBlock', foreign_keys=[id], uselist=False)
