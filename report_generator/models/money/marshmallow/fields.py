# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from marshmallow import fields as m_fields
from pyramid.threadlocal import get_current_request
from ..types import MoneyType
from ..defaults import money, account_exchange_rates


class MoneyField(m_fields.Float):
    def __init__(self, rate_field=None, request=None, **kwargs):
        self.rate_field = rate_field
        self._request = request
        super(MoneyField, self).__init__(**kwargs)

    @property
    def request(self):
        if self._request is None:
            print self.__class__.__name__, 'get_current_request'
            return get_current_request()
        return self._request

    @property
    def money(self):
        if self.request is not None and self.request.app is not None:
            return MoneyType[self.request.app.money_name]
        return money

    @property
    def exchange_rates(self):
        if self.rate_field is not None:
            return self.rate_field
        money = self.money
        rates = account_exchange_rates.get(money)
        if self.request is not None and self.request.app is not None and self.request.app.money_rate is not None:
            rates = self.request.app.money_rate
        return rates

    @staticmethod
    def _to_float(value):
        if isinstance(value, int) or isinstance(value, float) or value is None:
            return value
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    def _serialize(self, value, attr, obj):
        value = self._to_float(value)
        if isinstance(value, int) or isinstance(value, float):
            try:
                value = round((float(value) / self.exchange_rates), 4)
            except ZeroDivisionError:
                value = round((float(value) / 1), 4)
        return super(MoneyField, self)._serialize(value, attr, obj)

    def to_money(self, value):
        value = self._to_float(value)
        if isinstance(value, int) or isinstance(value, float):
            try:
                value = round((float(value) / self.exchange_rates), 4)
            except ZeroDivisionError:
                value = round((float(value) / 1), 4)
        return value

    def _deserialize(self, value, attr, data):
        value = self._to_float(value)
        if isinstance(value, int) or isinstance(value, float):
            value = round((float(value) * self.exchange_rates), 14)
        return super(MoneyField, self)._deserialize(value, attr, data)

    def from_money(self, value):
        value = self._to_float(value)
        if isinstance(value, int) or isinstance(value, float):
            value = round((float(value) * self.exchange_rates), 14)
        return value
