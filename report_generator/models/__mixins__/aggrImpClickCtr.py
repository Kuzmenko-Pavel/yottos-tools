# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

__author__ = 'kuzmenko-pavel'
from sqlalchemy import (Column, Integer, Float, text)
from sqlalchemy.ext.hybrid import hybrid_property


class AggrImpClickCtr(object):

    impressions_block = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                               doc='All block impressions count')
    impressions_block_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                     doc='Today block impressions count')
    impressions_block_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                         doc='Yesterday block impressions count')

    impressions_block_valid = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                     doc='All ActiveView block impressions count')
    impressions_block_valid_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                           doc='Today ActiveView block impressions count')
    impressions_block_valid_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                               doc='Yesterday ActiveView block impressions count')

    impressions_block_paid = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                    doc='All ActiveView block impressions count')
    impressions_block_paid_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                          doc='Today ActiveView block impressions count')
    impressions_block_paid_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                              doc='Yesterday ActiveView block impressions count')

    impressions_block_social = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                      doc='All social block impressions count')
    impressions_block_social_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                            doc='Today social block impressions count')
    impressions_block_social_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                                doc='Yesterday social block impressions count')

    impressions_block_valid_percent = Column(Integer, server_default=text("0.0"), default=0.0,
                                             doc='All ActiveView block impressions percent')

    impressions_block_valid_percent_today = Column(Integer, server_default=text("0.0"), default=0.0,
                                                   doc='All ActiveView block impressions percent')

    impressions_block_valid_percent_yesterday = Column(Integer, server_default=text("0.0"), default=0.0,
                                                       doc='All ActiveView block impressions percent')

    impressions_offer = Column(Integer, server_default=text("0"), default=0, doc='All block impressions count')
    impressions_offer_today = Column(Integer, server_default=text("0"), default=0, doc='Today block impressions count')
    impressions_offer_yesterday = Column(Integer, server_default=text("0"), default=0,
                                         doc='Yesterday block impressions count')

    impressions_offer_valid = Column(Integer, server_default=text("0"), default=0,
                                     doc='All ActiveView offer impressions count')
    impressions_offer_valid_today = Column(Integer, server_default=text("0"), default=0,
                                           doc='Today ActiveView offer impressions count')
    impressions_offer_valid_yesterday = Column(Integer, server_default=text("0"), default=0,
                                               doc='Yesterday ActiveView offer impressions count')

    impressions_offer_paid = Column(Integer, server_default=text("0"), default=0,
                                    doc='All paid offer impressions count')
    impressions_offer_paid_today = Column(Integer, server_default=text("0"), default=0,
                                          doc='Todat paid offer impressions count')
    impressions_offer_paid_yesterday = Column(Integer, server_default=text("0"), default=0,
                                              doc='Yesterday paid offer impressions count')

    impressions_offer_social = Column(Integer, server_default=text("0"), default=0,
                                      doc='All social offer impressions count')
    impressions_offer_social_today = Column(Integer, server_default=text("0"), default=0,
                                            doc='Todat social offer impressions count')
    impressions_offer_social_yesterday = Column(Integer, server_default=text("0"), default=0,
                                                doc='Yesterday social offer impressions count')

    impressions_offer_valid_percent = Column(Integer, server_default=text("0"), default=0,
                                             doc='All ActiveView offer impressions percent')
    impressions_offer_valid_percent_today = Column(Integer, server_default=text("0"), default=0,
                                                   doc='Today ActiveView offer impressions percent')
    impressions_offer_valid_percent_yesterday = Column(Integer, server_default=text("0"), default=0,
                                                       doc='Yesterday ActiveView offer impressions percent')

    clicks = Column(Integer, server_default=text("0"), default=0, doc='All click count')
    clicks_today = Column(Integer, server_default=text("0"), default=0, doc='Today click count')
    clicks_yesterday = Column(Integer, server_default=text("0"), default=0, doc='Yesterday click count')

    clicks_suspicious = Column(Integer, server_default=text("0"), default=0, doc='All suspicious click count')
    clicks_suspicious_today = Column(Integer, server_default=text("0"), default=0, doc='Today suspicious click count')
    clicks_suspicious_yesterday = Column(Integer, server_default=text("0"), default=0,
                                         doc='Yesterday suspicious click count')

    clicks_filtered = Column(Integer, server_default=text("0"), default=0, doc='All filtered click count')
    clicks_filtered_today = Column(Integer, server_default=text("0"), default=0, doc='Today filtered click count')
    clicks_filtered_yesterday = Column(Integer, server_default=text("0"), default=0,
                                       doc='Yesterday filtered click count')

    clicks_banned = Column(Integer, server_default=text("0"), default=0, doc='All banned click count')
    clicks_banned_today = Column(Integer, server_default=text("0"), default=0, doc='Today banned click count')
    clicks_banned_yesterday = Column(Integer, server_default=text("0"), default=0, doc='Yesterday banned click count')

    clicks_valid = Column(Integer, server_default=text("0"), default=0, doc='All valid click count')
    clicks_valid_today = Column(Integer, server_default=text("0"), default=0, doc='Today valid click count')
    clicks_valid_yesterday = Column(Integer, server_default=text("0"), default=0, doc='Yesterday valid click count')

    clicks_paid = Column(Integer, server_default=text("0"), default=0, doc='All paid click count')
    clicks_paid_today = Column(Integer, server_default=text("0"), default=0, doc='Today paid click count')
    clicks_paid_yesterday = Column(Integer, server_default=text("0"), default=0, doc='Yesterday paid click count')

    clicks_social = Column(Integer, server_default=text("0"), default=0, doc='All social click count')
    clicks_social_today = Column(Integer, server_default=text("0"), default=0, doc='Today social click count')
    clicks_social_yesterday = Column(Integer, server_default=text("0"), default=0, doc='Yesterday social click count')

    clicks_time = Column(Integer, server_default=text("0"), default=0, doc='All clicks time')
    clicks_time_today = Column(Integer, server_default=text("0"), default=0, doc='Today clicks time')
    clicks_time_yesterday = Column(Integer, server_default=text("0"), default=0,
                                   doc='Yesterday clicks time')

    clicks_delta_time = Column(Integer, server_default=text("0"), default=0, doc='All clicks time')
    clicks_delta_time_today = Column(Integer, server_default=text("0"), default=0, doc='Today clicks time')
    clicks_delta_time_yesterday = Column(Integer, server_default=text("0"), default=0, doc='Yesterday clicks time')

    clicks_cost = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='Summary cost clicks amount')
    clicks_cost_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                               doc='Summary cost clicks amount')
    clicks_cost_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                   doc='yesterday Summary cost clicks amount')

    impressions_cost = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                              doc='Summary cost impressions amount')
    impressions_cost_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                    doc='Summary cost impressions amount')
    impressions_cost_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                        doc='yesterday Summary cost impressions amount')

    clicks_remainder_cost = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                   doc='Summary remainder cost clicks amount')
    clicks_remainder_cost_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                         doc='Summary remainder cost clicks amount')
    clicks_remainder_cost_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                             doc='Yesterday summary remainder cost clicks amount')

    impressions_remainder_cost = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                        doc='Summary remainder cost impressions amount')
    impressions_remainder_cost_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                              doc='Summary remainder cost impressions amount')
    impressions_remainder_cost_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0,
                                                  doc='Yesterday summary remainder cost impressions amount')

    ctr_block = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    ctr_block_valid = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_valid_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_valid_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    ctr_block_paid = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_paid_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_paid_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    ctr_block_social = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_social_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_block_social_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    ctr_offer = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    ctr_offer_valid = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_valid_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_valid_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    ctr_offer_paid = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_paid_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_paid_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    ctr_offer_social = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_social_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0)
    ctr_offer_social_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0)

    payments = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    payments_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    payments_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')

    payments_remainder = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    payments_remainder_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    payments_remainder_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')

    average_cost = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    average_cost_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    average_cost_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')

    average_clicks_cost = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    average_clicks_cost_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    average_clicks_cost_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')

    average_impressions_cost = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    average_impressions_cost_today = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')
    average_impressions_cost_yesterday = Column(Float(precision=4), server_default=text("0.0"), default=0.0, doc='')

    goal = Column(Integer, server_default=text("0"), default=0, doc='All goal')
    goal_today = Column(Integer, server_default=text("0"), default=0, doc='Today goal')
    goal_yesterday = Column(Integer, server_default=text("0"), default=0, doc='Yesterday goal')

    @hybrid_property
    def all_banned_click(self):
        return self.clicks_filtered + self.clicks_banned

    @hybrid_property
    def all_banned_click_today(self):
        return self.clicks_filtered_today + self.clicks_banned_today

    @hybrid_property
    def all_banned_click_yesterday(self):
        return self.clicks_filtered_yesterday + self.clicks_banned_yesterday
